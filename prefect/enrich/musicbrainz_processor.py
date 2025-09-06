#!/usr/bin/env python3
"""
MusicBrainz data enrichment processor.

This module consolidates mbz_get_missing_artists.py, mbz_parse_artists.py, 
and mbz_parse_area_hierarchy.py from the original Fabric notebooks, replacing 
Spark operations with Polars and writing directly to parquet files.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List, Optional, Set
from pathlib import Path
import polars as pl
import pandas as pd

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from prefect.enrich.utils.api_clients import MusicBrainzClient
from prefect.enrich.utils.data_writer import ParquetDataWriter, EnrichmentTracker
from prefect.enrich.utils.polars_ops import (
    normalize_artist_json_data,
    process_area_hierarchy_data,
    create_artist_genre_table,
    batch_process_dataframe,
)

logger = logging.getLogger(__name__)


class MusicBrainzProcessor:
    """
    Handles MusicBrainz data enrichment for artists and geographic areas.

    Consolidates:
    - Artist discovery and fetching (mbz_get_missing_artists.py)
    - Artist data parsing and normalization (mbz_parse_artists.py)
    - Area hierarchy processing (mbz_parse_area_hierarchy.py)
    """

    def __init__(self, data_writer: ParquetDataWriter = None, cache_dir: str = None):
        self.data_writer = data_writer or ParquetDataWriter()
        self.tracker = EnrichmentTracker(self.data_writer)
        self.mbz_client = MusicBrainzClient(cache_dir=cache_dir)

        # Cache directory for storing raw JSON data
        self.cache_dir = Path(cache_dir) if cache_dir else Path("data/cache/mbz")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def discover_missing_artists(self) -> Dict[str, Any]:
        """
        Find artists that need MusicBrainz enrichment.
        Based on mbz_get_missing_artists.py logic.
        """
        logger.info("Discovering missing artists for MusicBrainz enrichment")

        missing_artists_df = self.tracker.get_missing_artists()

        if missing_artists_df.is_empty():
            return {
                "status": "no_updates",
                "message": "No missing artists found",
                "artists_found": 0,
            }

        logger.info(f"Found {len(missing_artists_df)} artists needing MBZ enrichment")
        return {
            "status": "success",
            "message": f"Found {len(missing_artists_df)} missing artists",
            "artists_found": len(missing_artists_df),
            "missing_artists": missing_artists_df,
        }

    def fetch_artist_data(self, missing_artists_df: pl.DataFrame) -> Dict[str, Any]:
        """
        Fetch artist data from MusicBrainz API and store as JSON files.
        Based on the fetching logic from mbz_get_missing_artists.py
        """
        logger.info(f"Fetching MusicBrainz data for {len(missing_artists_df)} artists")

        artists_fetched = 0
        artists_failed = []

        # Process artists in batches
        artist_rows = missing_artists_df.to_dicts()

        for i, row in enumerate(artist_rows):
            try:
                # Get artist MBID using ISRC
                artist_mbid = self.mbz_client.get_artist_by_isrc(row["track_isrc"])

                if not artist_mbid:
                    logger.info(
                        f"Could not find MBID for artist {row['artist']} using ISRC {row['track_isrc']}"
                    )
                    artists_failed.append(row)
                    continue

                # Get full artist data
                artist_data = self.mbz_client.get_artist_by_id(
                    artist_mbid, includes=["tags", "release-groups", "aliases"]
                )

                if not artist_data:
                    logger.info(f"Could not fetch artist data for MBID {artist_mbid}")
                    artists_failed.append(row)
                    continue

                # Add Spotify ID to the artist data
                artist_data["spotify_id"] = row["artist_id"]

                # Save to JSON file
                json_file = self.cache_dir / f"{artist_mbid}.json"
                with open(json_file, "w") as f:
                    json.dump(artist_data, f, indent=2, default=str)

                artists_fetched += 1

                # Progress logging
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(artist_rows)} artists")

            except Exception as e:
                logger.error(f"Error processing artist {row['artist']}: {e}")
                artists_failed.append(row)

        # Store failed artists for tracking
        if artists_failed:
            failed_df = pl.DataFrame(artists_failed)
            self.data_writer.write_table(
                failed_df, "mbz_artist_not_found", mode="merge"
            )

        logger.info(
            f"Successfully fetched {artists_fetched} artists, {len(artists_failed)} failed"
        )

        return {
            "status": "success",
            "artists_fetched": artists_fetched,
            "artists_failed": len(artists_failed),
            "cache_dir": str(self.cache_dir),
        }

    def parse_artist_json_files(self) -> Dict[str, Any]:
        """
        Parse JSON files and create normalized artist data tables.
        Based on mbz_parse_artists.py logic.
        """
        logger.info("Parsing MusicBrainz artist JSON files")

        # Find JSON files in cache
        json_files = list(self.cache_dir.glob("*.json"))

        if not json_files:
            return {"status": "no_updates", "message": "No JSON files found to process"}

        logger.info(f"Processing {len(json_files)} JSON files")

        # Process each JSON file
        artist_records = []
        processed_files = []

        for json_file in json_files:
            try:
                with open(json_file, "r") as f:
                    artist_data = json.load(f)

                # Normalize the JSON data
                normalized_data = normalize_artist_json_data(artist_data)

                # Replace dashes with underscores in column names
                normalized_data = {
                    key.replace("-", "_"): value
                    for key, value in normalized_data.items()
                }

                # Add source file info
                normalized_data["source_file"] = json_file.stem

                artist_records.append(normalized_data)
                processed_files.append(json_file)

            except Exception as e:
                logger.error(f"Error processing {json_file}: {e}")
                continue

        if not artist_records:
            return {"status": "error", "message": "No valid artist records processed"}

        # Create DataFrame from all records
        # Get all unique columns
        all_columns = set()
        for record in artist_records:
            all_columns.update(record.keys())

        # Ensure all records have all columns
        for record in artist_records:
            for col in all_columns:
                if col not in record:
                    record[col] = None

        artist_df = pl.DataFrame(artist_records)

        # Ensure schema compatibility with existing table
        existing_df = self.data_writer.read_table("mbz_artist_info")
        if existing_df is not None:
            # Get all columns from existing table
            existing_columns = set(existing_df.columns)
            new_columns = set(artist_df.columns)

            # Add missing columns with null values
            missing_columns = existing_columns - new_columns
            for col in missing_columns:
                artist_df = artist_df.with_columns(
                    pl.lit(None).cast(pl.Utf8).alias(col)
                )

            # Ensure column order matches existing table
            artist_df = artist_df.select(existing_df.columns)

        # Convert columns to strings, but handle complex types gracefully
        string_columns = []
        for col in artist_df.columns:
            try:
                # Try to cast to string, but handle complex types
                if artist_df.schema[col] in [pl.List, pl.Struct]:
                    # For complex types, convert to JSON string
                    string_columns.append(
                        pl.col(col)
                        .map_elements(
                            lambda x: json.dumps(x) if x is not None else None,
                            return_dtype=pl.Utf8,
                        )
                        .alias(col)
                    )
                else:
                    # For simple types, cast to string
                    string_columns.append(pl.col(col).cast(pl.Utf8).alias(col))
            except Exception as e:
                logger.warning(f"Could not convert column {col} to string: {e}")
                # Keep the original column if casting fails
                string_columns.append(pl.col(col).alias(col))

        artist_df = artist_df.with_columns(string_columns)

        # Write to parquet
        write_result = self.data_writer.write_table(
            artist_df, "mbz_artist_info", mode="merge"
        )

        # Create artist genre table if tag_list exists
        genre_result = None
        if "tag_list" in artist_df.columns:
            genre_df = create_artist_genre_table(artist_df)
            if not genre_df.is_empty():
                genre_result = self.data_writer.write_table(
                    genre_df, "mbz_artist_genre", mode="merge"
                )

        # Move processed files to processed directory
        processed_dir = self.cache_dir / "processed"
        processed_dir.mkdir(exist_ok=True)

        moved_files = 0
        for json_file in processed_files:
            try:
                processed_file = processed_dir / json_file.name
                json_file.rename(processed_file)
                moved_files += 1
            except Exception as e:
                logger.warning(f"Could not move {json_file}: {e}")

        logger.info(
            f"Processed {len(artist_records)} artists, moved {moved_files} files"
        )

        return {
            "status": "success",
            "artists_processed": len(artist_records),
            "files_moved": moved_files,
            "artist_table_result": write_result,
            "genre_table_result": genre_result,
        }

    def process_area_hierarchy(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Process area hierarchy data from MusicBrainz.
        Based on the working Fabric notebook implementation.

        Args:
            limit: Maximum number of areas to process
        """
        logger.info("Processing MusicBrainz area hierarchy")

        # Get area IDs that need processing
        area_ids = self._get_area_ids_for_processing()
        logger.info(f"Found {len(area_ids)} area IDs that need processing")

        if not area_ids:
            return {"status": "no_updates", "message": "No area IDs need processing"}

        # Apply limit if specified
        if limit is not None and len(area_ids) > limit:
            area_ids = area_ids[:limit]
            logger.info(f"Limited to {limit} areas for testing")

        logger.info(
            f"Processing {len(area_ids)} area IDs: {area_ids[:5]}..."
        )  # Log first 5 for debugging

        # Process areas and build hierarchy
        all_areas = {}

        # Set user agent for MusicBrainz API
        import musicbrainzngs

        musicbrainzngs.set_useragent("rws-music-enrichment", "1.0")

        for i, area_id in enumerate(area_ids):
            logger.info(f"Processing {i+1}/{len(area_ids)}: {area_id}")
            try:
                areas = self._get_area_with_parents(area_id)
                all_areas[area_id] = areas
            except Exception as e:
                logger.error(f"Error processing area {area_id}: {e}")
                continue

        if not all_areas:
            return {
                "status": "no_data",
                "message": "No area records were processed successfully",
            }

        # Convert to DataFrame format
        hierarchy_df = self._create_hierarchy_dataframe(all_areas)
        logger.info(f"Created hierarchy DataFrame with {len(hierarchy_df)} rows")

        # Write to parquet - use merge to preserve existing data
        write_result = self.data_writer.write_table(
            hierarchy_df, "mbz_area_hierarchy", mode="merge"
        )

        logger.info(
            f"Processed {len(all_areas)} area hierarchies, wrote {write_result.get('records_written', 0)} records"
        )

        return {
            "status": "success",
            "areas_processed": len(all_areas),
            "write_result": write_result,
        }

    def _get_area_with_parents(self, area_id: str) -> Dict[str, Any]:
        """
        Get area and all its parent areas in a flat structure.
        Based on the working Fabric notebook implementation.
        """
        import musicbrainzngs
        from time import sleep

        areas = {}
        visited = set()

        def fetch_parents(id: str):
            if id in visited:
                return
            visited.add(id)

            try:
                sleep(0.5)  # Rate limiting
                area_data = musicbrainzngs.get_area_by_id(id, includes=["area-rels"])[
                    "area"
                ]

                # Store this area
                area_type = (
                    area_data.get("type", "Unknown")
                    .lower()
                    .replace(" ", "_")
                    .replace("-", "_")
                )
                areas[area_type] = {
                    "id": area_data["id"],
                    "name": area_data["name"],
                    "sort_name": area_data.get("sort-name", area_data["name"]),
                    "type": area_data.get("type", "Unknown"),
                }

                # Get parents
                if "area-relation-list" in area_data:
                    for relation in area_data["area-relation-list"]:
                        if relation.get("direction") == "backward":
                            fetch_parents(relation["area"]["id"])

            except Exception as e:
                logger.warning(f"Error fetching area {id}: {e}")

        fetch_parents(area_id)
        return areas

    def _create_hierarchy_dataframe(self, all_areas: Dict[str, Any]) -> pl.DataFrame:
        """
        Convert the areas dictionary to a Polars DataFrame.
        Based on the working Fabric notebook implementation.
        """
        # Get all unique area types
        all_types = set()
        for areas in all_areas.values():
            all_types.update(areas.keys())

        sorted_types = sorted(all_types)

        # Create columns
        columns = ["area_id", "area_type", "area_name", "area_sort_name"]
        for area_type in sorted_types:
            columns.extend(
                [f"{area_type}_id", f"{area_type}_name", f"{area_type}_sort_name"]
            )

        # Create rows
        rows = []
        for root_area_id, areas in all_areas.items():
            row = {col: None for col in columns}
            row["area_id"] = root_area_id

            # Find the root area info
            for area_type, area_info in areas.items():
                if area_info["id"] == root_area_id:
                    row["area_type"] = area_info["type"]
                    row["area_name"] = area_info["name"]
                    row["area_sort_name"] = area_info["sort_name"]
                    break

            # Add all areas to the row
            for area_type, area_info in areas.items():
                row[f"{area_type}_id"] = area_info["id"]
                row[f"{area_type}_name"] = area_info["name"]
                row[f"{area_type}_sort_name"] = area_info["sort_name"]

            rows.append(row)

        # Create DataFrame and convert to strings
        df = pl.DataFrame(rows)

        # Convert all columns to strings and handle nulls
        string_columns = []
        for col in df.columns:
            string_columns.append(
                pl.col(col)
                .cast(pl.Utf8)
                .map_elements(
                    lambda x: None if x in ["None", "NaN", "nan", "<NA>", ""] else x
                )
                .alias(col)
            )

        return df.with_columns(string_columns)

    def _get_area_ids_for_processing(self) -> List[str]:
        """Get area IDs that need hierarchy processing."""
        # Read artist info to get area IDs
        artist_df = self.data_writer.read_table("mbz_artist_info")
        existing_hierarchy_df = self.data_writer.read_table("mbz_area_hierarchy")

        if artist_df is None:
            logger.info("No mbz_artist_info table found")
            return []

        logger.info(f"Found {len(artist_df)} artists in mbz_artist_info table")

        # Collect all area IDs from artist data
        area_ids = set()

        # Get area IDs from various columns
        for col in ["area_id", "begin_area_id", "end_area_id"]:
            if col in artist_df.columns:
                ids = artist_df.select(col).drop_nulls().to_series().to_list()
                area_ids.update(ids)
                logger.info(f"Found {len(ids)} area IDs in column {col}")

        logger.info(f"Total unique area IDs collected: {len(area_ids)}")

        # Filter out areas that already have hierarchy data
        if existing_hierarchy_df is not None:
            existing_ids = set(
                existing_hierarchy_df.select("area_id").to_series().to_list()
            )
            logger.info(f"Found {len(existing_ids)} existing area hierarchies")
            area_ids = area_ids - existing_ids
            logger.info(f"Area IDs needing processing after filtering: {len(area_ids)}")
        else:
            logger.info("No existing hierarchy data found")

        return sorted(list(area_ids))

    def run_full_enrichment(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Run the complete MusicBrainz enrichment pipeline.

        Args:
            limit: Maximum number of records to process for testing
        """
        logger.info("Starting full MusicBrainz enrichment")

        results = {
            "artist_discovery": None,
            "artist_fetching": None,
            "artist_parsing": None,
            "area_processing": None,
            "overall_status": "success",
        }

        try:
            # Step 1: Discover missing artists
            discovery_result = self.discover_missing_artists()
            results["artist_discovery"] = discovery_result

            if (
                discovery_result["status"] == "success"
                and discovery_result["artists_found"] > 0
            ):
                # Step 2: Fetch artist data
                missing_artists_df = discovery_result["missing_artists"]

                # Apply limit if specified
                if limit is not None and len(missing_artists_df) > limit:
                    missing_artists_df = missing_artists_df.head(limit)
                    logger.info(f"Limited to {limit} artists for testing")

                fetch_result = self.fetch_artist_data(missing_artists_df)
                results["artist_fetching"] = fetch_result

                if fetch_result["status"] != "success":
                    results["overall_status"] = "partial_failure"

            # Step 3: Parse existing JSON files
            parse_result = self.parse_artist_json_files()
            results["artist_parsing"] = parse_result

            if parse_result["status"] not in ["success", "no_data"]:
                results["overall_status"] = "partial_failure"

            # Step 4: Process area hierarchy
            area_result = self.process_area_hierarchy(limit=limit)
            results["area_processing"] = area_result

            if area_result["status"] not in ["success", "no_updates", "no_data"]:
                results["overall_status"] = "partial_failure"

            logger.info("MusicBrainz enrichment pipeline completed")
            return results

        except Exception as e:
            logger.error(f"MusicBrainz enrichment failed: {e}")
            results["overall_status"] = "error"
            results["error_message"] = str(e)
            return results


def main():
    """Main entry point for MusicBrainz processor."""
    import argparse

    parser = argparse.ArgumentParser(
        description="MusicBrainz Data Enrichment Processor"
    )
    parser.add_argument("--limit", type=int, help="Limit number of records to process")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    processor = MusicBrainzProcessor()
    result = processor.run_full_enrichment(limit=args.limit)

    print(f"MusicBrainz enrichment completed with status: {result['overall_status']}")
    for step, step_result in result.items():
        if step != "overall_status" and step_result:
            print(f"  {step}: {step_result.get('status', 'unknown')}")


if __name__ == "__main__":
    main()

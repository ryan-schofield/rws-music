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

from scripts.enrich.utils.api_clients import MusicBrainzClient
from scripts.enrich.utils.data_writer import ParquetDataWriter, EnrichmentTracker
from scripts.enrich.utils.polars_ops import (
    normalize_artist_json_data,
    process_area_hierarchy_data,
    create_artist_genre_table,
    batch_process_dataframe
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
        self.cache_dir = Path(cache_dir) if cache_dir else Path("dbt/data/cache/mbz")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Manual excludes from original notebook
        self.manual_excludes = [
            "Brian Eno", "Poppy", "Grouper", "James Blake", "Gnod", "food house"
        ]
    
    def discover_missing_artists(self) -> Dict[str, Any]:
        """
        Find artists that need MusicBrainz enrichment.
        Based on mbz_get_missing_artists.py logic.
        """
        logger.info("Discovering missing artists for MusicBrainz enrichment")
        
        missing_artists_df = self.tracker.get_missing_artists(self.manual_excludes)
        
        if missing_artists_df.is_empty():
            return {
                "status": "no_updates",
                "message": "No missing artists found",
                "artists_found": 0
            }
        
        logger.info(f"Found {len(missing_artists_df)} artists needing MBZ enrichment")
        return {
            "status": "success", 
            "message": f"Found {len(missing_artists_df)} missing artists",
            "artists_found": len(missing_artists_df),
            "missing_artists": missing_artists_df
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
                    logger.warning(f"Could not find MBID for artist {row['artist']} using ISRC {row['track_isrc']}")
                    artists_failed.append(row)
                    continue
                
                # Get full artist data
                artist_data = self.mbz_client.get_artist_by_id(
                    artist_mbid,
                    includes=["tags", "release-groups", "aliases"]
                )
                
                if not artist_data:
                    logger.warning(f"Could not fetch artist data for MBID {artist_mbid}")
                    artists_failed.append(row)
                    continue
                
                # Add Spotify ID to the artist data
                artist_data["spotify_id"] = row["artist_id"]
                
                # Save to JSON file
                json_file = self.cache_dir / f"{artist_mbid}.json"
                with open(json_file, 'w') as f:
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
                failed_df,
                "mbz_artist_not_found", 
                mode="append"
            )
        
        logger.info(f"Successfully fetched {artists_fetched} artists, {len(artists_failed)} failed")
        
        return {
            "status": "success",
            "artists_fetched": artists_fetched,
            "artists_failed": len(artists_failed),
            "cache_dir": str(self.cache_dir)
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
            return {
                "status": "no_data",
                "message": "No JSON files found to process"
            }
        
        logger.info(f"Processing {len(json_files)} JSON files")
        
        # Process each JSON file
        artist_records = []
        processed_files = []
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
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
            return {
                "status": "error",
                "message": "No valid artist records processed"
            }
        
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
        
        # Convert all columns to strings for consistency
        string_columns = []
        for col in artist_df.columns:
            string_columns.append(
                pl.col(col).cast(pl.Utf8).alias(col)
            )
        artist_df = artist_df.with_columns(string_columns)
        
        # Write to parquet
        write_result = self.data_writer.write_table(
            artist_df,
            "mbz_artist_info",
            mode="merge"
        )
        
        # Create artist genre table if tag_list exists
        genre_result = None
        if "tag_list" in artist_df.columns:
            genre_df = create_artist_genre_table(artist_df)
            if not genre_df.is_empty():
                genre_result = self.data_writer.write_table(
                    genre_df,
                    "mbz_artist_genre", 
                    mode="merge"
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
        
        logger.info(f"Processed {len(artist_records)} artists, moved {moved_files} files")
        
        return {
            "status": "success",
            "artists_processed": len(artist_records),
            "files_moved": moved_files,
            "artist_table_result": write_result,
            "genre_table_result": genre_result
        }
    
    def process_area_hierarchy(self) -> Dict[str, Any]:
        """
        Process area hierarchy data from MusicBrainz.
        Based on mbz_parse_area_hierarchy.py logic.
        """
        logger.info("Processing MusicBrainz area hierarchy")
        
        # Get area IDs that need processing
        area_ids = self._get_area_ids_for_processing()
        
        if not area_ids:
            return {
                "status": "no_updates",
                "message": "No area IDs need processing"
            }
        
        logger.info(f"Processing {len(area_ids)} area IDs")
        
        # Process areas and build hierarchy
        all_area_records = []
        
        for i, area_id in enumerate(area_ids):
            try:
                # Get area hierarchy from MusicBrainz
                area_hierarchy = self.mbz_client.get_area_hierarchy(area_id)
                
                if not area_hierarchy:
                    logger.warning(f"No hierarchy data for area {area_id}")
                    continue
                
                # Process into flat row structure
                area_record = process_area_hierarchy_data(area_hierarchy)
                area_record["area_id"] = area_id
                
                # Set root area info
                for area_type, area_info in area_hierarchy.items():
                    if area_info["id"] == area_id:
                        area_record["area_type"] = area_info["type"]
                        area_record["area_name"] = area_info["name"]
                        area_record["area_sort_name"] = area_info["sort_name"]
                        break
                
                all_area_records.append(area_record)
                
                # Progress logging
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(area_ids)} areas")
                
            except Exception as e:
                logger.error(f"Error processing area {area_id}: {e}")
                continue
        
        if not all_area_records:
            return {
                "status": "no_data",
                "message": "No area records were processed successfully"
            }
        
        # Create DataFrame
        hierarchy_df = pl.DataFrame(all_area_records)
        
        # Convert all columns to strings
        string_columns = []
        for col in hierarchy_df.columns:
            string_columns.append(
                pl.col(col).cast(pl.Utf8).alias(col)
            )
        hierarchy_df = hierarchy_df.with_columns(string_columns)
        
        # Write to parquet
        write_result = self.data_writer.write_table(
            hierarchy_df,
            "mbz_area_hierarchy",
            mode="merge"
        )
        
        logger.info(f"Processed {len(all_area_records)} area hierarchies")
        
        return {
            "status": "success",
            "areas_processed": len(all_area_records),
            "write_result": write_result
        }
    
    def _get_area_ids_for_processing(self) -> List[str]:
        """Get area IDs that need hierarchy processing."""
        # Read artist info to get area IDs
        artist_df = self.data_writer.read_table("mbz_artist_info")
        existing_hierarchy_df = self.data_writer.read_table("mbz_area_hierarchy")
        
        if artist_df is None:
            return []
        
        # Collect all area IDs from artist data
        area_ids = set()
        
        # Get area IDs from various columns
        for col in ["area_id", "begin_area_id", "end_area_id"]:
            if col in artist_df.columns:
                ids = artist_df.select(col).drop_nulls().to_series().to_list()
                area_ids.update(ids)
        
        # Filter out areas that already have hierarchy data
        if existing_hierarchy_df is not None:
            existing_ids = set(existing_hierarchy_df.select("area_id").to_series().to_list())
            area_ids = area_ids - existing_ids
        
        return sorted(list(area_ids))
    
    def run_full_enrichment(self) -> Dict[str, Any]:
        """
        Run the complete MusicBrainz enrichment pipeline.
        """
        logger.info("Starting full MusicBrainz enrichment")
        
        results = {
            "artist_discovery": None,
            "artist_fetching": None,
            "artist_parsing": None,
            "area_processing": None,
            "overall_status": "success"
        }
        
        try:
            # Step 1: Discover missing artists
            discovery_result = self.discover_missing_artists()
            results["artist_discovery"] = discovery_result
            
            if discovery_result["status"] == "success" and discovery_result["artists_found"] > 0:
                # Step 2: Fetch artist data
                missing_artists_df = discovery_result["missing_artists"]
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
            area_result = self.process_area_hierarchy()
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
    logging.basicConfig(level=logging.INFO)
    
    processor = MusicBrainzProcessor()
    result = processor.run_full_enrichment()
    
    print(f"MusicBrainz enrichment completed with status: {result['overall_status']}")
    for step, step_result in result.items():
        if step != "overall_status" and step_result:
            print(f"  {step}: {step_result.get('status', 'unknown')}")


if __name__ == "__main__":
    main()
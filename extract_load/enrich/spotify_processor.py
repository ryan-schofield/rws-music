#!/usr/bin/env python3
"""
Spotify data enrichment processor.

This module consolidates spotify_add_new_artists.py and spotify_add_new_albums.py 
from the original Fabric notebooks, replacing Spark operations with Polars and 
writing directly to parquet files.
"""

import sys
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from pathlib import Path
import polars as pl

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from extract_load.enrich.utils.api_clients import SpotifyAPIClient
from extract_load.enrich.utils.data_writer import ParquetDataWriter, EnrichmentTracker
from extract_load.enrich.utils.polars_ops import explode_genre_array, batch_process_dataframe

logger = logging.getLogger(__name__)


class SpotifyProcessor:
    """
    Handles Spotify API data enrichment for artists and albums.

    Consolidates:
    - Artist data fetching (spotify_add_new_artists.py)
    - Album data fetching (spotify_add_new_albums.py)
    """

    def __init__(
        self,
        data_writer: ParquetDataWriter = None,
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
    ):
        self.data_writer = data_writer or ParquetDataWriter()
        self.tracker = EnrichmentTracker(self.data_writer)
        self.spotify_client = SpotifyAPIClient(client_id, client_secret, refresh_token)

    def enrich_artists(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Enrich missing artist data from Spotify API.
        Based on spotify_add_new_artists.py logic.

        Args:
            limit: Maximum number of artists to process
        """
        logger.info("Starting Spotify artist enrichment")

        # Find artists that need enrichment
        missing_artists_df = self.tracker.get_missing_spotify_artists()

        if missing_artists_df.is_empty():
            return {
                "status": "no_updates",
                "message": "No missing artists found",
                "artists_processed": 0,
            }

        # Apply limit if specified
        if limit is not None and len(missing_artists_df) > limit:
            missing_artists_df = missing_artists_df.head(limit)
            logger.info(f"Limited to {limit} artists for testing")

        logger.info(
            f"Found {len(missing_artists_df)} artists needing Spotify enrichment"
        )

        # Extract artist IDs
        artist_ids = missing_artists_df.select("artist_id").to_series().to_list()

        # Fetch artist data from Spotify API in batches
        try:
            artist_data = self.spotify_client.get_artists_batch(
                artist_ids, batch_size=50
            )

            if not artist_data:
                return {
                    "status": "error",
                    "message": "No artist data retrieved from Spotify API",
                }

            # Create DataFrame from API response
            artist_df = pl.DataFrame(artist_data)

            # Add artist_mbid column (initially null, can be populated later)
            artist_df = artist_df.with_columns(
                pl.lit(None).cast(pl.Utf8).alias("artist_mbid")
            )

            # Rename columns to match expected schema
            column_mapping = {
                "id": "artist_id",
                "name": "artist_name",
                "popularity": "artist_popularity",
            }

            for old_col, new_col in column_mapping.items():
                if old_col in artist_df.columns and old_col != new_col:
                    artist_df = artist_df.rename({old_col: new_col})

            # Select only the expected columns to match existing schema
            expected_columns = [
                "artist_id",
                "artist_name",
                "artist_mbid",
                "artist_popularity",
            ]
            artist_df = artist_df.select(expected_columns)

            # Ensure data types match expected schema
            artist_df = artist_df.with_columns(
                pl.col("artist_popularity").cast(pl.Float32)
            )

            # Write to parquet
            write_result = self.data_writer.write_table(
                artist_df, "spotify_artists", mode="merge"
            )

            # Create genre table
            genre_result = self._create_artist_genre_table(artist_df)

            logger.info(f"Successfully enriched {len(artist_data)} artists")

            return {
                "status": "success",
                "artists_processed": len(artist_data),
                "records_written": write_result.get("records_written", 0),
                "artist_table_result": write_result,
                "genre_table_result": genre_result,
            }

        except Exception as e:
            logger.error(f"Error enriching artists: {e}")
            return {"status": "error", "message": str(e), "artists_processed": 0}

    def enrich_albums(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Enrich missing album data from Spotify API.
        Based on spotify_add_new_albums.py logic.

        Args:
            limit: Maximum number of albums to process
        """
        logger.info("Starting Spotify album enrichment")

        # Find albums that need enrichment
        missing_albums_df = self.tracker.get_missing_spotify_albums()

        if missing_albums_df.is_empty():
            return {
                "status": "no_updates",
                "message": "No missing albums found",
                "albums_processed": 0,
            }

        # Apply limit if specified
        if limit is not None and len(missing_albums_df) > limit:
            missing_albums_df = missing_albums_df.head(limit)
            logger.info(f"Limited to {limit} albums for testing")

        logger.info(f"Found {len(missing_albums_df)} albums needing Spotify enrichment")

        # Extract album IDs
        album_ids = missing_albums_df.select("album_id").to_series().to_list()

        # Fetch album data from Spotify API in batches
        try:
            album_data = self.spotify_client.get_albums_batch(album_ids, batch_size=20)

            if not album_data:
                return {
                    "status": "error",
                    "message": "No album data retrieved from Spotify API",
                }

            # Process album data into structured format
            processed_albums = []
            for album in album_data:
                # Extract primary artist information
                artists = album.get("artists", [])
                primary_artist = artists[0] if artists else {}

                processed_album = {
                    "album_type": album.get("album_type"),
                    "artist_id": primary_artist.get("id"),
                    "artist_name": primary_artist.get("name"),
                    "artist_type": primary_artist.get("type"),
                    "genres": album.get("genres", []),
                    "album_id": album.get("id"),
                    "label": album.get("label"),
                    "album_name": album.get("name"),
                    "popularity": album.get("popularity"),
                    "release_date": album.get("release_date"),
                    "release_date_precision": album.get("release_date_precision"),
                    "total_tracks": album.get("total_tracks"),
                    "last_modified": datetime.now(timezone.utc),
                }
                processed_albums.append(processed_album)

            # Create DataFrame
            album_df = pl.DataFrame(processed_albums)

            # Convert datetime column
            if "last_modified" in album_df.columns:
                album_df = album_df.with_columns(
                    pl.col("last_modified").cast(pl.Datetime)
                )

            # Write to parquet
            write_result = self.data_writer.write_table(
                album_df, "spotify_albums", mode="merge"
            )

            # Create album genre table
            genre_result = self._create_album_genre_table(album_df)

            logger.info(f"Successfully enriched {len(processed_albums)} albums")

            return {
                "status": "success",
                "albums_processed": len(processed_albums),
                "records_written": write_result.get("records_written", 0),
                "album_table_result": write_result,
                "genre_table_result": genre_result,
            }

        except Exception as e:
            logger.error(f"Error enriching albums: {e}")
            return {"status": "error", "message": str(e), "albums_processed": 0}

    def _create_artist_genre_table(
        self, artist_df: pl.DataFrame
    ) -> Optional[Dict[str, Any]]:
        """
        Create artist genre table from artist data.
        Based on the explode logic from spotify_add_new_artists.py
        """
        if "genres" not in artist_df.columns:
            return None

        try:
            # Select relevant columns and explode genres
            genre_base = artist_df.select(["artist_id", "artist_name", "genres"])

            # Explode the genres array
            genre_exploded = explode_genre_array(genre_base, "genres")

            # Rename genre column
            if "genres" in genre_exploded.columns:
                genre_exploded = genre_exploded.rename({"genres": "genre"})

            # Filter out null genres
            genre_exploded = genre_exploded.filter(
                pl.col("genre").is_not_null() & (pl.col("genre") != "")
            )

            if genre_exploded.is_empty():
                return {"status": "no_data", "message": "No genre data to write"}

            # Write to parquet
            genre_result = self.data_writer.write_table(
                genre_exploded, "spotify_artist_genre", mode="merge"
            )

            return genre_result

        except Exception as e:
            logger.error(f"Error creating artist genre table: {e}")
            return {"status": "error", "message": str(e)}

    def _create_album_genre_table(
        self, album_df: pl.DataFrame
    ) -> Optional[Dict[str, Any]]:
        """
        Create album genre table from album data.
        """
        if "genres" not in album_df.columns:
            return None

        try:
            # Select relevant columns and explode genres
            genre_base = album_df.select(["album_id", "album_name", "genres"])

            # Explode the genres array
            genre_exploded = explode_genre_array(genre_base, "genres")

            # Rename genre column
            if "genres" in genre_exploded.columns:
                genre_exploded = genre_exploded.rename({"genres": "genre"})

            # Filter out null genres
            genre_exploded = genre_exploded.filter(
                pl.col("genre").is_not_null() & (pl.col("genre") != "")
            )

            if genre_exploded.is_empty():
                return {"status": "no_data", "message": "No genre data to write"}

            # Write to parquet
            genre_result = self.data_writer.write_table(
                genre_exploded, "spotify_album_genre", mode="merge"
            )

            return genre_result

        except Exception as e:
            logger.error(f"Error creating album genre table: {e}")
            return {"status": "error", "message": str(e)}

    def update_artist_mbids(self) -> Dict[str, Any]:
        """
        Update artist MBIDs from MusicBrainz data.
        Links Spotify artist data with MusicBrainz IDs.
        """
        logger.info("Updating artist MBIDs from MusicBrainz data")

        try:
            # Read both tables
            spotify_artists_df = self.data_writer.read_table("spotify_artists")
            mbz_artist_df = self.data_writer.read_table("mbz_artist_info")

            if spotify_artists_df is None or mbz_artist_df is None:
                return {"status": "error", "message": "Required tables not found"}

            # Find artists that need MBID updates
            artists_needing_mbid = spotify_artists_df.filter(
                pl.col("artist_mbid").is_null()
            )

            if artists_needing_mbid.is_empty():
                return {
                    "status": "no_updates",
                    "message": "No artists need MBID updates",
                }

            # Join with MusicBrainz data
            updated_artists = artists_needing_mbid.join(
                mbz_artist_df.select(["spotify_id", "id"]).rename(
                    {"id": "artist_mbid_new"}
                ),
                left_on="artist_id",
                right_on="spotify_id",
                how="left",
            )

            # Update MBID where available
            updated_artists = updated_artists.with_columns(
                pl.coalesce([pl.col("artist_mbid_new"), pl.col("artist_mbid")]).alias(
                    "artist_mbid"
                )
            ).drop("artist_mbid_new")

            # Combine with unchanged artists
            unchanged_artists = spotify_artists_df.filter(
                pl.col("artist_mbid").is_not_null()
            )

            final_artists_df = pl.concat([unchanged_artists, updated_artists])

            # Write back to parquet - use merge to preserve existing data
            write_result = self.data_writer.write_table(
                final_artists_df, "spotify_artists", mode="merge"
            )

            # Count updates
            updates_made = updated_artists.filter(
                pl.col("artist_mbid").is_not_null()
            ).height

            logger.info(f"Updated {updates_made} artist MBIDs")

            return {
                "status": "success",
                "updates_made": updates_made,
                "write_result": write_result,
            }

        except Exception as e:
            logger.error(f"Error updating artist MBIDs: {e}")
            return {"status": "error", "message": str(e)}

    def run_full_enrichment(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Run the complete Spotify enrichment pipeline.

        Args:
            limit: Maximum number of records to process for testing
        """
        logger.info("Starting full Spotify enrichment")

        results = {
            "artist_enrichment": None,
            "album_enrichment": None,
            "mbid_updates": None,
            "overall_status": "success",
        }

        try:
            # Step 1: Enrich artist data
            artist_result = self.enrich_artists(limit=limit)
            results["artist_enrichment"] = artist_result

            if artist_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"

            # Step 2: Enrich album data
            album_result = self.enrich_albums(limit=limit)
            results["album_enrichment"] = album_result

            if album_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"

            # Step 3: Update artist MBIDs
            mbid_result = self.update_artist_mbids()
            results["mbid_updates"] = mbid_result

            if mbid_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"

            logger.info("Spotify enrichment pipeline completed")
            return results

        except Exception as e:
            logger.error(f"Spotify enrichment failed: {e}")
            results["overall_status"] = "error"
            results["error_message"] = str(e)
            return results


def main():
    """Main entry point for Spotify processor."""
    logging.basicConfig(level=logging.INFO)

    processor = SpotifyProcessor()
    result = processor.run_full_enrichment()

    print(f"Spotify enrichment completed with status: {result['overall_status']}")
    for step, step_result in result.items():
        if step != "overall_status" and step_result:
            print(f"  {step}: {step_result.get('status', 'unknown')}")


if __name__ == "__main__":
    main()

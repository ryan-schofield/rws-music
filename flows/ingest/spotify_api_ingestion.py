#!/usr/bin/env python3
"""
This script handles fetching recently played tracks from Spotify API
and storing them for further processing.
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import json
from pathlib import Path
import glob

from dotenv import load_dotenv
import polars as pl

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.enrich.utils.api_clients import SpotifyAPIClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SpotifyDataIngestion:
    """Handles ingestion of Spotify data."""

    def __init__(self):
        # Use absolute path for task-runner compatibility
        workspace_dir = Path("/home/runner/workspace")
        if not workspace_dir.exists():
            workspace_dir = Path.cwd()
        self.data_dir = workspace_dir / "data"
        self.raw_data_dir = self.data_dir / "raw" / "recently_played" / "detail"

        # Ensure directories exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Spotify client
        self.spotify_client = SpotifyAPIClient()

    def load_cursor(self) -> str:
        """Load cursor from JSON file."""
        cursor_path = self.data_dir / "cursor" / "cursor.json"
        if cursor_path.exists():
            with open(cursor_path, "r") as f:
                cursor = json.load(f)
            return cursor.get("after")
        return None

    def save_cursor(self, after: str):
        """Save cursor to JSON file."""
        cursor_path = self.data_dir / "cursor" / "cursor.json"
        cursor_path.parent.mkdir(parents=True, exist_ok=True)
        cursor = {"user_id": "fffv23", "after": after}
        with open(cursor_path, "w") as f:
            json.dump(cursor, f, indent=2)

    def fetch_recently_played(self, after: str = None) -> List[Dict[str, Any]]:
        """Fetch recently played tracks from Spotify API and flatten to required format."""
        try:
            logger.info("Fetching recently played tracks from Spotify")

            response = self.spotify_client.get_recently_played(after=after)

            items = response.get("items", [])
            logger.info(f"Retrieved {len(items)} tracks from Spotify")

            # Flatten items to required format
            flattened_data = []
            for item in items:
                track = item.get("track", {})
                if not track:
                    continue

                # Extract artist info (first artist)
                artists = track.get("artists", [])
                artist_id = artists[0].get("id") if artists else None
                artist_name = artists[0].get("name") if artists else None

                # Extract album info
                album = track.get("album", {})
                album_id = album.get("id")
                album_uri = album.get("uri")
                album_name = album.get("name")

                # Extract track info
                track_id = track.get("id")
                track_uri = track.get("uri")
                track_name = track.get("name")
                track_isrc = track.get("external_ids", {}).get("isrc")
                duration_ms = track.get("duration_ms")
                popularity = track.get("popularity", 0)

                # Context for play_source
                context = item.get("context", {})
                play_source = context.get("uri", "spotify") if context else "spotify"

                # Create flattened dict
                flattened_item = {
                    "user_id": "fffv23",
                    "track_id": track_id,
                    "uri": track_uri,
                    "track_isrc": track_isrc,
                    "track_name": track_name,
                    "album_id": album_id,
                    "album_uri": album_uri,
                    "album": album_name,
                    "artist_id": artist_id,
                    "artist_mbid": None,
                    "artist": artist_name,
                    "duration_ms": duration_ms,
                    "played_at": item.get("played_at"),
                    "popularity": popularity,
                    "request_after": after,
                    "play_source": play_source,
                }

                flattened_data.append(flattened_item)

            return flattened_data

        except Exception as e:
            logger.error(f"Error fetching recently played tracks: {e}")
            return []

    def save_raw_data(self, data: List[Dict[str, Any]]) -> str:
        """Save raw data to JSON file for processing."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"spotify_recently_played_{timestamp}.json"
        filepath = self.raw_data_dir / filename

        with open(filepath, "w") as f:
            json.dump(data, f, default=str)

        logger.info(f"Saved {len(data)} records to {filepath}")
        return str(filepath)

    def consolidate_to_csv(self) -> str:
        """Consolidate all JSON files from recently_played/detail directory to a single CSV."""
        logger.info("Starting consolidation of JSON files to CSV")

        # Find all JSON files in the played directory and subdirectories
        played_dir = self.data_dir / "raw" / "recently_played"
        json_pattern = str(played_dir / "**" / "*.json")
        json_files = glob.glob(json_pattern, recursive=True)

        if not json_files:
            logger.info("No JSON files found to consolidate")
            return None

        logger.info(f"Found {len(json_files)} JSON files to consolidate")

        # Collect all data from JSON files
        all_data = []
        for json_file in json_files:
            try:
                with open(json_file, "r") as f:
                    file_data = json.load(f)
                    if isinstance(file_data, list):
                        all_data.extend(file_data)
                    else:
                        all_data.append(file_data)
                logger.debug(f"Loaded data from {json_file}")
            except Exception as e:
                logger.error(f"Error reading {json_file}: {e}")
                continue

        if not all_data:
            logger.info("No valid data found in JSON files")
            return None

        # Use static CSV filename (overwrite each time)
        csv_filename = "recently_played.csv"
        csv_filepath = self.data_dir / csv_filename

        # Convert to Polars DataFrame and remove duplicates
        try:
            # Define explicit schema to handle mixed types (e.g., None from Navidrome, strings from Spotify)
            schema = {
                "user_id": pl.Utf8,
                "track_id": pl.Utf8,
                "uri": pl.Utf8,
                "track_isrc": pl.Utf8,
                "track_name": pl.Utf8,
                "album_id": pl.Utf8,
                "album_uri": pl.Utf8,
                "album": pl.Utf8,
                "artist_id": pl.Utf8,
                "artist_mbid": pl.Utf8,
                "artist": pl.Utf8,
                "duration_ms": pl.Float64,  # Use Float64 to handle None values
                "played_at": pl.Utf8,
                "popularity": pl.Float64,  # Use Float64 to handle None values
                "request_after": pl.Utf8,
                "play_source": pl.Utf8,
            }

            logger.info(f"Converting {len(all_data)} records to DataFrame")
            df = pl.DataFrame(all_data, schema=schema)

            # Convert played_at to datetime and duration_ms to seconds for calculations
            # Handle both Spotify (with Z) and Navidrome (with timezone offset) formats
            # Use str.to_datetime with ISO parsing for maximum flexibility
            df = df.with_columns(
                [
                    pl.col("played_at").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z", time_unit="us", time_zone=None).dt.replace_time_zone(None).alias("played_at_dt"),
                    (pl.col("duration_ms") / 1000).alias("duration_sec"),
                ]
            )

            # Step 1: Remove exact duplicates by grouping on track_id and played_at (same play event)
            df_step1 = (
                df.with_row_index()
                .with_columns(
                    pl.col("index")
                    .first()
                    .over(["track_id", "played_at"])
                    .alias("keep_row")
                )
                .filter(pl.col("index") == pl.col("keep_row"))
                .drop(["index", "keep_row"])
            )

            step1_count = len(df_step1)
            step1_removed = len(df) - step1_count

            # Step 2: Remove duplicates where same track (identified by track_name + artist) have plays within track duration
            # Filter out rows with null values in critical columns before sorting
            # Note: Navidrome entries may not have track_id - using track_name and artist as identifiers
            # Note: request_after may be null, which is expected - don't filter on it
            df_filtered = df_step1.filter(
                (pl.col("track_name").is_not_null())
                & (pl.col("artist").is_not_null())
                & (pl.col("played_at_dt").is_not_null())
            )

            df_unique = (
                df_filtered.sort(["track_name", "artist", "played_at_dt"])
                .with_columns(
                    [
                        pl.col("played_at_dt")
                        .shift(1)
                        .over(["track_name", "artist"])
                        .alias("prev_played_at"),
                        pl.col("duration_sec")
                        .shift(1)
                        .over(["track_name", "artist"])
                        .alias("prev_duration_sec"),
                    ]
                )
                .with_columns(
                    [
                        (
                            (
                                pl.col("played_at_dt") - pl.col("prev_played_at")
                            ).dt.total_seconds()
                        ).alias("time_diff_sec")
                    ]
                )
                .filter(
                    (pl.col("time_diff_sec").is_null())  # Keep first occurrence
                    | (
                        pl.col("time_diff_sec") > pl.col("prev_duration_sec")
                    )  # Keep if gap > previous track duration
                )
                .drop(
                    [
                        "played_at_dt",
                        "duration_sec",
                        "prev_played_at",
                        "prev_duration_sec",
                        "time_diff_sec",
                    ]
                )
            )

            # Log deduplication results
            original_count = len(all_data)
            step1_count = len(df_step1)
            filtered_count = len(df_filtered)
            unique_count = len(df_unique)
            step1_removed = original_count - step1_count
            filtered_removed = step1_count - filtered_count
            step2_removed = filtered_count - unique_count
            total_removed = original_count - unique_count

            if total_removed > 0:
                logger.info("Deduplication complete:")
                logger.info(
                    f"  - Step 1 (exact duplicates): {step1_removed} records removed"
                )
                if filtered_removed > 0:
                    logger.info(
                        f"  - Filtered (null values): {filtered_removed} records removed"
                    )
                logger.info(
                    f"  - Step 2 (duration-based): {step2_removed} records removed"
                )
                logger.info(f"  - Total removed: {total_removed} records")
            else:
                logger.info("No duplicate records found")

            # Write to CSV
            df_unique.write_csv(csv_filepath)

            logger.info(
                f"Successfully consolidated {unique_count} unique records to {csv_filepath}"
            )
            return str(csv_filepath)

        except Exception as e:
            logger.error(f"Error writing CSV file: {e}")
            raise

    def run_ingestion(self) -> Dict[str, Any]:
        """Run the complete ingestion process."""
        start_time = datetime.now(timezone.utc)
        logger.info("Starting Spotify data ingestion")

        try:
            # Load cursor
            after = self.load_cursor()

            # Fetch data from Spotify
            data = self.fetch_recently_played(after=after)
            if not data:
                logger.info("No new tracks retrieved from Spotify. Skipping CSV write.")
                return {
                    "status": "success",
                    "message": "No new data retrieved from Spotify",
                    "records_ingested": 0,
                }

            # Save to file
            saved_file = self.save_raw_data(data)

            # Consolidate all JSON files to CSV
            csv_file = self.consolidate_to_csv()

            # Update cursor with max played_at + 1 to prevent duplicates
            if data:
                max_played_at = data[0]["played_at"]
                dt = datetime.fromisoformat(max_played_at.replace("Z", "+00:00"))
                new_after = str(int(dt.timestamp() * 1000) + 1)
                self.save_cursor(new_after)

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "duration_seconds": duration,
                "records_ingested": len(data),
                "saved_file": saved_file,
                "csv_file": csv_file,
            }

            logger.info(f"Ingestion completed in {duration:.2f} seconds")
            return result

        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            return {"status": "error", "message": str(e)}


def main():
    """Main entry point."""
    ingestor = SpotifyDataIngestion()
    result = ingestor.run_ingestion()

    # Print result for logging/monitoring
    print(json.dumps(result, indent=2, default=str))

    # Exit with appropriate code
    if result.get("status") == "success" or result.get("status") == "no_data":
        exit(0)
    else:
        exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
This script handles fetching recently played tracks from Spotify API
and storing them for further processing.
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import json
from pathlib import Path
import base64
import csv
import glob

import requests
from dotenv import load_dotenv
import polars as pl

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SpotifyAPIClient:
    """Client for interacting with Spotify Web API."""

    BASE_URL = "https://api.spotify.com/v1"
    TOKEN_URL = "https://accounts.spotify.com/api/token"

    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
    ):
        self.client_id = client_id or os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("SPOTIFY_CLIENT_SECRET")
        self.refresh_token = refresh_token or os.getenv("SPOTIFY_REFRESH_TOKEN")
        self._access_token = None
        self._token_expires_at = None

        if not self.client_id or not self.client_secret or not self.refresh_token:
            raise ValueError("Spotify credentials not found")

    def _get_access_token(self) -> str:
        """Get or refresh access token."""
        now = datetime.now(timezone.utc)

        # Check if we have a valid token
        if (
            self._access_token
            and self._token_expires_at
            and now < self._token_expires_at
        ):
            return self._access_token

        # Get new token
        auth_string = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"grant_type": "refresh_token", "refresh_token": self.refresh_token}

        try:
            response = requests.post(self.TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()

            token_data = response.json()
            self._access_token = token_data["access_token"]
            expires_in = token_data["expires_in"]

            # Set expiration time (with 5 minute buffer)
            self._token_expires_at = now + timedelta(seconds=expires_in - 300)

            logger.info("Successfully obtained Spotify access token")
            return self._access_token

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get access token: {e}")
            raise

    def _make_request(
        self, endpoint: str, params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Make authenticated request to Spotify API."""
        token = self._get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            logger.debug(f"Spotify API response status: {response.status_code}")
            logger.debug(f"Spotify API response body: {response.text}")
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Spotify API request failed: {e}")
            if e.response is not None:
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise

    def get_recently_played(self, after: str = None) -> Dict[str, Any]:
        """Get recently played tracks."""
        params = {"limit": 50}

        if after:
            params["after"] = after

        return self._make_request("/me/player/recently-played", params)


class SpotifyDataIngestion:
    """Handles ingestion of Spotify data."""

    def __init__(self):
        self.data_dir = Path("data")
        self.raw_data_dir = self.data_dir / "raw" / "recently_played" / "detail"

        # Ensure directories exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Spotify client
        self.spotify_client = SpotifyAPIClient()

    def load_cursor(self) -> str:
        """Load cursor from JSON file."""
        cursor_path = Path("data/cursor/cursor.json")
        if cursor_path.exists():
            with open(cursor_path, "r") as f:
                cursor = json.load(f)
            return cursor.get("after")
        return None

    def save_cursor(self, after: str):
        """Save cursor to JSON file."""
        cursor_path = Path("data/cursor/cursor.json")
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
            logger.info(f"Converting {len(all_data)} records to DataFrame")
            df = pl.DataFrame(all_data)

            # Convert played_at to datetime and duration_ms to seconds for calculations
            df = df.with_columns(
                [
                    pl.col("played_at")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ")
                    .alias("played_at_dt"),
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

            # Step 2: Remove duplicates where same track_id and request_after have plays within track duration
            df_unique = (
                df_step1.sort(["track_id", "request_after", "played_at_dt"])
                .with_columns(
                    [
                        pl.col("played_at_dt")
                        .shift(1)
                        .over(["track_id", "request_after"])
                        .alias("prev_played_at"),
                        pl.col("duration_sec")
                        .shift(1)
                        .over(["track_id", "request_after"])
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
            unique_count = len(df_unique)
            step2_removed = step1_count - unique_count
            total_removed = original_count - unique_count

            if total_removed > 0:
                logger.info(f"Deduplication complete:")
                logger.info(
                    f"  - Step 1 (exact duplicates): {step1_removed} records removed"
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

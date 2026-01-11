#!/usr/bin/env python3
"""
This script handles fetching recently played tracks from Navidrome via ListenBrainz API
and storing them for further processing.
"""

import os
import sys
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import json
from pathlib import Path

from dotenv import load_dotenv

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables - override to replace system env vars with .env values
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class NavidromeDataIngestion:
    """Handles ingestion of Navidrome data via ListenBrainz API."""

    def __init__(self):
        # Use absolute path for task-runner compatibility
        workspace_dir = Path("/home/runner/workspace")
        if not workspace_dir.exists():
            workspace_dir = Path.cwd()
        self.data_dir = workspace_dir / "data"
        self.raw_data_dir = self.data_dir / "raw" / "recently_played" / "detail"

        # Ensure directories exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        # Load configuration from environment
        # Note: LB_API_ROOT should be the full API root, e.g., "https://api.listenbrainz.org/1"
        raw_api_root = os.getenv("LB_API_ROOT", "")
        logger.info(f"LB_API_ROOT env var: {raw_api_root}")
        
        if raw_api_root:
            self.lb_api_root = raw_api_root.rstrip("/")
        else:
            # Fallback - but this shouldn't happen if .env is loaded
            self.lb_api_root = "https://api.listenbrainz.org/1"
            logger.warning("LB_API_ROOT not set, using default")
        
        self.lb_user = os.getenv("LB_USER")
        self.lb_token = os.getenv("LB_TOKEN")
        self.max_items_per_request = int(os.getenv("MAX_ITEMS_PER_REQUEST", "1000"))
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        self.debug_mode = os.getenv("DEBUG_MODE", "false").lower() == "true"

        logger.info(f"LB_USER: {self.lb_user}")
        logger.info(f"LB_API_ROOT (processed): {self.lb_api_root}")

        # Validate required environment variables
        if not self.lb_user:
            raise ValueError("LB_USER environment variable is required")
        if not self.lb_token:
            raise ValueError("LB_TOKEN environment variable is required")

        # Initialize min_ts - will be overridden by cursor in run_ingestion
        self.min_ts = 0

    def load_cursor(self) -> int:
        """Load cursor (last seen timestamp) from JSON file."""
        cursor_path = self.data_dir / "cursor" / "navidrome_cursor.json"
        if cursor_path.exists():
            with open(cursor_path, "r") as f:
                cursor = json.load(f)
            return cursor.get("last_seen_ts", 0)
        return 0

    def save_cursor(self, last_seen_ts: int):
        """Save cursor (last seen timestamp) to JSON file."""
        cursor_path = self.data_dir / "cursor" / "navidrome_cursor.json"
        cursor_path.parent.mkdir(parents=True, exist_ok=True)
        cursor = {"user_id": self.lb_user, "last_seen_ts": last_seen_ts}
        with open(cursor_path, "w") as f:
            json.dump(cursor, f, indent=2)

    def fetch_recent_listens(self) -> List[Dict[str, Any]]:
        """Fetch recently played tracks from ListenBrainz API for Navidrome submissions."""
        headers = {
            "Authorization": f"Token {self.lb_token}",
            "Accept": "application/json",
        }
        listens_url = f"{self.lb_api_root}/user/{self.lb_user}/listens"

        logger.info(f"Fetching listens for user: {self.lb_user}")
        logger.info(f"API URL: {listens_url}")
        logger.info(f"API params: count={self.max_items_per_request}, min_ts={self.min_ts}")
        logger.info(f"TEST_MODE: {self.test_mode}")
        logger.info(f"DEBUG_MODE: {self.debug_mode}")

        new_listens = []
        request_count = 0
        max_requests = 10  # Limit requests to avoid excessive API calls

        while True:
            if request_count >= max_requests:
                logger.warning("Reached max requests limit")
                break

            params = {"count": self.max_items_per_request, "min_ts": self.min_ts}
            logger.info(f"Making API request with params: {params}")

            try:
                import requests

                r = requests.get(
                    listens_url, headers=headers, params=params, timeout=15
                )
                r.raise_for_status()
            except Exception as e:
                logger.error(f"API request failed: {e}")
                break

            request_count += 1

            # Check if response is JSON
            if "application/json" not in r.headers.get("Content-Type", ""):
                logger.error(f"Expected JSON but got {r.headers.get('Content-Type')}")
                logger.error(f"Response status: {r.status_code}")
                # Log first 500 chars of response for debugging
                logger.error(f"Response preview: {r.text[:500]}")
                break

            data = r.json()
            listens = data.get("payload", {}).get("listens", [])
            
            logger.info(f"API response: {len(listens)} listens in payload")
            
            if not listens:
                logger.warning(f"No listens in API response. Full response: {json.dumps(data, indent=2)[:2000]}")
                break

            # Track submission client types for debugging
            client_types = {}

            for item in listens:
                track_metadata = item.get("track_metadata", {})
                additional_info = track_metadata.get("additional_info", {})
                submission_client = additional_info.get("submission_client", "")

                # Track client types
                client_types[submission_client] = client_types.get(submission_client, 0) + 1

                # Only include navidrome submissions (case-insensitive, prefix match)
                if submission_client and not submission_client.lower().startswith("navidrome"):
                    continue

                ts = item.get("listened_at")
                if not ts:
                    continue

                mbid_mapping = track_metadata.get("mbid_mapping", {})

                # Convert Unix timestamp to ISO 8601 format
                played_at = datetime.utcfromtimestamp(ts).isoformat() + "Z"

                # Get artist MBID (first one if multiple)
                artist_mbids = mbid_mapping.get("artist_mbids", [])
                artist_mbid = artist_mbids[0] if artist_mbids else None

                new_listens.append(
                    {
                        "user_id": item.get("user_name"),
                        "track_id": None,
                        "uri": None,
                        "track_isrc": None,
                        "track_name": track_metadata.get("track_name"),
                        "album_id": None,
                        "album_uri": None,
                        "album": track_metadata.get("release_name"),
                        "artist_id": None,
                        "artist_mbid": artist_mbid,
                        "artist": track_metadata.get("artist_name"),
                        "duration_ms": additional_info.get("duration_ms"),
                        "played_at": played_at,
                        "popularity": None,
                        "request_after": ts * 1000,
                        "play_source": "navidrome",
                    }
                )

            # Log submission client types for debugging
            logger.info(f"Submission client types in batch: {client_types}")
            logger.info(
                f"Retrieved {len(listens)} listens, {len(new_listens)} navidrome tracks in this batch"
            )

            # Debug: output sample of listens if no navidrome tracks found
            if self.debug_mode and len(new_listens) == 0 and len(listens) > 0:
                logger.debug("Sample listens (first 3):")
                for sample in listens[:3]:
                    sample_metadata = sample.get("track_metadata", {})
                    sample_additional = sample_metadata.get("additional_info", {})
                    logger.debug(
                        f"  - {sample_metadata.get('track_name', 'Unknown')} by {sample_metadata.get('artist_name', 'Unknown')} "
                        f"(client: {sample_additional.get('submission_client', 'None')})"
                    )

            # If we got fewer listens than requested, we've reached the end
            if len(listens) < self.max_items_per_request:
                break

            # Set min_ts to the max listened_at timestamp from ALL listens for next iteration
            # This ensures we don't fetch any records we've already seen
            max_ts_in_batch = max(item.get("listened_at", 0) for item in listens)
            if max_ts_in_batch > 0:
                self.min_ts = max_ts_in_batch

            # Add delay to respect API rate limits
            time.sleep(5)

        logger.info(f"Total Navidrome tracks retrieved: {len(new_listens)}")
        return new_listens

    def save_raw_data(self, data: List[Dict[str, Any]]) -> str:
        """Save raw data to JSON file for processing."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"navidrome_recently_played_{timestamp}.json"
        filepath = self.raw_data_dir / filename

        with open(filepath, "w") as f:
            json.dump(data, f, default=str)

        logger.info(f"Saved {len(data)} records to {filepath}")
        return str(filepath)

    def run_ingestion(self) -> Dict[str, Any]:
        """Run the complete Navidrome ingestion process."""
        start_time = datetime.now(timezone.utc)
        logger.info("Starting Navidrome data ingestion via ListenBrainz")

        try:
            # Load cursor to get last seen timestamp
            last_seen_ts = self.load_cursor()
            logger.info(f"Loaded cursor last_seen_ts: {last_seen_ts}")
            if last_seen_ts > 0:
                self.min_ts = last_seen_ts
                logger.info(f"Resuming from last seen timestamp: {last_seen_ts}")

            # Fetch data from ListenBrainz
            data = self.fetch_recent_listens()

            if not data:
                logger.info(
                    "No new tracks retrieved from Navidrome. Skipping file write."
                )
                return {
                    "status": "success",
                    "message": "No new data retrieved from Navidrome",
                    "records_ingested": 0,
                }

            # Save to file
            saved_file = self.save_raw_data(data)

            # Update cursor with the last Navidrome track's timestamp
            if data:
                last_ts = data[-1]["request_after"] // 1000  # Convert from milliseconds to Unix timestamp
                self.save_cursor(last_ts)

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "duration_seconds": duration,
                "records_ingested": len(data),
                "saved_file": saved_file,
            }

            logger.info(f"Navidrome ingestion completed in {duration:.2f} seconds")
            return result

        except Exception as e:
            logger.error(f"Navidrome ingestion failed: {e}")
            return {"status": "error", "message": str(e)}


def main():
    """Main entry point."""
    ingestor = NavidromeDataIngestion()
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

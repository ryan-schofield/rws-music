#!/usr/bin/env python3
"""
Spotify API data ingestion using Polars and DuckDB.

This script handles fetching recently played tracks from Spotify API
and storing them in DuckDB for further processing.
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import json
from pathlib import Path
import base64

import requests
from dotenv import load_dotenv

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
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Spotify API request failed: {e}")
            raise

    def get_recently_played(self, after: str = None) -> Dict[str, Any]:
        """Get recently played tracks."""
        params = {"limit": 50}

        if after:
            params["after"] = after

        return self._make_request("/me/player/recently-played", params)

    def get_track_details(self, track_ids: List[str]) -> Dict[str, Any]:
        """Get detailed information for multiple tracks."""
        if not track_ids:
            return {"tracks": []}

        # Spotify allows up to 50 tracks per request
        track_ids_str = ",".join(track_ids[:50])
        return self._make_request(f"/tracks?ids={track_ids_str}")

    def get_artist_details(self, artist_ids: List[str]) -> Dict[str, Any]:
        """Get detailed information for multiple artists."""
        if not artist_ids:
            return {"artists": []}

        # Spotify allows up to 50 artists per request
        artist_ids_str = ",".join(artist_ids[:50])
        return self._make_request(f"/artists?ids={artist_ids_str}")

    def get_album_details(self, album_ids: List[str]) -> Dict[str, Any]:
        """Get detailed information for multiple albums."""
        if not album_ids:
            return {"albums": []}

        # Spotify allows up to 20 albums per request
        album_ids_str = ",".join(album_ids[:20])
        return self._make_request(f"/albums?ids={album_ids_str}")


class SpotifyDataIngestion:
    """Handles ingestion of Spotify data."""

    def __init__(self):
        self.data_dir = Path("dbt/data")
        self.raw_data_dir = self.data_dir / "raw"

        # Ensure directories exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Spotify client
        self.spotify_client = SpotifyAPIClient()

    def load_cursor(self) -> str:
        """Load cursor from JSON file."""
        cursor_path = Path("dbt/data/cursor/cursor.json")
        if cursor_path.exists():
            with open(cursor_path, "r") as f:
                cursor = json.load(f)
            return cursor.get("after")
        return None

    def save_cursor(self, after: str):
        """Save cursor to JSON file."""
        cursor_path = Path("dbt/data/cursor/cursor.json")
        cursor = {"user_id": "fffv23", "after": after}
        with open(cursor_path, "w") as f:
            json.dump(cursor, f, indent=2)

    def fetch_recently_played(self, after: str = None) -> List[Dict[str, Any]]:
        """Fetch recently played tracks from Spotify API."""
        try:
            logger.info("Fetching recently played tracks from Spotify")

            response = self.spotify_client.get_recently_played(after=after)

            items = response.get("items", [])
            logger.info(f"Retrieved {len(items)} tracks from Spotify")

            return items

        except Exception as e:
            logger.error(f"Error fetching recently played tracks: {e}")
            return []

    def enrich_track_data(
        self, tracks_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Enrich track data with additional information."""
        if not tracks_data:
            return []

        logger.info("Enriching track data with additional details")

        # Extract track, artist, and album IDs
        track_ids = []
        artist_ids = []
        album_ids = []

        for item in tracks_data:
            track = item.get("track", {})
            if track:
                track_ids.append(track.get("id"))
                album_ids.append(track.get("album", {}).get("id"))

                for artist in track.get("artists", []):
                    artist_ids.append(artist.get("id"))

        # Remove duplicates and None values
        track_ids = list(set(filter(None, track_ids)))
        artist_ids = list(set(filter(None, artist_ids)))
        album_ids = list(set(filter(None, album_ids)))

        # Fetch additional details
        track_details = {}
        artist_details = {}
        album_details = {}

        try:
            if track_ids:
                tracks_response = self.spotify_client.get_track_details(track_ids)
                for track in tracks_response.get("tracks", []):
                    if track:
                        track_details[track["id"]] = track

            if artist_ids:
                artists_response = self.spotify_client.get_artist_details(artist_ids)
                for artist in artists_response.get("artists", []):
                    if artist:
                        artist_details[artist["id"]] = artist

            if album_ids:
                albums_response = self.spotify_client.get_album_details(album_ids)
                for album in albums_response.get("albums", []):
                    if album:
                        album_details[album["id"]] = album

        except Exception as e:
            logger.warning(f"Error fetching additional details: {e}")

        # Enrich the original data
        enriched_data = []
        for item in tracks_data:
            try:
                enriched_item = self._enrich_single_track(
                    item, track_details, artist_details, album_details
                )
                enriched_data.append(enriched_item)
            except Exception as e:
                logger.warning(f"Error enriching track: {e}")
                continue

        logger.info(f"Enriched {len(enriched_data)} tracks")
        return enriched_data

    def _enrich_single_track(
        self,
        item: Dict[str, Any],
        track_details: Dict[str, Any],
        artist_details: Dict[str, Any],
        album_details: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Enrich a single track item with additional data."""
        track = item.get("track", {})
        if not track:
            return item

        track_id = track.get("id")
        album_id = track.get("album", {}).get("id")

        # Add track popularity if available
        if track_id and track_id in track_details:
            track_detail = track_details[track_id]
            track["popularity"] = track_detail.get("popularity", 0)

        # Add album details if available
        if album_id and album_id in album_details:
            album_detail = album_details[album_id]
            track["album"] = track.get("album", {})
            track["album"]["popularity"] = album_detail.get("popularity", 0)
            track["album"]["release_date"] = album_detail.get("release_date")

        # Add artist details if available
        for artist in track.get("artists", []):
            artist_id = artist.get("id")
            if artist_id and artist_id in artist_details:
                artist_detail = artist_details[artist_id]
                artist["popularity"] = artist_detail.get("popularity", 0)
                artist["genres"] = artist_detail.get("genres", [])
                artist["followers"] = artist_detail.get("followers", {}).get("total", 0)

        return item

    def save_raw_data(self, data: List[Dict[str, Any]]) -> str:
        """Save raw data to JSON file for processing."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"spotify_recently_played_{timestamp}.json"
        filepath = self.raw_data_dir / filename

        # Add metadata
        data_with_metadata = {
            "metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "record_count": len(data),
                "source": "spotify_api",
            },
            "data": data,
        }

        with open(filepath, "w") as f:
            json.dump(data_with_metadata, f, default=str)

        logger.info(f"Saved {len(data)} records to {filepath}")
        return str(filepath)

    def run_ingestion(self) -> Dict[str, Any]:
        """Run the complete ingestion process."""
        start_time = datetime.now(timezone.utc)
        logger.info("Starting Spotify data ingestion")

        try:
            # Load cursor
            after = self.load_cursor()

            # Fetch data from Spotify
            raw_data = self.fetch_recently_played(after=after)
            if not raw_data:
                return {
                    "status": "no_data",
                    "message": "No data retrieved from Spotify",
                }

            # Enrich data
            enriched_data = self.enrich_track_data(raw_data)

            # Save to file
            saved_file = self.save_raw_data(enriched_data)

            # Update cursor with the oldest played_at
            if raw_data:
                oldest_played_at = raw_data[-1]["played_at"]
                dt = datetime.fromisoformat(oldest_played_at.replace("Z", "+00:00"))
                new_after = str(int(dt.timestamp() * 1000))
                self.save_cursor(new_after)

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "duration_seconds": duration,
                "records_ingested": len(enriched_data),
                "saved_file": saved_file,
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
    if result.get("status") == "success":
        exit(0)
    else:
        exit(1)


if __name__ == "__main__":
    main()

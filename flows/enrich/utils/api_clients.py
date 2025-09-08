#!/usr/bin/env python3
"""
API clients for data enrichment services.

This module consolidates API access patterns from the original Fabric notebooks:
- Spotify Web API (from spotify_add_new_artists.py and spotify_add_new_albums.py)
- MusicBrainz API (from mbz_get_missing_artists.py and mbz_parse_area_hierarchy.py)
- OpenWeather Geo API (from geo_add_lat_long.py)
"""

import os
import logging
import base64
import requests
import musicbrainzngs as mbz
from time import sleep
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class SpotifyAPIClient:
    """
    Spotify Web API client with authentication and rate limiting.
    Consolidates patterns from spotify_add_new_artists.py and spotify_add_new_albums.py
    """

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

        if not all([self.client_id, self.client_secret, self.refresh_token]):
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

        response = requests.post(self.TOKEN_URL, headers=headers, data=data)
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data["access_token"]
        expires_in = token_data["expires_in"]

        # Set expiration time (with 5 minute buffer)
        self._token_expires_at = now + timedelta(seconds=expires_in - 300)

        logger.info("Successfully obtained Spotify access token")
        return self._access_token

    def _make_request(
        self, endpoint: str, params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Make authenticated request to Spotify API."""
        token = self._get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{self.BASE_URL}{endpoint}"

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_artist(self, artist_id: str) -> Dict[str, Any]:
        """Get artist information by Spotify ID."""
        return self._make_request(f"/artists/{artist_id}")

    def get_album(self, album_id: str) -> Dict[str, Any]:
        """Get album information by Spotify ID."""
        return self._make_request(f"/albums/{album_id}")

    def get_artists_batch(
        self, artist_ids: List[str], batch_size: int = 50
    ) -> List[Dict[str, Any]]:
        """Get multiple artists in batches with rate limiting."""
        results = []

        for i in range(0, len(artist_ids), batch_size):
            batch = artist_ids[i : i + batch_size]
            ids_param = ",".join(batch)

            try:
                response = self._make_request("/artists", {"ids": ids_param})
                results.extend(response.get("artists", []))

                # Rate limiting
                sleep(1)
                if (i // batch_size + 1) % 10 == 0:
                    logger.info(f"Processed {i + len(batch)} artists")
                    sleep(60)  # Longer pause every 10 batches

            except Exception as e:
                logger.error(f"Error fetching artists batch {i}-{i+len(batch)}: {e}")
                continue

        return results

    def get_albums_batch(
        self, album_ids: List[str], batch_size: int = 20
    ) -> List[Dict[str, Any]]:
        """Get multiple albums in batches with rate limiting."""
        results = []

        for i in range(0, len(album_ids), batch_size):
            batch = album_ids[i : i + batch_size]
            ids_param = ",".join(batch)

            try:
                response = self._make_request("/albums", {"ids": ids_param})
                results.extend(response.get("albums", []))

                # Rate limiting
                sleep(1)
                if (i // batch_size + 1) % 5 == 0:
                    logger.info(f"Processed {i + len(batch)} albums")
                    sleep(60)  # Longer pause every 5 batches

            except Exception as e:
                logger.error(f"Error fetching albums batch {i}-{i+len(batch)}: {e}")
                continue

        return results


class MusicBrainzClient:
    """
    MusicBrainz API client with rate limiting and caching.
    Consolidates patterns from mbz_get_missing_artists.py and mbz_parse_area_hierarchy.py
    """

    def __init__(
        self, user_agent: str = "fffv_tracks_history/0.1", cache_dir: str = None
    ):
        mbz.set_useragent(user_agent, "0.1")
        self.cache_dir = Path(cache_dir) if cache_dir else Path("data/cache/mbz")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_artist_by_isrc(self, isrc: str) -> Optional[str]:
        """Get artist MBID by ISRC code."""
        try:
            sleep(1)  # Rate limiting
            recording = mbz.get_recordings_by_isrc(isrc, includes=["artists"])
            return recording["isrc"]["recording-list"][0]["artist-credit"][0]["artist"][
                "id"
            ]
        except Exception as e:
            logger.warning(f"Could not find artist for ISRC {isrc}: {e}")
            return None

    def get_artist_by_id(
        self, artist_mbid: str, includes: List[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get artist information by MusicBrainz ID."""
        includes = includes or ["tags", "release-groups", "aliases"]

        try:
            sleep(1)  # Rate limiting
            result = mbz.get_artist_by_id(artist_mbid, includes=includes)
            return result["artist"]
        except Exception as e:
            logger.warning(f"Could not fetch artist {artist_mbid}: {e}")
            return None

    def get_area_by_id(
        self, area_id: str, includes: List[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get area information by MusicBrainz ID."""
        includes = includes or ["area-rels"]

        try:
            sleep(0.5)  # Rate limiting
            result = mbz.get_area_by_id(area_id, includes=includes)
            return result["area"]
        except Exception as e:
            logger.warning(f"Could not fetch area {area_id}: {e}")
            return None

    def get_area_hierarchy(self, area_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Get area and all its parent areas in a flat structure.
        Based on get_area_with_parents from mbz_parse_area_hierarchy.py
        """
        areas = {}
        visited = set()

        def fetch_parents(id: str):
            if id in visited:
                return
            visited.add(id)

            area_data = self.get_area_by_id(id)
            if not area_data:
                return

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

        fetch_parents(area_id)
        return areas


class OpenWeatherGeoClient:
    """
    OpenWeather Geocoding API client.
    Based on patterns from geo_add_lat_long.py
    """

    BASE_URL = "http://api.openweathermap.org/geo/1.0/direct"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("OPENWEATHER_API_KEY")
        if not self.api_key:
            raise ValueError("OpenWeather API key not found")

    def get_coordinates(self, query: str) -> Optional[Dict[str, float]]:
        """Get latitude and longitude for a location query."""
        params = {
            "q": query,
            "limit": 1,
            "appid": self.api_key,
        }

        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()

            content = response.json()

            # Check if this is an error response
            if isinstance(content, dict) and "cod" in content and content["cod"] != 200:
                logger.warning(
                    f"OpenWeather API error for '{query}': {content.get('message', 'Unknown error')}"
                )
                return None

            if content and isinstance(content, list) and len(content) > 0:
                first = content[0]
                if isinstance(first, dict) and "lat" in first and "lon" in first:
                    return {"lat": first["lat"], "long": first["lon"]}
                else:
                    logger.warning(f"Unexpected response format for '{query}': {first}")
            else:
                logger.warning(f"No results found for '{query}'")

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error for '{query}': {e}")
        except Exception as e:
            logger.warning(f"Could not get coordinates for '{query}': {e}")

        return None

    def get_coordinates_batch(self, queries: List[str]) -> Dict[str, Dict[str, float]]:
        """Get coordinates for multiple location queries with rate limiting."""
        results = {}

        for i, query in enumerate(queries):
            if not query:
                continue

            try:
                coords = self.get_coordinates(query)
                if coords:
                    results[query] = coords
            except Exception as e:
                logger.warning(f"Failed to get coordinates for '{query}': {e}")
                continue

            # Rate limiting - OpenWeather allows 60 calls per minute
            sleep(1.1)

            if (i + 1) % 50 == 0:
                logger.info(f"Processed {i + 1} location queries")

        return results

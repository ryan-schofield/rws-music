#!/usr/bin/env python3
"""
CLI commands for granular Spotify album enrichment tasks.

Similar to artist enrichment but for albums.
"""

import sys
import argparse
import json
from pathlib import Path
from typing import Dict, Any, List

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.base import CLICommand
from flows.enrich.utils.duckdb_queries import DuckDBQueryEngine
from flows.enrich.utils.data_writer import ParquetDataWriter
from flows.enrich.utils.api_clients import SpotifyAPIClient
from dotenv import load_dotenv
import polars as pl

load_dotenv()


class IdentifyMissingAlbumsCLI(CLICommand):
    """Identify albums that need Spotify enrichment."""

    def __init__(self):
        super().__init__(
            name="identify_missing_albums",
            timeout=60,
            retries=2,
        )
        self.query_engine = DuckDBQueryEngine()

    def execute(self, limit: int = None, batch_size: int = 20, **kwargs) -> Dict[str, Any]:
        """
        Identify missing albums and return batching information.

        Args:
            limit: Maximum total albums to process
            batch_size: Size of each batch (smaller than artists due to API limits)

        Returns:
            Result with missing album count and batch configuration
        """
        try:
            self.logger.info("Identifying missing Spotify albums")

            # Get count of missing albums
            missing_count = self.query_engine.get_missing_count("albums")

            if missing_count == 0:
                return self.no_updates_result("No missing albums found")

            # Apply limit if specified
            total_to_process = min(missing_count, limit) if limit else missing_count

            # Calculate number of batches
            num_batches = (total_to_process + batch_size - 1) // batch_size

            self.logger.info(
                f"Found {missing_count} missing albums, will process {total_to_process} in {num_batches} batches"
            )

            return self.success_result(
                message=f"Identified {total_to_process} albums for enrichment",
                data={
                    "total_missing": missing_count,
                    "to_process": total_to_process,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "batches": [
                        {"batch_index": i, "offset": i * batch_size, "size": batch_size}
                        for i in range(num_batches)
                    ],
                },
            )

        except Exception as e:
            self.logger.error(f"Error identifying missing albums: {str(e)}")
            return self.error_result(
                message="Failed to identify missing albums",
                errors=[str(e)],
            )


class FetchAlbumBatchCLI(CLICommand):
    """Fetch a batch of album data from Spotify API."""

    def __init__(self):
        super().__init__(
            name="fetch_album_batch",
            timeout=300,
            retries=3,
        )
        self.query_engine = DuckDBQueryEngine()
        self.spotify_client = SpotifyAPIClient()

    def execute(
        self, batch_index: int = 0, batch_size: int = 20, offset: int = None, **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch album data for a specific batch.

        Args:
            batch_index: Index of the batch
            batch_size: Number of albums to fetch (max 20 for Spotify API)
            offset: Explicit offset

        Returns:
            Result with fetched album data
        """
        try:
            # Calculate offset if not provided
            if offset is None:
                offset = batch_index * batch_size

            self.logger.info(
                f"Fetching album batch {batch_index} (offset={offset}, size={batch_size})"
            )

            # Get missing albums using DuckDB
            missing_albums_df = self.query_engine.get_missing_spotify_albums(limit=batch_size)

            if missing_albums_df.is_empty():
                return self.no_updates_result(f"No albums found at offset {offset}")

            album_ids = missing_albums_df["album_id"].to_list()[offset : offset + batch_size]

            if not album_ids:
                return self.no_updates_result(f"No albums in range")

            self.logger.info(f"Fetching data for {len(album_ids)} albums from Spotify API")

            # Fetch from Spotify API (max 20 per request)
            album_data = self.spotify_client.get_albums_batch(album_ids, batch_size=20)

            if not album_data:
                return self.error_result(
                    message="No data retrieved from Spotify API",
                    errors=["Empty response from Spotify"],
                )

            self.logger.info(f"Successfully fetched {len(album_data)} albums")

            return self.success_result(
                message=f"Fetched {len(album_data)} albums from Spotify API",
                data={
                    "batch_index": batch_index,
                    "offset": offset,
                    "albums_fetched": len(album_data),
                    "album_data": album_data,
                },
            )

        except Exception as e:
            self.logger.error(f"Error fetching album batch: {str(e)}")
            return self.error_result(
                message=f"Failed to fetch album batch {batch_index}",
                errors=[str(e)],
            )


class WriteAlbumDataCLI(CLICommand):
    """Write album data to parquet files."""

    def __init__(self):
        super().__init__(
            name="write_album_data",
            timeout=120,
            retries=3,
        )
        self.data_writer = ParquetDataWriter()

    def execute(self, album_data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Write album data to spotify_albums parquet table.

        Args:
            album_data: List of album dictionaries from Spotify API

        Returns:
            Result with write statistics
        """
        try:
            if not album_data:
                return self.no_updates_result("No album data to write")

            self.logger.info(f"Writing {len(album_data)} albums to parquet")

            # Process album data into structured format
            processed_albums = []
            for album in album_data:
                from datetime import datetime, timezone

                # Extract primary artist information
                artists = album.get("artists") or []
                primary_artist = artists[0] if artists else {}

                processed_album = {
                    "album_type": album.get("album_type"),
                    "artist_id": primary_artist.get("id"),
                    "artist_name": primary_artist.get("name"),
                    "artist_type": primary_artist.get("type"),
                    "genres": album.get("genres") or [],
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

            # Write to parquet with merge mode
            write_result = self.data_writer.write_table(
                album_df, "spotify_albums", mode="merge"
            )

            self.logger.info(f"Successfully wrote {len(processed_albums)} albums")

            return self.success_result(
                message=f"Wrote {len(processed_albums)} albums to parquet",
                data={
                    "albums_written": len(processed_albums),
                    "write_result": write_result,
                },
            )

        except Exception as e:
            self.logger.error(f"Error writing album data: {str(e)}")
            return self.error_result(
                message="Failed to write album data",
                errors=[str(e)],
            )


class ExtractAlbumGenresCLI(CLICommand):
    """Extract and write album genre data."""

    def __init__(self):
        super().__init__(
            name="extract_album_genres",
            timeout=120,
            retries=3,
        )
        self.data_writer = ParquetDataWriter()

    def execute(self, album_data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Extract genre data from album data and write to spotify_album_genre table.

        Args:
            album_data: List of album dictionaries from Spotify API

        Returns:
            Result with genre extraction statistics
        """
        try:
            if not album_data:
                return self.no_updates_result("No album data to extract genres from")

            self.logger.info(f"Extracting genres from {len(album_data)} albums")

            # Process albums and extract genres
            genre_records = []
            for album in album_data:
                album_id = album.get("id")
                album_name = album.get("name")
                genres = album.get("genres") or []

                for genre in genres:
                    if genre:
                        genre_records.append(
                            {
                                "album_id": album_id,
                                "album_name": album_name,
                                "genre": genre,
                            }
                        )

            if not genre_records:
                return self.no_updates_result("No valid genres found in album data")

            # Create DataFrame
            genre_df = pl.DataFrame(genre_records)

            # Write to parquet with merge mode
            genre_result = self.data_writer.write_table(
                genre_df, "spotify_album_genre", mode="merge"
            )

            self.logger.info(f"Successfully extracted {len(genre_records)} genre records")

            return self.success_result(
                message=f"Extracted {len(genre_records)} genre records",
                data={
                    "genres_extracted": len(genre_records),
                    "albums_processed": len(album_data),
                    "genre_result": genre_result,
                },
            )

        except Exception as e:
            self.logger.error(f"Error extracting album genres: {str(e)}")
            return self.error_result(
                message="Failed to extract album genres",
                errors=[str(e)],
            )


# Main entry points
def identify_missing_albums_main():
    parser = argparse.ArgumentParser(description="Identify missing Spotify albums")
    parser.add_argument("--limit", type=int, default=None, help="Maximum albums to process")
    parser.add_argument("--batch-size", type=int, default=20, help="Batch size")
    args = parser.parse_args()

    cli = IdentifyMissingAlbumsCLI()
    exit_code = cli.run(limit=args.limit, batch_size=args.batch_size)
    sys.exit(exit_code)


def fetch_album_batch_main():
    parser = argparse.ArgumentParser(description="Fetch album batch from Spotify")
    parser.add_argument("--batch-index", type=int, default=0, help="Batch index")
    parser.add_argument("--batch-size", type=int, default=20, help="Batch size")
    parser.add_argument("--offset", type=int, default=None, help="Explicit offset")
    args = parser.parse_args()

    cli = FetchAlbumBatchCLI()
    exit_code = cli.run(
        batch_index=args.batch_index,
        batch_size=args.batch_size,
        offset=args.offset,
    )
    sys.exit(exit_code)


def write_album_data_main():
    parser = argparse.ArgumentParser(description="Write album data to parquet")
    parser.add_argument("--data-file", required=True, help="JSON file with album data")
    args = parser.parse_args()

    with open(args.data_file, "r") as f:
        album_data = json.load(f)

    cli = WriteAlbumDataCLI()
    exit_code = cli.run(album_data=album_data)
    sys.exit(exit_code)


def extract_album_genres_main():
    parser = argparse.ArgumentParser(description="Extract album genre data")
    parser.add_argument("--data-file", required=True, help="JSON file with album data")
    args = parser.parse_args()

    with open(args.data_file, "r") as f:
        album_data = json.load(f)

    cli = ExtractAlbumGenresCLI()
    exit_code = cli.run(album_data=album_data)
    sys.exit(exit_code)

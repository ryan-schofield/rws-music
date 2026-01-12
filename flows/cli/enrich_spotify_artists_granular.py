#!/usr/bin/env python3
"""
CLI commands for granular Spotify artist enrichment tasks.

These commands break down the enrichment process into distinct steps:
1. Identify missing artists
2. Fetch artist batch from Spotify API
3. Write artist data to parquet
4. Extract and write genre data

This design is optimized for n8n workflows with better memory management.
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


class IdentifyMissingArtistsCLI(CLICommand):
    """Identify artists that need Spotify enrichment."""

    def __init__(self):
        super().__init__(
            name="identify_missing_artists",
            timeout=60,
            retries=2,
        )
        self.query_engine = DuckDBQueryEngine()

    def execute(
        self, limit: int = None, batch_size: int = 50, **kwargs
    ) -> Dict[str, Any]:
        """
        Identify missing artists and return batching information.

        Args:
            limit: Maximum total artists to process
            batch_size: Size of each batch for downstream processing

        Returns:
            Result with missing artist count and batch configuration
        """
        try:
            self.logger.info("Identifying missing Spotify artists")

            # Get count of missing artists using efficient DuckDB query
            missing_count = self.query_engine.get_missing_count("artists")

            if missing_count == 0:
                return self.no_updates_result("No missing artists found")

            # Apply limit if specified
            total_to_process = min(missing_count, limit) if limit else missing_count

            # Calculate number of batches
            num_batches = (total_to_process + batch_size - 1) // batch_size

            self.logger.info(
                f"Found {missing_count} missing artists, will process {total_to_process} in {num_batches} batches"
            )

            return self.success_result(
                message=f"Identified {total_to_process} artists for enrichment",
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
            self.logger.error(f"Error identifying missing artists: {str(e)}")
            return self.error_result(
                message="Failed to identify missing artists",
                errors=[str(e)],
            )


class FetchArtistBatchCLI(CLICommand):
    """Fetch a batch of artist data from Spotify API."""

    def __init__(self):
        super().__init__(
            name="fetch_artist_batch",
            timeout=300,
            retries=3,
        )
        self.query_engine = DuckDBQueryEngine()
        self.spotify_client = SpotifyAPIClient()

    def execute(
        self, batch_index: int = 0, batch_size: int = 50, offset: int = None, **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch artist data for a specific batch.

        Args:
            batch_index: Index of the batch (used to calculate offset if not provided)
            batch_size: Number of artists to fetch
            offset: Explicit offset (overrides batch_index calculation)

        Returns:
            Result with fetched artist data
        """
        try:
            # Calculate offset if not provided
            if offset is None:
                offset = batch_index * batch_size

            self.logger.info(
                f"Fetching artist batch {batch_index} (offset={offset}, size={batch_size})"
            )

            # Get missing artists for this batch using DuckDB
            missing_artists_df = self.query_engine.get_artists_batch(
                batch_size=batch_size, offset=offset
            )

            if missing_artists_df.is_empty():
                return self.no_updates_result(f"No artists found at offset {offset}")

            artist_ids = missing_artists_df["artist_id"].to_list()
            self.logger.info(
                f"Fetching data for {len(artist_ids)} artists from Spotify API"
            )

            # Fetch from Spotify API
            artist_data = self.spotify_client.get_artists_batch(
                artist_ids, batch_size=50
            )

            if not artist_data:
                return self.error_result(
                    message="No data retrieved from Spotify API",
                    errors=["Empty response from Spotify"],
                )

            self.logger.info(f"Successfully fetched {len(artist_data)} artists")

            return self.success_result(
                message=f"Fetched {len(artist_data)} artists from Spotify API",
                data={
                    "batch_index": batch_index,
                    "offset": offset,
                    "artists_fetched": len(artist_data),
                    "artist_data": artist_data,
                },
            )

        except Exception as e:
            self.logger.error(f"Error fetching artist batch: {str(e)}")
            return self.error_result(
                message=f"Failed to fetch artist batch {batch_index}",
                errors=[str(e)],
            )


class WriteArtistDataCLI(CLICommand):
    """Write artist data to parquet files."""

    def __init__(self):
        super().__init__(
            name="write_artist_data",
            timeout=120,
            retries=3,
        )
        self.data_writer = ParquetDataWriter()

    def execute(self, artist_data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Write artist data to spotify_artists parquet table.

        Args:
            artist_data: List of artist dictionaries from Spotify API

        Returns:
            Result with write statistics
        """
        try:
            if not artist_data:
                return self.no_updates_result("No artist data to write")

            self.logger.info(f"Writing {len(artist_data)} artists to parquet")

            # Create DataFrame from API response
            artist_df = pl.DataFrame(artist_data)

            # Add artist_mbid column (initially null)
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

            # Select only the expected columns
            expected_columns = [
                "artist_id",
                "artist_name",
                "artist_mbid",
                "artist_popularity",
            ]

            # Only select columns that exist
            available_columns = [
                col for col in expected_columns if col in artist_df.columns
            ]
            artist_df = artist_df.select(available_columns)

            # Ensure data types match expected schema
            if "artist_popularity" in artist_df.columns:
                artist_df = artist_df.with_columns(
                    pl.col("artist_popularity").cast(pl.Float32)
                )

            # Write to parquet with merge mode
            write_result = self.data_writer.write_table(
                artist_df, "spotify_artists", mode="merge"
            )

            self.logger.info(f"Successfully wrote {len(artist_data)} artists")

            return self.success_result(
                message=f"Wrote {len(artist_data)} artists to parquet",
                data={
                    "artists_written": len(artist_data),
                    "write_result": write_result,
                },
            )

        except Exception as e:
            self.logger.error(f"Error writing artist data: {str(e)}")
            return self.error_result(
                message="Failed to write artist data",
                errors=[str(e)],
            )


class ExtractArtistGenresCLI(CLICommand):
    """Extract and write artist genre data."""

    def __init__(self):
        super().__init__(
            name="extract_artist_genres",
            timeout=120,
            retries=3,
        )
        self.data_writer = ParquetDataWriter()

    def execute(self, artist_data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Extract genre data from artist data and write to spotify_artist_genre table.

        Args:
            artist_data: List of artist dictionaries from Spotify API

        Returns:
            Result with genre extraction statistics
        """
        try:
            if not artist_data:
                return self.no_updates_result("No artist data to extract genres from")

            self.logger.info(f"Extracting genres from {len(artist_data)} artists")

            # Create DataFrame from API response
            artist_df = pl.DataFrame(artist_data)

            # Check if genres column exists
            if "genres" not in artist_df.columns:
                return self.no_updates_result("No genre data in artist response")

            # Rename id column if needed
            if "id" in artist_df.columns:
                artist_df = artist_df.rename({"id": "artist_id"})
            if "name" in artist_df.columns:
                artist_df = artist_df.rename({"name": "artist_name"})

            # Select relevant columns and explode genres
            genre_base = artist_df.select(["artist_id", "artist_name", "genres"])

            # Explode the genres array
            genre_exploded = genre_base.explode("genres")

            # Rename genre column
            if "genres" in genre_exploded.columns:
                genre_exploded = genre_exploded.rename({"genres": "genre"})

            # Filter out null genres
            genre_exploded = genre_exploded.filter(
                pl.col("genre").is_not_null() & (pl.col("genre") != "")
            )

            if genre_exploded.is_empty():
                return self.no_updates_result("No valid genres found in artist data")

            # Write to parquet with merge mode
            genre_result = self.data_writer.write_table(
                genre_exploded, "spotify_artist_genre", mode="merge"
            )

            self.logger.info(
                f"Successfully extracted {len(genre_exploded)} genre records"
            )

            return self.success_result(
                message=f"Extracted {len(genre_exploded)} genre records",
                data={
                    "genres_extracted": len(genre_exploded),
                    "artists_processed": len(artist_data),
                    "genre_result": genre_result,
                },
            )

        except Exception as e:
            self.logger.error(f"Error extracting artist genres: {str(e)}")
            return self.error_result(
                message="Failed to extract artist genres",
                errors=[str(e)],
            )


# Main entry points for each command
def identify_missing_artists_main():
    parser = argparse.ArgumentParser(description="Identify missing Spotify artists")
    parser.add_argument(
        "--limit", type=int, default=None, help="Maximum artists to process"
    )
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size")
    args = parser.parse_args()

    cli = IdentifyMissingArtistsCLI()
    exit_code = cli.run(limit=args.limit, batch_size=args.batch_size)
    sys.exit(exit_code)


def fetch_artist_batch_main():
    parser = argparse.ArgumentParser(description="Fetch artist batch from Spotify")
    parser.add_argument("--batch-index", type=int, default=0, help="Batch index")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size")
    parser.add_argument("--offset", type=int, default=None, help="Explicit offset")
    args = parser.parse_args()

    cli = FetchArtistBatchCLI()
    exit_code = cli.run(
        batch_index=args.batch_index,
        batch_size=args.batch_size,
        offset=args.offset,
    )
    sys.exit(exit_code)


def write_artist_data_main():
    parser = argparse.ArgumentParser(description="Write artist data to parquet")
    parser.add_argument("--data-file", required=True, help="JSON file with artist data")
    args = parser.parse_args()

    with open(args.data_file, "r") as f:
        artist_data = json.load(f)

    cli = WriteArtistDataCLI()
    exit_code = cli.run(artist_data=artist_data)
    sys.exit(exit_code)


def extract_artist_genres_main():
    parser = argparse.ArgumentParser(description="Extract artist genre data")
    parser.add_argument("--data-file", required=True, help="JSON file with artist data")
    args = parser.parse_args()

    with open(args.data_file, "r") as f:
        artist_data = json.load(f)

    cli = ExtractArtistGenresCLI()
    exit_code = cli.run(artist_data=artist_data)
    sys.exit(exit_code)

#!/usr/bin/env python3
"""
Merge Spotify recently played tracks data using Polars.

This script replaces the Spark notebook nb_merge_spotify_recently_played.Notebook
and processes recently played tracks from JSON files, merging them into the
DuckDB database.
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Any
import json
from datetime import datetime, timezone

import polars as pl
import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SpotifyDataMerger:
    """Handles merging of Spotify recently played data."""

    def __init__(self, duckdb_path: str = None):
        self.duckdb_path = duckdb_path or os.getenv('DUCKDB_PATH', './data/music_tracker.duckdb')
        self.data_dir = Path('./data')
        self.raw_data_dir = self.data_dir / 'raw' / 'recently_played'

        # Ensure directories exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize DuckDB connection
        self.conn = duckdb.connect(self.duckdb_path)

    def find_source_files(self) -> List[Path]:
        """Find JSON files containing recently played data."""
        pattern = self.raw_data_dir / "detail" / "*.json"
        files = list(self.raw_data_dir.glob("detail/*.json"))
        logger.info(f"Found {len(files)} source files")
        return files

    def read_json_files(self, files: List[Path]) -> pl.DataFrame:
        """Read and combine JSON files into a Polars DataFrame."""
        if not files:
            return pl.DataFrame()

        dfs = []
        for file_path in files:
            try:
                # Read JSON file
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # Convert to Polars DataFrame
                df = pl.DataFrame(data)
                dfs.append(df)
                logger.debug(f"Processed {file_path.name}")

            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                continue

        if not dfs:
            return pl.DataFrame()

        # Combine all DataFrames
        combined_df = pl.concat(dfs, how='vertical')

        # Rename columns to match target schema
        column_mapping = {
            'request_after': 'request_cursor',
            'uri': 'track_uri'
        }

        for old_col, new_col in column_mapping.items():
            if old_col in combined_df.columns:
                combined_df = combined_df.rename({old_col: new_col})

        # Convert timestamp
        if 'played_at' in combined_df.columns:
            combined_df = combined_df.with_columns(
                pl.col('played_at').str.strptime(pl.Datetime, '%Y-%m-%dT%H:%M:%S.%fZ')
            )

        # Remove null user_id records
        combined_df = combined_df.filter(pl.col('user_id').is_not_null())

        logger.info(f"Combined {len(combined_df)} records from {len(dfs)} files")
        return combined_df

    def deduplicate_records(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remove duplicate records based on played_at, track_id, user_id."""
        if df.is_empty():
            return df

        # Sort by request_cursor descending to keep latest
        df = df.sort(['played_at', 'track_id', 'user_id', 'request_cursor'],
                    descending=[False, False, False, True])

        # Keep only the first occurrence of each combination
        df = df.unique(subset=['played_at', 'track_id', 'user_id'], keep='first')

        logger.info(f"After deduplication: {len(df)} records")
        return df

    def create_temp_tables(self, df: pl.DataFrame) -> None:
        """Create temporary tables in DuckDB for merging."""
        if df.is_empty():
            return

        # Create temp table for source data
        self.conn.execute("DROP TABLE IF EXISTS spotify_last_played_temp")
        self.conn.execute("""
            CREATE TABLE spotify_last_played_temp AS
            SELECT * FROM df
        """)

        # Create temp table for merge target
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS spotify_recently_played (
                user_id VARCHAR,
                track_id VARCHAR,
                track_uri VARCHAR,
                track_isrc VARCHAR,
                track_name VARCHAR,
                album_id VARCHAR,
                album_uri VARCHAR,
                album VARCHAR,
                artist_id VARCHAR,
                artist_mbid VARCHAR,
                artist VARCHAR,
                duration_ms BIGINT,
                played_at TIMESTAMP,
                popularity DOUBLE,
                request_cursor VARCHAR,
                play_source VARCHAR DEFAULT 'spotify'
            )
        """)

    def merge_to_recently_played(self) -> Dict[str, Any]:
        """Merge data into spotify_recently_played table."""
        try:
            # Check if temp table exists
            result = self.conn.execute("""
                SELECT COUNT(*) as count FROM information_schema.tables
                WHERE table_name = 'spotify_last_played_temp'
            """).fetchone()

            if result[0] == 0:
                return {"status": "no_data", "message": "No data to merge"}

            # Perform merge operation
            merge_sql = """
            INSERT INTO spotify_recently_played
            SELECT
                user_id,
                track_id,
                track_uri,
                track_isrc,
                track_name,
                album_id,
                album_uri,
                album,
                artist_id,
                CAST(NULL AS VARCHAR) AS artist_mbid,
                artist,
                duration_ms,
                played_at,
                popularity,
                request_cursor,
                'spotify' AS play_source
            FROM spotify_last_played_temp
            WHERE NOT EXISTS (
                SELECT 1 FROM spotify_recently_played srp
                WHERE srp.played_at = spotify_last_played_temp.played_at
                AND srp.track_id = spotify_last_played_temp.track_id
                AND srp.user_id = spotify_last_played_temp.user_id
            )
            """

            result = self.conn.execute(merge_sql)
            inserted_count = result.fetchall()[0][0] if result.description else 0

            return {
                "status": "success",
                "inserted_records": inserted_count,
                "table": "spotify_recently_played"
            }

        except Exception as e:
            logger.error(f"Error merging to recently played: {e}")
            return {"status": "error", "message": str(e)}

    def merge_to_tracks_played(self) -> Dict[str, Any]:
        """Merge data into tracks_played table."""
        try:
            # Create tracks_played table if it doesn't exist
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tracks_played (
                    user_id VARCHAR,
                    track_id VARCHAR,
                    track_uri VARCHAR,
                    track_isrc VARCHAR,
                    track_name VARCHAR,
                    album_id VARCHAR,
                    album_uri VARCHAR,
                    album VARCHAR,
                    artist_id VARCHAR,
                    artist_mbid VARCHAR,
                    artist VARCHAR,
                    duration_ms BIGINT,
                    played_at TIMESTAMP,
                    popularity DOUBLE,
                    request_cursor VARCHAR,
                    play_source VARCHAR DEFAULT 'spotify'
                )
            """)

            # Insert all records (tracks_played is a history table)
            insert_sql = """
            INSERT INTO tracks_played
            SELECT
                user_id,
                track_id,
                track_uri,
                track_isrc,
                track_name,
                album_id,
                album_uri,
                album,
                artist_id,
                CAST(NULL AS VARCHAR) AS artist_mbid,
                artist,
                duration_ms,
                played_at,
                popularity,
                request_cursor,
                'spotify' AS play_source
            FROM spotify_last_played_temp
            WHERE NOT EXISTS (
                SELECT 1 FROM tracks_played tp
                WHERE tp.played_at = spotify_last_played_temp.played_at
                AND tp.track_id = spotify_last_played_temp.track_id
                AND tp.user_id = spotify_last_played_temp.user_id
            )
            """

            result = self.conn.execute(insert_sql)
            inserted_count = result.fetchall()[0][0] if result.description else 0

            return {
                "status": "success",
                "inserted_records": inserted_count,
                "table": "tracks_played"
            }

        except Exception as e:
            logger.error(f"Error merging to tracks played: {e}")
            return {"status": "error", "message": str(e)}

    def update_artist_mbids(self) -> Dict[str, Any]:
        """Update artist MBIDs from MusicBrainz data."""
        try:
            update_sql = """
            UPDATE spotify_artists
            SET artist_mbid = mbz.artist_mbid
            FROM mbz_artist_info mbz
            WHERE spotify_artists.artist_id = mbz.spotify_id
            AND spotify_artists.artist_mbid IS NULL
            """

            result = self.conn.execute(update_sql)
            updated_count = result.fetchall()[0][0] if result.description else 0

            return {
                "status": "success",
                "updated_records": updated_count,
                "table": "spotify_artists"
            }

        except Exception as e:
            logger.error(f"Error updating artist MBIDs: {e}")
            return {"status": "error", "message": str(e)}

    def optimize_tables(self) -> None:
        """Optimize DuckDB tables."""
        try:
            self.conn.execute("OPTIMIZE spotify_recently_played")
            self.conn.execute("OPTIMIZE tracks_played")
            self.conn.execute("OPTIMIZE spotify_artists")
            logger.info("Tables optimized")
        except Exception as e:
            logger.warning(f"Error optimizing tables: {e}")

    def cleanup_temp_tables(self) -> None:
        """Clean up temporary tables."""
        try:
            self.conn.execute("DROP TABLE IF EXISTS spotify_last_played_temp")
            logger.info("Temporary tables cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up temp tables: {e}")

    def run_merge_process(self) -> Dict[str, Any]:
        """Run the complete merge process."""
        start_time = datetime.now(timezone.utc)
        logger.info("Starting Spotify data merge process")

        try:
            # Find source files
            source_files = self.find_source_files()
            if not source_files:
                return {"status": "no_files", "message": "No source files found"}

            # Read and process data
            df = self.read_json_files(source_files)
            if df.is_empty():
                return {"status": "no_data", "message": "No data found in files"}

            # Deduplicate
            df = self.deduplicate_records(df)

            # Create temp tables
            self.create_temp_tables(df)

            # Perform merges
            recently_played_result = self.merge_to_recently_played()
            tracks_played_result = self.merge_to_tracks_played()
            artist_update_result = self.update_artist_mbids()

            # Optimize and cleanup
            self.optimize_tables()
            self.cleanup_temp_tables()

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "duration_seconds": duration,
                "spotify_recently_played": recently_played_result,
                "tracks_played": tracks_played_result,
                "spotify_artists_updated": artist_update_result
            }

            logger.info(f"Merge process completed in {duration:.2f} seconds")
            return result

        except Exception as e:
            logger.error(f"Merge process failed: {e}")
            return {"status": "error", "message": str(e)}
        finally:
            self.conn.close()


def main():
    """Main entry point."""
    merger = SpotifyDataMerger()
    result = merger.run_merge_process()

    # Print result for logging/monitoring
    print(json.dumps(result, indent=2, default=str))

    # Exit with appropriate code
    if result.get("status") == "success":
        exit(0)
    else:
        exit(1)


if __name__ == "__main__":
    main()
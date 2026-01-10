"""Database connection and query utilities."""

import logging
import os
from contextlib import contextmanager

import duckdb
import polars as pl
import streamlit as st

logger = logging.getLogger(__name__)

# Get DuckDB path from environment variable or default
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "music_tracker.duckdb"),
)


@contextmanager
def get_duckdb_connection():
    """
    Context manager for DuckDB connections.
    
    Creates a fresh connection for each use and ensures it's properly closed,
    preventing file locks when running with Streamlit.
    
    Usage:
        with get_duckdb_connection() as conn:
            result = conn.execute(query).pl()
    """
    try:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        logger.info(f"Connected to DuckDB: {DUCKDB_PATH}")
        yield conn
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise
    finally:
        try:
            conn.close()
            logger.debug("DuckDB connection closed")
        except Exception as e:
            logger.warning(f"Error closing DuckDB connection: {e}")


def get_last_24h_tracks():
    """
    Fetch all tracks played in the last 24 hours from main_dw.recently_played view.

    Returns:
        DataFrame with columns: user_id, track_id, track_name, artist,
                                       artist_id, album, duration_ms, minutes_played,
                                       played_at, popularity, play_source
        None if query fails
    """
    try:
        with get_duckdb_connection() as conn:
            query = """
                SELECT
                    user_id,
                    track_id,
                    track_name,
                    artist,
                    artist_id,
                    album,
                    duration_ms,
                    minutes_played,
                    played_at,
                    popularity,
                    play_source
                FROM main_dw.recently_played
                WHERE played_at >= NOW() - INTERVAL 24 HOURS
                ORDER BY played_at DESC
            """

            result = conn.execute(query).pl()
            logger.info(f"Fetched {len(result)} tracks from last 24 hours")
            return result

    except Exception as e:
        logger.error(f"Failed to fetch last 24h tracks: {e}")
        return None


def get_artist_aggregates(df):
    """
    Aggregate track data by artist, calculating total minutes and track count.

    Args:
        df: Polars DataFrame from get_last_24h_tracks()

    Returns:
        Polars DataFrame with columns: artist, artist_id, total_minutes, track_count
        Sorted by total_minutes descending
    """
    if df is None or len(df) == 0:
        return pl.DataFrame()

    try:
        result = (
            df.group_by(["artist", "artist_id"])
            .agg(
                pl.col("minutes_played").sum().alias("total_minutes"),
                pl.col("track_id").count().alias("track_count")
            )
            .sort("total_minutes", descending=True)
        )
        return result

    except Exception as e:
        logger.error(f"Failed to aggregate artist data: {e}")
        return pl.DataFrame()


def get_tracks_for_artist(df, artist: str):
    """
    Get all tracks for a specific artist from the last 24h data.

    Args:
        df: Polars DataFrame from get_last_24h_tracks()
        artist: Artist name to filter

    Returns:
        Polars DataFrame with columns: track_name, album, minutes_played,
                                       played_at, popularity
        Sorted by played_at descending
    """
    if df is None or len(df) == 0:
        return pl.DataFrame()

    try:
        result = (
            df.filter(pl.col("artist") == artist)
            .select(["track_name", "album", "minutes_played", "played_at", "popularity"])
            .sort("played_at", descending=True)
        )
        return result

    except Exception as e:
        logger.error(f"Failed to get tracks for artist {artist}: {e}")
        return pl.DataFrame()

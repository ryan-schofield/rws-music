#!/usr/bin/env python3
"""
DuckDB query utilities for efficient parquet data access.

This module provides memory-efficient querying of parquet files using DuckDB,
reducing memory footprint compared to loading full tables into Polars.
"""
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class DuckDBQueryEngine:
    """
    Memory-efficient query engine using DuckDB for parquet files.
    """

    def __init__(self, base_path: str = "data/src"):
        # Use absolute path for task-runner compatibility
        if not base_path.startswith("/"):
            workspace_dir = Path("/home/runner/workspace")
            if not workspace_dir.exists():
                workspace_dir = Path.cwd()
            base_path = str(workspace_dir / base_path)

        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_table_path(self, table_name: str) -> str:
        """Get parquet file pattern for a table."""
        table_path = self.base_path / table_name
        return str(table_path / "*.parquet")

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> pl.DataFrame:
        """
        Execute a DuckDB query and return results as Polars DataFrame.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query results as Polars DataFrame
        """
        try:
            conn = duckdb.connect(":memory:")

            # Register parquet files as views
            for table_name in [
                "tracks_played",
                "spotify_artists",
                "spotify_albums",
                "mbz_artist_info",
                "mbz_area_hierarchy",
                "cities_with_lat_long",
            ]:
                table_path = self._get_table_path(table_name)
                if Path(table_path.replace("*.parquet", "")).exists():
                    conn.execute(
                        f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{table_path}')"
                    )

            # Execute query
            if params:
                result = conn.execute(query, params).pl()
            else:
                result = conn.execute(query).pl()

            conn.close()
            return result

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def get_missing_spotify_artists(self, limit: Optional[int] = None, offset: int = 0) -> pl.DataFrame:
        """
        Find Spotify artists that need enrichment using DuckDB.
        Memory-efficient alternative to loading full tables.
        
        Args:
            limit: Maximum number of artists to return
            offset: Starting offset for pagination
        """
        query = """
        SELECT DISTINCT
            tp.artist_id,
            tp.artist
        FROM tracks_played tp
        LEFT JOIN spotify_artists sa ON tp.artist_id = sa.artist_id
        WHERE sa.artist_id IS NULL
          AND tp.artist_id IS NOT NULL
        ORDER BY tp.artist
        """

        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"

        return self.execute_query(query)

    def get_missing_spotify_albums(self, limit: Optional[int] = None, offset: int = 0) -> pl.DataFrame:
        """
        Find Spotify albums that need enrichment using DuckDB.
        
        Args:
            limit: Maximum number of albums to return
            offset: Starting offset for pagination
        """
        query = """
        SELECT
            tp.album_id,
            COUNT(*) as play_count
        FROM tracks_played tp
        LEFT JOIN spotify_albums sa ON tp.album_id = sa.album_id
        WHERE tp.album_id IS NOT NULL
          AND sa.album_id IS NULL
        GROUP BY tp.album_id
        ORDER BY play_count DESC
        """

        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"

        return self.execute_query(query)

    def get_artists_batch(
        self, batch_size: int = 50, offset: int = 0
    ) -> pl.DataFrame:
        """
        Get a batch of missing artists for processing.

        Args:
            batch_size: Number of artists to return
            offset: Starting offset for pagination

        Returns:
            DataFrame with artist_id and artist columns
        """
        query = f"""
        SELECT DISTINCT
            tp.artist_id,
            tp.artist
        FROM tracks_played tp
        LEFT JOIN spotify_artists sa ON tp.artist_id = sa.artist_id
        WHERE sa.artist_id IS NULL
          AND tp.artist_id IS NOT NULL
        ORDER BY tp.artist
        LIMIT {batch_size} OFFSET {offset}
        """

        return self.execute_query(query)

    def get_missing_mbz_artists(self, limit: Optional[int] = None, offset: int = 0) -> pl.DataFrame:
        """
        Find artists needing MusicBrainz enrichment using DuckDB.
        
        Returns artists with ISRCs that don't have MBZ data yet.
        Filters to last 48 hours of play data.
        
        Args:
            limit: Maximum number of artists to return
            offset: Starting offset for pagination
        """
        query = """
        SELECT DISTINCT
            tp.artist_id,
            tp.artist,
            FIRST(tp.track_isrc) as track_isrc
        FROM tracks_played tp
        LEFT JOIN mbz_artist_info mbz ON tp.artist_id = mbz.spotify_id
        WHERE mbz.spotify_id IS NULL
          AND tp.track_isrc IS NOT NULL
          AND tp.artist_id IS NOT NULL
          AND tp.played_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
        GROUP BY tp.artist_id, tp.artist
        ORDER BY tp.artist
        """
        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"
        return self.execute_query(query)

    def get_mbz_artists_batch(
        self, batch_size: int = 10, offset: int = 0
    ) -> pl.DataFrame:
        """
        Get a batch of missing MBZ artists for processing.

        Args:
            batch_size: Number of artists to return (default 10 for rate limiting)
            offset: Starting offset for pagination
            
        Returns:
            DataFrame with artist_id, artist, and track_isrc columns
        """
        query = f"""
        SELECT DISTINCT
            tp.artist_id,
            tp.artist,
            FIRST(tp.track_isrc) as track_isrc
        FROM tracks_played tp
        LEFT JOIN mbz_artist_info mbz ON tp.artist_id = mbz.spotify_id
        WHERE mbz.spotify_id IS NULL
          AND tp.track_isrc IS NOT NULL
          AND tp.artist_id IS NOT NULL
          AND tp.played_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
        GROUP BY tp.artist_id, tp.artist
        ORDER BY tp.artist
        LIMIT {batch_size} OFFSET {offset}
        """
        return self.execute_query(query)

    def get_cities_needing_coordinates(self, limit: Optional[int] = None, offset: int = 0) -> pl.DataFrame:
        """
        Find cities that need coordinate lookup using DuckDB.
        
        Returns cities with geocoding params that don't have coordinates yet.
        
        Args:
            limit: Maximum number of cities to return
            offset: Starting offset for pagination
        """
        query = """
        SELECT DISTINCT
            ah.params,
            ah.city_name,
            ah.country_code,
            ah.country_name
        FROM mbz_area_hierarchy ah
        LEFT JOIN cities_with_lat_long c ON ah.params = c.params
        WHERE ah.params IS NOT NULL
          AND ah.params != ''
          AND c.params IS NULL
        ORDER BY ah.city_name
        """
        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"
        return self.execute_query(query)

    def get_cities_batch(
        self, batch_size: int = 50, offset: int = 0
    ) -> pl.DataFrame:
        """
        Get a batch of cities needing coordinate lookup.

        Args:
            batch_size: Number of cities to return (default 50)
            offset: Starting offset for pagination
            
        Returns:
            DataFrame with params, city_name, country_code, country_name columns
        """
        query = f"""
        SELECT DISTINCT
            ah.params,
            ah.city_name,
            ah.country_code,
            ah.country_name
        FROM mbz_area_hierarchy ah
        LEFT JOIN cities_with_lat_long c ON ah.params = c.params
        WHERE ah.params IS NOT NULL
          AND ah.params != ''
          AND c.params IS NULL
        ORDER BY ah.city_name
        LIMIT {batch_size} OFFSET {offset}
        """
        return self.execute_query(query)

    def get_missing_count(self, entity_type: str = "artists") -> int:
        """
        Get count of missing entities efficiently.

        Args:
            entity_type: 'artists', 'albums', 'mbz_artists', or 'cities'

        Returns:
            Count of missing entities
        """
        if entity_type == "artists":
            query = """
            SELECT COUNT(DISTINCT tp.artist_id) as count
            FROM tracks_played tp
            LEFT JOIN spotify_artists sa ON tp.artist_id = sa.artist_id
            WHERE sa.artist_id IS NULL
              AND tp.artist_id IS NOT NULL
            """
        elif entity_type == "albums":
            query = """
            SELECT COUNT(DISTINCT tp.album_id) as count
            FROM tracks_played tp
            LEFT JOIN spotify_albums sa ON tp.album_id = sa.album_id
            WHERE tp.album_id IS NOT NULL
              AND sa.album_id IS NULL
            """
        elif entity_type == "mbz_artists":
            query = """
            SELECT COUNT(DISTINCT tp.artist_id) as count
            FROM tracks_played tp
            LEFT JOIN mbz_artist_info mbz ON tp.artist_id = mbz.spotify_id
            WHERE mbz.spotify_id IS NULL
              AND tp.track_isrc IS NOT NULL
              AND tp.artist_id IS NOT NULL
              AND tp.played_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
            """
        elif entity_type == "cities":
            query = """
            SELECT COUNT(DISTINCT ah.params) as count
            FROM mbz_area_hierarchy ah
            LEFT JOIN cities_with_lat_long c ON ah.params = c.params
            WHERE ah.params IS NOT NULL
              AND ah.params != ''
              AND c.params IS NULL
            """
        else:
            raise ValueError(f"Unknown entity_type: {entity_type}")

        result = self.execute_query(query)
        return result.item(0, "count") if not result.is_empty() else 0

    def check_artist_exists(self, artist_ids: List[str]) -> Dict[str, bool]:
        """
        Check which artist IDs already exist in spotify_artists table.

        Args:
            artist_ids: List of artist IDs to check

        Returns:
            Dictionary mapping artist_id to exists boolean
        """
        if not artist_ids:
            return {}

        # Create a temporary table from the list
        ids_df = pl.DataFrame({"artist_id": artist_ids})

        try:
            conn = duckdb.connect(":memory:")

            # Register tables
            table_path = self._get_table_path("spotify_artists")
            if Path(table_path.replace("*.parquet", "")).exists():
                conn.execute(
                    f"CREATE OR REPLACE VIEW spotify_artists AS SELECT * FROM read_parquet('{table_path}')"
                )

            # Register the input list
            conn.register("input_ids", ids_df)

            # Query to check existence
            query = """
            SELECT
                i.artist_id,
                CASE WHEN sa.artist_id IS NOT NULL THEN true ELSE false END as exists
            FROM input_ids i
            LEFT JOIN spotify_artists sa ON i.artist_id = sa.artist_id
            """

            result = conn.execute(query).pl()
            conn.close()

            return dict(zip(result["artist_id"], result["exists"]))

        except Exception as e:
            logger.error(f"Error checking artist existence: {e}")
            return {aid: False for aid in artist_ids}

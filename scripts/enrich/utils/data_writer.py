#!/usr/bin/env python3
"""
Data writer utilities for parquet file operations.

This module replaces Spark DataFrame operations with Polars for writing
enriched data to the dbt/data/src/ parquet files.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import polars as pl

logger = logging.getLogger(__name__)


class ParquetDataWriter:
    """
    Handles writing enriched data to parquet files in dbt/data/src/.
    Replaces Spark saveAsTable operations with optimized parquet writes.
    """
    
    def __init__(self, base_path: str = "dbt/data/src"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    def write_table(self, df: pl.DataFrame, table_name: str, mode: str = "overwrite") -> Dict[str, Any]:
        """
        Write DataFrame to parquet file, replacing Spark saveAsTable operation.
        
        Args:
            df: Polars DataFrame to write
            table_name: Name of the table/directory
            mode: Write mode - 'overwrite', 'append', or 'merge'
        """
        table_path = self.base_path / table_name
        table_path.mkdir(exist_ok=True)
        
        try:
            if mode == "overwrite":
                return self._overwrite_table(df, table_path, table_name)
            elif mode == "append":
                return self._append_table(df, table_path, table_name)
            elif mode == "merge":
                return self._merge_table(df, table_path, table_name)
            else:
                raise ValueError(f"Unsupported write mode: {mode}")
                
        except Exception as e:
            logger.error(f"Error writing table {table_name}: {e}")
            return {"status": "error", "message": str(e)}
    
    def _overwrite_table(self, df: pl.DataFrame, table_path: Path, table_name: str) -> Dict[str, Any]:
        """Overwrite existing parquet files."""
        # Remove existing parquet files
        for existing_file in table_path.glob("*.parquet"):
            existing_file.unlink()
        
        # Write new parquet file
        output_file = table_path / f"{table_name}.parquet"
        df.write_parquet(
            output_file,
            compression="snappy",
            row_group_size=10000,
            use_pyarrow=True
        )
        
        logger.info(f"Overwrote {table_name} with {len(df)} records")
        return {
            "status": "success",
            "operation": "overwrite",
            "records_written": len(df),
            "file_path": str(output_file)
        }
    
    def _append_table(self, df: pl.DataFrame, table_path: Path, table_name: str) -> Dict[str, Any]:
        """Append to existing parquet files."""
        existing_files = list(table_path.glob("*.parquet"))
        
        if existing_files:
            # Read existing data
            existing_df = pl.read_parquet(existing_files)
            # Combine with new data
            combined_df = pl.concat([existing_df, df], how="vertical")
        else:
            combined_df = df
        
        # Remove old files and write combined data
        for existing_file in existing_files:
            existing_file.unlink()
        
        output_file = table_path / f"{table_name}.parquet"
        combined_df.write_parquet(
            output_file,
            compression="snappy",
            row_group_size=10000,
            use_pyarrow=True
        )
        
        logger.info(f"Appended {len(df)} records to {table_name}")
        return {
            "status": "success",
            "operation": "append",
            "records_written": len(df),
            "total_records": len(combined_df),
            "file_path": str(output_file)
        }
    
    def _merge_table(self, df: pl.DataFrame, table_path: Path, table_name: str, 
                    merge_keys: List[str] = None) -> Dict[str, Any]:
        """
        Merge new data with existing data (upsert operation).
        Replaces Spark MERGE INTO operations.
        """
        existing_files = list(table_path.glob("*.parquet"))
        
        if not existing_files:
            # No existing data, just write new data
            return self._overwrite_table(df, table_path, table_name)
        
        # Default merge keys if not provided
        if merge_keys is None:
            merge_keys = self._infer_merge_keys(table_name)
        
        # Read existing data
        existing_df = pl.read_parquet(existing_files)
        
        # Perform merge/upsert
        if merge_keys:
            # Remove existing records that match on merge keys
            merged_df = existing_df.join(
                df.select(merge_keys), 
                on=merge_keys, 
                how="anti"  # Keep records that don't match
            )
            # Add new/updated records
            merged_df = pl.concat([merged_df, df], how="vertical")
        else:
            # If no merge keys, just append
            merged_df = pl.concat([existing_df, df], how="vertical")
            merged_df = merged_df.unique()
        
        # Write merged data
        for existing_file in existing_files:
            existing_file.unlink()
        
        output_file = table_path / f"{table_name}.parquet"
        merged_df.write_parquet(
            output_file,
            compression="snappy",
            row_group_size=10000,
            use_pyarrow=True
        )
        
        records_updated = len(df)
        records_total = len(merged_df)
        
        logger.info(f"Merged {records_updated} records into {table_name} (total: {records_total})")
        return {
            "status": "success",
            "operation": "merge",
            "records_updated": records_updated,
            "total_records": records_total,
            "file_path": str(output_file)
        }
    
    def _infer_merge_keys(self, table_name: str) -> List[str]:
        """Infer merge keys based on table name."""
        merge_key_mapping = {
            "spotify_artists": ["artist_id"],
            "spotify_albums": ["album_id"],
            "spotify_artist_genre": ["artist_id", "genre"],
            "mbz_artist_info": ["id"],
            "mbz_area_hierarchy": ["area_id"],
            "cities_with_lat_long": ["params"],
            "tracks_played": ["played_at", "track_id", "user_id"]
        }
        return merge_key_mapping.get(table_name, [])
    
    def read_table(self, table_name: str) -> Optional[pl.DataFrame]:
        """Read existing parquet table."""
        table_path = self.base_path / table_name
        parquet_files = list(table_path.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning(f"No parquet files found for table {table_name}")
            return None
        
        try:
            return pl.read_parquet(parquet_files)
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {e}")
            return None
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        table_path = self.base_path / table_name
        return table_path.exists() and any(table_path.glob("*.parquet"))
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        df = self.read_table(table_name)
        if df is None:
            return {"exists": False}
        
        table_path = self.base_path / table_name
        parquet_files = list(table_path.glob("*.parquet"))
        
        return {
            "exists": True,
            "record_count": len(df),
            "columns": df.columns,
            "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "file_count": len(parquet_files),
            "file_sizes": [f.stat().st_size for f in parquet_files]
        }
    
    def cleanup_old_files(self, table_name: str, keep_latest: int = 1):
        """Clean up old parquet files, keeping only the latest N files."""
        table_path = self.base_path / table_name
        if not table_path.exists():
            return
        
        parquet_files = sorted(table_path.glob("*.parquet"), key=lambda x: x.stat().st_mtime)
        
        # Remove older files
        files_to_remove = parquet_files[:-keep_latest] if len(parquet_files) > keep_latest else []
        
        for file_to_remove in files_to_remove:
            file_to_remove.unlink()
            logger.info(f"Removed old file: {file_to_remove}")


class EnrichmentTracker:
    """
    Tracks enrichment progress and identifies records that need processing.
    Replaces Spark SQL queries for finding missing/new records.
    """
    
    def __init__(self, data_writer: ParquetDataWriter):
        self.data_writer = data_writer
    
    def get_missing_artists(self, exclude_list: List[str] = None) -> pl.DataFrame:
        """
        Find artists that need MusicBrainz enrichment.
        Replaces the missing_sql query from mbz_get_missing_artists.py
        """
        exclude_list = exclude_list or []
        
        # Read source data
        tracks_df = self.data_writer.read_table("tracks_played")
        mbz_artists_df = self.data_writer.read_table("mbz_artist_info")
        
        if tracks_df is None:
            logger.warning("tracks_played table not found")
            return pl.DataFrame()
        
        # Filter tracks with ISRC and not in exclude list
        tracks_filtered = tracks_df.filter(
            (pl.col("track_isrc").is_not_null()) &
            (~pl.col("artist").is_in(exclude_list))
        )
        
        # Get distinct artist info with highest popularity track ISRC
        artist_tracks = (tracks_filtered
            .group_by(["artist_id", "artist"])
            .agg([
                pl.col("track_isrc").first().alias("track_isrc"),
                pl.col("popularity").max().alias("max_popularity")
            ])
            .sort("artist")
        )
        
        if mbz_artists_df is not None:
            # Exclude artists that already have MBZ info
            existing_spotify_ids = mbz_artists_df.select("spotify_id").to_series().to_list()
            missing_artists = artist_tracks.filter(
                ~pl.col("artist_id").is_in(existing_spotify_ids)
            )
        else:
            missing_artists = artist_tracks
        
        return missing_artists
    
    def get_missing_spotify_artists(self) -> pl.DataFrame:
        """Find Spotify artists that need enrichment."""
        tracks_df = self.data_writer.read_table("tracks_played")
        spotify_artists_df = self.data_writer.read_table("spotify_artists")
        
        if tracks_df is None:
            return pl.DataFrame()
        
        # Get distinct artists from tracks
        track_artists = tracks_df.select(["artist_id", "artist"]).unique()
        
        if spotify_artists_df is not None:
            # Exclude artists that already exist
            existing_ids = spotify_artists_df.select("artist_id").to_series().to_list()
            missing_artists = track_artists.filter(
                ~pl.col("artist_id").is_in(existing_ids)
            )
        else:
            missing_artists = track_artists
        
        return missing_artists
    
    def get_missing_spotify_albums(self) -> pl.DataFrame:
        """Find Spotify albums that need enrichment."""
        tracks_df = self.data_writer.read_table("tracks_played")
        spotify_albums_df = self.data_writer.read_table("spotify_albums")
        
        if tracks_df is None:
            return pl.DataFrame()
        
        # Get distinct albums from tracks, ordered by play count
        track_albums = (tracks_df
            .filter(pl.col("album_id").is_not_null())
            .group_by("album_id")
            .agg(pl.len().alias("play_count"))
            .sort("play_count", descending=True)
        )
        
        if spotify_albums_df is not None:
            # Exclude albums that already exist
            existing_ids = spotify_albums_df.select("album_id").to_series().to_list()
            missing_albums = track_albums.filter(
                ~pl.col("album_id").is_in(existing_ids)
            )
        else:
            missing_albums = track_albums
        
        return missing_albums.select("album_id")
    
    def get_areas_needing_enrichment(self) -> pl.DataFrame:
        """Find areas that need geographic enrichment."""
        mbz_area_df = self.data_writer.read_table("mbz_area_hierarchy")
        cities_df = self.data_writer.read_table("cities_with_lat_long")
        
        if mbz_area_df is None:
            return pl.DataFrame()
        
        # Find areas without continent information
        areas_no_continent = mbz_area_df.filter(
            (pl.col("continent").is_null()) |
            (pl.col("continent") == "Unknown") |
            (pl.col("continent") == "")
        )
        
        # Find areas that need lat/long lookup
        areas_needing_coords = mbz_area_df.filter(pl.col("params").is_not_null())
        
        if cities_df is not None:
            existing_params = cities_df.select("params").to_series().to_list()
            areas_needing_coords = areas_needing_coords.filter(
                ~pl.col("params").is_in(existing_params)
            )
        
        return {
            "continent_enrichment": areas_no_continent,
            "coordinate_enrichment": areas_needing_coords
        }
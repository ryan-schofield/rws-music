import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl


def harmonize_dataframe_schemas(dataframes):
    """
    Harmonize schemas across multiple dataframes by ensuring all have the same columns
    and consistent types (casting Null columns to String).

    Args:
        dataframes: List of polars DataFrames

    Returns:
        List of harmonized polars DataFrames ready for concatenation
    """
    if not dataframes:
        return []

    # Get all unique column names across all dataframes
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)

    # Ensure all dataframes have the same columns with consistent types
    harmonized_dfs = []
    for df in dataframes:
        # Add missing columns as null strings
        for col in all_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))

        # Cast all null columns to string to ensure consistent schema
        for col in df.columns:
            if df[col].dtype == pl.Null:
                df = df.with_columns(pl.col(col).cast(pl.String))

        harmonized_dfs.append(df)

    return harmonized_dfs


def main():
    # Define paths using absolute path for task-runner compatibility
    workspace_dir = Path("/home/runner/workspace")
    if not workspace_dir.exists():
        workspace_dir = Path.cwd()
    base_path = workspace_dir / "data"
    detail_path = base_path / "raw" / "recently_played" / "detail"
    src_tracks_path = base_path / "src" / "tracks_played"
    processed_path = base_path / "raw" / "recently_played" / "processed"

    # Ensure processed directory exists
    processed_path.mkdir(parents=True, exist_ok=True)

    # Step 1: Read all JSON files from detail folder
    json_files = list(detail_path.glob("*.json"))
    new_data_frames = []

    # Define explicit schema for the DataFrame to handle mixed types
    # Use Float64 for numeric columns first (handles None gracefully), then cast to Int64
    schema = {
        "user_id": pl.Utf8,
        "track_id": pl.Utf8,
        "uri": pl.Utf8,
        "track_isrc": pl.Utf8,
        "track_name": pl.Utf8,
        "album_id": pl.Utf8,
        "album_uri": pl.Utf8,
        "album": pl.Utf8,
        "artist_id": pl.Utf8,
        "artist_mbid": pl.Utf8,
        "artist": pl.Utf8,
        "duration_ms": pl.Float64,
        "played_at": pl.Utf8,
        "popularity": pl.Float64,
        "request_after": pl.Utf8,
        "play_source": pl.Utf8,
    }

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pl.DataFrame(data, schema=schema)

        # Cast numeric columns to Int64 to match parquet schema (None becomes null)
        df = df.with_columns(
            pl.col("duration_ms").cast(pl.Int64),
            pl.col("popularity").cast(pl.Int64),
        )

        # Rename columns to match existing schema (only if they exist)
        rename_mapping = {}
        if "uri" in df.columns:
            rename_mapping["uri"] = "track_uri"
        if "request_after" in df.columns:
            rename_mapping["request_after"] = "request_cursor"
        if rename_mapping:
            df = df.rename(rename_mapping)

        # Cast played_at to datetime (only if it exists)
        # Handle both formats: "2026-01-04T02:55:58.123Z" (Spotify) and "2026-01-04T02:55:58+00:00Z" (Navidrome)
        if "played_at" in df.columns:
            df = df.with_columns(
                pl.col("played_at")
                .str.strip_chars("Z")  # Remove trailing Z to handle both formats
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False)
                .dt.replace_time_zone("UTC")
                .dt.cast_time_unit("us")
            )
        new_data_frames.append(df)

    if new_data_frames:
        harmonized_dfs = harmonize_dataframe_schemas(new_data_frames)
        new_df = pl.concat(harmonized_dfs)
    else:
        new_df = pl.DataFrame()

    # Step 2: Read existing parquet files
    parquet_files = list(src_tracks_path.glob("*.parquet"))
    if parquet_files:
        existing_df = pl.read_parquet(parquet_files)
    else:
        existing_df = pl.DataFrame()

    # Step 3: Concatenate new data with existing
    if not existing_df.is_empty() and not new_df.is_empty():
        harmonized_dfs = harmonize_dataframe_schemas([existing_df, new_df])
        combined_df = pl.concat(harmonized_dfs)
    elif not existing_df.is_empty():
        combined_df = existing_df
    else:
        combined_df = new_df

    # Step 4: Write back to parquet with optimization
    if not combined_df.is_empty():
        # Remove old parquet files
        for pq_file in parquet_files:
            pq_file.unlink()

        # Write optimized parquet
        combined_df.write_parquet(
            src_tracks_path / "tracks_played.parquet",
            compression="snappy",
            row_group_size=10000,
        )

    # Step 5: Move processed JSON files
    for json_file in json_files:
        shutil.move(str(json_file), str(processed_path / json_file.name))

    # Step 6: Clean up old processed files
    current_time = datetime.now(timezone.utc)
    cutoff_date = current_time - timedelta(days=7)

    for processed_file in processed_path.glob("*.json"):
        # Parse date from filename: spotify_recently_played_YYYYMMDD_HHMMSS.json
        filename = processed_file.name
        if filename.startswith("spotify_recently_played_") and filename.endswith(
            ".json"
        ):
            date_str = filename.split("_")[3]  # YYYYMMDD
            try:
                file_date = datetime.strptime(date_str, "%Y%m%d").replace(
                    tzinfo=timezone.utc
                )
                if file_date < cutoff_date:
                    processed_file.unlink()
            except ValueError:
                pass  # Skip if date parsing fails


if __name__ == "__main__":
    main()

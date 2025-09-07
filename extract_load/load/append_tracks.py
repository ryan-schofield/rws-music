import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl


def main():
    # Define paths
    base_path = Path("data")
    detail_path = base_path / "raw" / "recently_played" / "detail"
    src_tracks_path = base_path / "src" / "tracks_played"
    processed_path = base_path / "raw" / "recently_played" / "processed"

    # Ensure processed directory exists
    processed_path.mkdir(parents=True, exist_ok=True)

    # Step 1: Read all JSON files from detail folder
    json_files = list(detail_path.glob("*.json"))
    new_data_frames = []

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pl.DataFrame(data)
        # Rename columns to match existing schema
        df = df.rename({"uri": "track_uri", "request_after": "request_cursor"})
        # Cast played_at to datetime
        df = df.with_columns(
            pl.col("played_at")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.3fZ")
            .dt.replace_time_zone("UTC")
            .dt.cast_time_unit("us")
        )
        new_data_frames.append(df)

    if new_data_frames:
        new_df = pl.concat(new_data_frames)
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
        combined_df = pl.concat([existing_df, new_df])
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

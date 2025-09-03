#!/usr/bin/env python3
"""
Common Polars operations for data enrichment.

This module provides utility functions that replace Spark SQL operations
used throughout the original Fabric notebooks.
"""

import logging
from typing import Dict, Any, List, Optional, Union
import polars as pl
import json

logger = logging.getLogger(__name__)


def create_continent_lookup_df(continent_mapping: Dict[str, Dict[str, str]]) -> pl.DataFrame:
    """
    Create a Polars DataFrame from continent mapping data.
    Replaces spark.createDataFrame() from geo_add_continent.py
    """
    records = []
    for country, info in continent_mapping.items():
        records.append({
            "country": country,
            "continent": info.get("continent"),
            "country_code": info.get("country_code"),
            "continent_code": info.get("continent_code")
        })
    
    return pl.DataFrame(records)


def merge_continent_data(area_df: pl.DataFrame, continent_df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge area hierarchy data with continent information.
    Replaces the complex SQL merge from geo_add_continent.py
    """
    # Create coalesce column for joining
    area_with_join_key = area_df.with_columns(
        pl.coalesce([pl.col("country_name"), pl.col("island_name")]).alias("join_country")
    )
    
    # Perform left join
    merged = area_with_join_key.join(
        continent_df,
        left_on="join_country",
        right_on="country",
        how="left"
    )
    
    # Update existing columns with coalesce logic
    update_fields = ["continent", "country_code", "continent_code"]
    
    for field in update_fields:
        if field in area_df.columns:
            # Update existing field with coalesce
            merged = merged.with_columns(
                pl.coalesce([pl.col(f"{field}_right"), pl.col(field)]).alias(field)
            )
        else:
            # Add new field
            merged = merged.with_columns(
                pl.col(f"{field}_right").alias(field)
            )
    
    # Remove temporary columns
    cols_to_keep = [col for col in merged.columns 
                   if not col.endswith("_right") and col != "join_country"]
    
    return merged.select(cols_to_keep)


def add_state_codes_and_params(area_df: pl.DataFrame, state_codes_df: pl.DataFrame) -> pl.DataFrame:
    """
    Add state codes and location parameters for geocoding.
    Replaces the complex SQL from geo_add_continent.py
    """
    # Join with state codes for US locations
    merged = area_df.join(
        state_codes_df,
        left_on=["subdivision_name", "country_code"],
        right_on=["name", "US"],  # Assuming state codes table structure
        how="left"
    )
    
    # Create params column for geocoding
    merged = merged.with_columns(
        pl.concat_str([
            pl.coalesce([pl.col("city_name"), pl.col("municipality_name")]),
            pl.lit(","),
            pl.when(pl.col("code").is_not_null())
            .then(pl.concat_str([pl.col("code"), pl.lit(",")]))
            .otherwise(pl.lit("")),
            pl.col("country_code")
        ]).alias("params")
    )
    
    return merged


def clean_municipality_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean municipality names by removing 'municipality' suffix.
    Replaces the SQL UPDATE from geo_add_continent.py
    """
    return df.with_columns(
        pl.when(pl.col("municipality_name").str.to_lowercase().str.contains("municipality"))
        .then(
            pl.col("municipality_name")
            .str.to_lowercase()
            .str.replace("municipality", "")
            .str.strip_chars()
            .str.to_titlecase()
        )
        .otherwise(pl.col("municipality_name"))
        .alias("municipality_name")
    )


def parse_location_params(params_list: List[str]) -> pl.DataFrame:
    """
    Parse location parameter strings into structured data.
    Replaces the parsing logic from geo_add_lat_long.py
    """
    records = []
    for param in params_list:
        if not param:
            continue
            
        split_vals = param.split(",")
        record = {
            "city_name": split_vals[0] if len(split_vals) > 0 else "",
            "state_code": split_vals[1] if len(split_vals) == 3 else "",
            "country_code": split_vals[-1] if len(split_vals) > 1 else "",
            "params": param
        }
        records.append(record)
    
    return pl.DataFrame(records)


def flatten_json_column(df: pl.DataFrame, json_col: str, prefix: str = None) -> pl.DataFrame:
    """
    Flatten a JSON column into separate columns.
    Replaces complex JSON parsing from mbz_parse_artists.py
    """
    if json_col not in df.columns:
        return df
    
    # First, try to parse JSON strings
    try:
        # Convert JSON strings to structured data
        df_with_parsed = df.with_columns(
            pl.col(json_col).str.json_extract().alias(f"_parsed_{json_col}")
        )
        
        # Get the first non-null parsed value to determine structure
        sample_row = df_with_parsed.filter(
            pl.col(f"_parsed_{json_col}").is_not_null()
        ).limit(1).row(0, named=True)
        
        if not sample_row or f"_parsed_{json_col}" not in sample_row:
            return df
        
        sample_json = sample_row[f"_parsed_{json_col}"]
        if not isinstance(sample_json, dict):
            return df
        
        # Create new columns for each JSON field
        new_columns = []
        for key in sample_json.keys():
            col_name = f"{prefix}_{key}" if prefix else key
            col_name = col_name.replace("-", "_")  # Replace dashes
            new_columns.append(
                pl.col(f"_parsed_{json_col}").map_elements(
                    lambda x: x.get(key) if isinstance(x, dict) else None,
                    return_dtype=pl.Utf8
                ).alias(col_name)
            )
        
        # Add the new columns and remove the temporary parsed column
        result_df = df_with_parsed.with_columns(new_columns)
        return result_df.drop(f"_parsed_{json_col}")
        
    except Exception as e:
        logger.warning(f"Could not flatten JSON column {json_col}: {e}")
        return df


def explode_genre_array(df: pl.DataFrame, array_col: str = "genres") -> pl.DataFrame:
    """
    Explode an array column into separate rows.
    Replaces the explode_outer functionality from Spark SQL.
    """
    if array_col not in df.columns:
        return pl.DataFrame()
    
    # Convert string representation of array to actual array if needed
    if df.schema[array_col] == pl.Utf8:
        df = df.with_columns(
            pl.col(array_col).str.json_extract().alias(array_col)
        )
    
    # Explode the array column
    return df.explode(array_col)


def deduplicate_with_priority(df: pl.DataFrame, subset: List[str], 
                            priority_col: str = None, ascending: bool = False) -> pl.DataFrame:
    """
    Remove duplicates keeping records with highest/lowest priority.
    Replaces Spark's dropDuplicates with window functions.
    """
    if not subset:
        return df.unique()
    
    if priority_col and priority_col in df.columns:
        # Sort by priority and keep first occurrence
        return (df
                .sort(subset + [priority_col], descending=[False] * len(subset) + [not ascending])
                .unique(subset=subset, keep="first"))
    else:
        # Simple deduplication
        return df.unique(subset=subset, keep="first")


def create_artist_genre_table(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create artist genre table from MusicBrainz tag data.
    Replaces the complex SQL from mbz_parse_artists.py
    """
    if "tag_list" not in df.columns:
        return pl.DataFrame()
    
    # Parse tag_list JSON and explode
    try:
        tags_expanded = df.with_columns(
            pl.col("tag_list").str.json_extract().alias("parsed_tags")
        ).explode("parsed_tags")
        
        # Extract tag information
        genre_table = tags_expanded.with_columns([
            pl.col("id").alias("artist_mbid"),
            pl.col("spotify_id").alias("artist_spotify_id"),
            pl.col("name").alias("artist_name"),
            pl.col("parsed_tags").map_elements(
                lambda x: x.get("count") if isinstance(x, dict) else None,
                return_dtype=pl.Int64
            ).alias("count_genre_tags"),
            pl.col("parsed_tags").map_elements(
                lambda x: x.get("name") if isinstance(x, dict) else None,
                return_dtype=pl.Utf8
            ).alias("mbz_genre")
        ]).filter(
            pl.col("mbz_genre").is_not_null()
        )
        
        # Group by artist and genre to sum counts
        return (genre_table
                .group_by(["artist_mbid", "artist_spotify_id", "artist_name", "mbz_genre"])
                .agg(pl.col("count_genre_tags").sum())
                .sort(["artist_name", "count_genre_tags"], descending=[False, True]))
        
    except Exception as e:
        logger.error(f"Error creating artist genre table: {e}")
        return pl.DataFrame()


def normalize_artist_json_data(json_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize artist JSON data by filtering out list keys and flattening nested objects.
    Based on filter_non_list_keys from mbz_parse_artists.py
    """
    filtered_data = {}
    
    for key, value in json_data.items():
        # Skip list keys except tag-list
        if "-list" in key and key != "tag-list":
            continue
        
        # Flatten nested dictionaries
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                if "-list" not in nested_key:
                    filtered_data[f"{key}_{nested_key}"] = nested_value
        else:
            filtered_data[key] = value
    
    return filtered_data


def process_area_hierarchy_data(area_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Process area hierarchy data into a flat row structure.
    Based on process_areas from mbz_parse_area_hierarchy.py
    """
    # Get all unique area types
    all_types = set(area_data.keys())
    
    # Create base row structure
    row = {"area_id": None, "area_type": None, "area_name": None, "area_sort_name": None}
    
    # Add columns for each area type
    for area_type in sorted(all_types):
        row[f"{area_type}_id"] = None
        row[f"{area_type}_name"] = None
        row[f"{area_type}_sort_name"] = None
    
    # Find the root area (the one that matches the requested ID)
    # This would need to be determined by the calling code
    
    # Populate area data
    for area_type, area_info in area_data.items():
        row[f"{area_type}_id"] = area_info["id"]
        row[f"{area_type}_name"] = area_info["name"]
        row[f"{area_type}_sort_name"] = area_info["sort_name"]
    
    return row


def batch_process_dataframe(df: pl.DataFrame, batch_size: int = 1000) -> List[pl.DataFrame]:
    """
    Split a DataFrame into batches for processing.
    Useful for API calls with rate limiting.
    """
    batches = []
    total_rows = len(df)
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch = df.slice(start_idx, end_idx - start_idx)
        batches.append(batch)
    
    return batches


def safe_json_extract(df: pl.DataFrame, json_col: str, extract_path: str) -> pl.DataFrame:
    """
    Safely extract values from JSON column with error handling.
    """
    try:
        return df.with_columns(
            pl.col(json_col).str.json_path_match(extract_path).alias(f"extracted_{extract_path}")
        )
    except Exception as e:
        logger.warning(f"Could not extract {extract_path} from {json_col}: {e}")
        return df.with_columns(
            pl.lit(None).alias(f"extracted_{extract_path}")
        )
#!/usr/bin/env python3
"""
Geographic data enrichment processor.

This module consolidates geo_add_continent.py and geo_add_lat_long.py from 
the original Fabric notebooks, replacing Spark operations with Polars and 
writing directly to parquet files.
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import polars as pl
import pycountry
import pycountry_convert as pc

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.enrich.utils.api_clients import OpenWeatherGeoClient
from flows.enrich.utils.data_writer import ParquetDataWriter
from flows.enrich.utils.polars_ops import (
    create_continent_lookup_df,
    merge_continent_data,
    clean_municipality_names,
    parse_location_params,
)

logger = logging.getLogger(__name__)


class GeographicProcessor:
    """
    Handles geographic enrichment of area hierarchy data.

    Consolidates:
    - Continent mapping (geo_add_continent.py)
    - Latitude/longitude lookup (geo_add_lat_long.py)
    """

    def __init__(
        self, data_writer: ParquetDataWriter = None, openweather_api_key: str = None
    ):
        logger.info("Initializing GeographicProcessor")
        try:
            self.data_writer = data_writer or ParquetDataWriter()
            logger.info("Data writer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize data writer: {e}", exc_info=True)
            raise

        try:
            self.geo_client = OpenWeatherGeoClient(openweather_api_key)
            self.has_api_key = True
            logger.info("OpenWeather API client initialized successfully")
        except ValueError as e:
            logger.warning(f"OpenWeather API key not found - coordinate enrichment will be skipped: {e}")
            self.geo_client = None
            self.has_api_key = False
        except Exception as e:
            logger.error(f"Failed to initialize OpenWeather API client: {e}", exc_info=True)
            self.geo_client = None
            self.has_api_key = False

        # Country name mappings from original notebook
        self.name_mappings = {
            "Kingdom of the Netherlands": "Netherlands",
            "South Korea": "Korea, Republic of",
            "Democratic Republic of the Congo": "Congo, The Democratic Republic of the",
            # Island-to-country mappings
            "Bermuda": "United Kingdom",
            "Cayman Islands": "United Kingdom",
            "British Virgin Islands": "United Kingdom",
            "US Virgin Islands": "United States",
            "Puerto Rico": "United States",
            "Guam": "United States",
            "American Samoa": "United States",
            "Northern Mariana Islands": "United States",
            "Martinique": "France",
            "Guadeloupe": "France",
            "French Guiana": "France",
            "Réunion": "France",
            "Mayotte": "France",
            "New Caledonia": "France",
            "French Polynesia": "France",
            "Aruba": "Netherlands",
            "Curaçao": "Netherlands",
            "Sint Maarten": "Netherlands",
            "Greenland": "Denmark",
            "Faroe Islands": "Denmark",
            "Gibraltar": "United Kingdom",
            "Isle of Man": "United Kingdom",
            "Jersey": "United Kingdom",
            "Guernsey": "United Kingdom",
            "Hong Kong": "China",
            "Macau": "China",
            "Taiwan": "Taiwan",
            "Cook Islands": "New Zealand",
            "Niue": "New Zealand",
            "Tokelau": "New Zealand",
        }

    def get_continent_info(
        self, country_name: str
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Get continent information for a country name using pycountry libraries.
        Based on the original function from geo_add_continent.py

        Returns:
            Tuple of (continent_name, country_code, continent_code) or (None, None, None)
        """
        # Use mapped name if available
        lookup_name = self.name_mappings.get(country_name, country_name)

        try:
            country = None

            # Try exact match first
            try:
                country = pycountry.countries.lookup(lookup_name)
            except LookupError:
                # Try fuzzy search
                for c in pycountry.countries:
                    if (
                        lookup_name.lower() in c.name.lower()
                        or c.name.lower() in lookup_name.lower()
                    ):
                        country = c
                        break

            if country:
                country_code = country.alpha_2
                continent_code = pc.country_alpha2_to_continent_code(country_code)
                continent_name = pc.convert_continent_code_to_continent_name(
                    continent_code
                )
                return (continent_name, country_code, continent_code)
            else:
                return (None, None, None)

        except Exception as e:
            logger.warning(f"Error processing country {country_name}: {e}")
            return (None, None, None)

    def enrich_continents(self) -> Dict[str, Any]:
        """
        Add continent information to area hierarchy data.
        Replaces the continent enrichment logic from geo_add_continent.py
        """
        logger.info("Starting continent enrichment")

        # Read area hierarchy data
        logger.info("Reading mbz_area_hierarchy table")
        area_df = self.data_writer.read_table("mbz_area_hierarchy")
        if area_df is None:
            logger.error("mbz_area_hierarchy table not found or could not be read")
            return {"status": "error", "message": "mbz_area_hierarchy table not found or could not be read"}
        logger.info(f"Successfully read {len(area_df)} records from mbz_area_hierarchy")

        # Find countries that need continent information
        # Check if continent column exists, if not, assume all need enrichment
        if "continent" in area_df.columns:
            countries_needing_enrichment = (
                area_df.filter(
                    (pl.col("country_name").is_not_null() | pl.col("island_name").is_not_null())
                    & ((pl.col("continent").is_null()) | (pl.col("continent") == "Unknown"))
                )
                .select(
                    pl.coalesce([pl.col("country_name"), pl.col("island_name")]).alias(
                        "country_name"
                    )
                )
                .unique()
                .to_series()
                .to_list()
            )
        else:
            # If continent column doesn't exist, all countries need enrichment
            countries_needing_enrichment = (
                area_df.filter(
                    pl.col("country_name").is_not_null() | pl.col("island_name").is_not_null()
                )
                .select(
                    pl.coalesce([pl.col("country_name"), pl.col("island_name")]).alias(
                        "country_name"
                    )
                )
                .unique()
                .to_series()
                .to_list()
            )

        if not countries_needing_enrichment:
            logger.info("No countries need continent enrichment")
            return {"status": "no_updates", "message": "No countries need enrichment"}

        logger.info(
            f"Processing continent info for {len(countries_needing_enrichment)} countries"
        )

        # Get continent information for each country
        continent_results = []
        for country in countries_needing_enrichment:
            continent_info = self.get_continent_info(country)
            continent_results.append(
                {
                    "country": country,
                    "continent": continent_info[0],
                    "country_code": continent_info[1],
                    "continent_code": continent_info[2],
                }
            )

        # Create continent lookup DataFrame
        continent_df = pl.DataFrame(continent_results)

        # Merge with area hierarchy data
        updated_area_df = merge_continent_data(area_df, continent_df)

        # Clean municipality names
        updated_area_df = clean_municipality_names(updated_area_df)

        # Write back to parquet - use merge to preserve existing data
        write_result = self.data_writer.write_table(
            updated_area_df, "mbz_area_hierarchy", mode="merge"
        )

        if write_result["status"] == "success":
            logger.info(
                f"Successfully enriched continent data for {len(continent_results)} countries"
            )
            return {
                "status": "success",
                "countries_processed": len(continent_results),
                "records_updated": write_result["records_written"],
            }
        else:
            return write_result

    def add_geocoding_params(self) -> Dict[str, Any]:
        """
        Add state codes and geocoding parameters to area hierarchy.
        Based on the parameter creation logic from geo_add_continent.py
        """
        logger.info("Adding geocoding parameters")

        # Read area hierarchy data
        area_df = self.data_writer.read_table("mbz_area_hierarchy")
        if area_df is None:
            logger.error("mbz_area_hierarchy table not found or could not be read")
            return {"status": "error", "message": "mbz_area_hierarchy table not found or could not be read"}

        # Create params column, but only for records that have a city/municipality name
        city_expr = pl.coalesce([pl.col("city_name"), pl.col("municipality_name")])

        # Filter out records where city name would be empty
        filtered_df = area_df.filter(city_expr.is_not_null())

        # Create params for valid records
        updated_df = filtered_df.with_columns(
            pl.concat_str(
                [
                    city_expr,
                    pl.lit(","),
                    pl.col("country_code").fill_null(""),
                ]
            ).alias("params")
        )

        # Write back to parquet - use merge to preserve existing data
        write_result = self.data_writer.write_table(
            updated_df, "mbz_area_hierarchy", mode="merge"
        )

        return {
            "status": "success",
            "message": "Added geocoding parameters",
            "records_updated": write_result.get("records_written", 0),
        }

    def enrich_coordinates(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Add latitude/longitude coordinates using OpenWeather API.
        Replaces the coordinate lookup logic from geo_add_lat_long.py

        Args:
            limit: Maximum number of locations to process
        """
        logger.info("Starting coordinate enrichment")

        # Check if API key is available
        if not self.has_api_key:
            logger.info("OpenWeather API key not available - skipping coordinate enrichment")
            return {"status": "skipped", "message": "OpenWeather API key not available"}

        # Read area hierarchy data
        logger.info("Reading mbz_area_hierarchy and cities_with_lat_long tables")
        area_df = self.data_writer.read_table("mbz_area_hierarchy")
        cities_df = self.data_writer.read_table("cities_with_lat_long")

        if area_df is None:
            logger.error("mbz_area_hierarchy table not found or could not be read")
            return {"status": "error", "message": "mbz_area_hierarchy table not found or could not be read"}
        logger.info(f"Read {len(area_df)} records from mbz_area_hierarchy")
        if cities_df is not None:
            logger.info(f"Read {len(cities_df)} existing city records")

        # Find parameters that need coordinate lookup
        area_params = (
            area_df.filter(pl.col("params").is_not_null())
            .select("params")
            .unique()
            .to_series()
            .to_list()
        )

        # Exclude parameters that already have coordinates
        if cities_df is not None:
            existing_params = cities_df.select("params").to_series().to_list()
            new_params = [p for p in area_params if p not in existing_params]
        else:
            new_params = area_params

        if not new_params:
            logger.info("No new locations need coordinate enrichment")
            return {"status": "no_updates", "message": "No new locations to process"}

        # Apply limit if specified
        if limit is not None and len(new_params) > limit:
            new_params = new_params[:limit]
            logger.info(f"Limited to {limit} locations for testing")

        logger.info(f"Looking up coordinates for {len(new_params)} locations")

        # Parse parameters into structured data (simplified version)
        recs = []
        for p in new_params:
            if not p:
                continue

            d = {}
            split_vals = p.split(",")
            d["city_name"] = split_vals[0] if len(split_vals) > 0 else ""
            d["state_code"] = ""
            d["country_code"] = split_vals[-1] if len(split_vals) > 1 else ""
            d["params"] = p

            if len(split_vals) == 3:
                d["state_code"] = split_vals[1]
            recs.append(d)

        logger.info(f"Parsed {len(recs)} location parameters")

        # Get coordinates from OpenWeather API
        for rec in recs:
            q = rec.get("params")
            if not q:
                continue

            coords = self.geo_client.get_coordinates(q)
            if coords:
                rec["lat"] = str(coords.get("lat"))
                rec["long"] = str(coords.get("long"))

        enriched_records = recs

        # Create DataFrame and write to cities_with_lat_long
        if enriched_records:
            cities_update_df = pl.DataFrame(enriched_records)

            write_result = self.data_writer.write_table(
                cities_update_df, "cities_with_lat_long", mode="append"
            )

            successful_lookups = sum(
                1 for r in enriched_records if r["lat"] is not None
            )

            logger.info(
                f"Successfully added coordinates for {successful_lookups}/{len(enriched_records)} locations"
            )

            return {
                "status": "success",
                "locations_processed": len(enriched_records),
                "successful_lookups": successful_lookups,
                "records_written": write_result.get("records_written", 0),
            }
        else:
            return {"status": "no_updates", "message": "No coordinate data to write"}

    def run_full_enrichment(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Run the complete geographic enrichment pipeline.

        Args:
            limit: Maximum number of records to process for testing
        """
        logger.info("Starting full geographic enrichment")

        results = {
            "continent_enrichment": None,
            "parameter_addition": None,
            "coordinate_enrichment": None,
            "overall_status": "success",
        }

        try:
            # Step 1: Add continent information
            logger.info("Step 1: Starting continent enrichment")
            continent_result = self.enrich_continents()
            results["continent_enrichment"] = continent_result
            logger.info(f"Continent enrichment result: {continent_result.get('status', 'unknown')}")

            if continent_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"
                logger.warning(f"Continent enrichment failed with status: {continent_result.get('status')}")

            # Step 2: Add geocoding parameters
            logger.info("Step 2: Adding geocoding parameters")
            params_result = self.add_geocoding_params()
            results["parameter_addition"] = params_result
            logger.info(f"Parameter addition result: {params_result.get('status', 'unknown')}")

            if params_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"
                logger.warning(f"Parameter addition failed with status: {params_result.get('status')}")

            # Step 3: Lookup coordinates
            logger.info("Step 3: Starting coordinate enrichment")
            coords_result = self.enrich_coordinates(limit=limit)
            results["coordinate_enrichment"] = coords_result
            logger.info(f"Coordinate enrichment result: {coords_result.get('status', 'unknown')}")

            if coords_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"
                logger.warning(f"Coordinate enrichment failed with status: {coords_result.get('status')}")

            logger.info(f"Geographic enrichment pipeline completed with overall status: {results['overall_status']}")
            results["status"] = results["overall_status"]  # Add status field for compatibility
            return results

        except Exception as e:
            logger.error(f"Geographic enrichment failed with exception: {e}", exc_info=True)
            results["overall_status"] = "error"
            results["status"] = "error"  # Also set status for compatibility
            results["message"] = f"Geographic enrichment failed: {str(e)}"
            results["error_message"] = str(e)
            results["error_type"] = type(e).__name__
            return results


def main():
    """Main entry point for geographic processor."""
    logging.basicConfig(level=logging.INFO)

    processor = GeographicProcessor()
    result = processor.run_full_enrichment()

    print(f"Geographic enrichment completed with status: {result['overall_status']}")
    for step, step_result in result.items():
        if step != "overall_status" and step_result:
            print(f"  {step}: {step_result.get('status', 'unknown')}")


if __name__ == "__main__":
    main()

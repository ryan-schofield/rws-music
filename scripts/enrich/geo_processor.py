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

from scripts.enrich.utils.api_clients import OpenWeatherGeoClient
from scripts.enrich.utils.data_writer import ParquetDataWriter
from scripts.enrich.utils.polars_ops import (
    create_continent_lookup_df, 
    merge_continent_data,
    clean_municipality_names,
    parse_location_params
)

logger = logging.getLogger(__name__)


class GeographicProcessor:
    """
    Handles geographic enrichment of area hierarchy data.
    
    Consolidates:
    - Continent mapping (geo_add_continent.py)
    - Latitude/longitude lookup (geo_add_lat_long.py)
    """
    
    def __init__(self, data_writer: ParquetDataWriter = None, openweather_api_key: str = None):
        self.data_writer = data_writer or ParquetDataWriter()
        self.geo_client = OpenWeatherGeoClient(openweather_api_key)
        
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
    
    def get_continent_info(self, country_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
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
                    if (lookup_name.lower() in c.name.lower() or 
                        c.name.lower() in lookup_name.lower()):
                        country = c
                        break
            
            if country:
                country_code = country.alpha_2
                continent_code = pc.country_alpha2_to_continent_code(country_code)
                continent_name = pc.convert_continent_code_to_continent_name(continent_code)
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
        area_df = self.data_writer.read_table("mbz_area_hierarchy")
        if area_df is None:
            return {"status": "error", "message": "mbz_area_hierarchy table not found"}
        
        # Find countries that need continent information
        countries_needing_enrichment = (
            area_df
            .select(pl.coalesce([pl.col("country_name"), pl.col("island_name")]).alias("country_name"))
            .filter(
                (pl.col("country_name").is_not_null()) &
                ((pl.col("continent").is_null()) | (pl.col("continent") == "Unknown"))
            )
            .unique()
            .to_series()
            .to_list()
        )
        
        if not countries_needing_enrichment:
            logger.info("No countries need continent enrichment")
            return {"status": "no_updates", "message": "No countries need enrichment"}
        
        logger.info(f"Processing continent info for {len(countries_needing_enrichment)} countries")
        
        # Get continent information for each country
        continent_results = []
        for country in countries_needing_enrichment:
            continent_info = self.get_continent_info(country)
            continent_results.append({
                "country": country,
                "continent": continent_info[0],
                "country_code": continent_info[1], 
                "continent_code": continent_info[2]
            })
        
        # Create continent lookup DataFrame
        continent_df = pl.DataFrame(continent_results)
        
        # Merge with area hierarchy data
        updated_area_df = merge_continent_data(area_df, continent_df)
        
        # Clean municipality names
        updated_area_df = clean_municipality_names(updated_area_df)
        
        # Write back to parquet
        write_result = self.data_writer.write_table(
            updated_area_df, 
            "mbz_area_hierarchy", 
            mode="overwrite"
        )
        
        if write_result["status"] == "success":
            logger.info(f"Successfully enriched continent data for {len(continent_results)} countries")
            return {
                "status": "success",
                "countries_processed": len(continent_results),
                "records_updated": write_result["records_written"]
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
            return {"status": "error", "message": "mbz_area_hierarchy table not found"}
        
        # Create state codes lookup (simplified - would need actual state_codes table)
        # For now, we'll create parameters without state codes
        updated_df = area_df.with_columns(
            pl.concat_str([
                pl.coalesce([pl.col("city_name"), pl.col("municipality_name")]).fill_null(""),
                pl.lit(","),
                pl.col("country_code").fill_null("")
            ]).alias("params")
        )
        
        # Write back to parquet
        write_result = self.data_writer.write_table(
            updated_df,
            "mbz_area_hierarchy", 
            mode="overwrite"
        )
        
        return {
            "status": "success",
            "message": "Added geocoding parameters",
            "records_updated": write_result.get("records_written", 0)
        }
    
    def enrich_coordinates(self) -> Dict[str, Any]:
        """
        Add latitude/longitude coordinates using OpenWeather API.
        Replaces the coordinate lookup logic from geo_add_lat_long.py
        """
        logger.info("Starting coordinate enrichment")
        
        # Read area hierarchy data
        area_df = self.data_writer.read_table("mbz_area_hierarchy")
        cities_df = self.data_writer.read_table("cities_with_lat_long")
        
        if area_df is None:
            return {"status": "error", "message": "mbz_area_hierarchy table not found"}
        
        # Find parameters that need coordinate lookup
        area_params = (
            area_df
            .filter(pl.col("params").is_not_null())
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
        
        logger.info(f"Looking up coordinates for {len(new_params)} locations")
        
        # Parse parameters into structured data
        params_df = parse_location_params(new_params)
        
        # Get coordinates from OpenWeather API
        coordinate_results = self.geo_client.get_coordinates_batch(new_params)
        
        # Create records with coordinate information
        enriched_records = []
        for _, row in params_df.iter_rows(named=True):
            params = row["params"]
            coords = coordinate_results.get(params)
            
            record = {
                "city_name": row["city_name"],
                "state_code": row["state_code"], 
                "country_code": row["country_code"],
                "params": params,
                "lat": str(coords["lat"]) if coords and coords.get("lat") else None,
                "long": str(coords["long"]) if coords and coords.get("long") else None
            }
            enriched_records.append(record)
        
        # Create DataFrame and write to cities_with_lat_long
        if enriched_records:
            cities_update_df = pl.DataFrame(enriched_records)
            
            write_result = self.data_writer.write_table(
                cities_update_df,
                "cities_with_lat_long",
                mode="append"
            )
            
            successful_lookups = sum(1 for r in enriched_records if r["lat"] is not None)
            
            logger.info(f"Successfully added coordinates for {successful_lookups}/{len(enriched_records)} locations")
            
            return {
                "status": "success",
                "locations_processed": len(enriched_records),
                "successful_lookups": successful_lookups,
                "records_written": write_result.get("records_written", 0)
            }
        else:
            return {"status": "no_updates", "message": "No coordinate data to write"}
    
    def run_full_enrichment(self) -> Dict[str, Any]:
        """
        Run the complete geographic enrichment pipeline.
        """
        logger.info("Starting full geographic enrichment")
        
        results = {
            "continent_enrichment": None,
            "parameter_addition": None,
            "coordinate_enrichment": None,
            "overall_status": "success"
        }
        
        try:
            # Step 1: Add continent information
            continent_result = self.enrich_continents()
            results["continent_enrichment"] = continent_result
            
            if continent_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"
            
            # Step 2: Add geocoding parameters
            params_result = self.add_geocoding_params()
            results["parameter_addition"] = params_result
            
            if params_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"
            
            # Step 3: Lookup coordinates
            coords_result = self.enrich_coordinates()
            results["coordinate_enrichment"] = coords_result
            
            if coords_result["status"] not in ["success", "no_updates"]:
                results["overall_status"] = "partial_failure"
            
            logger.info("Geographic enrichment pipeline completed")
            return results
            
        except Exception as e:
            logger.error(f"Geographic enrichment failed: {e}")
            results["overall_status"] = "error"
            results["error_message"] = str(e)
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
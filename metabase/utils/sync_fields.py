"""
Sync Fields from Metabase API

This script fetches field definitions directly from a running Metabase instance
and updates the fields.json file. This ensures that fields.json stays in sync
with the actual Metabase schema.

Usage:
    python metabase/utils/sync_fields.py
    
    Or with specific table IDs:
    python metabase/utils/sync_fields.py --tables 69 97 70
"""

import requests
import json
import os
import argparse
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables
load_dotenv()

METABASE_PORT = os.getenv("METABASE_PORT", "3000")
METABASE_HOST = f"http://localhost:{METABASE_PORT}"
METABASE_API_KEY = os.getenv("METABASE_API_KEY")


def fetch_fields_from_metabase(table_ids=None):
    """
    Fetch field definitions from Metabase API for specified tables.
    
    Args:
        table_ids: List of table IDs to fetch. If None, defaults to [69, 97, 70]
    
    Returns:
        List of field dictionaries with table_id included
    """
    if table_ids is None:
        table_ids = [69, 97, 70]
    
    base_url = f"{METABASE_HOST}/api"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": METABASE_API_KEY
    }
    
    print(f"Connecting to Metabase at {base_url}")
    
    try:
        # Fetch all tables
        tables_response = requests.get(f"{base_url}/table", headers=headers, timeout=10)
        
        if tables_response.status_code != 200:
            print(f"Error: Failed to fetch tables (status {tables_response.status_code})")
            return None
        
        all_tables = tables_response.json()
        print(f"Found {len(all_tables)} total tables in Metabase")
        
        # Filter for target tables
        valid_table_ids = set(table_ids)
        target_tables = [t for t in all_tables if t.get('id') in valid_table_ids]
        
        if not target_tables:
            print(f"Error: No tables found with IDs {table_ids}")
            return None
        
        print(f"Found {len(target_tables)} target tables")
        
        # Fetch fields for each target table
        all_fields = []
        for table in target_tables:
            table_id = table.get('id')
            table_name = table.get('display_name', table.get('name'))
            
            try:
                metadata_response = requests.get(
                    f"{base_url}/table/{table_id}/query_metadata",
                    headers=headers,
                    timeout=10
                )
                
                if metadata_response.status_code == 200:
                    metadata = metadata_response.json()
                    fields = metadata.get('fields', [])
                    
                    # Add table_id to each field
                    for field in fields:
                        field['table_id'] = table_id
                    
                    all_fields.extend(fields)
                    print(f"  Table {table_id} ({table_name}): {len(fields)} fields")
                else:
                    print(f"  Error: Failed to fetch metadata for table {table_id} (status {metadata_response.status_code})")
                    
            except Exception as e:
                print(f"  Error fetching metadata for table {table_id}: {e}")
        
        if not all_fields:
            print("Error: No fields were fetched")
            return None
        
        print(f"\nTotal fields fetched: {len(all_fields)}")
        
        # Show summary
        by_table = defaultdict(int)
        for field in all_fields:
            by_table[field.get('table_id')] += 1
        
        print("\nFields by table:")
        for table_id in sorted(by_table.keys()):
            print(f"  Table {table_id}: {by_table[table_id]} fields")
        
        return all_fields
        
    except requests.exceptions.ConnectionError:
        print(f"Error: Cannot connect to Metabase at {METABASE_HOST}")
        print("Make sure Metabase is running: docker compose up")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def save_fields_to_json(fields, output_path=None):
    """
    Save fields to fields.json file.
    
    Args:
        fields: List of field dictionaries
        output_path: Path to save to. If None, uses metabase/fields.json
    
    Returns:
        True if successful, False otherwise
    """
    if output_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "..", "fields.json")
    
    try:
        output_data = {"fields": fields}
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=4)
        
        print(f"\nSuccessfully saved {len(fields)} fields to {output_path}")
        return True
    except Exception as e:
        print(f"Error saving to {output_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Sync field definitions from Metabase to fields.json"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        type=int,
        default=[69, 97, 70],
        help="Table IDs to sync (default: 69 97 70)"
    )
    parser.add_argument(
        "--output",
        help="Output path for fields.json (default: metabase/fields.json)"
    )
    
    args = parser.parse_args()
    
    print("=== Metabase Fields Sync ===\n")
    
    # Fetch fields from Metabase
    fields = fetch_fields_from_metabase(args.tables)
    
    if fields is None:
        print("\nFailed to fetch fields from Metabase")
        return False
    
    # Save to JSON
    if save_fields_to_json(fields, args.output):
        print("\n=== Sync Complete ===")
        return True
    else:
        print("\n=== Sync Failed ===")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

"""
Metabase Template Cleaner

This script cleans and prepares Metabase export files for import by:
1. Fetching the actual table and field IDs from the running Metabase
2. Removing references to tables not in the target system
3. Updating all table and field ID references in the template files
4. Ensuring questions.json only contains valid questions

This should be run BEFORE import_metabase.py to ensure clean mapping.

Usage:
    python metabase/utils/clean_templates.py
"""

import requests
import json
import os
from collections import defaultdict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

METABASE_PORT = os.getenv("METABASE_PORT", "3000")
METABASE_HOST = f"http://localhost:{METABASE_PORT}"
METABASE_API_KEY = os.getenv("METABASE_API_KEY")

# Valid table names to keep (we'll match by name)
VALID_TABLE_NAMES = {"artist_genre_plays", "recently_played", "track_plays"}


def get_metabase_tables():
    """Fetch all current tables from Metabase"""
    base_url = f"{METABASE_HOST}/api"
    headers = {"Content-Type": "application/json", "x-api-key": METABASE_API_KEY}
    
    try:
        response = requests.get(f"{base_url}/table", headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching tables: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error: {e}")
        return []


def get_metabase_fields_for_table(table_id):
    """Fetch fields for a specific table from Metabase"""
    base_url = f"{METABASE_HOST}/api"
    headers = {"Content-Type": "application/json", "x-api-key": METABASE_API_KEY}
    
    try:
        response = requests.get(
            f"{base_url}/table/{table_id}/query_metadata",
            headers=headers,
            timeout=10
        )
        if response.status_code == 200:
            metadata = response.json()
            fields = metadata.get('fields', [])
            for field in fields:
                field['table_id'] = table_id
            return fields
        else:
            print(f"Error fetching fields for table {table_id}: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error: {e}")
        return []


def create_id_mappings(metabase_tables):
    """
    Create mappings from old IDs to new IDs by matching names.
    
    Returns:
        (table_mapping: dict, field_mapping: dict)
    """
    print("\n=== Creating ID Mappings ===\n")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Load source tables from export
    try:
        with open(os.path.join(script_dir, "..", "tables.json"), "r") as f:
            source_data = json.load(f)
        source_tables = source_data.get("tables", [])
    except FileNotFoundError:
        print("Error: tables.json not found")
        return {}, {}
    
    # Create table mapping by name
    table_mapping = {}
    table_name_to_id = {}
    
    for source_table in source_tables:
        source_id = source_table.get("id")
        source_name = source_table.get("name", "").lower()
        
        # Find matching Metabase table by name
        for metabase_table in metabase_tables:
            metabase_id = metabase_table.get("id")
            metabase_name = metabase_table.get("name", "").lower()
            
            if metabase_name == source_name:
                table_mapping[source_id] = metabase_id
                table_name_to_id[source_name] = metabase_id
                print(f"Mapped table '{source_name}': {source_id} -> {metabase_id}")
                break
    
    # Create field mapping by fetching all Metabase fields
    field_mapping = {}
    
    try:
        with open(os.path.join(script_dir, "..", "fields.json"), "r") as f:
            fields_data = json.load(f)
        source_fields = fields_data.get("fields", [])
    except FileNotFoundError:
        print("fields.json not found - skipping field mapping")
        source_fields = []
    
    # Get all target fields
    target_fields = []
    for table_id in set(table_mapping.values()):
        target_fields.extend(get_metabase_fields_for_table(table_id))
    
    # Map fields by table and name
    for source_field in source_fields:
        source_field_id = source_field.get("id")
        source_field_name = source_field.get("name")
        source_table_id = source_field.get("table_id")
        
        # Skip if we don't have a table mapping
        if source_table_id not in table_mapping:
            continue
        
        target_table_id = table_mapping[source_table_id]
        
        # Find matching target field
        for target_field in target_fields:
            if (target_field.get("name") == source_field_name and 
                target_field.get("table_id") == target_table_id):
                target_field_id = target_field.get("id")
                field_mapping[source_field_id] = target_field_id
                break
    
    print(f"\nCreated {len(table_mapping)} table mappings")
    print(f"Created {len(field_mapping)} field mappings")
    
    return table_mapping, field_mapping


def update_tables_json(table_mapping):
    """Update tables.json to only include valid tables with new IDs"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tables_path = os.path.join(script_dir, "..", "tables.json")
    
    try:
        with open(tables_path, "r") as f:
            data = json.load(f)
        
        original_count = len(data.get("tables", []))
        
        # Only keep tables that are in our mapping
        data["tables"] = [t for t in data.get("tables", []) if t.get("id") in table_mapping]
        
        # Update IDs
        for table in data["tables"]:
            old_id = table.get("id")
            if old_id in table_mapping:
                table["id"] = table_mapping[old_id]
        
        with open(tables_path, "w") as f:
            json.dump(data, f, indent=4)
        
        print(f"Updated tables.json: {original_count} -> {len(data['tables'])} tables")
        return True
    except Exception as e:
        print(f"Error updating tables.json: {e}")
        return False


def update_field_references(obj, field_mapping):
    """Recursively update field references in a nested structure"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            obj[key] = update_field_references(value, field_mapping)
        return obj
    elif isinstance(obj, list):
        # Handle field references like ["field", field_id, {...}]
        if (len(obj) >= 2 and obj[0] == "field" and 
            isinstance(obj[1], int) and obj[1] in field_mapping):
            obj[1] = field_mapping[obj[1]]
        return [update_field_references(item, field_mapping) for item in obj]
    else:
        return obj


def update_table_references(obj, table_mapping):
    """Recursively update table references (source-table) in a nested structure"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "source-table" and isinstance(value, int) and value in table_mapping:
                obj[key] = table_mapping[value]
            else:
                obj[key] = update_table_references(value, table_mapping)
        return obj
    elif isinstance(obj, list):
        return [update_table_references(item, table_mapping) for item in obj]
    else:
        return obj


def update_questions_json(table_mapping, field_mapping):
    """Update questions.json with new table and field IDs"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    questions_path = os.path.join(script_dir, "..", "questions.json")
    
    try:
        with open(questions_path, "r") as f:
            data = json.load(f)
        
        original_count = len(data.get("questions", []))
        
        # Only keep questions that reference tables we have mappings for
        valid_questions = []
        for question in data.get("questions", []):
            table_id = question.get("table_id")
            if table_id in table_mapping:
                # Update table_id
                question["table_id"] = table_mapping[table_id]
                
                # Update dataset_query references
                question["dataset_query"] = update_table_references(
                    question.get("dataset_query", {}), table_mapping
                )
                question["dataset_query"] = update_field_references(
                    question["dataset_query"], field_mapping
                )
                
                # Ensure visualization_settings is always a dict (required by Metabase API)
                if "visualization_settings" not in question or question["visualization_settings"] is None:
                    question["visualization_settings"] = {}
                
                valid_questions.append(question)
        
        data["questions"] = valid_questions
        
        with open(questions_path, "w") as f:
            json.dump(data, f, indent=4)
        
        print(f"Updated questions.json: {original_count} -> {len(data['questions'])} questions")
        return True
    except Exception as e:
        print(f"Error updating questions.json: {e}")
        return False


def update_dashboard_cards_json(table_mapping):
    """Update dashboard_cards.json with new table IDs"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cards_path = os.path.join(script_dir, "..", "dashboard_cards.json")
    
    try:
        with open(cards_path, "r") as f:
            data = json.load(f)
        
        original_count = len(data.get("dashboard_cards", []))
        
        # Update table IDs in nested card objects
        for card in data.get("dashboard_cards", []):
            if "card" in card:
                table_id = card["card"].get("table_id")
                if table_id in table_mapping:
                    card["card"]["table_id"] = table_mapping[table_id]
        
        with open(cards_path, "w") as f:
            json.dump(data, f, indent=4)
        
        print(f"Updated dashboard_cards.json: {len(data['dashboard_cards'])} cards processed")
        return True
    except Exception as e:
        print(f"Error updating dashboard_cards.json: {e}")
        return False


def main():
    print("=== Metabase Template Cleaner ===")
    print(f"Connecting to Metabase at {METABASE_HOST}\n")
    
    # Get current Metabase state
    metabase_tables = get_metabase_tables()
    if not metabase_tables:
        print("Error: Could not fetch tables from Metabase")
        return False
    
    print(f"Found {len(metabase_tables)} tables in Metabase")
    
    # Create mappings
    table_mapping, field_mapping = create_id_mappings(metabase_tables)
    
    if not table_mapping:
        print("Error: Could not create table mappings")
        return False
    
    print("\n=== Updating Template Files ===\n")
    
    # Update files
    success = True
    success = update_tables_json(table_mapping) and success
    success = update_questions_json(table_mapping, field_mapping) and success
    success = update_dashboard_cards_json(table_mapping) and success
    
    if success:
        print("\n=== Template Cleaning Complete ===")
        print("Template files are now ready for import_metabase.py")
        return True
    else:
        print("\n=== Template Cleaning Failed ===")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

import requests
from requests.exceptions import HTTPError
import json
import os
import argparse
from dotenv import load_dotenv

load_dotenv()

METABASE_PORT = os.getenv("METABASE_PORT")
METABASE_HOST = "http://127.0.0.1:" + METABASE_PORT
METABASE_API_KEY = os.getenv("METABASE_API_KEY")


def find_existing_item_by_name(existing_items, name):
    """Find existing item by name to enable updates instead of recreation."""
    for item in existing_items:
        if item.get("name") == name and item.get("creator_id") != 13371338:
            return item
    return None


def get_existing_items(endpoint, headers):
    """Get existing items from Metabase API."""
    try:
        response = requests.get(f"{METABASE_HOST}/api/{endpoint}", headers=headers)
        response.raise_for_status()
        return response.json()
    except HTTPError as e:
        print(f"Failed to fetch existing {endpoint}: {e}")
        return []


def import_or_update_item(item, item_type, endpoint, existing_items, headers):
    """Import or update an item, returning the item's ID."""
    name = item.get("name", "Unknown")
    existing_item = find_existing_item_by_name(existing_items, name)

    if existing_item:
        # Update existing item
        item_id = existing_item["id"]
        cleaned_payload = clean_payload(item, item_type)
        try:
            response = requests.put(
                f"{METABASE_HOST}/api/{endpoint}/{item_id}",
                headers=headers,
                json=cleaned_payload,
            )
            response.raise_for_status()
            print(f"Updated {item_type}: {name}")
            return item_id
        except HTTPError as e:
            print(f"Failed to update {item_type} '{name}': {e}")
            return None
    else:
        # Create new item
        cleaned_payload = clean_payload(item, item_type)
        try:
            response = requests.post(
                f"{METABASE_HOST}/api/{endpoint}", headers=headers, json=cleaned_payload
            )
            response.raise_for_status()
            new_item = response.json()
            print(f"Created {item_type}: {name}")
            return new_item["id"]
        except HTTPError as e:
            print(f"Failed to create {item_type} '{name}': {e}")
            return None


def update_dashboard_with_cards(dashboard_id, dashboard_cards, headers):
    """Update a dashboard with its cards using the dashboard PUT endpoint."""
    try:
        # Get current dashboard
        print(f"Getting current dashboard {dashboard_id}...")
        response = requests.get(
            f"{METABASE_HOST}/api/dashboard/{dashboard_id}", headers=headers
        )
        response.raise_for_status()
        dashboard = response.json()
        print(
            f"Current dashboard has {len(dashboard.get('dashcards', []))} existing cards"
        )

        # Let's try a different approach: keep existing card structure but update the card_id
        existing_cards = dashboard.get("dashcards", [])
        updated_cards = []

        # First, let's see if we can map existing cards to imported cards
        for existing_card in existing_cards:
            existing_card_id = existing_card.get("card_id")
            print(f"Found existing card with card_id: {existing_card_id}")

            # Find matching imported card
            matching_imported_card = None
            for imported_card in dashboard_cards:
                if imported_card.get("card_id") == existing_card_id:
                    matching_imported_card = imported_card
                    break

            if matching_imported_card:
                print(f"Found matching imported card for card_id {existing_card_id}")
                # Keep the existing card but update it with imported data
                updated_card = existing_card.copy()
                # Update specific fields from the imported card
                for key in [
                    "size_x",
                    "size_y",
                    "row",
                    "col",
                    "parameter_mappings",
                    "visualization_settings",
                ]:
                    if key in matching_imported_card:
                        updated_card[key] = matching_imported_card[key]
                updated_cards.append(updated_card)
            else:
                print(
                    f"No matching imported card found for existing card_id {existing_card_id}"
                )
                # Keep the existing card as-is
                updated_cards.append(existing_card)

        # Add any new imported cards that don't exist yet
        for imported_card in dashboard_cards:
            imported_card_id = imported_card.get("card_id")
            # Check if this card_id already exists
            exists = any(
                card.get("card_id") == imported_card_id for card in updated_cards
            )
            if not exists:
                print(f"Adding new card with card_id {imported_card_id}")
                # For new cards, we need to provide a temporary negative ID that Metabase will replace
                # This is a common pattern in Metabase - use negative IDs for new items
                temp_id = -(len(updated_cards) + 1)

                new_card = {
                    "id": temp_id,  # Temporary negative ID for new cards
                    "size_x": imported_card.get("size_x", 4),
                    "size_y": imported_card.get("size_y", 4),
                    "row": imported_card.get("row", 0),
                    "col": imported_card.get("col", 0),
                    "card_id": imported_card.get("card_id"),
                    "parameter_mappings": imported_card.get("parameter_mappings", []),
                    "visualization_settings": imported_card.get(
                        "visualization_settings", {}
                    ),
                    "series": imported_card.get("series", []),
                }

                # Add optional fields if they exist and are not None
                for optional_field in ["dashboard_tab_id", "action_id"]:
                    if imported_card.get(optional_field) is not None:
                        new_card[optional_field] = imported_card[optional_field]

                updated_cards.append(new_card)

        print(f"Final card count: {len(updated_cards)} cards")
        for i, card in enumerate(updated_cards):
            print(
                f"Card {i+1}: id={card.get('id', 'NEW')}, card_id={card.get('card_id')}"
            )

        # Update dashboard with cards
        dashboard["dashcards"] = updated_cards

        # Clean dashboard payload for PUT request
        clean_dashboard = {
            k: v
            for k, v in dashboard.items()
            if k
            not in [
                "id",
                "created_at",
                "updated_at",
                "entity_id",
                "last-edit-info",
                "last_viewed_at",
                "last_used_param_values",
                "view_count",
            ]
        }

        # Update the dashboard
        print(f"Sending PUT request to update dashboard {dashboard_id}...")
        response = requests.put(
            f"{METABASE_HOST}/api/dashboard/{dashboard_id}",
            headers=headers,
            json=clean_dashboard,
        )

        # Enhanced error handling
        if response.status_code != 200:
            print(f"HTTP {response.status_code}: {response.reason}")
            print(f"Response text: {response.text}")
            try:
                error_data = response.json()
                print(f"Error details: {error_data}")
            except:
                print("Could not parse error response as JSON")

        response.raise_for_status()
        print(
            f"Successfully updated dashboard {dashboard_id} with {len(updated_cards)} card(s)"
        )
        return True
    except HTTPError as e:
        print(f"Failed to update dashboard {dashboard_id} with cards: {e}")
        if hasattr(e, "response") and e.response:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
        return False


def clean_payload(item, item_type="card"):
    """Clean the payload to only include fields allowed for creation/update."""
    if item_type == "card":
        allowed_fields = {
            "name",
            "dataset_query",
            "display",
            "visualization_settings",
            "collection_id",
            "description",
            "parameters",
            "parameter_mappings",
            "cache_ttl",
            "enable_embedding",
            "embedding_params",
            "public_uuid",
            "collection_position",
            "collection_preview",
            "archived",
            "type",
            "query_type",
            "table_id",
            "result_metadata",
        }
        # Ensure required fields
        cleaned = {k: v for k, v in item.items() if k in allowed_fields}
        if "name" not in cleaned:
            cleaned["name"] = item.get("name", "Unnamed Question")
        if "dataset_query" not in cleaned:
            cleaned["dataset_query"] = item.get("dataset_query")
        if "display" not in cleaned:
            cleaned["display"] = item.get("display", "table")
        if "type" not in cleaned:
            cleaned["type"] = "question"
    elif item_type == "dashboard":
        allowed_fields = {
            "name",
            "description",
            "parameters",
            "collection_id",
            "collection_position",
            "archived",
            "enable_embedding",
            "embedding_params",
            "cache_ttl",
        }
        cleaned = {k: v for k, v in item.items() if k in allowed_fields}
        if "name" not in cleaned:
            cleaned["name"] = item.get("name", "Unnamed Dashboard")
    elif item_type == "query_snippet":
        allowed_fields = {"name", "description", "content", "collection_id", "archived"}
        cleaned = {k: v for k, v in item.items() if k in allowed_fields}
        if "name" not in cleaned:
            cleaned["name"] = item.get("name", "Unnamed Query Snippet")
        if "content" not in cleaned:
            cleaned["content"] = item.get("content", "")

    return cleaned


def create_table_mapping(source_tables, target_tables):
    """Create mapping from source table_id to target table_id by matching name and schema."""
    table_mapping = {}

    print(
        f"Creating table mapping with {len(source_tables)} source tables and {len(target_tables)} target tables"
    )

    for source_table in source_tables:
        source_id = source_table.get("id")
        source_name = source_table.get("name")
        source_schema = source_table.get("schema")
        source_db_id = source_table.get("db_id")

        print(
            f"Looking for target match for source table: id={source_id}, name='{source_name}', schema='{source_schema}', db_id={source_db_id}"
        )

        # Find matching target table
        for target_table in target_tables:
            target_name = target_table.get("name")
            target_schema = target_table.get("schema")
            target_db_id = target_table.get("db_id")
            target_id = target_table.get("id")

            if (
                target_name == source_name
                and target_schema == source_schema
                and target_db_id == source_db_id
            ):
                table_mapping[source_id] = target_id
                print(
                    f"✓ Mapped table '{source_name}' (schema: {source_schema}, db: {source_db_id}) from {source_id} to {target_id}"
                )
                break
        else:
            print(
                f"✗ No match found for source table: id={source_id}, name='{source_name}', schema='{source_schema}', db_id={source_db_id}"
            )

    print(f"Final table mapping: {table_mapping}")
    return table_mapping


def create_field_mapping(source_fields, target_fields, table_mapping):
    """Create mapping from source field_id to target field_id by matching name and table."""
    field_mapping = {}

    print(
        f"Creating field mapping with {len(source_fields)} source fields and {len(target_fields)} target fields"
    )
    print(f"Table mapping: {table_mapping}")

    for source_field in source_fields:
        source_id = source_field.get("id")
        source_name = source_field.get("name")
        source_table_id = source_field.get("table_id")

        # Only map fields if we have a table mapping for this field's table
        if source_table_id in table_mapping:
            target_table_id = table_mapping[source_table_id]

            # Find matching target field
            for target_field in target_fields:
                target_name = target_field.get("name")
                target_table_id_check = target_field.get("table_id")
                target_id = target_field.get("id")

                if (
                    target_name == source_name
                    and target_table_id_check == target_table_id
                ):
                    field_mapping[source_id] = target_id
                    print(
                        f"✓ Mapped field '{source_name}' (table: {source_table_id} -> {target_table_id}) from {source_id} to {target_id}"
                    )
                    break
            else:
                print(
                    f"✗ No match found for source field: id={source_id}, name='{source_name}', table_id={source_table_id}"
                )
        else:
            print(
                f"  Skipping field {source_id} - no table mapping for table_id {source_table_id}"
            )

    return field_mapping


def update_field_references(obj, field_mapping):
    """Recursively update field references in a nested data structure."""
    if isinstance(obj, dict):
        # Recursively process all values in dict
        for key, value in obj.items():
            obj[key] = update_field_references(value, field_mapping)
        return obj
    elif isinstance(obj, list):
        # Handle field references like ["field", field_id, {...}]
        if (
            len(obj) >= 2
            and obj[0] == "field"
            and isinstance(obj[1], int)
            and obj[1] in field_mapping
        ):
            old_field_id = obj[1]
            obj[1] = field_mapping[old_field_id]
            print(f"Updated field reference from {old_field_id} to {obj[1]}")

        # Recursively process all items in list
        return [update_field_references(item, field_mapping) for item in obj]
    else:
        return obj


def update_table_references(obj, table_mapping):
    """Recursively update table references (source-table) in a nested data structure."""
    if isinstance(obj, dict):
        # Recursively process all values in dict
        for key, value in obj.items():
            if (
                key == "source-table"
                and isinstance(value, int)
                and value in table_mapping
            ):
                print(
                    f"  ✓ Updated source-table from {value} to {table_mapping[value]}"
                )
                obj[key] = table_mapping[value]
            else:
                obj[key] = update_table_references(value, table_mapping)
        return obj
    elif isinstance(obj, list):
        # Recursively process all items in list
        return [update_table_references(item, table_mapping) for item in obj]
    else:
        return obj


def update_table_id_in_question(question, table_mapping, field_mapping):
    """Update table_id and field references in question's dataset_query and top-level table_id if they exist."""
    question_name = question.get("name", "Unknown")
    print(f"\nUpdating question '{question_name}'")

    # Update top-level table_id
    top_level_table_id = question.get("table_id")
    print(f"  Top-level table_id: {top_level_table_id}")
    if (
        top_level_table_id
        and isinstance(top_level_table_id, int)
        and top_level_table_id in table_mapping
    ):
        old_table_id = top_level_table_id
        question["table_id"] = table_mapping[top_level_table_id]
        print(
            f"  ✓ Updated top-level table_id from {old_table_id} to {question['table_id']}"
        )
    else:
        print(f"  ✗ No mapping found for top-level table_id {top_level_table_id}")

    # Update source-table references and field references in dataset_query
    dataset_query = question.get("dataset_query", {})

    # Update all source-table references recursively
    print("  Updating table references...")
    dataset_query = update_table_references(dataset_query, table_mapping)

    # Update field references throughout the query
    print("  Updating field references...")
    dataset_query = update_field_references(dataset_query, field_mapping)

    return question


def import_metabase_content():
    global METABASE_API_KEY
    script_dir = os.path.dirname(os.path.abspath(__file__))
    headers = {"Content-Type": "application/json", "x-api-key": METABASE_API_KEY}

    print("Starting improved Metabase content import...")

    # Load source tables and create mapping
    try:
        with open(os.path.join(script_dir, "..", "tables.json"), "r") as f:
            tables_data = json.load(f)
        source_tables = tables_data.get("tables", [])
    except FileNotFoundError:
        print("tables.json not found, skipping table mapping")
        source_tables = []

    # Load source fields
    try:
        with open(os.path.join(script_dir, "..", "fields.json"), "r") as f:
            fields_data = json.load(f)
        source_fields = fields_data.get("fields", [])
    except FileNotFoundError:
        print("fields.json not found, skipping field mapping")
        source_fields = []

    # Get target tables
    target_tables = get_existing_items("table", headers)

    # Get target fields by fetching metadata for each target table
    target_fields = []
    for table in target_tables:
        try:
            table_metadata_response = requests.get(
                f"{METABASE_HOST}/api/table/{table['id']}/query_metadata",
                headers=headers,
            )
            table_metadata_response.raise_for_status()
            table_metadata = table_metadata_response.json()
            table_fields = table_metadata.get("fields", [])
            for field in table_fields:
                field["table_id"] = table["id"]  # Associate field with its table
                target_fields.append(field)
        except Exception as e:
            print(f"Failed to fetch fields for target table {table['id']}: {e}")

    # Create table ID mapping
    table_mapping = create_table_mapping(source_tables, target_tables)

    # Create field ID mapping
    field_mapping = create_field_mapping(source_fields, target_fields, table_mapping)

    # Get existing items to enable updates instead of recreating
    existing_queries = get_existing_items("native-query-snippet", headers)
    existing_questions = get_existing_items("card", headers)
    existing_dashboards = get_existing_items("dashboard", headers)

    # Import query snippets first (dependencies for questions)
    print("\n=== Importing Query Snippets ===")
    try:
        with open(os.path.join(script_dir, "..", "query_snippets.json"), "r") as f:
            queries_data = json.load(f)
        queries = queries_data.get("queries", [])
    except FileNotFoundError:
        print("query_snippets.json not found, skipping query snippets import")
        queries = []

    for query in queries:
        import_or_update_item(
            query, "query_snippet", "native-query-snippet", existing_queries, headers
        )

    # Import questions second (dependencies for dashboards)
    print("\n=== Importing Questions ===")
    with open(os.path.join(script_dir, "..", "questions.json"), "r") as f:
        questions_data = json.load(f)
    questions = questions_data.get("questions", [])

    # Keep track of old question ID -> new question ID mapping
    question_id_mapping = {}

    for question in questions:
        # Update table_id and field references in question before importing
        updated_question = update_table_id_in_question(
            question, table_mapping, field_mapping
        )

        old_question_id = question.get("id")
        new_question_id = import_or_update_item(
            updated_question, "card", "card", existing_questions, headers
        )
        if old_question_id and new_question_id:
            question_id_mapping[old_question_id] = new_question_id

    # Import dashboards third
    print("\n=== Importing Dashboards ===")
    with open(os.path.join(script_dir, "..", "dashboards.json"), "r") as f:
        dashboards_data = json.load(f)
    dashboards = dashboards_data.get("dashboards", [])

    # Keep track of old dashboard ID -> new dashboard ID mapping
    dashboard_id_mapping = {}

    for dashboard in dashboards:
        old_dashboard_id = dashboard.get("id")
        new_dashboard_id = import_or_update_item(
            dashboard, "dashboard", "dashboard", existing_dashboards, headers
        )
        if old_dashboard_id and new_dashboard_id:
            dashboard_id_mapping[old_dashboard_id] = new_dashboard_id

    # Import dashboard cards using the dashboard PUT endpoint
    print("\n=== Importing Dashboard Cards ===")
    try:
        with open(os.path.join(script_dir, "..", "dashboard_cards.json"), "r") as f:
            dashboard_cards_data = json.load(f)
        dashboard_cards = dashboard_cards_data.get("dashboard_cards", [])
    except FileNotFoundError:
        print("dashboard_cards.json not found, skipping dashboard cards import")
        dashboard_cards = []

    # Group cards by dashboard and update card_id references
    dashboard_cards_map = {}
    for dashboard_card in dashboard_cards:
        old_dashboard_id = dashboard_card.get("dashboard_id")
        if old_dashboard_id and old_dashboard_id in dashboard_id_mapping:
            new_dashboard_id = dashboard_id_mapping[old_dashboard_id]

            # Update the card_id reference to the new question ID
            old_card_id = dashboard_card.get("card_id")
            if old_card_id and old_card_id in question_id_mapping:
                new_card_id = question_id_mapping[old_card_id]
                dashboard_card["card_id"] = new_card_id

                if new_dashboard_id not in dashboard_cards_map:
                    dashboard_cards_map[new_dashboard_id] = []
                dashboard_cards_map[new_dashboard_id].append(dashboard_card)
            else:
                print(f"Warning: Could not find mapping for card_id {old_card_id}")

    # Update each dashboard with its cards
    for new_dashboard_id, cards in dashboard_cards_map.items():
        print(f"\nProcessing dashboard {new_dashboard_id} with {len(cards)} card(s)")
        update_dashboard_with_cards(new_dashboard_id, cards, headers)

    print("\n=== Import Complete ===")
    print("Dashboard cards should now be visible in your dashboards!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import Metabase content (improved version)"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local API key (METABASE_API_KEY_LOCAL)",
    )
    args = parser.parse_args()

    if args.local:
        METABASE_API_KEY = os.getenv("METABASE_API_KEY_LOCAL")
    else:
        METABASE_API_KEY = os.getenv("METABASE_API_KEY")

    import_metabase_content()

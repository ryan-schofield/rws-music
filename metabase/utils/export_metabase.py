import requests
import json
import os
import argparse
from dotenv import load_dotenv

load_dotenv()

METABASE_PORT = os.getenv("METABASE_PORT")
METABASE_HOST = "http://127.0.0.1:" + METABASE_PORT
METABASE_API_KEY = os.getenv("METABASE_API_KEY")


def export_metabase_content():
    global METABASE_API_KEY
    script_dir = os.path.dirname(os.path.abspath(__file__))
    headers = {"Content-Type": "application/json", "x-api-key": METABASE_API_KEY}

    # Export questions
    questions_response = requests.get(f"{METABASE_HOST}/api/card", headers=headers)
    questions_response.raise_for_status()
    questions = questions_response.json()
    # Filter out default/example items (creator_id == 13371338)
    questions = [q for q in questions if q.get("creator_id") != 13371338]
    # Save to file
    with open(os.path.join(script_dir, "..", "questions.json"), "w") as f:
        json.dump({"questions": questions}, f, indent=4)

    # Export dashboards
    dashboards_response = requests.get(
        f"{METABASE_HOST}/api/dashboard", headers=headers
    )
    dashboards_response.raise_for_status()
    dashboards = dashboards_response.json()
    # Filter out default/example items (creator_id == 13371338)
    dashboards = [d for d in dashboards if d.get("creator_id") != 13371338]

    # Export dashboard cards for each dashboard
    dashboard_cards = []
    for dashboard in dashboards:
        try:
            dashboard_response = requests.get(
                f"{METABASE_HOST}/api/dashboard/{dashboard['id']}", headers=headers
            )
            dashboard_response.raise_for_status()
            dashboard_data = dashboard_response.json()

            cards = dashboard_data.get("dashcards", [])
            if not cards:
                print(f"No cards found in 'dashcards' for dashboard {dashboard['id']}")
            for card in cards:
                card["dashboard_id"] = dashboard["id"]  # Associate with dashboard
                dashboard_cards.append(card)
        except Exception as e:
            print(f"Failed to export cards for dashboard {dashboard['id']}: {e}")

    # Save dashboards to file
    with open(os.path.join(script_dir, "..", "dashboards.json"), "w") as f:
        json.dump({"dashboards": dashboards}, f, indent=4)

    # Save dashboard cards to file
    with open(os.path.join(script_dir, "..", "dashboard_cards.json"), "w") as f:
        json.dump({"dashboard_cards": dashboard_cards}, f, indent=4)

    # Export tables
    tables_response = requests.get(f"{METABASE_HOST}/api/table", headers=headers)
    tables_response.raise_for_status()
    tables = tables_response.json()

    # Export field metadata for each table
    fields = []
    for table in tables:
        try:
            table_metadata_response = requests.get(
                f"{METABASE_HOST}/api/table/{table['id']}/query_metadata", headers=headers
            )
            table_metadata_response.raise_for_status()
            table_metadata = table_metadata_response.json()
            table_fields = table_metadata.get("fields", [])
            for field in table_fields:
                field["table_id"] = table["id"]  # Associate field with its table
                fields.append(field)
        except Exception as e:
            print(f"Failed to export fields for table {table['id']}: {e}")

    # Save tables and fields to files
    with open(os.path.join(script_dir, "..", "tables.json"), "w") as f:
        json.dump({"tables": tables}, f, indent=4)
    with open(os.path.join(script_dir, "..", "fields.json"), "w") as f:
        json.dump({"fields": fields}, f, indent=4)

    # Export queries
    queries_response = requests.get(
        f"{METABASE_HOST}/api/native-query-snippet", headers=headers
    )
    queries_response.raise_for_status()
    queries = queries_response.json()
    # Filter out default/example items (creator_id == 13371338)
    queries = [q for q in queries if q.get("creator_id") != 13371338]
    # Save to file
    with open(os.path.join(script_dir, "..", "query_snippets.json"), "w") as f:
        json.dump({"queries": queries}, f, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Metabase content")
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

    export_metabase_content()

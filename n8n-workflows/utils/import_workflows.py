#!/usr/bin/env python
"""
n8n Workflow Import Utility

Imports workflows from JSON files to n8n via API.
Returns workflow IDs for reference in other scripts.

Usage:
    python import_workflows.py [--host localhost] [--port 5678] [--workflow-file workflow.json]
"""

import os
import sys
import json
import argparse
import requests
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


class N8nWorkflowImporter:
    """Handles import of n8n workflows from JSON files."""

    def __init__(self, host: str = "localhost", port: int = 5678, api_key: str = None):
        """
        Initialize the importer.

        Args:
            host: n8n host address
            port: n8n port number
            api_key: Optional API key for authentication
        """
        self.host = host
        self.port = port
        self.api_key = api_key
        self.base_url = f"http://{host}:{port}/api/v1"
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["X-N8N-API-KEY"] = api_key

    def workflow_exists(self, workflow_name: str) -> Dict[str, Any]:
        """
        Check if a workflow exists by name.

        Args:
            workflow_name: Name of the workflow to check

        Returns:
            Workflow data if exists, None otherwise
        """
        try:
            response = requests.get(
                f"{self.base_url}/workflows",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            workflows = response.json()

            # Handle both paginated and direct responses
            if isinstance(workflows, dict) and "data" in workflows:
                workflows = workflows["data"]

            for wf in workflows:
                if wf.get("name") == workflow_name:
                    return wf

            return None

        except Exception as e:
            print(f"Warning: Could not check existing workflows: {e}")
            return None

    def import_workflow(self, workflow_data: Dict[str, Any], update: bool = False) -> Dict[str, Any]:
        """
        Import or update a workflow.

        Args:
            workflow_data: Workflow definition dictionary
            update: If True, update existing workflow; if False, create new

        Returns:
            Response from n8n API
        """
        workflow_name = workflow_data.get("name", "Untitled")

        # Check if workflow already exists
        existing = self.workflow_exists(workflow_name)

        if existing and not update:
            print(f"Workflow '{workflow_name}' already exists (ID: {existing['id']})")
            return existing

        # Prepare payload - remove id if creating new
        payload = workflow_data.copy()
        if not update or not existing:
            payload.pop("id", None)

        try:
            if update and existing:
                # Update existing workflow
                workflow_id = existing["id"]
                response = requests.put(
                    f"{self.base_url}/workflows/{workflow_id}",
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )
                response.raise_for_status()
                result = response.json()
                print(f"✓ Updated: {workflow_name} (ID: {workflow_id})")
                return result
            else:
                # Create new workflow
                response = requests.post(
                    f"{self.base_url}/workflows",
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )
                response.raise_for_status()
                result = response.json()
                workflow_id = result.get("id", result.get("workflow", {}).get("id"))
                print(f"✓ Created: {workflow_name} (ID: {workflow_id})")
                return result

        except requests.exceptions.HTTPError as e:
            raise Exception(f"API error importing {workflow_name}: {e.response.status_code} {e.response.text}")
        except Exception as e:
            raise Exception(f"Error importing {workflow_name}: {e}")

    def import_from_file(self, filepath: str, update: bool = False) -> Dict[str, Any]:
        """
        Import workflow from JSON file.

        Args:
            filepath: Path to workflow JSON file
            update: If True, update existing workflow

        Returns:
            Response from n8n API
        """
        with open(filepath, "r") as f:
            workflow_data = json.load(f)

        return self.import_workflow(workflow_data, update=update)

    def import_from_directory(self, directory: str, update: bool = False) -> List[Dict[str, Any]]:
        """
        Import all workflow JSON files from a directory.

        Args:
            directory: Directory containing workflow JSON files
            update: If True, update existing workflows

        Returns:
            List of import results
        """
        dir_path = Path(directory)
        workflow_files = sorted(dir_path.glob("*.json"))

        # Skip metadata.json
        workflow_files = [f for f in workflow_files if f.name != "metadata.json"]

        if not workflow_files:
            print(f"No workflow JSON files found in {directory}")
            return []

        results = []
        print(f"Importing {len(workflow_files)} workflow(s) from {directory}...")

        for filepath in workflow_files:
            try:
                result = self.import_from_file(str(filepath), update=update)
                results.append(result)
            except Exception as e:
                print(f"✗ Failed to import {filepath.name}: {e}")

        return results

    def verify_connection(self) -> bool:
        """
        Verify connection to n8n API.

        Returns:
            True if connection successful
        """
        try:
            response = requests.get(
                f"{self.base_url}/health",
                headers=self.headers,
                timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Import n8n workflows from JSON files to n8n"
    )
    parser.add_argument(
        "--host",
        default=os.getenv("N8N_HOST", "localhost"),
        help="n8n host address (default: localhost, can set via N8N_HOST env var)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("N8N_PORT", "5678")),
        help="n8n port number (default: 5678, can set via N8N_PORT env var)"
    )
    parser.add_argument(
        "--workflow-file",
        help="Path to single workflow JSON file to import"
    )
    parser.add_argument(
        "--workflow-dir",
        default=".",
        help="Directory containing workflow JSON files (default: current directory)"
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("N8N_API_KEY"),
        help="n8n API key (can set via N8N_API_KEY env var)"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing workflows instead of skipping them"
    )

    args = parser.parse_args()

    importer = N8nWorkflowImporter(
        host=args.host,
        port=args.port,
        api_key=args.api_key
    )

    # Verify connection
    print(f"Connecting to n8n at {args.host}:{args.port}...")
    if not importer.verify_connection():
        print("✗ Failed to connect to n8n API")
        sys.exit(1)

    print("✓ Connection successful")

    try:
        if args.workflow_file:
            # Import single file
            result = importer.import_from_file(args.workflow_file, update=args.update)
            workflow_id = result.get("id", result.get("workflow", {}).get("id"))
            print(f"\n✓ Import completed. Workflow ID: {workflow_id}")
            return 0
        else:
            # Import from directory
            results = importer.import_from_directory(args.workflow_dir, update=args.update)
            print(f"\n✓ Import completed. {len(results)} workflow(s) imported")
            return 0

    except Exception as e:
        print(f"\n✗ Import failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

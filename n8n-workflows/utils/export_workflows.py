#!/usr/bin/env python
"""
n8n Workflow Export Utility

Exports all workflows from n8n API to JSON files for version control.
Can be run manually or via cron for automated backups.

Usage:
    python export_workflows.py [--host localhost] [--port 5678]
"""

import os
import sys
import json
import argparse
import requests
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


class N8nWorkflowExporter:
    """Handles export of n8n workflows to JSON files."""

    def __init__(self, host: str = "localhost", port: int = 5678, api_key: str = None):
        """
        Initialize the exporter.

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

    def get_workflows(self) -> List[Dict[str, Any]]:
        """
        Fetch all workflows from n8n API.

        Returns:
            List of workflow dictionaries

        Raises:
            Exception: If API call fails
        """
        try:
            response = requests.get(
                f"{self.base_url}/workflows",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            # Handle both paginated and direct responses
            if isinstance(data, dict) and "data" in data:
                return data["data"]
            elif isinstance(data, list):
                return data
            else:
                raise ValueError(f"Unexpected API response format: {data}")

        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Failed to connect to n8n at {self.base_url}: {e}")
        except requests.exceptions.HTTPError as e:
            raise Exception(f"API error: {e.response.status_code} {e.response.text}")

    def export_workflows(self, output_dir: str = ".") -> Dict[str, str]:
        """
        Export all workflows to JSON files.

        Args:
            output_dir: Directory to save workflow files

        Returns:
            Dictionary mapping workflow IDs to file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        workflows = self.get_workflows()
        exported = {}

        print(f"Exporting {len(workflows)} workflow(s)...")

        for workflow in workflows:
            workflow_id = workflow.get("id")
            workflow_name = workflow.get("name", "untitled").replace(" ", "_").lower()

            # Determine output filename
            filename = f"{workflow_name}_{workflow_id}.json"
            filepath = output_path / filename

            # Save workflow JSON
            with open(filepath, "w") as f:
                json.dump(workflow, f, indent=2)

            exported[workflow_id] = str(filepath)
            print(f"✓ Exported: {workflow_name} (ID: {workflow_id}) → {filename}")

        # Write metadata file
        metadata = {
            "exported_at": datetime.now().isoformat(),
            "total_workflows": len(workflows),
            "workflows": {
                wf.get("id"): wf.get("name")
                for wf in workflows
            }
        }

        metadata_path = output_path / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"\n✓ Metadata saved to {metadata_path}")
        return exported

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
        description="Export n8n workflows to JSON files for version control"
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
        "--output",
        default=".",
        help="Output directory for workflow JSON files (default: current directory)"
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("N8N_API_KEY"),
        help="n8n API key (can set via N8N_API_KEY env var)"
    )

    args = parser.parse_args()

    exporter = N8nWorkflowExporter(
        host=args.host,
        port=args.port,
        api_key=args.api_key
    )

    # Verify connection
    print(f"Connecting to n8n at {args.host}:{args.port}...")
    if not exporter.verify_connection():
        print("✗ Failed to connect to n8n API")
        sys.exit(1)

    print("✓ Connection successful")

    try:
        exporter.export_workflows(output_dir=args.output)
        print("\n✓ Export completed successfully")
        return 0
    except Exception as e:
        print(f"\n✗ Export failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

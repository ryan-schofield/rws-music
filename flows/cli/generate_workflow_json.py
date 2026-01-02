#!/usr/bin/env python3
"""
Generate n8n workflow JSON files from workflow definitions.

This script generates the actual JSON files that will be imported into n8n,
making them easy to source control, inspect, and debug.
"""

import json
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from workflow_builders import build_spotify_ingestion_workflow, build_daily_etl_workflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Output directory for JSON files
OUTPUT_DIR = Path(__file__).parent.parent.parent / "n8n-workflows"
OUTPUT_DIR.mkdir(exist_ok=True)

# Define workflows to generate
WORKFLOWS = {
    "spotify_ingestion": {
        "builder": build_spotify_ingestion_workflow,
        "description": "Ingests recently played tracks every 6 hours",
    },
    "daily_etl": {
        "builder": build_daily_etl_workflow,
        "description": "Complete ETL pipeline running daily at 2 AM",
    },
}


def generate_workflows():
    """Generate all workflow JSON files."""
    logger.info(f"Generating workflow JSON files to {OUTPUT_DIR}")
    
    for workflow_key, workflow_info in WORKFLOWS.items():
        logger.info(f"\nGenerating: {workflow_key}")
        
        # Build workflow (returns dict)
        workflow_dict = workflow_info["builder"]()
        
        # Output path
        output_path = OUTPUT_DIR / f"{workflow_key}.json"
        
        # Write JSON file with pretty formatting
        with open(output_path, "w") as f:
            json.dump(workflow_dict, f, indent=2)
        
        logger.info(f"  ✓ Generated {output_path}")
        logger.info(f"    Nodes: {len(workflow_dict.get('nodes', []))}")
        logger.info(f"    Connections: {len(workflow_dict.get('connections', {}))}")
        
        # Print first few nodes for inspection
        nodes = workflow_dict.get("nodes", [])
        for node in nodes[:3]:
            print(f"    - {node['name']} ({node['type']})")


if __name__ == "__main__":
    generate_workflows()
    logger.info(f"\n✅ All workflow JSON files generated successfully")

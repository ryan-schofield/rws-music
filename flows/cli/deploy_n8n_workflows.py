#!/usr/bin/env python3
"""
n8n workflow deployment script.

Imports and deploys n8n workflows from JSON files.
Handles workflow deployment, updates, and status checking.

Workflow JSON files are source-controlled in n8n-workflows/ directory.

Usage:
    python flows/cli/deploy_n8n_workflows.py --action deploy
    python flows/cli/deploy_n8n_workflows.py --action status
"""

import sys
import json
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flows.cli.n8n_client import N8NClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class WorkflowDeployer:
    """Manages n8n workflow deployment from JSON files."""

    def __init__(
        self,
        n8n_client: N8NClient = None,
        workflows_dir: Path = None,
    ):
        """
        Initialize deployer.

        Args:
            n8n_client: N8NClient instance (default: creates new)
            workflows_dir: Directory containing workflow JSON files (default: n8n-workflows/)
        """
        self.client = n8n_client or N8NClient()
        self.workflows_dir = workflows_dir or Path("n8n-workflows")

        # Discover JSON workflow files
        self.workflows = self._discover_workflows()

    def _discover_workflows(self) -> Dict[str, Path]:
        """
        Discover all workflow JSON files.

        Returns:
            Dict mapping workflow name to file path
        """
        workflows = {}

        if not self.workflows_dir.exists():
            logger.warning(f"Workflows directory not found: {self.workflows_dir}")
            return workflows

        for json_file in self.workflows_dir.glob("*.json"):
            workflow_name = json_file.stem
            workflows[workflow_name] = json_file
            logger.debug(f"Found workflow: {workflow_name} -> {json_file}")

        return workflows

    def load_workflow(self, workflow_path: Path) -> Dict[str, Any]:
        """
        Load workflow definition from JSON file.

        Args:
            workflow_path: Path to workflow JSON file

        Returns:
            Workflow definition dict
        """
        with open(workflow_path, "r") as f:
            return json.load(f)

    def check_connectivity(self) -> bool:
        """
        Check if n8n instance is accessible.

        Returns:
            True if accessible, False otherwise
        """
        logger.info(f"Checking n8n connectivity at {self.client.base_url}...")

        if self.client.is_accessible():
            logger.info("✅ n8n is accessible")
            return True
        else:
            logger.error(f"❌ Failed to connect to n8n at {self.client.base_url}")
            logger.error("   Ensure n8n is running and N8N_BASE_URL is configured")
            return False

    def deploy_all_workflows(self) -> Dict[str, Any]:
        """
        Deploy or update all workflows from JSON files.

        Returns:
            Deployment results
        """
        logger.info("=" * 80)
        logger.info("DEPLOYING ALL WORKFLOWS")
        logger.info("=" * 80)

        if not self.check_connectivity():
            return {"status": "error", "message": "n8n not accessible"}

        if not self.workflows:
            logger.warning(f"No workflow files found in {self.workflows_dir}")
            return {"status": "error", "message": "No workflows found"}

        results = {
            "status": "success",
            "workflows": {},
        }

        for workflow_key, workflow_path in self.workflows.items():
            logger.info(f"\nDeploying workflow: {workflow_key}")
            logger.info(f"  File: {workflow_path}")

            # Load workflow definition from JSON file
            try:
                workflow_def = self.load_workflow(workflow_path)
                logger.info(f"  ✓ Loaded workflow from file")
                logger.info(f"    Nodes: {len(workflow_def.get('nodes', []))}")
            except Exception as e:
                logger.error(f"  ✗ Failed to load workflow: {str(e)}")
                results["workflows"][workflow_key] = {
                    "status": "error",
                    "message": f"Failed to load workflow: {str(e)}",
                }
                results["status"] = "partial"
                continue

            # Check if workflow exists
            workflow_name = workflow_def["name"]
            existing = self.client.find_workflow_by_name(workflow_name)

            if existing:
                # Delete existing workflow first (some n8n versions don't support PATCH update)
                logger.info(f"  Deleting existing workflow (ID: {existing['id']})")
                deleted = self.client.delete_workflow(existing["id"])

                if not deleted:
                    logger.error(f"  ✗ Failed to delete existing workflow")
                    results["workflows"][workflow_key] = {
                        "status": "error",
                        "message": "Failed to delete existing workflow",
                    }
                    results["status"] = "partial"
                    continue

            # Create new/replacement workflow
            logger.info(f"  Creating workflow")
            created = self.client.create_workflow(workflow_def)

            if created:
                logger.info(f"  ✓ Created workflow {created.get('id')}")
                results["workflows"][workflow_key] = {
                    "status": "created",
                    "id": created.get("id"),
                    "name": workflow_name,
                }
            else:
                logger.error(f"  ✗ Failed to create workflow")
                results["workflows"][workflow_key] = {
                    "status": "error",
                    "message": "Failed to create workflow",
                }
                results["status"] = "partial"
                continue

        return results

    def export_all_workflows(self) -> Dict[str, Any]:
        """
        Export all deployed workflows to JSON files.

        Returns:
            Export results
        """
        logger.info("=" * 80)
        logger.info("EXPORTING WORKFLOWS")
        logger.info("=" * 80)

        if not self.check_connectivity():
            return {"status": "error", "message": "n8n not accessible"}

        self.workflows_dir.mkdir(parents=True, exist_ok=True)

        workflows = self.client.list_workflows()
        logger.info(f"Found {len(workflows)} workflows in n8n")

        results = {
            "status": "success",
            "exported": [],
            "skipped": [],
        }

        for workflow in workflows:
            workflow_name = workflow.get("name")
            workflow_id = workflow.get("id")

            # Check if this is one of our managed workflows
            is_managed = any(
                workflow_name == info["builder"]()["name"]
                for info in self.workflow_builders.values()
            )

            if is_managed:
                # Export managed workflow
                for workflow_key, workflow_info in self.workflow_builders.items():
                    if workflow_name == workflow_info["builder"]()["name"]:
                        filepath = self.workflows_dir / workflow_info["filename"]

                        if self.client.export_workflow(workflow_id, filepath):
                            logger.info(f"✓ Exported {workflow_name} to {filepath}")
                            results["exported"].append(
                                {
                                    "name": workflow_name,
                                    "id": workflow_id,
                                    "file": str(filepath),
                                }
                            )
                        else:
                            logger.error(f"✗ Failed to export {workflow_name}")
                            results["status"] = "partial"
                        break
            else:
                # Skip unmanaged workflows
                results["skipped"].append(
                    {
                        "name": workflow_name,
                        "id": workflow_id,
                    }
                )

        logger.info(f"\nExported {len(results['exported'])} workflows")
        if results["skipped"]:
            logger.info(f"Skipped {len(results['skipped'])} unmanaged workflows")

        return results

    def status(self) -> Dict[str, Any]:
        """
        Check status of deployed workflows.

        Returns:
            Status information
        """
        logger.info("=" * 80)
        logger.info("WORKFLOW STATUS")
        logger.info("=" * 80)

        if not self.check_connectivity():
            return {"status": "error", "message": "n8n not accessible"}

        results = {
            "status": "success",
            "workflows": {},
        }

        workflows = self.client.list_workflows()
        logger.info(f"Found {len(workflows)} total workflows\n")

        for workflow_key, workflow_info in self.workflow_builders.items():
            workflow_name = workflow_info["builder"]()["name"]
            existing = self.client.find_workflow_by_name(workflow_name)

            if existing:
                status = "active" if existing.get("active") else "inactive"
                logger.info(f"✓ {workflow_name}: {status} (ID: {existing.get('id')})")
                results["workflows"][workflow_key] = {
                    "deployed": True,
                    "id": existing.get("id"),
                    "active": existing.get("active"),
                }
            else:
                logger.info(f"✗ {workflow_name}: not deployed")
                results["workflows"][workflow_key] = {
                    "deployed": False,
                }

        return results

    def activate_workflow(self, workflow_name: str) -> bool:
        """
        Activate a workflow by name.

        Args:
            workflow_name: Name of workflow to activate

        Returns:
            True if successful
        """
        logger.info(f"Activating workflow: {workflow_name}")

        existing = self.client.find_workflow_by_name(workflow_name)
        if not existing:
            logger.error(f"Workflow not found: {workflow_name}")
            return False

        return self.client.activate_workflow(existing["id"])

    def deactivate_workflow(self, workflow_name: str) -> bool:
        """
        Deactivate a workflow by name.

        Args:
            workflow_name: Name of workflow to deactivate

        Returns:
            True if successful
        """
        logger.info(f"Deactivating workflow: {workflow_name}")

        existing = self.client.find_workflow_by_name(workflow_name)
        if not existing:
            logger.error(f"Workflow not found: {workflow_name}")
            return False

        return self.client.deactivate_workflow(existing["id"])


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Deploy and manage n8n workflows",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python flows/cli/deploy_n8n_workflows.py --action deploy
  python flows/cli/deploy_n8n_workflows.py --action export
  python flows/cli/deploy_n8n_workflows.py --action status
  python flows/cli/deploy_n8n_workflows.py --action activate --workflow "Daily ETL"
        """,
    )

    parser.add_argument(
        "--action",
        choices=["deploy", "export", "status", "activate", "deactivate"],
        default="status",
        help="Action to perform (default: status)",
    )
    parser.add_argument(
        "--workflow",
        type=str,
        help="Workflow name (required for activate/deactivate)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        help="n8n base URL (default: from N8N_BASE_URL env var)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="n8n API key (default: from N8N_API_KEY env var)",
    )

    args = parser.parse_args()

    # Create deployer
    client = N8NClient(
        base_url=args.base_url,
        api_key=args.api_key,
    )
    deployer = WorkflowDeployer(client)

    # Execute action
    try:
        if args.action == "deploy":
            result = deployer.deploy_all_workflows()
            logger.info("\n" + "=" * 80)
            logger.info("DEPLOYMENT COMPLETE")
            logger.info("=" * 80)
            for workflow_key, info in result.get("workflows", {}).items():
                logger.info(f"{workflow_key}: {info['status']}")

        elif args.action == "export":
            result = deployer.export_all_workflows()
            logger.info("\n" + "=" * 80)
            logger.info("EXPORT COMPLETE")
            logger.info("=" * 80)

        elif args.action == "status":
            result = deployer.status()
            logger.info("\n" + "=" * 80)
            logger.info("STATUS COMPLETE")
            logger.info("=" * 80)

        elif args.action == "activate":
            if not args.workflow:
                logger.error("--workflow required for activate action")
                sys.exit(1)
            success = deployer.activate_workflow(args.workflow)
            sys.exit(0 if success else 1)

        elif args.action == "deactivate":
            if not args.workflow:
                logger.error("--workflow required for deactivate action")
                sys.exit(1)
            success = deployer.deactivate_workflow(args.workflow)
            sys.exit(0 if success else 1)

        sys.exit(0)

    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

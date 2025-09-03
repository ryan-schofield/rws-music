#!/usr/bin/env python3
"""
Production-ready deployment script for Prefect flows.

This script deploys all music tracking flows to your Prefect server.
Ensure your Prefect server is running before executing this script.

Usage:
    # Basic deployment
    uv run python scripts/orchestrate/deploy_flows.py
    
    # With validation
    uv run python scripts/orchestrate/deploy_flows.py --validate
    
    # Deploy specific flows  
    uv run python scripts/orchestrate/deploy_flows.py --spotify-only
    uv run python scripts/orchestrate/deploy_flows.py --etl-only
"""

import os
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from prefect.deployments import Deployment
from scripts.orchestrate.prefect_flows import spotify_ingestion_flow, daily_etl_flow
from scripts.orchestrate.prefect_config import PrefectConfig

# Load environment variables
load_dotenv()


class FlowDeployer:
    """Handles deployment of Prefect flows."""

    def __init__(self):
        self.deployed_flows = {}

    def validate_environment(self):
        """Validate environment before deployment."""
        print("Validating environment...")

        # Check Prefect API connection
        try:
            from prefect import get_client

            client = get_client()
            print("[OK] Connected to Prefect server")
        except Exception as e:
            print(f"[ERROR] Cannot connect to Prefect server: {e}")
            print(
                "Make sure Prefect server is running: docker-compose up -d prefect-server"
            )
            return False

        # Validate configuration
        validation = PrefectConfig.validate_configuration()
        if validation["valid"]:
            print("[OK] Configuration validation passed")
        else:
            print("[WARN] Configuration issues found:")
            for missing in validation["missing"]:
                print(f"  - Missing: {missing}")

        return True

    def deploy_spotify_flow(self):
        """Deploy the Spotify ingestion flow."""
        print("\nDeploying Spotify ingestion flow...")

        try:
            deployment = Deployment.build_from_flow(
                flow=spotify_ingestion_flow,
                name="spotify-ingestion",
                version="1.1.0",
                description="Fetch recently played tracks from Spotify API",
                parameters={"limit": 50},
                tags=["spotify", "ingestion", "automated"],
                work_pool_name=None,  # Use default work pool
            )

            deployment_id = deployment.apply()
            self.deployed_flows["spotify"] = {
                "name": "spotify-ingestion",
                "id": deployment_id,
                "status": "success",
            }
            print(f"[OK] Spotify flow deployed (ID: {deployment_id})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to deploy Spotify flow: {e}")
            self.deployed_flows["spotify"] = {"status": "failed", "error": str(e)}
            return False

    def deploy_etl_flow(self):
        """Deploy the daily ETL flow."""
        print("\nDeploying daily ETL flow...")

        try:
            deployment = Deployment.build_from_flow(
                flow=daily_etl_flow,
                name="daily-etl",
                version="1.1.0",
                description="Daily ETL pipeline: Load → Enrich → Transform → Report",
                tags=["etl", "daily", "processing", "automated"],
                work_pool_name=None,  # Use default work pool
            )

            deployment_id = deployment.apply()
            self.deployed_flows["etl"] = {
                "name": "daily-etl",
                "id": deployment_id,
                "status": "success",
            }
            print(f"[OK] ETL flow deployed (ID: {deployment_id})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to deploy ETL flow: {e}")
            self.deployed_flows["etl"] = {"status": "failed", "error": str(e)}
            return False

    def deploy_all(self, spotify_only=False, etl_only=False):
        """Deploy all flows."""
        success_count = 0

        if not spotify_only and not etl_only:
            # Deploy both flows
            if self.deploy_spotify_flow():
                success_count += 1
            if self.deploy_etl_flow():
                success_count += 1
        elif spotify_only:
            if self.deploy_spotify_flow():
                success_count += 1
        elif etl_only:
            if self.deploy_etl_flow():
                success_count += 1

        return success_count

    def print_summary(self):
        """Print deployment summary."""
        print("\n" + "=" * 60)
        print("DEPLOYMENT SUMMARY")
        print("=" * 60)

        successful_flows = []
        failed_flows = []

        for flow_type, info in self.deployed_flows.items():
            if info["status"] == "success":
                successful_flows.append(info)
                print(f"[OK] {info['name']} (ID: {info['id']})")
            else:
                failed_flows.append(info)
                print(f"[ERROR] {flow_type}: {info.get('error', 'Unknown error')}")

        print(
            f"\nResults: {len(successful_flows)} successful, {len(failed_flows)} failed"
        )

        if successful_flows:
            print("\n[SUCCESS] Next Steps:")
            print("1. Open Prefect UI: http://localhost:4200")
            print("2. Navigate to 'Deployments' tab")
            print("3. Click 'Run' on any deployment to test")
            print("4. Monitor execution in 'Flow Runs' tab")

        return len(failed_flows) == 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Deploy Prefect flows for Music Tracker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--validate", action="store_true", help="Validate environment before deployment"
    )
    parser.add_argument(
        "--spotify-only", action="store_true", help="Deploy only Spotify ingestion flow"
    )
    parser.add_argument(
        "--etl-only", action="store_true", help="Deploy only daily ETL flow"
    )

    args = parser.parse_args()

    # Initialize deployer
    deployer = FlowDeployer()

    print("=" * 60)
    print("PREFECT FLOW DEPLOYMENT")
    print("=" * 60)
    print(f"Project: {PrefectConfig.PROJECT_NAME}")
    print(f"Environment: {PrefectConfig.ENVIRONMENT}")

    # Validate environment if requested
    if args.validate:
        if not deployer.validate_environment():
            sys.exit(1)

    # Deploy flows
    success_count = deployer.deploy_all(
        spotify_only=args.spotify_only, etl_only=args.etl_only
    )

    # Print summary
    all_successful = deployer.print_summary()

    # Exit with appropriate code
    sys.exit(0 if all_successful else 1)


if __name__ == "__main__":
    main()

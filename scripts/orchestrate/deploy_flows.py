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

# Add project root to path (more robust approach)
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import subprocess
from dotenv import load_dotenv
from scripts.orchestrate.flow_config import FlowConfig

# Load environment variables
load_dotenv()


class FlowDeployer:
    """Handles deployment of Prefect flows."""

    def __init__(self):
        self.deployed_flows = {}
        self.config = FlowConfig.from_env()

    def validate_environment(self):
        """Validate environment before deployment."""
        print("Validating environment...")

        # Check Prefect API connection (skip if server not running)
        try:
            from prefect import get_client

            # Try to create a client - this will work even if server is not running
            client = get_client()
            print("[OK] Prefect client initialized successfully")
        except Exception as e:
            print(f"[WARN] Cannot initialize Prefect client: {e}")
            print("This may be expected if the Prefect server is not running yet")

        # Validate configuration
        print(f"[OK] Configuration loaded - Environment: {self.config.environment}")
        print(f"     Data path: {self.config.data_base_path}")
        print(
            f"     Limits - Spotify: {self.config.spotify_artist_limit}, MBZ: {self.config.musicbrainz_fetch_limit}"
        )

        return True

    def deploy_spotify_flow(self):
        """Deploy the Spotify ingestion flow using CLI command."""
        print("\nDeploying Spotify Ingestion Flow...")

        try:
            # Use CLI command for proper module-based deployment
            cmd = [
                "prefect", "deploy",
                "scripts.orchestrate.prefect_flows:spotify_ingestion_flow",
                "-n", "spotify-ingestion",
                "-p", "default-agent-pool",
                "--cron", "*/10 * * * *",
                "--timezone", "UTC"
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.returncode == 0:
                print("[OK] Spotify Flow deployed successfully")
                self.deployed_flows["spotify"] = {
                    "name": "spotify-ingestion",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(f"[ERROR] Failed to deploy Spotify Flow: {error_msg}")
                self.deployed_flows["spotify"] = {"status": "failed", "error": error_msg}
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["spotify"] = {"status": "failed", "error": error_msg}
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy Spotify Flow: {e}")
            self.deployed_flows["spotify"] = {"status": "failed", "error": str(e)}
            return False

    def deploy_etl_flow(self):
        """Deploy the daily ETL flow using CLI command."""
        print("\nDeploying Daily ETL Flow...")

        try:
            # Use CLI command for proper module-based deployment
            cmd = [
                "prefect", "deploy",
                "scripts.orchestrate.prefect_flows:daily_etl_flow",
                "-n", "daily-etl",
                "-p", "default-agent-pool",
                "--cron", "0 2 * * *",
                "--timezone", "America/Denver"
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.returncode == 0:
                print("[OK] Daily ETL Flow deployed successfully")
                self.deployed_flows["etl"] = {
                    "name": "daily-etl",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(f"[ERROR] Failed to deploy Daily ETL Flow: {error_msg}")
                self.deployed_flows["etl"] = {"status": "failed", "error": error_msg}
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["etl"] = {"status": "failed", "error": error_msg}
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy Daily ETL Flow: {e}")
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
                print(f"[OK] {info['name']}")
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
    config = FlowConfig.from_env()
    print(f"Project: Music Tracker")
    print(f"Environment: {config.environment}")

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

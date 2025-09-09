#!/usr/bin/env python3
"""
Production-ready deployment script for Prefect flows.

This script deploys all music tracking flows to your Prefect server.
Ensure your Prefect server is running before executing this script.

Usage:
    # Basic deployment (deploys all flows and subflows)
    uv run python flows/orchestrate/deploy_flows.py

    # With validation
    uv run python flows/orchestrate/deploy_flows.py --validate

    # Deploy specific flows
    uv run python flows/orchestrate/deploy_flows.py --spotify-only
    uv run python flows/orchestrate/deploy_flows.py --etl-only

    # Deploy individual subflows
    uv run python flows/orchestrate/deploy_flows.py --data-prep-only
    uv run python flows/orchestrate/deploy_flows.py --enrichment-only
    uv run python flows/orchestrate/deploy_flows.py --transformation-only
    uv run python flows/orchestrate/deploy_flows.py --spotify-enrichment-only
    uv run python flows/orchestrate/deploy_flows.py --musicbrainz-enrichment-only
    uv run python flows/orchestrate/deploy_flows.py --geographic-enrichment-only

    # Deploy all subflows independently
    uv run python flows/orchestrate/deploy_flows.py --subflows-only

    # Deploy only main flows (no subflows)
    uv run python flows/orchestrate/deploy_flows.py --main-flows-only
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path

# Add project root to path (more robust approach)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Also add the flows directory to the path
flows_dir = Path(__file__).parent.parent
sys.path.insert(0, str(flows_dir))

from dotenv import load_dotenv

# Import flow config with error handling
try:
    from flows.orchestrate.flow_config import FlowConfig
except ImportError:
    # Fallback import approach
    sys.path.insert(0, str(Path(__file__).parent))
    from flow_config import FlowConfig

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
            from flows import get_client

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
                "prefect",
                "deploy",
                "flows.orchestrate.flows.spotify_ingestion:spotify_ingestion_flow",
                "-n",
                "spotify-ingestion",
                "-p",
                "default-agent-pool",
                "--cron",
                "*/30 * * * *",
                "--timezone",
                "UTC",
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

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
                self.deployed_flows["spotify"] = {
                    "status": "failed",
                    "error": error_msg,
                }
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
                "prefect",
                "deploy",
                "flows.orchestrate.flows.daily_etl:daily_etl_flow",
                "-n",
                "daily-etl",
                "-p",
                "default-agent-pool",
                "--cron",
                "0 2 * * *",
                "--timezone",
                "America/Denver",
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

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

    def deploy_data_preparation_subflow(self):
        """Deploy the data preparation subflow using CLI command."""
        print("\nDeploying Data Preparation Subflow...")

        try:
            cmd = [
                "prefect",
                "deploy",
                "flows.orchestrate.subflows.data_preparation:data_preparation_subflow",
                "-n",
                "data-preparation-subflow",
                "-p",
                "default-agent-pool",
                # Note: No cron schedule - subflows are called by parent flows only
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                print("[OK] Data Preparation Subflow deployed successfully")
                self.deployed_flows["data_preparation"] = {
                    "name": "data-preparation-subflow",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(f"[ERROR] Failed to deploy Data Preparation Subflow: {error_msg}")
                self.deployed_flows["data_preparation"] = {
                    "status": "failed",
                    "error": error_msg,
                }
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["data_preparation"] = {
                "status": "failed",
                "error": error_msg,
            }
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy Data Preparation Subflow: {e}")
            self.deployed_flows["data_preparation"] = {
                "status": "failed",
                "error": str(e),
            }
            return False

    def deploy_enrichment_coordination_subflow(self):
        """Deploy the enrichment coordination subflow using CLI command."""
        print("\nDeploying Enrichment Coordination Subflow...")

        try:
            cmd = [
                "prefect",
                "deploy",
                "flows.orchestrate.subflows.enrichment_coordination:enrichment_coordination_subflow",
                "-n",
                "enrichment-coordination-subflow",
                "-p",
                "default-agent-pool",
                # Note: No cron schedule - subflows are called by parent flows only
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                print("[OK] Enrichment Coordination Subflow deployed successfully")
                self.deployed_flows["enrichment"] = {
                    "name": "enrichment-coordination-subflow",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(
                    f"[ERROR] Failed to deploy Enrichment Coordination Subflow: {error_msg}"
                )
                self.deployed_flows["enrichment"] = {
                    "status": "failed",
                    "error": error_msg,
                }
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["enrichment"] = {"status": "failed", "error": error_msg}
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy Enrichment Coordination Subflow: {e}")
            self.deployed_flows["enrichment"] = {"status": "failed", "error": str(e)}
            return False

    def deploy_transformation_subflow(self):
        """Deploy the transformation subflow using CLI command."""
        print("\nDeploying Transformation Subflow...")

        try:
            cmd = [
                "prefect",
                "deploy",
                "flows.orchestrate.subflows.transformation:transformation_subflow",
                "-n",
                "transformation-subflow",
                "-p",
                "default-agent-pool",
                # Note: No cron schedule - subflows are called by parent flows only
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                print("[OK] Transformation Subflow deployed successfully")
                self.deployed_flows["transformation"] = {
                    "name": "transformation-subflow",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(f"[ERROR] Failed to deploy Transformation Subflow: {error_msg}")
                self.deployed_flows["transformation"] = {
                    "status": "failed",
                    "error": error_msg,
                }
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["transformation"] = {
                "status": "failed",
                "error": error_msg,
            }
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy Transformation Subflow: {e}")
            self.deployed_flows["transformation"] = {
                "status": "failed",
                "error": str(e),
            }
            return False

    def deploy_spotify_enrichment_subflow(self):
        """Deploy the Spotify enrichment subflow using CLI command."""
        print("\nDeploying Spotify Enrichment Subflow...")

        try:
            cmd = [
                "prefect",
                "deploy",
                "flows.orchestrate.subflows.spotify_enrichment:spotify_enrichment_subflow",
                "-n",
                "spotify-enrichment-subflow",
                "-p",
                "default-agent-pool",
                # Note: No cron schedule - subflows are called by parent flows only
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                print("[OK] Spotify Enrichment Subflow deployed successfully")
                self.deployed_flows["spotify_enrichment"] = {
                    "name": "spotify-enrichment-subflow",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(
                    f"[ERROR] Failed to deploy Spotify Enrichment Subflow: {error_msg}"
                )
                self.deployed_flows["spotify_enrichment"] = {
                    "status": "failed",
                    "error": error_msg,
                }
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["spotify_enrichment"] = {
                "status": "failed",
                "error": error_msg,
            }
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy Spotify Enrichment Subflow: {e}")
            self.deployed_flows["spotify_enrichment"] = {
                "status": "failed",
                "error": str(e),
            }
            return False

    def deploy_musicbrainz_enrichment_subflow(self):
        """Deploy the MusicBrainz enrichment subflow using CLI command."""
        print("\nDeploying MusicBrainz Enrichment Subflow...")

        try:
            cmd = [
                "prefect",
                "deploy",
                "flows.orchestrate.subflows.musicbrainz_enrichment:musicbrainz_enrichment_subflow",
                "-n",
                "musicbrainz-enrichment-subflow",
                "-p",
                "default-agent-pool",
                # Note: No cron schedule - subflows are called by parent flows only
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                print("[OK] MusicBrainz Enrichment Subflow deployed successfully")
                self.deployed_flows["musicbrainz_enrichment"] = {
                    "name": "musicbrainz-enrichment-subflow",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(
                    f"[ERROR] Failed to deploy MusicBrainz Enrichment Subflow: {error_msg}"
                )
                self.deployed_flows["musicbrainz_enrichment"] = {
                    "status": "failed",
                    "error": error_msg,
                }
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["musicbrainz_enrichment"] = {
                "status": "failed",
                "error": error_msg,
            }
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy MusicBrainz Enrichment Subflow: {e}")
            self.deployed_flows["musicbrainz_enrichment"] = {
                "status": "failed",
                "error": str(e),
            }
            return False

    def deploy_geographic_enrichment_subflow(self):
        """Deploy the geographic enrichment subflow using CLI command."""
        print("\nDeploying Geographic Enrichment Subflow...")

        try:
            cmd = [
                "prefect",
                "deploy",
                "flows.orchestrate.subflows.geographic_enrichment:geographic_enrichment_subflow",
                "-n",
                "geographic-enrichment-subflow",
                "-p",
                "default-agent-pool",
                # Note: No cron schedule - subflows are called by parent flows only
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                print("[OK] Geographic Enrichment Subflow deployed successfully")
                self.deployed_flows["geographic_enrichment"] = {
                    "name": "geographic-enrichment-subflow",
                    "status": "success",
                }
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown deployment error"
                print(
                    f"[ERROR] Failed to deploy Geographic Enrichment Subflow: {error_msg}"
                )
                self.deployed_flows["geographic_enrichment"] = {
                    "status": "failed",
                    "error": error_msg,
                }
                return False

        except subprocess.TimeoutExpired:
            error_msg = "Deployment timed out after 120 seconds"
            print(f"[ERROR] {error_msg}")
            self.deployed_flows["geographic_enrichment"] = {
                "status": "failed",
                "error": error_msg,
            }
            return False
        except Exception as e:
            print(f"[ERROR] Failed to deploy Geographic Enrichment Subflow: {e}")
            self.deployed_flows["geographic_enrichment"] = {
                "status": "failed",
                "error": str(e),
            }
            return False

    def deploy_all(self, **options):
        """Deploy flows based on options."""
        success_count = 0

        # Extract options
        spotify_only = options.get("spotify_only", False)
        etl_only = options.get("etl_only", False)
        data_prep_only = options.get("data_prep_only", False)
        enrichment_only = options.get("enrichment_only", False)
        transformation_only = options.get("transformation_only", False)
        subflows_only = options.get("subflows_only", False)
        main_flows_only = options.get("main_flows_only", False)
        spotify_enrichment_only = options.get("spotify_enrichment_only", False)
        musicbrainz_enrichment_only = options.get("musicbrainz_enrichment_only", False)
        geographic_enrichment_only = options.get("geographic_enrichment_only", False)

        # Determine what to deploy
        has_specific_option = any(
            [
                spotify_only,
                etl_only,
                data_prep_only,
                enrichment_only,
                transformation_only,
                subflows_only,
                main_flows_only,
                spotify_enrichment_only,
                musicbrainz_enrichment_only,
                geographic_enrichment_only,
            ]
        )

        # Default behavior: deploy everything if no specific options
        deploy_main_flows = (
            not has_specific_option or spotify_only or etl_only or main_flows_only
        )
        deploy_subflows = (
            not has_specific_option
            or subflows_only
            or data_prep_only
            or enrichment_only
            or transformation_only
        )

        # Deploy main flows
        if deploy_main_flows:
            if not spotify_only and not etl_only:
                # Deploy both main flows (default or main_flows_only)
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

        # Deploy subflows
        if deploy_subflows:
            if not any(
                [
                    data_prep_only,
                    enrichment_only,
                    transformation_only,
                    spotify_enrichment_only,
                    musicbrainz_enrichment_only,
                    geographic_enrichment_only,
                ]
            ):
                # Deploy all subflows (default or subflows_only)
                if self.deploy_data_preparation_subflow():
                    success_count += 1
                if self.deploy_enrichment_coordination_subflow():
                    success_count += 1
                if self.deploy_transformation_subflow():
                    success_count += 1
                if self.deploy_spotify_enrichment_subflow():
                    success_count += 1
                if self.deploy_musicbrainz_enrichment_subflow():
                    success_count += 1
                if self.deploy_geographic_enrichment_subflow():
                    success_count += 1
            else:
                # Deploy specific subflows
                if data_prep_only:
                    if self.deploy_data_preparation_subflow():
                        success_count += 1
                if enrichment_only:
                    if self.deploy_enrichment_coordination_subflow():
                        success_count += 1
                if transformation_only:
                    if self.deploy_transformation_subflow():
                        success_count += 1
                if spotify_enrichment_only:
                    if self.deploy_spotify_enrichment_subflow():
                        success_count += 1
                if musicbrainz_enrichment_only:
                    if self.deploy_musicbrainz_enrichment_subflow():
                        success_count += 1
                if geographic_enrichment_only:
                    if self.deploy_geographic_enrichment_subflow():
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

            # Group deployments by type
            main_flows = [
                f
                for f in successful_flows
                if f["name"] in ["spotify-ingestion", "daily-etl"]
            ]
            subflows = [f for f in successful_flows if f["name"].endswith("-subflow")]

            if main_flows:
                print(f"\nMain Flows Deployed: {len(main_flows)}")
                for flow in main_flows:
                    print(f"  - {flow['name']}")

            if subflows:
                print(f"\nSubflows Deployed: {len(subflows)}")
                for flow in subflows:
                    print(f"  - {flow['name']}")
                print("\nSubflows can be run manually for testing and debugging!")
                print(
                    "Note: Subflows have no automatic schedule - they are triggered by parent flows."
                )

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

    # Main flow options
    parser.add_argument(
        "--spotify-only", action="store_true", help="Deploy only Spotify ingestion flow"
    )
    parser.add_argument(
        "--etl-only", action="store_true", help="Deploy only daily ETL flow"
    )
    parser.add_argument(
        "--main-flows-only",
        action="store_true",
        help="Deploy only main flows (no subflows)",
    )

    # Subflow options
    parser.add_argument(
        "--data-prep-only",
        action="store_true",
        help="Deploy only data preparation subflow",
    )
    parser.add_argument(
        "--enrichment-only",
        action="store_true",
        help="Deploy only enrichment coordination subflow",
    )
    parser.add_argument(
        "--transformation-only",
        action="store_true",
        help="Deploy only transformation subflow",
    )
    parser.add_argument(
        "--spotify-enrichment-only",
        action="store_true",
        help="Deploy only Spotify enrichment subflow",
    )
    parser.add_argument(
        "--musicbrainz-enrichment-only",
        action="store_true",
        help="Deploy only MusicBrainz enrichment subflow",
    )
    parser.add_argument(
        "--geographic-enrichment-only",
        action="store_true",
        help="Deploy only geographic enrichment subflow",
    )
    parser.add_argument(
        "--subflows-only",
        action="store_true",
        help="Deploy all subflows (data prep, enrichment, transformation, spotify, musicbrainz, geographic)",
    )

    args = parser.parse_args()

    # Validate mutually exclusive options
    deployment_options = [
        args.spotify_only,
        args.etl_only,
        args.main_flows_only,
        args.data_prep_only,
        args.enrichment_only,
        args.transformation_only,
        args.subflows_only,
        args.spotify_enrichment_only,
        args.musicbrainz_enrichment_only,
        args.geographic_enrichment_only,
    ]

    if sum(deployment_options) > 1:
        print("ERROR: Only one deployment option can be specified at a time")
        sys.exit(1)

    # Initialize deployer
    deployer = FlowDeployer()

    print("=" * 60)
    print("PREFECT FLOW DEPLOYMENT")
    print("=" * 60)
    config = FlowConfig.from_env()
    print(f"Project: Music Tracker")
    print(f"Environment: {config.environment}")

    # Show what will be deployed
    if not any(deployment_options):
        print("Default: Deploying all flows and subflows")
    elif args.main_flows_only:
        print("Deploying main flows only")
    elif args.subflows_only:
        print("Deploying subflows only")
    # ...other specific options already logged by individual methods

    # Validate environment if requested
    if args.validate:
        if not deployer.validate_environment():
            sys.exit(1)

    # Deploy flows
    success_count = deployer.deploy_all(
        spotify_only=args.spotify_only,
        etl_only=args.etl_only,
        main_flows_only=args.main_flows_only,
        data_prep_only=args.data_prep_only,
        enrichment_only=args.enrichment_only,
        transformation_only=args.transformation_only,
        subflows_only=args.subflows_only,
        spotify_enrichment_only=args.spotify_enrichment_only,
        musicbrainz_enrichment_only=args.musicbrainz_enrichment_only,
        geographic_enrichment_only=args.geographic_enrichment_only,
    )

    # Print summary
    all_successful = deployer.print_summary()

    # Exit with appropriate code
    sys.exit(0 if all_successful else 1)


if __name__ == "__main__":
    main()

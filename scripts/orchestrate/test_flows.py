#!/usr/bin/env python3
"""
Test script for Prefect flows in the music tracking system.

This script allows testing flows locally without deploying to Prefect server.

Usage:
    python scripts/orchestrate/test_flows.py --flow spotify
    python scripts/orchestrate/test_flows.py --flow etl
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def test_spotify_ingestion_flow(limit: int = 10) -> Dict[str, Any]:
    """
    Test the Spotify ingestion flow locally.

    Args:
        limit: Number of tracks to fetch (smaller for testing)

    Returns:
        Dict containing test results
    """
    print("Testing Spotify Ingestion Flow")
    print(f"   Limit: {limit} tracks")

    try:
        from scripts.orchestrate.prefect_flows import spotify_ingestion_flow

        # Run the flow
        result = spotify_ingestion_flow(limit=limit)

        print("Spotify ingestion flow test completed successfully")
        print(f"   Status: {result.get('status')}")
        print(".2f")
        print(
            f"   Records ingested: {result.get('ingestion_result', {}).get('records_ingested', 0)}"
        )

        return {"test": "spotify_ingestion", "success": True, "result": result}

    except Exception as e:
        print(f"Spotify ingestion flow test failed: {e}")
        return {"test": "spotify_ingestion", "success": False, "error": str(e)}


def test_daily_etl_flow() -> Dict[str, Any]:
    """
    Test the daily ETL flow locally.

    Returns:
        Dict containing test results
    """
    print("Testing Daily ETL Flow")

    try:
        from scripts.orchestrate.prefect_flows import daily_etl_flow

        # Run the flow
        result = daily_etl_flow()

        print("Daily ETL flow test completed successfully")
        print(f"   Status: {result.get('status')}")
        print(".2f")

        # Show stage results
        stages = result.get("stages", {})
        for stage_name, stage_result in stages.items():
            status = stage_result.get("status", "unknown")
            if status == "success":
                print(f"    {stage_name}: {status}")
            else:
                print(f"    {stage_name}: {status}")

        return {"test": "daily_etl", "success": True, "result": result}

    except Exception as e:
        print(f"Daily ETL flow test failed: {e}")
        return {"test": "daily_etl", "success": False, "error": str(e)}


def test_individual_tasks() -> Dict[str, Any]:
    """
    Test individual tasks to isolate issues.

    Returns:
        Dict containing test results
    """
    print("Testing Individual Tasks")

    results = {"test": "individual_tasks", "tasks": {}, "success": True}

    try:
        from scripts.orchestrate.prefect_flows import (
            run_spotify_ingestion,
            load_raw_data,
            run_dbt_transformations,
        )

        # Test Spotify ingestion task
        print("   Testing Spotify ingestion task...")
        try:
            spotify_result = run_spotify_ingestion(limit=5)
            results["tasks"]["spotify_ingestion"] = {
                "success": True,
                "result": spotify_result,
            }
            print("    Spotify ingestion task passed")
        except Exception as e:
            results["tasks"]["spotify_ingestion"] = {"success": False, "error": str(e)}
            results["success"] = False
            print(f"    Spotify ingestion task failed: {e}")

        # Test data loading task (only if there are raw files)
        raw_files = list(Path("dbt/data/raw/recently_played/detail").glob("*.json"))
        if raw_files:
            print("   Testing data loading task...")
            try:
                load_result = load_raw_data()
                results["tasks"]["load_raw_data"] = {
                    "success": True,
                    "result": load_result,
                }
                print("    Data loading task passed")
            except Exception as e:
                results["tasks"]["load_raw_data"] = {"success": False, "error": str(e)}
                results["success"] = False
                print(f"    Data loading task failed: {e}")
        else:
            print("     Skipping data loading test (no raw files found)")
            results["tasks"]["load_raw_data"] = {
                "success": True,
                "message": "Skipped - no raw files",
            }

        # Test dbt transformations
        print("   Testing dbt transformations task...")
        try:
            dbt_result = run_dbt_transformations()
            results["tasks"]["dbt_transformations"] = {
                "success": True,
                "result": dbt_result,
            }
            print("    DBT transformations task passed")
        except Exception as e:
            results["tasks"]["dbt_transformations"] = {
                "success": False,
                "error": str(e),
            }
            results["success"] = False
            print(f"    DBT transformations task failed: {e}")

    except Exception as e:
        print(f" Error testing individual tasks: {e}")
        results["success"] = False
        results["error"] = str(e)

    return results


def validate_environment() -> bool:
    """
    Validate that the test environment is properly set up.

    Returns:
        True if environment is valid, False otherwise
    """
    print("Validating Test Environment")

    issues = []

    # Check required environment variables
    required_vars = [
        "SPOTIFY_CLIENT_ID",
        "SPOTIFY_CLIENT_SECRET",
        "SPOTIFY_REFRESH_TOKEN",
    ]
    for var in required_vars:
        if not os.getenv(var):
            issues.append(f"Missing environment variable: {var}")

    # Check required directories
    required_dirs = [
        "dbt/data/raw/recently_played/detail",
        "dbt/data/src/tracks_played",
        "dbt",
    ]
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            issues.append(f"Missing directory: {dir_path}")

    # Check required files
    required_files = ["dbt/dbt_project.yml", "dbt/profiles.yml"]
    for file_path in required_files:
        if not Path(file_path).exists():
            issues.append(f"Missing file: {file_path}")

    if issues:
        print("Environment validation failed:")
        for issue in issues:
            print(f"   - {issue}")
        return False
    else:
        print("Environment validation passed")
        return True


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Test Prefect flows for Music Tracker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/orchestrate/test_flows.py --flow spotify
  python scripts/orchestrate/test_flows.py --flow etl
  python scripts/orchestrate/test_flows.py --tasks
  python scripts/orchestrate/test_flows.py --validate
        """,
    )

    parser.add_argument("--flow", choices=["spotify", "etl"], help="Test specific flow")
    parser.add_argument("--tasks", action="store_true", help="Test individual tasks")
    parser.add_argument(
        "--validate", action="store_true", help="Validate test environment"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit for Spotify ingestion test (default: 10)",
    )

    args = parser.parse_args()

    # If no arguments provided, show help
    if not any([args.flow, args.tasks, args.validate]):
        parser.print_help()
        return

    print("Music Tracker - Prefect Flow Tests")
    print("=" * 50)

    all_results = {"tests": {}, "overall_success": True}

    # Validate environment first
    if not validate_environment():
        print("\nCannot proceed with tests due to environment issues")
        sys.exit(1)

    # Run requested tests
    if args.validate:
        # Already validated above
        pass

    if args.flow == "spotify":
        result = test_spotify_ingestion_flow(limit=args.limit)
        all_results["tests"]["spotify_flow"] = result
        if not result["success"]:
            all_results["overall_success"] = False

    if args.flow == "etl":
        result = test_daily_etl_flow()
        all_results["tests"]["etl_flow"] = result
        if not result["success"]:
            all_results["overall_success"] = False

    if args.tasks:
        result = test_individual_tasks()
        all_results["tests"]["individual_tasks"] = result
        if not result["success"]:
            all_results["overall_success"] = False

    # Print summary
    print("\n" + "=" * 50)
    if all_results["overall_success"]:
        print("All tests completed successfully!")
    else:
        print("Some tests failed. Check the output above for details.")

    # Save detailed results to file
    results_file = Path("logs/test_results.json")
    results_file.parent.mkdir(exist_ok=True)

    with open(results_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\nDetailed results saved to: {results_file}")

    # Exit with appropriate code
    sys.exit(0 if all_results["overall_success"] else 1)


if __name__ == "__main__":
    main()

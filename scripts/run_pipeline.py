#!/usr/bin/env python3
"""
Main data pipeline orchestrator for the music tracking system.

This script coordinates the entire data flow:
1. Ingest data from Spotify API
2. Process and merge data using Polars
3. Run dbt transformations
4. Update reporting data
"""

import os
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any
import json

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MusicTrackerPipeline:
    """Main pipeline orchestrator."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.scripts_dir = self.project_root / "scripts"
        self.dbt_dir = self.project_root / "dbt"

        # Ensure we're in the right directory
        os.chdir(self.project_root)

    def run_spotify_ingestion(self) -> Dict[str, Any]:
        """Run Spotify API data ingestion."""
        logger.info("Starting Spotify data ingestion")

        try:
            cmd = [
                sys.executable,
                str(self.scripts_dir / "ingest" / "spotify_api_ingestion.py"),
                "--limit",
                "50",
            ]

            result = subprocess.run(
                cmd, capture_output=True, text=True, cwd=self.project_root
            )

            if result.returncode == 0:
                ingestion_result = json.loads(result.stdout)
                logger.info("Spotify ingestion completed successfully")
                return ingestion_result
            else:
                logger.error(f"Spotify ingestion failed: {result.stderr}")
                return {
                    "status": "error",
                    "stage": "ingestion",
                    "message": result.stderr,
                }

        except Exception as e:
            logger.error(f"Error running Spotify ingestion: {e}")
            return {"status": "error", "stage": "ingestion", "message": str(e)}

    def run_data_processing(self) -> Dict[str, Any]:
        """Run data processing and merging."""
        logger.info("Starting data processing")

        try:
            cmd = [sys.executable, str(self.scripts_dir / "load" / "append_tracks.py")]

            result = subprocess.run(
                cmd, capture_output=True, text=True, cwd=self.project_root
            )

            if result.returncode == 0:
                processing_result = json.loads(result.stdout)
                logger.info("Data processing completed successfully")
                return processing_result
            else:
                logger.error(f"Data processing failed: {result.stderr}")
                return {
                    "status": "error",
                    "stage": "processing",
                    "message": result.stderr,
                }

        except Exception as e:
            logger.error(f"Error running data processing: {e}")
            return {"status": "error", "stage": "processing", "message": str(e)}

    def run_dbt_transformations(self) -> Dict[str, Any]:
        """Run dbt transformations."""
        logger.info("Starting dbt transformations")

        try:
            # Ensure dbt dependencies are installed
            self._ensure_dbt_deps()

            # Run dbt build
            cmd = [
                "dbt",
                "build",
                "--profiles-dir",
                str(self.dbt_dir),
                "--project-dir",
                str(self.dbt_dir),
            ]

            result = subprocess.run(
                cmd, capture_output=True, text=True, cwd=self.dbt_dir
            )

            if result.returncode == 0:
                logger.info("dbt transformations completed successfully")
                return {"status": "success", "stage": "dbt", "output": result.stdout}
            else:
                logger.error(f"dbt transformations failed: {result.stderr}")
                return {"status": "error", "stage": "dbt", "message": result.stderr}

        except Exception as e:
            logger.error(f"Error running dbt transformations: {e}")
            return {"status": "error", "stage": "dbt", "message": str(e)}

    def _ensure_dbt_deps(self) -> None:
        """Ensure dbt dependencies are installed."""
        try:
            cmd = [
                "dbt",
                "deps",
                "--profiles-dir",
                str(self.dbt_dir),
                "--project-dir",
                str(self.dbt_dir),
            ]

            result = subprocess.run(
                cmd, capture_output=True, text=True, cwd=self.dbt_dir
            )

            if result.returncode != 0:
                logger.warning(f"dbt deps failed: {result.stderr}")

        except Exception as e:
            logger.warning(f"Error ensuring dbt deps: {e}")

    def run_musicbrainz_enrichment(self) -> Dict[str, Any]:
        """Run MusicBrainz data enrichment."""
        logger.info("Starting MusicBrainz data enrichment")

        try:
            # Import the processor
            from scripts.enrich.musicbrainz_processor import MusicBrainzProcessor

            # Create processor instance
            processor = MusicBrainzProcessor()

            # Run full enrichment
            result = processor.run_full_enrichment()

            if result["overall_status"] in ["success", "partial_failure"]:
                logger.info("MusicBrainz enrichment completed")
                return {
                    "status": "success",
                    "stage": "musicbrainz",
                    "result": result,
                }
            else:
                logger.error(f"MusicBrainz enrichment failed: {result}")
                return {
                    "status": "error",
                    "stage": "musicbrainz",
                    "message": f"Enrichment failed with status: {result['overall_status']}",
                    "result": result,
                }

        except Exception as e:
            logger.error(f"Error running MusicBrainz enrichment: {e}")
            return {
                "status": "error",
                "stage": "musicbrainz",
                "message": str(e),
            }

    def update_reporting_data(self) -> Dict[str, Any]:
        """Update reporting data (placeholder for Metabase integration)."""
        logger.info("Updating reporting data - placeholder for Metabase integration")
        return {
            "status": "success",
            "stage": "reporting",
            "message": "Metabase integration pending",
        }

    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete data pipeline."""
        start_time = datetime.now(timezone.utc)
        logger.info("Starting full music tracker pipeline")

        pipeline_results = {"pipeline_start": start_time.isoformat(), "stages": {}}

        try:
            # Stage 1: Data Ingestion
            ingestion_result = self.run_spotify_ingestion()
            pipeline_results["stages"]["ingestion"] = ingestion_result

            if ingestion_result.get("status") == "no_data":
                logger.info("No new data ingested. Skipping further processing.")
                return self._finalize_pipeline(pipeline_results, "success")
            elif ingestion_result.get("status") != "success":
                logger.error("Pipeline failed at ingestion stage")
                return self._finalize_pipeline(pipeline_results, "failed")

            # Stage 2: Data Processing
            processing_result = self.run_data_processing()
            pipeline_results["stages"]["processing"] = processing_result

            if processing_result.get("status") != "success":
                logger.error("Pipeline failed at processing stage")
                return self._finalize_pipeline(pipeline_results, "failed")

            # Stage 3: MusicBrainz Enrichment
            enrichment_result = self.run_musicbrainz_enrichment()
            pipeline_results["stages"]["enrichment"] = enrichment_result

            # Stage 4: dbt Transformations
            dbt_result = self.run_dbt_transformations()
            pipeline_results["stages"]["dbt"] = dbt_result

            if dbt_result.get("status") != "success":
                logger.error("Pipeline failed at dbt stage")
                return self._finalize_pipeline(pipeline_results, "failed")

            # Stage 5: Update Reporting
            reporting_result = self.update_reporting_data()
            pipeline_results["stages"]["reporting"] = reporting_result

            # Pipeline completed successfully
            logger.info("Full pipeline completed successfully")
            return self._finalize_pipeline(pipeline_results, "success")

        except Exception as e:
            logger.error(f"Pipeline failed with exception: {e}")
            pipeline_results["stages"]["error"] = {"status": "error", "message": str(e)}
            return self._finalize_pipeline(pipeline_results, "failed")

    def _finalize_pipeline(
        self, results: Dict[str, Any], status: str
    ) -> Dict[str, Any]:
        """Finalize pipeline results."""
        end_time = datetime.now(timezone.utc)
        start_time = datetime.fromisoformat(results["pipeline_start"])

        results.update(
            {
                "pipeline_end": end_time.isoformat(),
                "duration_seconds": (end_time - start_time).total_seconds(),
                "overall_status": status,
            }
        )

        return results

    def run_incremental_pipeline(self) -> Dict[str, Any]:
        """Run incremental pipeline (only processing, no ingestion)."""
        logger.info("Starting incremental pipeline")

        pipeline_results = {
            "pipeline_type": "incremental",
            "pipeline_start": datetime.now(timezone.utc).isoformat(),
            "stages": {},
        }

        try:
            # Skip ingestion, start from processing
            processing_result = self.run_data_processing()
            pipeline_results["stages"]["processing"] = processing_result

            enrichment_result = self.run_musicbrainz_enrichment()
            pipeline_results["stages"]["enrichment"] = enrichment_result

            dbt_result = self.run_dbt_transformations()
            pipeline_results["stages"]["dbt"] = dbt_result

            reporting_result = self.update_reporting_data()
            pipeline_results["stages"]["reporting"] = reporting_result

            return self._finalize_pipeline(pipeline_results, "success")

        except Exception as e:
            logger.error(f"Incremental pipeline failed: {e}")
            return self._finalize_pipeline(pipeline_results, "failed")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Music Tracker Data Pipeline")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Run incremental pipeline (skip ingestion)",
    )
    parser.add_argument(
        "--stage",
        choices=["ingestion", "processing", "dbt"],
        help="Run only specific stage",
    )

    args = parser.parse_args()

    pipeline = MusicTrackerPipeline()

    if args.stage:
        # Run specific stage
        if args.stage == "ingestion":
            result = pipeline.run_spotify_ingestion()
        elif args.stage == "processing":
            result = pipeline.run_data_processing()
        elif args.stage == "dbt":
            result = pipeline.run_dbt_transformations()
    else:
        # Run full or incremental pipeline
        if args.incremental:
            result = pipeline.run_incremental_pipeline()
        else:
            result = pipeline.run_full_pipeline()

    # Print result for logging/monitoring
    print(json.dumps(result, indent=2, default=str))

    # Exit with appropriate code
    if result.get("overall_status") == "success" or result.get("status") == "success":
        exit(0)
    else:
        exit(1)


if __name__ == "__main__":
    main()

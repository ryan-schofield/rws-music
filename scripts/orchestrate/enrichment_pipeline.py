#!/usr/bin/env python3
"""
Data enrichment pipeline orchestrator.

This script coordinates the execution of all enrichment processors in the correct 
dependency order, replacing the individual Fabric notebook executions with a 
unified pipeline that uses Polars/DuckDB and writes to parquet files.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pathlib import Path
import argparse

# Add the project root to the Python path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Import the enrichment processors with absolute imports
from scripts.enrich import (
    SpotifyProcessor,
    MusicBrainzProcessor,
    GeographicProcessor,
    ParquetDataWriter,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class EnrichmentPipeline:
    """
    Orchestrates data enrichment across Spotify, MusicBrainz, and Geographic processors.

    Execution order:
    1. Spotify enrichment (artists and albums)
    2. MusicBrainz enrichment (artists and area hierarchy)
    3. Geographic enrichment (continent and coordinate data)
    """

    def __init__(self, data_base_path: str = "data/src", limit: Optional[int] = None):
        self.data_writer = ParquetDataWriter(data_base_path)
        self.start_time = datetime.now(timezone.utc)
        self.limit = limit

        # Initialize processors
        self.spotify_processor = SpotifyProcessor(self.data_writer)
        self.musicbrainz_processor = MusicBrainzProcessor(self.data_writer)
        self.geo_processor = GeographicProcessor(self.data_writer)

        # Pipeline state tracking
        self.pipeline_state = {
            "pipeline_id": f"enrichment_{self.start_time.strftime('%Y%m%d_%H%M%S')}",
            "start_time": self.start_time.isoformat(),
            "processors_completed": [],
            "processors_failed": [],
            "overall_status": "running",
        }

    def validate_prerequisites(self) -> Dict[str, Any]:
        """
        Validate that required source data is available.
        """
        logger.info("Validating pipeline prerequisites")

        validation_results = {
            "tracks_played_exists": self.data_writer.table_exists("tracks_played"),
            "tracks_played_info": None,
        }

        if validation_results["tracks_played_exists"]:
            tracks_info = self.data_writer.get_table_info("tracks_played")
            validation_results["tracks_played_info"] = tracks_info
            logger.info(
                f"tracks_played table contains {tracks_info.get('record_count', 0)} records"
            )
        else:
            logger.warning(
                "tracks_played table not found - enrichment may have limited data"
            )

        return validation_results

    def run_spotify_enrichment(self, skip_on_error: bool = False) -> Dict[str, Any]:
        """
        Run Spotify data enrichment.
        """
        logger.info("=== Starting Spotify Enrichment ===")

        try:
            result = self.spotify_processor.run_full_enrichment(limit=self.limit)

            if result["overall_status"] == "success":
                self.pipeline_state["processors_completed"].append("spotify")
                logger.info("Spotify enrichment completed successfully")
                return result
            else:
                self.pipeline_state["processors_failed"].append("spotify")
                error_msg = f"Spotify enrichment failed with status: {result['overall_status']}"
                logger.error(error_msg)

                if not skip_on_error:
                    logger.error("Spotify enrichment failed - stopping pipeline")
                    raise Exception(error_msg)
                else:
                    logger.warning("Spotify enrichment failed - continuing pipeline")
                    return result

        except Exception as e:
            error_msg = f"Spotify enrichment failed with exception: {e}"
            logger.error(error_msg)

            self.pipeline_state["processors_failed"].append("spotify")

            if not skip_on_error:
                raise

            return {"overall_status": "error", "error_message": error_msg}

    def run_musicbrainz_enrichment(self, skip_on_error: bool = False) -> Dict[str, Any]:
        """
        Run MusicBrainz data enrichment.
        """
        logger.info("=== Starting MusicBrainz Enrichment ===")

        try:
            result = self.musicbrainz_processor.run_full_enrichment(limit=self.limit)

            if result["overall_status"] in ["success", "partial_failure"]:
                self.pipeline_state["processors_completed"].append("musicbrainz")
                logger.info("MusicBrainz enrichment completed")
                return result
            else:
                self.pipeline_state["processors_failed"].append("musicbrainz")
                error_msg = f"MusicBrainz enrichment failed with status: {result['overall_status']}"
                logger.error(error_msg)

                if not skip_on_error:
                    logger.error("MusicBrainz enrichment failed - stopping pipeline")
                    raise Exception(error_msg)
                else:
                    logger.warning("MusicBrainz enrichment failed - continuing pipeline")
                    return result

        except Exception as e:
            error_msg = f"MusicBrainz enrichment failed with exception: {e}"
            logger.error(error_msg)

            self.pipeline_state["processors_failed"].append("musicbrainz")

            if not skip_on_error:
                raise

            return {"overall_status": "error", "error_message": error_msg}

    def run_geographic_enrichment(self, skip_on_error: bool = False) -> Dict[str, Any]:
        """
        Run geographic data enrichment.
        """
        logger.info("=== Starting Geographic Enrichment ===")

        try:
            result = self.geo_processor.run_full_enrichment(limit=self.limit)

            if result["overall_status"] in ["success", "partial_failure"]:
                self.pipeline_state["processors_completed"].append("geographic")
                logger.info("Geographic enrichment completed")
                return result
            else:
                self.pipeline_state["processors_failed"].append("geographic")
                error_msg = f"Geographic enrichment failed with status: {result['overall_status']}"
                logger.error(error_msg)

                if not skip_on_error:
                    logger.error("Geographic enrichment failed - stopping pipeline")
                    raise Exception(error_msg)
                else:
                    logger.warning("Geographic enrichment failed - continuing pipeline")
                    return result

        except Exception as e:
            error_msg = f"Geographic enrichment failed with exception: {e}"
            logger.error(error_msg)

            self.pipeline_state["processors_failed"].append("geographic")

            if not skip_on_error:
                raise

            return {"overall_status": "error", "error_message": error_msg}

    def run_spotify_enrichment_task(self) -> Dict[str, Any]:
        """
        Run Spotify enrichment as a standalone task.
        """
        logger.info("Running Spotify enrichment task")
        return self.run_spotify_enrichment(skip_on_error=True)

    def run_musicbrainz_enrichment_task(self) -> Dict[str, Any]:
        """
        Run MusicBrainz enrichment as a standalone task.
        """
        logger.info("Running MusicBrainz enrichment task")
        return self.run_musicbrainz_enrichment(skip_on_error=True)

    def run_geographic_enrichment_task(self) -> Dict[str, Any]:
        """
        Run Geographic enrichment as a standalone task.
        """
        logger.info("Running Geographic enrichment task")
        return self.run_geographic_enrichment(skip_on_error=True)

    def run_mbz_discover_task(self) -> Dict[str, Any]:
        """
        Run MusicBrainz artist discovery as a standalone task.
        """
        logger.info("Running MusicBrainz artist discovery task")
        try:
            result = self.musicbrainz_processor.discover_missing_artists()
            return {
                "overall_status": result.get("status", "unknown"),
                "step": "discover",
                "result": result
            }
        except Exception as e:
            logger.error(f"MusicBrainz discovery failed: {e}")
            return {"overall_status": "error", "error_message": str(e)}

    def run_mbz_fetch_task(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Run MusicBrainz artist fetching as a standalone task.
        """
        logger.info("Running MusicBrainz artist fetching task")
        try:
            # First discover artists
            discovery_result = self.musicbrainz_processor.discover_missing_artists()
            if discovery_result["status"] != "success":
                return {
                    "overall_status": "error",
                    "step": "fetch",
                    "error_message": "Artist discovery failed"
                }

            missing_artists_df = discovery_result["missing_artists"]

            # Apply limit if specified
            if limit is not None and len(missing_artists_df) > limit:
                missing_artists_df = missing_artists_df.head(limit)
                logger.info(f"Limited to {limit} artists for fetching")

            result = self.musicbrainz_processor.fetch_artist_data(missing_artists_df)
            return {
                "overall_status": result.get("status", "unknown"),
                "step": "fetch",
                "artists_to_fetch": len(missing_artists_df),
                "result": result
            }
        except Exception as e:
            logger.error(f"MusicBrainz fetching failed: {e}")
            return {"overall_status": "error", "error_message": str(e)}

    def run_mbz_parse_task(self) -> Dict[str, Any]:
        """
        Run MusicBrainz JSON parsing as a standalone task.
        """
        logger.info("Running MusicBrainz JSON parsing task")
        try:
            result = self.musicbrainz_processor.parse_artist_json_files()
            return {
                "overall_status": result.get("status", "unknown"),
                "step": "parse",
                "result": result
            }
        except Exception as e:
            logger.error(f"MusicBrainz parsing failed: {e}")
            return {"overall_status": "error", "error_message": str(e)}

    def run_mbz_hierarchy_task(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Run MusicBrainz area hierarchy processing as a standalone task.
        """
        logger.info("Running MusicBrainz area hierarchy task")
        try:
            result = self.musicbrainz_processor.process_area_hierarchy(limit=limit)
            return {
                "overall_status": result.get("status", "unknown"),
                "step": "hierarchy",
                "result": result
            }
        except Exception as e:
            logger.error(f"MusicBrainz hierarchy processing failed: {e}")
            return {"overall_status": "error", "error_message": str(e)}

    def run_full_pipeline(
        self, skip_on_error: bool = False, processors: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Run the complete enrichment pipeline.

        Args:
            skip_on_error: Continue pipeline even if individual processors fail
            processors: List of processors to run (default: all)
        """
        logger.info("Starting full enrichment pipeline")

        # Default to all processors in dependency order
        if processors is None:
            processors = ["spotify", "musicbrainz", "geographic"]

        # Validate prerequisites
        prereq_results = self.validate_prerequisites()

        pipeline_results = {
            "pipeline_info": self.pipeline_state.copy(),
            "prerequisites": prereq_results,
            "processor_results": {},
        }

        # Run processors in order
        if "spotify" in processors:
            spotify_result = self.run_spotify_enrichment(skip_on_error)
            pipeline_results["processor_results"]["spotify"] = spotify_result

            if spotify_result["overall_status"] == "error" and not skip_on_error:
                return self._finalize_pipeline(pipeline_results, "failed")

        if "musicbrainz" in processors:
            mbz_result = self.run_musicbrainz_enrichment(skip_on_error)
            pipeline_results["processor_results"]["musicbrainz"] = mbz_result

            if mbz_result["overall_status"] == "error" and not skip_on_error:
                return self._finalize_pipeline(pipeline_results, "failed")

        if "geographic" in processors:
            geo_result = self.run_geographic_enrichment(skip_on_error)
            pipeline_results["processor_results"]["geographic"] = geo_result

            if geo_result["overall_status"] == "error" and not skip_on_error:
                return self._finalize_pipeline(pipeline_results, "failed")

        # Determine overall status
        if self.pipeline_state["processors_failed"]:
            if len(self.pipeline_state["processors_completed"]) > 0:
                overall_status = "partial_success"
            else:
                overall_status = "failed"
        else:
            overall_status = "success"

        return self._finalize_pipeline(pipeline_results, overall_status)

    def _finalize_pipeline(
        self, results: Dict[str, Any], status: str
    ) -> Dict[str, Any]:
        """
        Finalize pipeline execution and generate final results.
        """
        end_time = datetime.now(timezone.utc)
        duration = (end_time - self.start_time).total_seconds()

        self.pipeline_state.update(
            {
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "overall_status": status,
            }
        )

        results["pipeline_info"] = self.pipeline_state.copy()
        results["summary"] = {
            "status": status,
            "duration_seconds": duration,
            "processors_completed": len(self.pipeline_state["processors_completed"]),
            "processors_failed": len(self.pipeline_state["processors_failed"]),
        }

        # Log final status
        logger.info(f"Pipeline completed with status: {status}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(
            f"Completed processors: {', '.join(self.pipeline_state['processors_completed'])}"
        )

        if self.pipeline_state["processors_failed"]:
            logger.warning(
                f"Failed processors: {', '.join(self.pipeline_state['processors_failed'])}"
            )

        return results

    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status."""
        return {
            "pipeline_info": self.pipeline_state.copy(),
            "table_status": self._get_table_status(),
        }

    def _get_table_status(self) -> Dict[str, Any]:
        """Get status of all data tables."""
        tables = [
            "tracks_played",
            "spotify_artists",
            "spotify_albums",
            "spotify_artist_genre",
            "mbz_artist_info",
            "mbz_area_hierarchy",
            "cities_with_lat_long",
        ]

        table_status = {}
        for table in tables:
            if self.data_writer.table_exists(table):
                info = self.data_writer.get_table_info(table)
                table_status[table] = {
                    "exists": True,
                    "record_count": info.get("record_count", 0),
                    "columns": len(info.get("columns", [])),
                }
            else:
                table_status[table] = {"exists": False}

        return table_status


def main():
    """Main entry point for the enrichment pipeline."""
    parser = argparse.ArgumentParser(description="Data Enrichment Pipeline")
    parser.add_argument(
        "--processors",
        nargs="+",
        choices=["spotify", "musicbrainz", "geographic"],
        required=True,
        help="Specific processors to run",
    )
    parser.add_argument(
        "--skip-on-error",
        action="store_true",
        help="Continue pipeline even if individual processors fail",
    )
    parser.add_argument(
        "--status-only",
        action="store_true",
        help="Show pipeline status only (no processing)",
    )
    parser.add_argument(
        "--data-path", default="data/src", help="Base path for data files"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of records to process for testing (applied to each processor)"
    )
    parser.add_argument(
        "--mbz-step",
        choices=["discover", "fetch", "parse", "hierarchy"],
        help="Specific MusicBrainz step to run (requires --processors musicbrainz)"
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = EnrichmentPipeline(args.data_path, limit=args.limit)

    if args.status_only:
        # Show status only
        status = pipeline.get_pipeline_status()
        print(json.dumps(status, indent=2, default=str))
        return

    # Run pipeline
    try:
        if len(args.processors) == 1:
            # Run single processor
            processor = args.processors[0]
            if processor == "spotify":
                result = pipeline.run_spotify_enrichment_task()
            elif processor == "musicbrainz":
                # Handle MusicBrainz step-specific execution
                if args.mbz_step:
                    if args.mbz_step == "discover":
                        result = pipeline.run_mbz_discover_task()
                    elif args.mbz_step == "fetch":
                        result = pipeline.run_mbz_fetch_task(limit=args.limit)
                    elif args.mbz_step == "parse":
                        result = pipeline.run_mbz_parse_task()
                    elif args.mbz_step == "hierarchy":
                        result = pipeline.run_mbz_hierarchy_task(limit=args.limit)
                else:
                    result = pipeline.run_musicbrainz_enrichment_task()
            elif processor == "geographic":
                result = pipeline.run_geographic_enrichment_task()

            # Wrap result for consistent output
            result = {
                "pipeline_info": pipeline.pipeline_state,
                "processor_results": {processor: result},
                "summary": {
                    "status": result.get("overall_status", "unknown"),
                    "duration_seconds": pipeline.pipeline_state.get("duration_seconds", 0),
                    "processors_completed": [processor] if result.get("overall_status") in ["success", "partial_failure"] else [],
                    "processors_failed": [] if result.get("overall_status") in ["success", "partial_failure"] else [processor],
                }
            }
        else:
            # Run multiple specific processors
            result = pipeline.run_full_pipeline(
                skip_on_error=args.skip_on_error, processors=args.processors
            )

        # Print results
        print(json.dumps(result, indent=2, default=str))

        # Exit with appropriate code
        if result["summary"]["status"] in ["success", "partial_success"]:
            exit(0)
        else:
            exit(1)

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        exit(2)
    except Exception as e:
        logger.error(f"Pipeline failed with exception: {e}")
        exit(1)


if __name__ == "__main__":
    main()

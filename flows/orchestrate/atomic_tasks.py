#!/usr/bin/env python3
"""
Atomic task implementations for Prefect flows.

These tasks replace the duplicated subprocess calls with direct processor usage,
following DRY principles and enabling better error handling and monitoring.
"""

import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List

from prefect import task, get_run_logger

# Add project root to path for imports (more robust approach)
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from flows.orchestrate.flow_config import FlowConfig
from flows.orchestrate.base_tasks import (
    BaseTask,
    BaseProcessorTask,
    BaseEnrichmentTask,
    TaskResult,
    TaskMetrics,
    ValidationHelper,
    create_processor_task,
    create_enrichment_task,
)

# Import processor classes
from flows.enrich import (
    SpotifyProcessor,
    MusicBrainzProcessor,
    GeographicProcessor,
    ParquetDataWriter,
)


# DATA LOADING TASKS


class LoadRawTracksTask(BaseTask):
    """Task to load raw JSON track data into parquet format."""

    def execute(self, **kwargs) -> TaskResult:
        """Execute the raw tracks loading process."""
        metrics = TaskMetrics()

        try:
            # Use the existing append_tracks.py logic but with better error handling
            from flows.load.append_tracks import main as append_tracks_main

            self.logger.info("Starting raw tracks data loading")

            # Execute the append_tracks logic
            append_tracks_main()

            # Get metrics by checking the data
            tracks_info = self.data_writer.get_table_info("tracks_played")
            record_count = tracks_info.get("record_count", 0)

            metrics.record("records_loaded", record_count)

            if record_count > 0:
                return TaskResult(
                    status="success",
                    message=f"Successfully loaded tracks data - {record_count} total records",
                    metrics=metrics.finalize(),
                )
            else:
                return TaskResult(
                    status="no_updates",
                    message="No new tracks data to load",
                    metrics=metrics.finalize(),
                )

        except Exception as e:
            return self._handle_error(e, "Raw tracks loading failed")


class ValidateDataQualityTask(BaseTask):
    """Task to validate the quality and integrity of loaded data."""

    def execute(self, **kwargs) -> TaskResult:
        """Execute data quality validation."""
        metrics = TaskMetrics()

        try:
            validation_results = {}

            # Check tracks_played table
            tracks_result = ValidationHelper.validate_table_exists(
                self.data_writer, "tracks_played"
            )
            validation_results["tracks_played"] = tracks_result.to_dict()

            if not tracks_result.is_success():
                return TaskResult(
                    status="error",
                    message="Core data validation failed - tracks_played table issue",
                    data=validation_results,
                    metrics=metrics.finalize(),
                )

            # Get data quality metrics
            tracks_info = self.data_writer.get_table_info("tracks_played")
            metrics.record("total_tracks", tracks_info.get("record_count", 0))
            metrics.record("columns_count", len(tracks_info.get("columns", [])))

            # Additional quality checks
            quality_issues = []

            # Check for required columns
            required_columns = ["track_id", "artist_id", "album_id", "played_at"]
            available_columns = tracks_info.get("columns", [])

            for col in required_columns:
                if col not in available_columns:
                    quality_issues.append(f"Missing required column: {col}")

            if quality_issues:
                return TaskResult(
                    status="error",
                    message="Data quality validation failed",
                    data=validation_results,
                    errors=quality_issues,
                    metrics=metrics.finalize(),
                )

            return TaskResult(
                status="success",
                message=f"Data quality validation passed - {tracks_info.get('record_count', 0)} tracks validated",
                data=validation_results,
                metrics=metrics.finalize(),
            )

        except Exception as e:
            return self._handle_error(e, "Data quality validation failed")


# SPOTIFY INGESTION TASK


class SpotifyIngestionTask(BaseTask):
    """Task for Spotify API data ingestion."""

    def execute(self, limit: int = 50, **kwargs) -> TaskResult:
        """Execute Spotify API ingestion."""
        metrics = TaskMetrics()

        try:
            self.logger.info(f"Starting Spotify ingestion with limit: {limit}")

            # Use subprocess for now to maintain compatibility with existing script
            # TODO: Refactor spotify_api_ingestion.py to be importable
            script_path = project_root / "flows" / "ingest" / "spotify_api_ingestion.py"
            cmd = [sys.executable, str(script_path), "--limit", str(limit)]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=project_root,
                timeout=self.config.api_timeout,
            )

            if result.returncode == 0:
                try:
                    ingestion_result = json.loads(result.stdout)
                    records_ingested = ingestion_result.get("records_ingested", 0)

                    metrics.record("records_ingested", records_ingested)

                    if records_ingested > 0:
                        status = "success"
                        message = f"Successfully ingested {records_ingested} tracks from Spotify API"
                    else:
                        status = "no_updates"
                        message = "No new tracks to ingest from Spotify API"

                    return TaskResult(
                        status=status,
                        message=message,
                        data=ingestion_result,
                        metrics=metrics.finalize(),
                    )

                except json.JSONDecodeError:
                    return TaskResult(
                        status="error",
                        message="Failed to parse Spotify ingestion result",
                        errors=[{"type": "JSONDecodeError", "output": result.stdout}],
                        metrics=metrics.finalize(),
                    )
            else:
                return TaskResult(
                    status="error",
                    message=f"Spotify ingestion script failed: {result.stderr}",
                    errors=[{"type": "subprocess_error", "stderr": result.stderr}],
                    metrics=metrics.finalize(),
                )

        except subprocess.TimeoutExpired:
            return TaskResult(
                status="error",
                message=f"Spotify ingestion timed out after {self.config.api_timeout} seconds",
                metrics=metrics.finalize(),
            )
        except Exception as e:
            return self._handle_error(e, "Spotify ingestion failed")


# SPOTIFY ENRICHMENT TASKS


class SpotifyArtistEnrichmentTask(BaseEnrichmentTask):
    """Task for enriching Spotify artist data."""

    def _execute_processor(
        self, limit: Optional[int] = None, **kwargs
    ) -> Dict[str, Any]:
        """Execute Spotify artist enrichment."""
        # Apply config limit if not specified
        if limit is None:
            limit = self.config.spotify_artist_limit

        return self.processor.enrich_artists(limit=limit)


class SpotifyAlbumEnrichmentTask(BaseEnrichmentTask):
    """Task for enriching Spotify album data."""

    def _execute_processor(
        self, limit: Optional[int] = None, **kwargs
    ) -> Dict[str, Any]:
        """Execute Spotify album enrichment."""
        # Apply config limit if not specified
        if limit is None:
            limit = self.config.spotify_album_limit

        return self.processor.enrich_albums(limit=limit)


class SpotifyMBIDUpdateTask(BaseEnrichmentTask):
    """Task for updating Spotify artist MBIDs from MusicBrainz data."""

    def _execute_processor(self, **kwargs) -> Dict[str, Any]:
        """Execute MBID updates."""
        return self.processor.update_artist_mbids()


# MUSICBRAINZ ENRICHMENT TASKS


class MusicBrainzDiscoveryTask(BaseEnrichmentTask):
    """Task for discovering artists that need MusicBrainz enrichment."""

    def _execute_processor(self, **kwargs) -> Dict[str, Any]:
        """Execute MusicBrainz artist discovery."""
        return self.processor.discover_missing_artists()


class MusicBrainzFetchTask(BaseEnrichmentTask):
    """Task for fetching MusicBrainz artist data."""

    def _execute_processor(
        self, limit: Optional[int] = None, **kwargs
    ) -> Dict[str, Any]:
        """Execute MusicBrainz artist data fetching."""
        # Apply config limit if not specified
        if limit is None:
            limit = self.config.musicbrainz_fetch_limit

        # First discover artists
        discovery_result = self.processor.discover_missing_artists()

        if discovery_result["status"] != "success":
            return discovery_result

        missing_artists_df = discovery_result["missing_artists"]

        # Apply limit if specified
        if limit is not None and len(missing_artists_df) > limit:
            missing_artists_df = missing_artists_df.head(limit)

        return self.processor.fetch_artist_data(missing_artists_df)


class MusicBrainzParseTask(BaseEnrichmentTask):
    """Task for parsing MusicBrainz JSON files."""

    def _execute_processor(self, **kwargs) -> Dict[str, Any]:
        """Execute MusicBrainz JSON parsing."""
        return self.processor.parse_artist_json_files()


class MusicBrainzHierarchyTask(BaseEnrichmentTask):
    """Task for processing MusicBrainz area hierarchy."""

    def _execute_processor(
        self, limit: Optional[int] = None, **kwargs
    ) -> Dict[str, Any]:
        """Execute MusicBrainz area hierarchy processing."""
        # Apply config limit if not specified
        if limit is None:
            limit = self.config.musicbrainz_hierarchy_limit

        return self.processor.process_area_hierarchy(limit=limit)


# GEOGRAPHIC ENRICHMENT TASKS


class GeographicEnrichmentTask(BaseEnrichmentTask):
    """Task for geographic data enrichment."""

    def _validate_prerequisites(self) -> TaskResult:
        """Validate that mbz_area_hierarchy table exists for geographic enrichment."""
        try:
            self.logger.info("Validating prerequisites for geographic enrichment")
            self.logger.info("Checking if mbz_area_hierarchy table exists")

            table_exists = self.data_writer.table_exists("mbz_area_hierarchy")
            self.logger.info(f"mbz_area_hierarchy table exists: {table_exists}")

            if not table_exists:
                return TaskResult(
                    status="error",
                    message="mbz_area_hierarchy table not found - cannot perform geographic enrichment",
                )

            self.logger.info("Getting table info for mbz_area_hierarchy")
            area_info = self.data_writer.get_table_info("mbz_area_hierarchy")
            record_count = area_info.get("record_count", 0)
            self.logger.info(f"mbz_area_hierarchy record count: {record_count}")

            if record_count == 0:
                return TaskResult(
                    status="error",
                    message="mbz_area_hierarchy table is empty - cannot perform geographic enrichment",
                )

            return TaskResult(
                status="success",
                message=f"Prerequisites validated - {record_count} area records available",
            )
        except Exception as e:
            self.logger.error(
                f"Error during prerequisite validation: {e}", exc_info=True
            )
            return TaskResult(
                status="error",
                message=f"Prerequisite validation failed: {str(e)}",
            )

    def _execute_processor(self, **kwargs) -> Dict[str, Any]:
        """Execute geographic enrichment."""
        return self.processor.run_full_enrichment()


# TRANSFORMATION TASKS


class DBTTransformationTask(BaseTask):
    """Task for running DBT transformations."""

    def execute(self, **kwargs) -> TaskResult:
        """Execute DBT transformations."""
        metrics = TaskMetrics()

        try:
            self.logger.info("Starting DBT transformations")

            dbt_dir = self.config.dbt_dir

            # Log the dbt directory and cwd for debugging
            self.logger.info(f"DBT directory: {dbt_dir}")
            self.logger.info(f"Current working directory will be: {dbt_dir}")

            # Ensure dbt dependencies
            deps_cmd = [
                "uv",
                "run",
                "dbt",
                "deps",
                "--profiles-dir",
                ".",
                "--project-dir",
                ".",
            ]

            self.logger.info(f"Running dbt deps command: {' '.join(deps_cmd)}")

            deps_result = subprocess.run(
                deps_cmd,
                capture_output=True,
                text=True,
                cwd=dbt_dir,
                timeout=300,
            )

            if deps_result.returncode != 0:
                self.logger.warning("dbt deps failed")
                if deps_result.stderr.strip():
                    for line in deps_result.stderr.strip().split("\n"):
                        if line.strip():
                            self.logger.warning(f"DBT Deps: {line}")

            # Run dbt build
            build_cmd = [
                "uv",
                "run",
                "dbt",
                "build",
                "--profiles-dir",
                ".",
                "--project-dir",
                ".",
            ]

            self.logger.info(f"Running dbt build command: {' '.join(build_cmd)}")

            build_result = subprocess.run(
                build_cmd,
                capture_output=True,
                text=True,
                cwd=dbt_dir,
                timeout=self.config.dbt_timeout,
            )

            if build_result.returncode == 0:
                self.logger.info("DBT transformations completed successfully")

                # Log dbt stdout if present
                if build_result.stdout.strip():
                    for line in build_result.stdout.strip().split("\n"):
                        if line.strip():
                            self.logger.info(f"DBT: {line}")

                # Log any stderr warnings even on success
                if build_result.stderr.strip():
                    for line in build_result.stderr.strip().split("\n"):
                        if line.strip():
                            self.logger.warning(f"DBT Warning: {line}")

                return TaskResult(
                    status="success",
                    message="DBT transformations completed successfully",
                    data={"stdout": build_result.stdout, "stderr": build_result.stderr},
                    metrics=metrics.finalize(),
                )
            else:
                self.logger.error("DBT build failed")

                # Log stdout if present (may contain useful context)
                if build_result.stdout.strip():
                    for line in build_result.stdout.strip().split("\n"):
                        if line.strip():
                            self.logger.info(f"DBT Output: {line}")

                # Log stderr errors
                if build_result.stderr.strip():
                    for line in build_result.stderr.strip().split("\n"):
                        if line.strip():
                            self.logger.error(f"DBT Error: {line}")

                return TaskResult(
                    status="error",
                    message=f"DBT build failed: {build_result.stderr}",
                    data={"stdout": build_result.stdout, "stderr": build_result.stderr},
                    metrics=metrics.finalize(),
                )

        except subprocess.TimeoutExpired:
            return TaskResult(
                status="error",
                message=f"DBT transformations timed out after {self.config.dbt_timeout} seconds",
                metrics=metrics.finalize(),
            )
        except Exception as e:
            return self._handle_error(e, "DBT transformations failed")


class ReportingUpdateTask(BaseTask):
    """Task for updating reporting data."""

    def execute(self, **kwargs) -> TaskResult:
        """Execute reporting data updates."""
        metrics = TaskMetrics()

        try:
            # Placeholder for Metabase integration
            self.logger.info("Updating reporting data (placeholder)")

            return TaskResult(
                status="success",
                message="Reporting data update completed (placeholder)",
                metrics=metrics.finalize(),
            )

        except Exception as e:
            return self._handle_error(e, "Reporting update failed")


# PREFECT TASK DEFINITIONS


# Data loading tasks
@task(
    name="Load Raw Tracks Data",
    description="Load raw JSON track files into parquet format",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=600,
)
def load_raw_tracks_data(config: FlowConfig) -> Dict[str, Any]:
    """Load raw tracks data task."""
    logger = get_run_logger()
    task_instance = LoadRawTracksTask(config)
    result = task_instance.execute()

    if result.is_success():
        logger.info(f"Data loading completed: {result.message}")
    else:
        logger.error(f"Data loading failed: {result.message}")

    return result.to_dict()


@task(
    name="Validate Data Quality",
    description="Validate the quality and integrity of loaded data",
    retries=1,
    retry_delay_seconds=10,
    timeout_seconds=300,
)
def validate_data_quality(config: FlowConfig) -> Dict[str, Any]:
    """Validate data quality task."""
    logger = get_run_logger()
    task_instance = ValidateDataQualityTask(config)
    result = task_instance.execute()

    if result.is_success():
        logger.info(f"Data validation completed: {result.message}")
    else:
        logger.error(f"Data validation failed: {result.message}")

    return result.to_dict()


# Spotify ingestion task
@task(
    name="Spotify API Ingestion",
    description="Fetch recently played tracks from Spotify API",
    retries=3,
    retry_delay_seconds=60,
    timeout_seconds=300,
)
def spotify_api_ingestion(config: FlowConfig, limit: int = 50) -> Dict[str, Any]:
    """Spotify API ingestion task."""
    logger = get_run_logger()
    task_instance = SpotifyIngestionTask(config)
    result = task_instance.execute(limit=limit)

    if result.is_success():
        logger.info(f"Spotify ingestion completed: {result.message}")
    else:
        logger.error(f"Spotify ingestion failed: {result.message}")

    return result.to_dict()


# Create enrichment tasks using the factory functions
spotify_artist_enrichment = create_enrichment_task(
    task_class=SpotifyArtistEnrichmentTask,
    processor_class=SpotifyProcessor,
    task_name="Enrich Spotify Artists",
    description="Enrich missing artist data from Spotify API",
    timeout_seconds=900,
)

spotify_album_enrichment = create_enrichment_task(
    task_class=SpotifyAlbumEnrichmentTask,
    processor_class=SpotifyProcessor,
    task_name="Enrich Spotify Albums",
    description="Enrich missing album data from Spotify API",
    timeout_seconds=900,
)

spotify_mbid_update = create_enrichment_task(
    task_class=SpotifyMBIDUpdateTask,
    processor_class=SpotifyProcessor,
    task_name="Update Spotify Artist MBIDs",
    description="Update Spotify artist MBIDs from MusicBrainz data",
    timeout_seconds=600,
)

musicbrainz_discovery = create_enrichment_task(
    task_class=MusicBrainzDiscoveryTask,
    processor_class=MusicBrainzProcessor,
    task_name="Discover Missing MBZ Artists",
    description="Discover artists needing MusicBrainz enrichment",
    timeout_seconds=300,
)

musicbrainz_fetch = create_enrichment_task(
    task_class=MusicBrainzFetchTask,
    processor_class=MusicBrainzProcessor,
    task_name="Fetch MBZ Artist Data",
    description="Fetch artist data from MusicBrainz API",
    timeout_seconds=900,
)

musicbrainz_parse = create_enrichment_task(
    task_class=MusicBrainzParseTask,
    processor_class=MusicBrainzProcessor,
    task_name="Parse MBZ JSON Files",
    description="Parse MusicBrainz JSON files into structured data",
    timeout_seconds=600,
)

musicbrainz_hierarchy = create_enrichment_task(
    task_class=MusicBrainzHierarchyTask,
    processor_class=MusicBrainzProcessor,
    task_name="Process MBZ Area Hierarchy",
    description="Process MusicBrainz area hierarchy data",
    timeout_seconds=1200,
)

geographic_enrichment = create_enrichment_task(
    task_class=GeographicEnrichmentTask,
    processor_class=GeographicProcessor,
    task_name="Geographic Data Enrichment",
    description="Process geographic coordinate and continent data",
    timeout_seconds=600,
    retries=0,  # Disable retries to ensure immediate failure on error
)


# Transformation tasks
@task(
    name="DBT Transformations",
    description="Run DBT transformations for star schema",
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=1200,
)
def dbt_transformations(config: FlowConfig) -> Dict[str, Any]:
    """DBT transformations task."""
    logger = get_run_logger()
    task_instance = DBTTransformationTask(config)
    result = task_instance.execute()

    if result.is_success():
        logger.info(f"DBT transformations completed: {result.message}")
    else:
        logger.error(f"DBT transformations failed: {result.message}")

    return result.to_dict()


@task(
    name="Update Reporting Data",
    description="Update reporting data and refresh dashboards",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=300,
)
def update_reporting_data(config: FlowConfig) -> Dict[str, Any]:
    """Update reporting data task."""
    logger = get_run_logger()
    task_instance = ReportingUpdateTask(config)
    result = task_instance.execute()

    if result.is_success():
        logger.info(f"Reporting update completed: {result.message}")
    else:
        logger.error(f"Reporting update failed: {result.message}")

    return result.to_dict()

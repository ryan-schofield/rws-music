#!/usr/bin/env python3
"""
Spotify Ingestion Flow.

This module contains the Prefect flow for ingesting data from the Spotify API.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from prefect import flow, get_run_logger
from prefect.states import Failed

# Add project root to path for imports (more robust approach)
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from flows.orchestrate.flow_config import FlowConfig, get_flow_config
from flows.orchestrate.monitoring import metrics_collector, monitor_flow, send_alert
from flows.orchestrate.atomic_tasks import spotify_api_ingestion


# SPOTIFY INGESTION FLOW


@flow(
    name="Spotify Ingestion Flow",
    description="Spotify API ingestion with optimized error handling",
    version="1.0.0",
)
def spotify_ingestion_flow(
    limit: int = 50, config: Optional[FlowConfig] = None
) -> Dict[str, Any]:
    """
    Spotify ingestion flow with the following features:
    - Direct task usage (no subprocess duplication)
    - Comprehensive error handling
    - Consistent result format
    - Configurable limits

    Args:
        limit: Maximum number of tracks to fetch per run
        config: Flow configuration (uses default if None)

    Returns:
        Dict containing flow execution results
    """
    logger = get_run_logger()
    config = config or get_flow_config()

    # Override limit with config if not specified
    if limit == 50 and config.spotify_artist_limit is not None:
        limit = min(limit, config.spotify_artist_limit)

    with monitor_flow("spotify_ingestion", {"limit": limit}) as execution_id:
        logger.info(f"Starting Spotify ingestion flow (execution: {execution_id})")

        try:
            # Execute ingestion task
            ingestion_result = spotify_api_ingestion(config, limit=limit)

            # Prepare flow result
            flow_result = {
                "flow_name": "spotify_ingestion",
                "execution_id": execution_id,
                "status": ingestion_result.get("status", "unknown"),
                "task_results": {"spotify_api_ingestion": ingestion_result},
                "summary": {
                    "records_ingested": ingestion_result.get("metrics", {}).get(
                        "records_ingested", 0
                    ),
                    "limit_used": limit,
                },
            }

            if ingestion_result.get("status") in ["success", "no_updates"]:
                logger.info("Spotify ingestion flow completed successfully")
                return flow_result
            else:
                logger.error(
                    f"Spotify ingestion flow failed: {ingestion_result.get('message', 'Unknown error')}"
                )
                raise Failed(
                    message=f"Spotify ingestion failed: {ingestion_result.get('message')}",
                    result=flow_result,
                )

        except Exception as e:
            error_msg = f"Spotify ingestion flow failed: {e}"
            logger.error(error_msg)

            # Send alert for ingestion failures
            send_alert(
                f"Spotify ingestion flow failed: {str(e)}",
                level="error",
                execution_id=execution_id,
                flow_name="spotify_ingestion",
            )

            raise Failed(
                message=error_msg,
                result={
                    "flow_name": "spotify_ingestion",
                    "execution_id": execution_id,
                    "status": "failed",
                    "error": str(e),
                },
            )
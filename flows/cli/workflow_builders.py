#!/usr/bin/env python3
"""
Workflow builders for music tracker ETL pipelines.

Constructs n8n workflow definitions for:
- Spotify Ingestion: Scheduled ingestion of recently played tracks
- Daily ETL: Full data pipeline (ingestion → enrichment → transformation)
"""

from flows.cli.workflow_definitions import (
    N8NWorkflow,
    create_cli_execution_node,
    create_cron_trigger_node,
    create_condition_node,
    create_wait_node,
)


def build_spotify_ingestion_workflow() -> dict:
    """
    Build Spotify ingestion workflow.
    
    Scheduled to run every 30 minutes to fetch recently played tracks.
    
    Nodes:
    1. Cron trigger (every 30 minutes)
    2. Ingest Spotify tracks
    3. Load raw tracks
    4. Validate data
    5. Return result
    
    Returns:
        Workflow JSON definition
    """
    workflow = N8NWorkflow(
        name="Spotify Ingestion",
        description="Scheduled ingestion of recently played tracks from Spotify API every 30 minutes",
        active=True,
    )
    
    # Node positions (x, y)
    x_spacing = 300
    y_spacing = 100
    
    # 1. Cron trigger - every 30 minutes
    workflow.add_node(
        create_cron_trigger_node(
            node_name="Trigger - Every 30min",
            expression="*/30 * * * *",  # Every 30 minutes
            position=[0, 100],
        )
    )
    
    # 2. Ingest Spotify tracks
    workflow.add_node(
        create_cli_execution_node(
            node_name="Ingest Spotify Tracks",
            cli_script="flows/cli/ingest_spotify.py",
            cli_args={"limit": "50"},
            position=[x_spacing, 100],
        )
    )
    
    # 3. Load raw tracks
    workflow.add_node(
        create_cli_execution_node(
            node_name="Load Raw Tracks",
            cli_script="flows/cli/load_raw_tracks.py",
            position=[x_spacing * 2, 100],
        )
    )
    
    # 4. Validate data
    workflow.add_node(
        create_cli_execution_node(
            node_name="Validate Data",
            cli_script="flows/cli/validate_data.py",
            position=[x_spacing * 3, 100],
        )
    )
    
    # Connect nodes in sequence
    workflow.connect("Trigger - Every 30min", "Ingest Spotify Tracks")
    workflow.connect("Ingest Spotify Tracks", "Load Raw Tracks")
    workflow.connect("Load Raw Tracks", "Validate Data")
    
    # Set workflow settings
    workflow.set_settings({
        "errorHandler": {
            "stopOnError": False,
            "retries": 1,
            "retryInterval": 60,
        },
        "executionData": {
            "timeout": 300,  # 5 minutes - quick execution
        },
    })
        },
    })
    
    return workflow.to_dict()


def build_daily_etl_workflow() -> dict:
    """
    Build daily ETL workflow.
    
    Scheduled to run daily at 2 AM. Runs complete data pipeline:
    - Ingestion (Spotify tracks)
    - Loading (raw data)
    - Validation (data quality)
    - Enrichment (Spotify, MusicBrainz, Geographic)
    - Transformation (dbt build)
    
    Nodes:
    1. Cron trigger (daily at 2 AM)
    2. Ingest Spotify
    3. Load raw tracks
    4. Validate data
    5. Enrich Spotify artists (parallel)
    6. Enrich Spotify albums (parallel)
    7. Discover MusicBrainz artists (sequential after Spotify enrichment)
    8. Fetch MusicBrainz artists
    9. Parse MusicBrainz data
    10. Process MusicBrainz hierarchy
    11. Enrich geography
    12. Update MBIDs
    13. Run dbt build
    14. Check result and notify
    
    Returns:
        Workflow JSON definition
    """
    workflow = N8NWorkflow(
        name="Daily ETL",
        description="Complete daily ETL pipeline: ingest, enrich, and transform data (runs daily at 2 AM UTC)",
        active=True,
    )
    
    # Node positions for layout
    x_ingestion = 0
    x_loading = 300
    x_validation = 600
    x_spotify_enrich = 900
    x_mbz_discover = 1200
    x_mbz_fetch = 1500
    x_mbz_parse = 1800
    x_mbz_hierarchy = 2100
    x_geo = 2400
    x_mbid_update = 2700
    x_dbt = 3000
    x_result = 3300
    
    # 1. Cron trigger - daily at 2 AM UTC
    workflow.add_node(
        create_cron_trigger_node(
            node_name="Trigger - Daily 2 AM",
            expression="0 2 * * *",  # 2 AM UTC daily
            position=[x_ingestion, 100],
        )
    )
    
    # 2. Ingest Spotify tracks
    workflow.add_node(
        create_cli_execution_node(
            node_name="Ingest Spotify",
            cli_script="flows/cli/ingest_spotify.py",
            cli_args={"limit": "100"},
            position=[x_loading, 100],
        )
    )
    
    # 3. Load raw tracks
    workflow.add_node(
        create_cli_execution_node(
            node_name="Load Raw Tracks",
            cli_script="flows/cli/load_raw_tracks.py",
            position=[x_validation, 100],
        )
    )
    
    # 4. Validate data
    workflow.add_node(
        create_cli_execution_node(
            node_name="Validate Data",
            cli_script="flows/cli/validate_data.py",
            position=[x_spotify_enrich, 100],
        )
    )
    
    # 5. Enrich Spotify artists (parallel 1)
    workflow.add_node(
        create_cli_execution_node(
            node_name="Enrich Spotify Artists",
            cli_script="flows/cli/enrich_spotify_artists.py",
            cli_args={"limit": "50"},
            position=[x_spotify_enrich, 200],
        )
    )
    
    # 6. Enrich Spotify albums (parallel 2)
    workflow.add_node(
        create_cli_execution_node(
            node_name="Enrich Spotify Albums",
            cli_script="flows/cli/enrich_spotify_albums.py",
            cli_args={"limit": "50"},
            position=[x_spotify_enrich, 300],
        )
    )
    
    # Wait node to ensure Spotify enrichment is complete
    workflow.add_node(
        create_wait_node(
            node_name="Wait for Spotify",
            wait_type="seconds",
            amount=30,
            position=[x_mbz_discover, 250],
        )
    )
    
    # 7. Discover MusicBrainz artists (sequential after Spotify)
    workflow.add_node(
        create_cli_execution_node(
            node_name="Discover MBZ Artists",
            cli_script="flows/cli/discover_mbz_artists.py",
            position=[x_mbz_fetch, 100],
        )
    )
    
    # 8. Fetch MusicBrainz artists
    workflow.add_node(
        create_cli_execution_node(
            node_name="Fetch MBZ Artists",
            cli_script="flows/cli/fetch_mbz_artists.py",
            cli_args={"limit": "100", "workers": "5"},
            position=[x_mbz_parse, 100],
        )
    )
    
    # 9. Parse MusicBrainz data
    workflow.add_node(
        create_cli_execution_node(
            node_name="Parse MBZ Data",
            cli_script="flows/cli/parse_mbz_data.py",
            position=[x_mbz_hierarchy, 100],
        )
    )
    
    # 10. Process MusicBrainz hierarchy
    workflow.add_node(
        create_cli_execution_node(
            node_name="Process MBZ Hierarchy",
            cli_script="flows/cli/process_mbz_hierarchy.py",
            position=[x_geo, 100],
        )
    )
    
    # 11. Enrich geography
    workflow.add_node(
        create_cli_execution_node(
            node_name="Enrich Geography",
            cli_script="flows/cli/enrich_geography.py",
            position=[x_mbid_update, 100],
        )
    )
    
    # 12. Update MBIDs
    workflow.add_node(
        create_cli_execution_node(
            node_name="Update MBIDs",
            cli_script="flows/cli/update_mbids.py",
            position=[x_dbt, 100],
        )
    )
    
    # 13. Run dbt build
    workflow.add_node(
        create_cli_execution_node(
            node_name="Run dbt Build",
            cli_script="flows/cli/run_dbt.py",
            cli_args={"command": "build"},
            position=[x_result, 100],
        )
    )
    
    # Connect nodes in sequence
    # Ingestion pipeline
    workflow.connect("Trigger - Daily 2 AM", "Ingest Spotify")
    workflow.connect("Ingest Spotify", "Load Raw Tracks")
    workflow.connect("Load Raw Tracks", "Validate Data")
    
    # Validation splits to parallel Spotify enrichment
    workflow.connect("Validate Data", "Enrich Spotify Artists")
    workflow.connect("Validate Data", "Enrich Spotify Albums")
    
    # Both Spotify enrichment tasks connect to wait
    workflow.connect("Enrich Spotify Artists", "Wait for Spotify")
    workflow.connect("Enrich Spotify Albums", "Wait for Spotify")
    
    # Sequential MusicBrainz pipeline
    workflow.connect("Wait for Spotify", "Discover MBZ Artists")
    workflow.connect("Discover MBZ Artists", "Fetch MBZ Artists")
    workflow.connect("Fetch MBZ Artists", "Parse MBZ Data")
    workflow.connect("Parse MBZ Data", "Process MBZ Hierarchy")
    
    # Geographic enrichment
    workflow.connect("Process MBZ Hierarchy", "Enrich Geography")
    
    # MBID update then transformation
    workflow.connect("Enrich Geography", "Update MBIDs")
    workflow.connect("Update MBIDs", "Run dbt Build")
    
    # Set workflow settings
    workflow.set_settings({
        "errorHandler": {
            "stopOnError": False,
            "retries": 1,
            "retryInterval": 60,
        },
        "executionData": {
            "timeout": 3600,  # 1 hour total timeout
            "maxConcurrentExecutions": 1,
        },
    })
    
    return workflow.to_dict()

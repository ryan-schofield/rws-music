# n8n Workflows

This directory contains workflow definitions for the Music Tracker application's orchestration layer using n8n.

## Overview

The n8n workflows replace the previous Prefect orchestration system. All Python data processing code and dbt transformations remain unchanged - n8n is only responsible for scheduling and coordinating the execution of these existing scripts.

### Architecture

- **Main Workflows**: Top-level workflows that define the complete ETL pipelines
- **Sub-workflows**: Reusable sub-workflows that handle specific data processing domains
- **Version Control**: All workflow definitions are exported as JSON for Git tracking

### Workflow Structure

```
n8n-workflows/
├── README.md                              # This file
├── metadata.json                          # Auto-generated workflow metadata
├── spotify_ingestion_workflow.json         # Main: Daily Spotify data ingestion
├── daily_etl_workflow.json                 # Main: Complete daily ETL pipeline
├── subflows/
│   ├── data_preparation_workflow.json      # Load and validate raw data
│   ├── spotify_enrichment_workflow.json     # Enrich Spotify data (artists, albums, MBIDs)
│   ├── musicbrainz_enrichment_workflow.json # Enrich MusicBrainz data (artists, hierarchy, areas)
│   ├── geographic_enrichment_workflow.json  # Geographic enrichment
│   └── transformation_workflow.json         # Run dbt transformations
└── utils/
    ├── export_workflows.py                 # Export all workflows from n8n to JSON
    ├── import_workflows.py                 # Import workflows from JSON to n8n
    └── __init__.py
```

## Workflows

### Main Workflows

#### Spotify Ingestion Workflow (`spotify_ingestion_workflow.json`)
- **Purpose**: Daily ingestion of recently played tracks from Spotify API
- **Schedule**: Configured via n8n UI
- **Python CLI**: `flows/cli/ingest_spotify.py`
- **Output**: Raw CSV file and DuckDB table

#### Daily ETL Workflow (`daily_etl_workflow.json`)
- **Purpose**: Complete daily data pipeline combining all enrichment and transformation tasks
- **Sequence**: Data Prep → Spotify Enrichment → MusicBrainz Enrichment → Geographic Enrichment → dbt Transformations
- **Sub-workflows**: Orchestrates all four sub-workflows in sequence
- **Estimated Duration**: 30-45 minutes depending on data volume

### Sub-workflows

#### Data Preparation (`subflows/data_preparation_workflow.json`)
- **Tasks**:
  1. Load raw tracks from CSV to DuckDB
  2. Validate data quality
- **Python CLIs**: `flows/cli/load_raw_tracks.py`, `flows/cli/validate_data.py`
- **Retry**: 3 attempts, 60s delay
- **Timeout**: 5 minutes

#### Spotify Enrichment (`subflows/spotify_enrichment_workflow.json`)
- **Tasks**:
  1. Enrich Spotify artists (parallel)
  2. Enrich Spotify albums (parallel)
  3. Update MusicBrainz IDs from Spotify data
- **Python CLIs**: `flows/cli/enrich_spotify_artists.py`, `flows/cli/enrich_spotify_albums.py`, `flows/cli/update_mbids.py`
- **Retry**: 3 attempts, 60s delay
- **Timeout**: 10 minutes
- **Note**: Artist and album enrichment run in parallel for efficiency

#### MusicBrainz Enrichment (`subflows/musicbrainz_enrichment_workflow.json`)
- **Tasks**:
  1. Discover artists needing enrichment
  2. Fetch MusicBrainz artist data (skipped if no work)
  3. Parse JSON responses
  4. Process area hierarchy
- **Python CLIs**: `flows/cli/discover_mbz_artists.py`, `flows/cli/fetch_mbz_artists.py`, `flows/cli/parse_mbz_data.py`, `flows/cli/process_mbz_hierarchy.py`
- **Retry**: 3 attempts for fetch, 2 for processing
- **Timeout**: 900s fetch, 600s others

#### Geographic Enrichment (`subflows/geographic_enrichment_workflow.json`)
- **Task**: Enrich tracks with geographic data based on artist locations
- **Python CLI**: `flows/cli/enrich_geography.py`
- **Retry**: 0 (fail fast for data integrity)
- **Timeout**: 30 minutes

#### Transformation (`subflows/transformation_workflow.json`)
- **Task**: Run dbt to build dimensional and reporting models
- **Python CLI**: `flows/cli/run_dbt.py`
- **Retry**: 2 attempts, 30s delay
- **Timeout**: 20 minutes

## Workflow Management

### Exporting Workflows

Export all workflows from n8n to JSON files for version control:

```bash
cd n8n-workflows/utils
python export_workflows.py --host localhost --port 5678 --output ..
```

**Options**:
- `--host`: n8n host (default: localhost, env: N8N_HOST)
- `--port`: n8n port (default: 5678, env: N8N_PORT)
- `--api-key`: API key for authentication (env: N8N_API_KEY)
- `--output`: Output directory (default: current directory)

**Output**:
- Individual workflow JSON files (e.g., `spotify_ingestion_workflow_123.json`)
- `metadata.json` with workflow list and export timestamp

### Importing Workflows

Import workflows from JSON files to n8n:

```bash
# Import single workflow
cd n8n-workflows/utils
python import_workflows.py --workflow-file ../spotify_ingestion_workflow.json

# Import all workflows from directory
python import_workflows.py --workflow-dir ..

# Update existing workflows
python import_workflows.py --workflow-dir .. --update
```

**Options**:
- `--host`: n8n host (default: localhost, env: N8N_HOST)
- `--port`: n8n port (default: 5678, env: N8N_PORT)
- `--api-key`: API key for authentication (env: N8N_API_KEY)
- `--workflow-file`: Single workflow JSON file to import
- `--workflow-dir`: Directory with workflow JSON files (default: current)
- `--update`: Update existing workflows (default: skip existing)

## Python CLI Scripts

All workflows execute Python CLI scripts located in `flows/cli/`. These scripts:
- Accept command-line arguments
- Output JSON results to stdout
- Exit with code 0 on success, 1 on failure
- Include retry logic and timeouts as configured per workflow

### CLI Naming Convention
- `ingest_*`: Data ingestion from external sources
- `load_*`: Loading data into DuckDB
- `enrich_*`: Data enrichment tasks
- `validate_*`: Data quality validation
- `run_*`: Complex operations (e.g., dbt runs)

### CLI Base Class
All CLI scripts inherit from `flows/cli/base.py` which provides:
- Standardized argument parsing
- JSON output formatting
- Error handling and reporting
- Logging configuration

## Configuration

### Environment Variables

Required in `.env` or Docker environment:
- `N8N_HOST`: n8n hostname (default: localhost)
- `N8N_PORT`: n8n port (default: 5678)
- `N8N_API_KEY`: API key for programmatic access (optional)
- `TIMEZONE`: Timezone for scheduling (default: UTC)

### Workflow IDs

Each workflow has a unique ID assigned by n8n. Store these in a configuration file or environment for reference:

```json
{
  "workflows": {
    "spotify_ingestion": "123abc",
    "daily_etl": "456def",
    "subflows": {
      "data_preparation": "789ghi",
      "spotify_enrichment": "012jkl",
      "musicbrainz_enrichment": "345mno",
      "geographic_enrichment": "678pqr",
      "transformation": "901stu"
    }
  }
}
```

## Monitoring & Logging

### n8n Logs
- Access via n8n UI: http://localhost:5678
- View execution history and error details
- Webhook logs for debugging webhook-triggered workflows

### Application Logs
- Python logs: `logs/` directory (mounted from container)
- dbt logs: `dbt/logs/` directory with run details
- Flow metrics: `logs/flow_metrics.json` tracks execution stats

## Deployment

### Local Development
1. Ensure n8n is running: `docker-compose up n8n`
2. Access UI at http://localhost:5678
3. Create workflows via UI or import from JSON

### Production Deployment
1. Set `N8N_HOST` to production hostname
2. Configure `N8N_API_KEY` for programmatic access
3. Use `import_workflows.py` to deploy workflow definitions
4. Update Docker secrets/env for sensitive configuration

## Migration Notes

**From Prefect to n8n**:
- All Python code and business logic remains unchanged
- Workflow definitions stored as JSON instead of Python code
- API-first approach allows version control and reproducible deployments
- Memory footprint reduced from ~900MB (Prefect) to ~300MB (n8n)
- Single-instance architecture simplifies operations vs Prefect multi-service setup

**Known Differences**:
- Scheduling configured in n8n UI or via API (not Python decorators)
- No built-in Prefect Blocks - external service integrations via HTTP nodes
- Workflow execution history available via API and UI

## Support & Troubleshooting

### n8n Not Responding
```bash
docker-compose logs n8n
docker-compose restart n8n
```

### Workflow Import Fails
- Verify n8n is running and accessible
- Check workflow JSON format is valid
- Ensure API credentials are correct

### CLI Script Errors
- Check Python environment has required packages: `uv sync`
- Verify data paths exist and are readable
- Review logs in `logs/` directory

## References

- **n8n Documentation**: https://docs.n8n.io
- **n8n API**: https://docs.n8n.io/api/
- **GitHub**: https://github.com/n8n-io/n8n
- **Music Tracker**: See parent README.md

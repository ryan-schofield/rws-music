# n8n Workflows

This directory contains n8n workflow definitions for the music tracker ETL pipeline.

## Overview

Two main workflows orchestrate the complete data pipeline:

1. **Spotify Ingestion** (`spotify_ingestion_workflow.json`)
   - Scheduled every 6 hours
   - Fetches recently played tracks from Spotify API
   - Loads and validates raw data
   
2. **Daily ETL** (`daily_etl_workflow.json`)
   - Scheduled daily at 2 AM UTC
   - Complete enrichment and transformation pipeline
   - Ingests → Enriches (Spotify, MusicBrainz, Geography) → Transforms (dbt)

## Quick Start

### Deploy Workflows

```bash
# Deploy all workflows to n8n instance
python flows/cli/deploy_n8n_workflows.py --action deploy

# Verify deployment
python flows/cli/deploy_n8n_workflows.py --action status

# Export for version control
python flows/cli/deploy_n8n_workflows.py --action export
```

### Access n8n UI

Open browser to: **http://localhost:5678**

- View workflow definitions
- Monitor execution history
- Manually trigger workflows
- Check for errors

### Manage Workflows

```bash
# Check status
python flows/cli/deploy_n8n_workflows.py --action status

# Activate workflow
python flows/cli/deploy_n8n_workflows.py --action activate --workflow "Spotify Ingestion"

# Deactivate workflow
python flows/cli/deploy_n8n_workflows.py --action deactivate --workflow "Spotify Ingestion"
```

## Workflow Definitions

### Spotify Ingestion (Every 6h)

**File**: `spotify_ingestion_workflow.json`

**Schedule**: Every 6 hours (0, 6, 12, 18 UTC)

**Pipeline**:
1. Trigger on cron schedule
2. Ingest recently played tracks from Spotify
3. Load raw data into structured format
4. Validate data quality

**Typical Duration**: 5-10 minutes

**Purpose**: Keep recently played tracks data fresh (max 6 hours old)

### Daily ETL (2 AM UTC)

**File**: `daily_etl_workflow.json`

**Schedule**: Daily at 2 AM UTC

**Pipeline**:
1. Trigger on cron schedule
2. Ingest Spotify tracks
3. Load raw data
4. Validate data quality
5. Enrich Spotify artists & albums (parallel)
6. Wait for enrichment to complete
7. Discover missing MusicBrainz artists
8. Fetch MusicBrainz artist data
9. Parse MusicBrainz JSON
10. Process geographic hierarchy
11. Enrich geographic data
12. Update Spotify artists with MBID
13. Run dbt transformations

**Typical Duration**: 45-90 minutes

**Purpose**: Complete data enrichment and transformation

## File Structure

```
n8n-workflows/
├── README.md                           # This file
├── PHASE_3_GUIDE.md                   # Detailed Phase 3 documentation
├── spotify_ingestion_workflow.json      # Spotify ingestion workflow
├── daily_etl_workflow.json             # Daily ETL workflow
└── utils/
    ├── __init__.py
    ├── export_workflows.py             # Export workflows to JSON
    └── import_workflows.py             # Import workflows from JSON
```

## Utilities

### Export Workflows

```bash
# Export all workflows to JSON files
python n8n-workflows/utils/export_workflows.py
```

This is useful for:
- Backing up workflow definitions
- Version control
- Sharing workflows
- Disaster recovery

### Import Workflows

```bash
# Import workflows from JSON files
python n8n-workflows/utils/import_workflows.py
```

Use when:
- Deploying to new n8n instance
- Restoring from backup
- Migrating between environments

## Configuration

### Environment Variables

Set in `.env`:

```bash
# n8n instance URL
N8N_BASE_URL=http://localhost:5678

# Optional: API key for authentication
N8N_API_KEY=your_api_key_here
```

### Workflow Settings

Default settings are configured in `flows/cli/workflow_builders.py`:

- **Timeout**: 1 hour (daily ETL), 15 min (Spotify ingestion)
- **Retries**: 1-3 depending on task criticality
- **Retry delay**: 60 seconds
- **Error handling**: Continue on error (non-blocking)

## Monitoring

### Check Execution History

1. Open http://localhost:5678
2. Click on workflow name
3. View "Executions" tab
4. Click on execution to see details

### Common Issues

**Workflow doesn't start**: 
- Check if workflow is active (blue toggle)
- Verify cron expression is correct
- Check n8n logs: `docker logs music-tracker-n8n`

**CLI script fails**:
- Run CLI manually to debug
- Check stderr output in n8n UI
- Verify environment variables are set

**Workflow times out**:
- Check system resources
- Review n8n memory limits
- Consider reducing data volume

## Version Control

Workflow definitions are stored as JSON in version control:

```bash
# Track workflow changes
git add n8n-workflows/*.json
git commit -m "Update n8n workflows"
git push
```

To update from version control:

```bash
# Fetch latest workflow definitions
git pull

# Re-deploy to n8n
python flows/cli/deploy_n8n_workflows.py --action deploy
```

## Development

### Create New Workflow

1. Build workflow in `flows/cli/workflow_builders.py`:
   ```python
   def build_my_workflow():
       workflow = N8NWorkflow("My Workflow")
       # Add nodes
       return workflow.to_dict()
   ```

2. Register in `WorkflowDeployer`
3. Deploy: `python flows/cli/deploy_n8n_workflows.py --action deploy`
4. Export: `python flows/cli/deploy_n8n_workflows.py --action export`

### Modify Existing Workflow

1. Update builder function in `workflow_builders.py`
2. Re-deploy: `python flows/cli/deploy_n8n_workflows.py --action deploy`
3. Verify in n8n UI
4. Export: `python flows/cli/deploy_n8n_workflows.py --action export`
5. Commit changes: `git add n8n-workflows/*.json && git commit`

## Documentation

- **PHASE_3_GUIDE.md**: Complete Phase 3 implementation guide
- **flows/cli/README.md**: CLI command documentation
- **PREFECT_TO_N8N_MIGRATION_PLAN.md**: Migration plan and rationale

## Related

- **Phase 1**: Infrastructure (docker-compose, n8n setup)
- **Phase 2**: CLI wrappers (standalone scripts)
- **Phase 3**: This directory (workflow orchestration)
- **Phase 4**: Testing & cutover (data validation, gradual migration)

## Support

For detailed information, see:
- PHASE_3_GUIDE.md (this directory)
- flows/cli/README.md (CLI documentation)
- http://localhost:5678 (n8n web interface)
- `docker logs music-tracker-n8n` (n8n logs)
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

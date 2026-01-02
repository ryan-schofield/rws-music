# Phase 3: n8n Workflow Creation and Deployment

This directory contains the complete n8n workflow orchestration system that replaces Prefect for the music tracker ETL pipeline.

## Overview

Phase 3 implements n8n workflow management through:

1. **n8n API Client** (`n8n_client.py`) - REST API integration for workflow CRUD operations
2. **Workflow Definitions** (`workflow_definitions.py`) - Reusable node and workflow builders
3. **Workflow Builders** (`workflow_builders.py`) - Specific workflow implementations
4. **Deployment Script** (`deploy_n8n_workflows.py`) - CLI for deploying and managing workflows

## Architecture

### Workflow Structure

Each workflow consists of:

- **Nodes**: Individual tasks (CLI execution, triggers, conditions, waits)
- **Connections**: Links between nodes defining execution order
- **Settings**: Workflow-level configuration (timeout, error handling)

### Node Types

The system supports several n8n node types:

- **Cron Trigger**: Schedule-based execution
- **HTTP Webhook**: Manual/API-based triggering
- **Execute Command**: Run CLI scripts via shell
- **Wait**: Delay execution
- **Condition**: Conditional logic
- **Respond to Webhook**: Return HTTP responses

## Workflows

### 1. Spotify Ingestion (Every 6 Hours)

**File**: `spotify_ingestion_workflow.json`

**Schedule**: Every 6 hours (0, 6, 12, 18 UTC)

**Purpose**: Continuously refresh recently played tracks

**Pipeline**:
```
Cron (6h) → Ingest Spotify → Load Raw Tracks → Validate Data
```

**Nodes**:
1. `Trigger - Every 6h`: Cron at 0 */6 * * *
2. `Ingest Spotify Tracks`: CLI - `flows/cli/ingest_spotify.py --limit 50`
3. `Load Raw Tracks`: CLI - `flows/cli/load_raw_tracks.py`
4. `Validate Data`: CLI - `flows/cli/validate_data.py`

**Metrics**:
- Ingestion frequency: 4x per day
- Typical duration: 5-10 minutes
- Data freshness: Max 6 hours old

---

### 2. Daily ETL (Daily at 2 AM UTC)

**File**: `daily_etl_workflow.json`

**Schedule**: Daily at 2 AM UTC

**Purpose**: Complete data enrichment and transformation pipeline

**Pipeline**:
```
Cron (2 AM) 
  ↓
Ingest Spotify 
  ↓
Load Raw Tracks 
  ↓
Validate Data 
  ├→ Enrich Spotify Artists (parallel)
  ├→ Enrich Spotify Albums (parallel)
  ↓
Wait 30s 
  ↓
Discover MBZ Artists 
  ↓
Fetch MBZ Artists 
  ↓
Parse MBZ Data 
  ↓
Process MBZ Hierarchy 
  ↓
Enrich Geography 
  ↓
Update MBIDs 
  ↓
Run dbt Build
```

**Nodes** (13 total):
1. `Trigger - Daily 2 AM`: Cron at 0 2 * * *
2. `Ingest Spotify`: CLI - `flows/cli/ingest_spotify.py --limit 100`
3. `Load Raw Tracks`: CLI - `flows/cli/load_raw_tracks.py`
4. `Validate Data`: CLI - `flows/cli/validate_data.py`
5. `Enrich Spotify Artists`: CLI - `flows/cli/enrich_spotify_artists.py --limit 50`
6. `Enrich Spotify Albums`: CLI - `flows/cli/enrich_spotify_albums.py --limit 50`
7. `Wait for Spotify`: Wait 30 seconds
8. `Discover MBZ Artists`: CLI - `flows/cli/discover_mbz_artists.py`
9. `Fetch MBZ Artists`: CLI - `flows/cli/fetch_mbz_artists.py --limit 100 --workers 5`
10. `Parse MBZ Data`: CLI - `flows/cli/parse_mbz_data.py`
11. `Process MBZ Hierarchy`: CLI - `flows/cli/process_mbz_hierarchy.py`
12. `Enrich Geography`: CLI - `flows/cli/enrich_geography.py`
13. `Update MBIDs`: CLI - `flows/cli/update_mbids.py`
14. `Run dbt Build`: CLI - `flows/cli/run_dbt.py --command build`

**Characteristics**:
- **Parallelization**: Spotify tasks run concurrently
- **Sequential**: MusicBrainz pipeline is sequential
- **Typical duration**: 45-90 minutes
- **Error handling**: Continue on error (non-blocking)
- **Timeout**: 1 hour total

---

## Components

### n8n Client (`n8n_client.py`)

REST API wrapper for n8n operations:

```python
from flows.cli.n8n_client import N8NClient

client = N8NClient(base_url="http://localhost:5678")

# Check connectivity
if client.is_accessible():
    # List workflows
    workflows = client.list_workflows()
    
    # Get specific workflow
    workflow = client.get_workflow("workflow_id")
    
    # Create workflow
    new_workflow = client.create_workflow(definition_dict)
    
    # Update workflow
    updated = client.update_workflow("workflow_id", updated_definition)
    
    # Activate/Deactivate
    client.activate_workflow("workflow_id")
    client.deactivate_workflow("workflow_id")
    
    # Export to file
    client.export_workflow("workflow_id", Path("workflow.json"))
    
    # Import from file
    client.import_workflow(Path("workflow.json"))
```

**Methods**:
- `is_accessible()`: Verify n8n instance connectivity
- `list_workflows()`: Get all workflows
- `get_workflow(id)`: Get specific workflow
- `find_workflow_by_name(name)`: Search by name
- `create_workflow(def)`: Create new workflow
- `update_workflow(id, def)`: Update workflow
- `delete_workflow(id)`: Delete workflow
- `activate_workflow(id)`: Enable execution
- `deactivate_workflow(id)`: Disable execution
- `export_workflow(id, path)`: Export to JSON
- `import_workflow(path)`: Import from JSON

### Workflow Definitions (`workflow_definitions.py`)

Builders for n8n workflow components:

```python
from flows.cli.workflow_definitions import (
    N8NNode,
    N8NConnection,
    N8NWorkflow,
    create_cli_execution_node,
    create_cron_trigger_node,
    create_condition_node,
    create_wait_node,
)

# Create workflow
workflow = N8NWorkflow(
    name="My Workflow",
    description="Does something useful",
    active=True,
)

# Add nodes
workflow.add_node(
    create_cron_trigger_node(
        node_name="Trigger",
        expression="0 2 * * *",  # Daily at 2 AM
    )
)

workflow.add_node(
    create_cli_execution_node(
        node_name="Run Script",
        cli_script="flows/cli/my_script.py",
        cli_args={"limit": "50"},
    )
)

# Connect nodes
workflow.connect("Trigger", "Run Script")

# Export to JSON
workflow_dict = workflow.to_dict()
```

### Workflow Builders (`workflow_builders.py`)

Pre-built workflow definitions:

```python
from flows.cli.workflow_builders import (
    build_spotify_ingestion_workflow,
    build_daily_etl_workflow,
)

# Build workflow JSON
spotify_def = build_spotify_ingestion_workflow()
etl_def = build_daily_etl_workflow()

# Deploy to n8n
client.create_workflow(spotify_def)
client.create_workflow(etl_def)
```

### Deployment Script (`deploy_n8n_workflows.py`)

CLI tool for managing workflows:

```bash
# Check status of deployed workflows
python flows/cli/deploy_n8n_workflows.py --action status

# Deploy all workflows
python flows/cli/deploy_n8n_workflows.py --action deploy

# Export workflows to JSON (for version control)
python flows/cli/deploy_n8n_workflows.py --action export

# Activate a workflow
python flows/cli/deploy_n8n_workflows.py --action activate --workflow "Daily ETL"

# Deactivate a workflow
python flows/cli/deploy_n8n_workflows.py --action deactivate --workflow "Daily ETL"
```

---

## Deployment Workflow

### Initial Setup

```bash
# 1. Start n8n (via docker-compose)
docker-compose up -d n8n

# 2. Wait for n8n to be ready (5-10 seconds)
sleep 10

# 3. Deploy all workflows
python flows/cli/deploy_n8n_workflows.py --action deploy

# 4. Verify deployment
python flows/cli/deploy_n8n_workflows.py --action status

# 5. Export workflows to version control
python flows/cli/deploy_n8n_workflows.py --action export

# 6. Commit exported workflows
git add n8n-workflows/*.json
git commit -m "Add n8n workflow definitions"
```

### After Deployment

Workflows are automatically:
- **Created** if not present
- **Updated** if already exist
- **Exported** to JSON for version control
- **Activated** according to their configuration

### Managing Workflows

```bash
# View all workflows and their status
python flows/cli/deploy_n8n_workflows.py --action status

# Temporarily disable a workflow
python flows/cli/deploy_n8n_workflows.py --action deactivate --workflow "Spotify Ingestion"

# Re-enable a workflow
python flows/cli/deploy_n8n_workflows.py --action activate --workflow "Spotify Ingestion"

# Update workflows (re-run deploy)
python flows/cli/deploy_n8n_workflows.py --action deploy

# Re-export after manual changes (not recommended)
python flows/cli/deploy_n8n_workflows.py --action export
```

---

## Configuration

### Environment Variables

Set these in `.env`:

```bash
# n8n location
N8N_BASE_URL=http://localhost:5678

# Optional: API authentication (if n8n requires it)
N8N_API_KEY=your_api_key_here
```

### Workflow Settings

Each workflow has default settings:

- **Timeout**: Total execution time limit (1 hour for daily ETL)
- **Retries**: Failed task retry count
- **Retry Interval**: Delay between retries (60 seconds)
- **Stop on Error**: Whether to halt on task failure (disabled for robustness)

Override in `workflow_builders.py`:

```python
workflow.set_settings({
    "errorHandler": {
        "stopOnError": False,
        "retries": 3,
        "retryInterval": 120,
    },
    "executionData": {
        "timeout": 7200,  # 2 hours
    },
})
```

---

## Monitoring and Logging

### n8n UI

Access n8n web interface at `http://localhost:5678` to:
- View execution history
- Check workflow status
- Monitor task performance
- Debug issues

### CLI Output

Deployment script provides detailed logging:

```
2024-01-02 12:00:00,000 - root - INFO - Checking n8n connectivity...
2024-01-02 12:00:00,100 - root - INFO - ✅ n8n is accessible
2024-01-02 12:00:00,200 - root - INFO - Deploying workflow: spotify_ingestion
2024-01-02 12:00:00,500 - root - INFO - ✓ Created workflow abc123
2024-01-02 12:00:00,600 - root - INFO - Workflow deployed successfully
```

### JSON Exports

Exported workflows in `n8n-workflows/` contain full execution state:

```json
{
  "id": "workflow_id",
  "name": "Spotify Ingestion",
  "active": true,
  "nodes": [...],
  "connections": {...},
  "settings": {...},
  "createdAt": "2024-01-02T12:00:00Z",
  "updatedAt": "2024-01-02T12:00:00Z"
}
```

---

## Troubleshooting

### n8n Not Accessible

**Error**: `Failed to connect to n8n at http://localhost:5678`

**Solution**:
1. Verify n8n container is running: `docker ps | grep n8n`
2. Check if port 5678 is open: `curl http://localhost:5678`
3. Check n8n logs: `docker logs music-tracker-n8n`

### Workflow Deployment Fails

**Error**: `Failed to create workflow: ...`

**Solutions**:
1. Check n8n is accessible (see above)
2. Verify workflow definition is valid JSON
3. Check n8n logs for detailed error message
4. Ensure n8n has sufficient memory

### CLI Command Timeout

**Symptom**: Workflow executions timeout

**Solutions**:
1. Increase timeout in `workflow_builders.py`
2. Reduce data volume (use `--limit` flags)
3. Check system resources and n8n memory limits
4. Consider splitting into smaller workflows

### Workflows Not Triggering

**Symptom**: Scheduled workflows don't execute at expected times

**Solutions**:
1. Verify workflow is active: `--action status`
2. Check n8n system timezone: `docker exec music-tracker-n8n date`
3. Verify cron expressions are correct
4. Check n8n logs for trigger events

---

## Development

### Adding New Workflows

1. Create builder function in `workflow_builders.py`:
   ```python
   def build_my_workflow() -> dict:
       workflow = N8NWorkflow(
           name="My Workflow",
           description="Does something",
           active=True,
       )
       # Add nodes and connections
       return workflow.to_dict()
   ```

2. Register in `WorkflowDeployer.workflow_builders`:
   ```python
   self.workflow_builders = {
       "my_workflow": {
           "builder": build_my_workflow,
           "filename": "my_workflow.json",
           "description": "...",
       },
   }
   ```

3. Deploy: `python flows/cli/deploy_n8n_workflows.py --action deploy`

### Testing Workflows

1. Deploy to test n8n instance
2. Manually trigger execution via UI
3. Check execution history for errors
4. Verify CLI outputs are valid JSON
5. Export and version control final definition

---

## Performance Considerations

### Parallelization

The daily ETL workflow uses parallelization where possible:
- Spotify enrichment (artists + albums) run concurrently
- MusicBrainz pipeline is sequential (dependencies)
- Geographic enrichment runs after area hierarchy

Estimated timing:
- Ingest + Load: 5 min
- Validate: 2 min
- Spotify enrichment (parallel): 15 min
- MusicBrainz pipeline: 20 min
- Geography enrichment: 10 min
- dbt build: 15 min
- **Total**: ~60 minutes

### Resource Usage

- **CPU**: Minimal (mostly I/O bound)
- **Memory**: ~300MB for n8n + script execution
- **Disk**: Data reads/writes for enrichment
- **Network**: Spotify, MusicBrainz, OpenWeather API calls

### Scaling

To handle more data:
1. Increase `--limit` parameters in CLI commands
2. Increase `--workers` for parallel MusicBrainz fetches
3. Run Spotify ingestion more frequently
4. Split daily ETL into smaller sub-workflows

---

## Migration from Prefect

### What Changed

| Aspect | Prefect | n8n |
|--------|---------|-----|
| **Orchestration** | Python-based server | Node.js lightweight |
| **Definition** | Python code | JSON workflows |
| **Scheduling** | Prefect server | n8n schedules |
| **Tasks** | Python @task decorators | CLI scripts |
| **Memory** | ~900MB (server + DB) | ~300MB |
| **Version Control** | Code-based | JSON exports |
| **UI** | Web dashboard | Built-in |

### Migration Steps

1. Phase 1: Infrastructure Setup ✅
   - Add n8n to docker-compose.yml
   - Remove Prefect services
   - Set up version control structure

2. Phase 2: CLI Wrappers ✅
   - Convert Prefect tasks to standalone CLIs
   - Implement retry and timeout logic
   - Test CLI execution

3. Phase 3: n8n Workflows ✅
   - Create workflow definitions
   - Deploy to n8n instance
   - Export and version control

4. Phase 4: Testing & Cutover (Next)
   - Run both systems in parallel
   - Validate data consistency
   - Gradual cutover to n8n
   - Archive Prefect configuration

---

## References

- [n8n Documentation](https://docs.n8n.io/)
- [n8n REST API](https://docs.n8n.io/api/api-reference/)
- [n8n Workflows](https://docs.n8n.io/workflows/)
- [Workflow JSON Format](https://docs.n8n.io/workflows/json-schema/)
- n8n-workflows/ directory structure

---

## Support

For issues or questions:

1. Check n8n logs: `docker logs music-tracker-n8n`
2. Review workflow definition in n8n UI
3. Test CLI commands manually
4. Check system resources and memory
5. Consult Phase 3 documentation


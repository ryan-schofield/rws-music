# n8n Workflow Operations Guide

Complete reference for operating and maintaining n8n workflows in the music tracker application. This guide covers workflow deployment, monitoring, debugging, and advanced operations.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Workflow Deployment](#workflow-deployment)
4. [Workflow Management](#workflow-management)
5. [Monitoring and Observability](#monitoring-and-observability)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Operations](#advanced-operations)
8. [Best Practices](#best-practices)

## Quick Start

### Access n8n Web UI

```bash
# Local development
http://localhost:5678

# Synology NAS
http://<your-synology-ip>:5678
```

### Initial Setup

1. **Create Admin Account** (first time only)
   - Navigate to n8n URL
   - Create admin user account
   - Set secure password

2. **Configure Credentials** (optional, for advanced operations)
   - Go to Settings > Credentials
   - Add API credentials for external services
   - Test connections before use

### Deploy Workflows

```bash
# Deploy all production workflows
python flows/cli/deploy_n8n_workflows.py --action deploy

# Check deployment status
python flows/cli/deploy_n8n_workflows.py --action status

# Export workflows to version control
python flows/cli/deploy_n8n_workflows.py --action export
```

## Architecture Overview

### Workflow Components

The music tracker uses two main n8n workflows:

#### 1. Spotify Ingestion Workflow

**Schedule**: Every 30 minutes (cron: `*/30 * * * *`)  
**Duration**: ~3-5 minutes  
**Nodes**: 4

```
Trigger (Every 30min)
  ↓
Ingest Spotify Data (Python CLI)
  ↓
Load Raw Tracks (Python CLI)
  ↓
Validate Data (Python CLI)
```

**Purpose**: Fetches recently played tracks from Spotify API and loads them into the data warehouse.

**Characteristics**:
- Non-blocking error handling (continues if a step fails)
- 1 retry attempt on failure
- Timeout: 300 seconds (5 minutes)
- Runs frequently throughout the day

#### 2. Daily ETL Workflow

**Schedule**: Daily at 2:00 AM UTC (cron: `0 2 * * *`)  
**Duration**: ~30-60 minutes  
**Nodes**: 13

```
Trigger (Daily 2 AM)
  ↓
Data Ingestion (3 parallel nodes)
  ├─ Ingest Spotify
  ├─ Load Raw Tracks
  └─ Validate Data
  ↓
Wait for Ingestion (30 seconds)
  ↓
Spotify Enrichment (2 parallel nodes)
  ├─ Enrich Spotify Artists
  └─ Enrich Spotify Albums
  ↓
MusicBrainz Pipeline (4 sequential nodes)
  ├─ Discover MusicBrainz Artists
  ├─ Fetch MusicBrainz Data
  ├─ Parse MusicBrainz JSON
  └─ Process MusicBrainz Hierarchy
  ↓
Geographic Enrichment
  ↓
Update MusicBrainz IDs
  ↓
dbt Transformations
```

**Purpose**: Comprehensive daily data processing pipeline that enriches, validates, and transforms all music data.

**Characteristics**:
- Sequential primary flow with parallel enrichment stages
- Non-blocking error handling (continues on enrichment failures)
- 1 retry attempt on failure
- Timeout: 3600 seconds (1 hour total)
- Runs once daily during low-traffic hours

### CLI Wrappers

All data processing is executed through Python CLI wrappers in `flows/cli/`. Each wrapper:

- Runs as a standalone Python script
- Outputs JSON format for n8n integration
- Implements retry logic (configurable per task)
- Enforces timeout limits (prevents runaway processes)
- Handles errors with detailed logging

#### Available CLI Commands

| Command | Timeout | Retries | Purpose |
|---------|---------|---------|---------|
| `ingest_spotify` | 300s | 3 | Fetch Spotify API data |
| `load_raw_tracks` | 300s | 1 | Load data into database |
| `validate_data` | 300s | 0 | Quality checks |
| `enrich_spotify_artists` | 600s | 3 | Add artist metadata |
| `enrich_spotify_albums` | 600s | 3 | Add album metadata |
| `update_mbids` | 600s | 3 | Update MusicBrainz IDs |
| `discover_mbz_artists` | 600s | 2 | Find artists needing enrichment |
| `fetch_mbz_artists` | 900s | 3 | Query MusicBrainz API |
| `parse_mbz_data` | 600s | 2 | Parse JSON responses |
| `process_mbz_hierarchy` | 600s | 2 | Build geographic hierarchy |
| `enrich_geography` | 1800s | 0 | Add geographic data |
| `run_dbt` | 1200s | 2 | Transform data with dbt |

## Workflow Deployment

### Automated Deployment

The `deploy_n8n_workflows.py` script handles all deployment operations.

```bash
# Full deployment (create or update workflows)
python flows/cli/deploy_n8n_workflows.py --action deploy

# Deployment output
INFO: Connecting to n8n at http://localhost:5678
INFO: Workflow 'Spotify Ingestion' deployed successfully (ID: 12345)
INFO: Workflow 'Daily ETL' deployed successfully (ID: 12346)
INFO: All workflows deployed successfully
```

### Deployment Process

1. **Validation**: Verify n8n is accessible
2. **Workflow Check**: Look for existing workflows by name
3. **Create or Update**: 
   - If workflow exists: Update its definition
   - If new: Create with all nodes and connections
4. **Activation**: Enable workflows for scheduling
5. **Export**: Save workflow JSON to `n8n-workflows/` for version control

### Configuration

Before deploying, ensure these environment variables are set:

```bash
# .env file
N8N_HOST=localhost              # n8n server hostname
N8N_PORT=5678                   # n8n server port (default)
N8N_PROTOCOL=http               # http or https
N8N_API_KEY=your_api_key        # Optional, for production
```

### Manual Workflow Creation (Alternative)

If you prefer to create workflows in the n8n UI:

1. Open n8n web interface
2. Click "Create new workflow"
3. Add nodes based on workflow definitions (see `flows/cli/workflow_builders.py`)
4. Configure node parameters
5. Add cron trigger for scheduling
6. Activate workflow

## Workflow Management

### Activating/Deactivating Workflows

```bash
# Activate a specific workflow
python flows/cli/deploy_n8n_workflows.py --action activate

# Deactivate a specific workflow
python flows/cli/deploy_n8n_workflows.py --action deactivate

# Check workflow status
python flows/cli/deploy_n8n_workflows.py --action status
```

### Manual Workflow Execution

Sometimes you need to run a workflow outside its normal schedule:

#### Method 1: n8n Web UI

1. Open n8n web interface
2. Find the workflow in the Workflows tab
3. Click the workflow name to open it
4. Click the "Execute Workflow" button (play icon)
5. Monitor execution in real-time

#### Method 2: Webhook Trigger

```bash
# Manually trigger Spotify Ingestion via webhook
curl -X POST http://localhost:5678/webhook/spotify-ingestion

# Manually trigger Daily ETL via webhook
curl -X POST http://localhost:5678/webhook/daily-etl
```

#### Method 3: API Call (with credentials)

```bash
# Execute workflow via n8n API
curl -X POST http://localhost:5678/api/v1/workflows/12345/execute \
  -H "X-N8N-API-KEY: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"data": {}}'
```

### Updating Workflow Schedules

To change a workflow's execution schedule:

1. **In n8n UI**:
   - Open workflow
   - Click on the Cron trigger node
   - Modify the cron expression
   - Save workflow

2. **Via CLI** (recommended for production):
   - Edit `flows/cli/workflow_builders.py`
   - Modify the cron expression in the corresponding builder function
   - Run deployment script: `python flows/cli/deploy_n8n_workflows.py --action deploy`

**Common Cron Expressions**:

```
*/30 * * * *      # Every 30 minutes
0 * * * *         # Every hour at minute 0
0 2 * * *         # Daily at 2 AM UTC
0 9 * * MON       # Monday at 9 AM UTC
0 0 1 * *         # First day of month at midnight
```

### Disabling a Workflow

```bash
# Via CLI
python flows/cli/deploy_n8n_workflows.py --action deactivate

# Via n8n UI
# Open workflow → Click "Disable" in top menu
```

## Monitoring and Observability

### Web UI Monitoring

The n8n web interface provides comprehensive monitoring:

1. **Workflows Tab**: List all workflows with status
2. **Execution History**: View past executions with details
3. **Execution Timeline**: See execution duration and timing
4. **Logs Panel**: View detailed logs for debugging

### Accessing Execution History

1. Open n8n web interface
2. Click "Workflows" tab
3. Find workflow in list
4. Click workflow name
5. Scroll to "Executions" section
6. Click an execution to view details

### Key Metrics to Monitor

| Metric | Healthy Range | Warning Sign |
|--------|---------------|--------------|
| Spotify Ingestion Duration | 3-5 minutes | > 10 minutes |
| Daily ETL Duration | 30-60 minutes | > 90 minutes |
| Success Rate | > 95% | < 90% |
| Execution Frequency | As scheduled | Missed executions |
| Error Count | 0 per day | > 1 per day |

### Workflow Logs

Each CLI script writes JSON output containing:

```json
{
  "status": "success",
  "message": "Data ingestion completed",
  "processed_records": 150,
  "timestamp": "2026-01-02T10:30:45Z",
  "duration_seconds": 45,
  "execution_id": "exec-12345"
}
```

### Performance Analysis

To check workflow performance over time:

1. Open n8n web interface
2. Select a workflow
3. Scroll through executions
4. Note patterns:
   - Peak execution times
   - Average duration
   - Failure frequency
   - Error types

## Troubleshooting

### Common Issues and Solutions

#### Workflow Not Executing

**Symptom**: Workflow is scheduled but doesn't run

**Troubleshooting**:

```bash
# 1. Check if workflow is activated
python flows/cli/deploy_n8n_workflows.py --action status

# 2. Verify n8n is running
docker ps | grep n8n

# 3. Check n8n logs
docker logs music-tracker-n8n

# 4. Verify cron syntax in workflow
# Open n8n UI → Click workflow → Check trigger node
```

**Solutions**:
- Activate workflow: `--action activate`
- Restart n8n container: `docker restart music-tracker-n8n`
- Fix cron expression (must be valid)
- Check time zone matches UTC

#### Workflow Execution Fails

**Symptom**: Workflow starts but fails during execution

**Troubleshooting**:

```bash
# 1. Check execution logs in n8n UI
# Open workflow → View latest execution → Check error message

# 2. Test CLI command manually
docker exec music-tracker-pipeline python flows/cli/ingest_spotify.py

# 3. Check data-pipeline logs
docker logs music-tracker-pipeline

# 4. Verify environment variables
docker exec music-tracker-pipeline env | grep SPOTIFY
```

**Common Causes**:
- Missing/expired API credentials
- Network connectivity issues
- Data validation failures
- Insufficient memory/resources
- Database connection errors

#### Slow Workflow Execution

**Symptom**: Workflow takes longer than usual

**Troubleshooting**:

```bash
# 1. Check system resources
docker stats music-tracker-pipeline

# 2. Monitor memory usage
free -h

# 3. Check disk space
df -h

# 4. Review n8n execution metrics
# Open workflow → Check execution durations
```

**Solutions**:
- Increase container memory limit in `docker-compose.yml`
- Clean up old logs and temporary files
- Optimize CLI scripts (add `--limit` parameter if available)
- Check for concurrent workflows running

#### Connection Refused Errors

**Symptom**: "Connection refused" errors in n8n logs

**Troubleshooting**:

```bash
# 1. Verify n8n container is running
docker ps | grep n8n

# 2. Check port availability
netstat -tuln | grep 5678

# 3. Test connectivity
curl http://localhost:5678/api/v1/workflows

# 4. Check container logs
docker logs music-tracker-n8n -f
```

**Solutions**:
- Start n8n: `docker-compose up -d n8n`
- Free port 5678: Check for other services using it
- Verify network connectivity
- Restart Docker daemon

### Workflow Debug Mode

To enable detailed logging for debugging:

1. **In n8n UI**:
   - Open workflow settings (gear icon)
   - Enable "Save Data" option
   - Set "Save Data" to all executions
   - Save workflow

2. **View Debug Info**:
   - Open workflow execution
   - Click "JSON" tab
   - Review full execution data

### Log Collection for Support

If you need help troubleshooting, collect these logs:

```bash
# n8n application logs
docker logs music-tracker-n8n > n8n-logs.txt

# Data pipeline logs
docker logs music-tracker-pipeline > pipeline-logs.txt

# System information
docker stats --no-stream > docker-stats.txt
docker exec music-tracker-n8n env > n8n-env.txt

# Create archive
tar czf troubleshooting-logs.tar.gz n8n-logs.txt pipeline-logs.txt docker-stats.txt n8n-env.txt
```

## Advanced Operations

### Exporting Workflows for Version Control

Regularly export workflows to maintain version history:

```bash
# Export all workflows
python flows/cli/deploy_n8n_workflows.py --action export

# Output location
n8n-workflows/subflows/
├── spotify_ingestion_workflow.json
└── daily_etl_workflow.json
```

**Use Cases**:
- Track workflow changes in Git
- Deploy to different environments
- Restore previous versions
- Compare workflow definitions

### Importing Workflows from JSON

To restore workflows from exported JSON:

```bash
# Via the deploy script
python flows/cli/deploy_n8n_workflows.py --action deploy

# The script will:
# 1. Read workflow definitions from Python code
# 2. Create/update workflows in n8n
# 3. Export them to JSON files
```

### Backing Up n8n Data

Backup your n8n database and workflows:

```bash
# Stop n8n container
docker-compose stop n8n

# Backup n8n data volume
docker run --rm -v music-tracker-n8n:/data -v $(pwd):/backup \
  alpine tar czf /backup/n8n-backup.tar.gz /data

# Restart n8n
docker-compose start n8n
```

**Restore from Backup**:

```bash
# Stop n8n
docker-compose stop n8n

# Restore backup
docker run --rm -v music-tracker-n8n:/data -v $(pwd):/backup \
  alpine tar xzf /backup/n8n-backup.tar.gz -C /

# Restart n8n
docker-compose start n8n
```

### Scaling Workflow Execution

For high-volume scenarios, consider:

1. **Increase Timeouts**: Edit timeout values in `flows/cli/base.py`
2. **Add Retries**: Configure retry attempts per CLI script
3. **Parallel Execution**: n8n already parallelizes compatible nodes
4. **Resource Allocation**: Increase memory in `docker-compose.yml`

### Adding New Workflows

To create a new workflow:

1. **Create CLI Wrapper** (if needed):
   - Add Python script to `flows/cli/`
   - Implement `CLICommand` base class
   - Test manually

2. **Create Workflow Definition**:
   - Edit `flows/cli/workflow_builders.py`
   - Add builder function following existing patterns
   - Define nodes, connections, and scheduling

3. **Update Deployment Script**:
   - Edit `flows/cli/deploy_n8n_workflows.py`
   - Add workflow to registry
   - Test deployment

4. **Deploy**:
   ```bash
   python flows/cli/deploy_n8n_workflows.py --action deploy
   ```

### Modifying Existing Workflows

To change workflow behavior:

1. **Edit Workflow Definition**:
   - Update `flows/cli/workflow_builders.py`
   - Modify nodes, connections, or schedule as needed

2. **Redeploy**:
   ```bash
   python flows/cli/deploy_n8n_workflows.py --action deploy
   ```

3. **Verify Changes**:
   - Check n8n UI to confirm updates
   - Test manual execution
   - Monitor next scheduled run

## Best Practices

### Scheduling Best Practices

1. **Avoid Peak Hours**: Run heavy workflows during low-traffic times
2. **Stagger Execution**: Don't overlap long-running workflows
3. **Use Cron Expressions**: Clear, standardized scheduling syntax
4. **Document Schedules**: Maintain schedule documentation
5. **Test Schedule Changes**: Test new schedules before deploying

### Error Handling Best Practices

1. **Use Non-Blocking Errors**: Allow workflows to continue on non-critical failures
2. **Set Appropriate Retries**: Retry transient failures, fail fast on permanent errors
3. **Implement Timeouts**: Prevent runaway processes
4. **Monitor Success Rates**: Alert on persistent failures
5. **Log Errors**: Ensure all errors are captured for debugging

### Security Best Practices

1. **Secure Credentials**: Use n8n credentials management, not environment variables
2. **Restrict Access**: Use authentication for n8n UI
3. **Version Control**: Export workflows to Git for audit trail
4. **Backup Regularly**: Maintain backups of n8n data
5. **Monitor Execution**: Track who runs workflows and when

### Performance Best Practices

1. **Monitor Metrics**: Track execution times and resource usage
2. **Optimize Queries**: Ensure database queries are efficient
3. **Batch Processing**: Use `--limit` parameters for large datasets
4. **Parallel Execution**: Design workflows to leverage parallel nodes
5. **Resource Allocation**: Monitor memory and CPU usage

### Operational Best Practices

1. **Document Changes**: Note workflow modifications and reasons
2. **Test First**: Test changes in development before production
3. **Gradual Rollout**: Deploy changes incrementally
4. **Monitor After Deploy**: Watch for issues after deployment
5. **Maintain Backups**: Regular backup schedule for all data

### CLI Script Best Practices

1. **Use Appropriate Timeouts**: Balance between reliability and responsiveness
2. **Configure Retries**: More retries for flaky operations
3. **JSON Output**: Always use JSON format for n8n integration
4. **Error Codes**: Use consistent exit codes (0=success, 1=error)
5. **Logging**: Implement structured logging with timestamps

## Related Documentation

- [Main README.md](README.md) - Overall project documentation
- [Migration Plan](PREFECT_TO_N8N_MIGRATION_PLAN.md) - Migration details
- [Synology Deployment Guide](SYNOLOGY_DEPLOYMENT_GUIDE.md) - NAS deployment
- [CLI README](flows/cli/README.md) - CLI script documentation
- [Phase 3 Guide](flows/cli/PHASE_3_GUIDE.md) - Technical Phase 3 details

## Getting Help

1. **Check Logs**: Always start with log files
2. **Review Documentation**: Consult relevant docs
3. **Test Manually**: Run CLI commands directly
4. **Check n8n UI**: Verify workflow configuration
5. **Consult Migration Plan**: Reference technical details

## Support Resources

- [n8n Documentation](https://docs.n8n.io/)
- [n8n Community Forum](https://community.n8n.io/)
- [n8n GitHub Issues](https://github.com/n8n-io/n8n/issues)
- [Project Migration Plan](PREFECT_TO_N8N_MIGRATION_PLAN.md)

---

**Last Updated**: January 2, 2026  
**Version**: 1.0  
**Status**: Production Ready

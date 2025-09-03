# Prefect Orchestration

This directory contains the Prefect workflow orchestration for the music tracking system.

## Quick Start

### 1. Start Prefect Server
```bash
docker-compose up -d prefect-server prefect-db
```

### 2. Deploy Your Flows
```bash
# Deploy all flows with validation
uv run python scripts/orchestrate/deploy_flows.py --validate

# Deploy specific flows
uv run python scripts/orchestrate/deploy_flows.py --spotify-only
uv run python scripts/orchestrate/deploy_flows.py --etl-only
```

### 3. Access Prefect UI
Open http://localhost:4200 to view and manage your flows.

## Files Overview

### Core Files
- **`prefect_flows.py`** - Flow definitions for Spotify ingestion and ETL
- **`deploy_flows.py`** - Production deployment script (use this!)
- **`prefect_config.py`** - Configuration settings and validation

### Supporting Files
- **`enrichment_pipeline.py`** - Data enrichment orchestration
- **`monitoring.py`** - Flow monitoring and alerts
- **`test_flows.py`** - Flow testing utilities

## Available Flows

### Spotify Ingestion Flow
- **Name**: `spotify-ingestion`
- **Purpose**: Fetches recently played tracks from Spotify API
- **Parameters**: `limit` (default: 50 tracks)
- **Tags**: spotify, ingestion, automated

### Daily ETL Flow  
- **Name**: `daily-etl`
- **Purpose**: Complete data pipeline (Load → Enrich → Transform → Report)
- **Tags**: etl, daily, processing, automated

## Configuration

Required environment variables:
```bash
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret  
SPOTIFY_REFRESH_TOKEN=your_spotify_refresh_token
DUCKDB_PATH=./data/music_tracker.duckdb
```

## Usage Examples

### Deploy and Run Flows
```bash
# Deploy flows
uv run python scripts/orchestrate/deploy_flows.py

# Run flows via CLI
prefect deployment run spotify-ingestion
prefect deployment run daily-etl
```

### Test Configuration
```bash
# Validate your setup
uv run python scripts/orchestrate/prefect_config.py

# Test flows individually
uv run python scripts/orchestrate/test_flows.py
```

## Troubleshooting

### Common Issues

**"Cannot connect to Prefect server"**
- Ensure Prefect server is running: `docker-compose up -d prefect-server`
- Check server logs: `docker-compose logs prefect-server`

**"Missing Spotify credentials"**
- Set up your `.env` file with Spotify API credentials
- Run with `--validate` to check configuration

**"Flow deployment failed"**
- Check that all file paths are correct
- Verify your flows can be imported: `python -c "from scripts.orchestrate.prefect_flows import *"`

### Getting Help

1. Check flow execution in Prefect UI: http://localhost:4200
2. View logs: `docker-compose logs prefect-server`
3. Test configuration: `uv run python scripts/orchestrate/deploy_flows.py --validate`

## Development Workflow

1. **Modify flows** in `prefect_flows.py`
2. **Test locally** with `test_flows.py`
3. **Deploy** with `deploy_flows.py`
4. **Monitor** in Prefect UI
5. **Debug** using flow run logs

---

For more details, see the main project [README.md](../../README.md).
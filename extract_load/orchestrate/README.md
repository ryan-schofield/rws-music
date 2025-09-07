# Prefect Orchestration

This directory contains the Prefect workflow orchestration for the music tracking system.

## Key Features

- **Code Reduction** through DRY principles and base classes
- **Performance Improvement** through concurrent execution
- **Atomic Task Components** for better testability and reliability
- **Modular Subflows** for improved organization
- **Dynamic Configuration** for flexible environments

## Quick Start

### 1. Start Prefect Server
```bash
docker-compose up -d prefect-server prefect-db
```

### 2. Deploy Flows
```bash
# Deploy all flows with validation
uv run python prefect/orchestrate/deploy_flows.py --validate

# Deploy specific flows
uv run python prefect/orchestrate/deploy_flows.py --spotify-only
uv run python prefect/orchestrate/deploy_flows.py --etl-only
```

### 3. Access Prefect UI
Open http://localhost:4200 to view and manage your flows.

## Architecture Overview

### New File Structure
```
prefect/orchestrate/
├── flows/
│   ├── spotify_ingestion.py  # Spotify Ingestion Flow
│   └── daily_etl.py          # Daily ETL Flow
├── subflows/
│   ├── data_preparation.py
│   ├── enrichment_coordination.py
│   ├── spotify_enrichment.py
│   ├── musicbrainz_enrichment.py
│   ├── geographic_enrichment.py
│   ├── transformation.py
│   └── utils.py              # Helper functions for subflows
├── atomic_tasks.py         # Discrete, testable task implementations
├── base_tasks.py           # Base classes eliminating code duplication
├── deploy_flows.py        # Deployment script
├── flow_config.py          # Centralized configuration management
├── monitoring.py          # Monitoring and alerting
└── README.md              # This documentation
```

### Core Components

#### 1. **Configuration Management** ([`flow_config.py`](prefect/orchestrate/flow_config.py))
- Environment-aware configuration
- Dynamic parameter management
- Centralized limits and timeouts

#### 2. **Base Infrastructure** ([`base_tasks.py`](prefect/orchestrate/base_tasks.py))
- `BaseTask` - Common task patterns
- `BaseProcessorTask` - Direct processor integration
- `BaseEnrichmentTask` - Enrichment-specific functionality
- `TaskResult` - Standardized result format

#### 3. **Atomic Tasks** ([`atomic_tasks.py`](prefect/orchestrate/atomic_tasks.py))
- Single-responsibility tasks
- Direct processor usage (no subprocess duplication)
- Comprehensive error handling
- Granular retry strategies

#### 4. **Concurrent Subflows** (within `prefect/orchestrate/subflows/`)
- `data_preparation_subflow` - Sequential data loading & validation
- `spotify_enrichment_subflow` - Parallel artist & album enrichment
- `musicbrainz_enrichment_subflow` - Optimized MBZ pipeline
- `geographic_enrichment_subflow` - Geographic data processing
- `enrichment_coordination_subflow` - Parallel enrichment orchestration

## Available Flows

### Spotify Ingestion Flow
- **Name**: `spotify-ingestion`
- **Purpose**: Spotify API ingestion with optimized error handling
- **Parameters**:
  - `limit` (int): Number of tracks to fetch (default: 50)
  - `config` (FlowConfig): Configuration object (optional)
- **Features**:
  - Direct task usage (no subprocess)
  - Configurable limits
  - Comprehensive error reporting

### Daily ETL Flow
- **Name**: `daily-etl`
- **Purpose**: Concurrent ETL pipeline
- **Parameters**:
  - `config` (FlowConfig): Configuration object (optional)
- **Architecture**:
  ```
  Data Preparation (Sequential)
        ↓
  Enrichment Coordination (Concurrent)
    ├── Spotify Enrichment
    ├── MusicBrainz Enrichment
    └── Geographic Enrichment
        ↓
  Transformations (Sequential)
  ```
- **Performance**: Optimized through parallel execution

## Configuration

### Environment Variables
```bash
# Spotify API (required)
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret  
SPOTIFY_REFRESH_TOKEN=your_spotify_refresh_token

# Environment (optional)
ENVIRONMENT=development  # development, testing, production
DATA_BASE_PATH=data/src
CACHE_DIR=data/cache

# Processing Limits (optional)
SPOTIFY_ARTIST_LIMIT=200
MUSICBRAINZ_FETCH_LIMIT=50
DEFAULT_TIMEOUT=300
```

### Configuration Environments

**Development** (default):
- No processing limits
- Standard timeouts
- Full logging

**Testing**:
- Small limits for fast execution
- Shorter timeouts
- Minimal processing

**Production**:  
- Reasonable limits to prevent API rate limiting
- Optimized timeouts
- Production-grade error handling

## Usage Examples

### Deploy Flows
```bash
# Deploy both flows
uv run python prefect/orchestrate/deploy_flows.py

# Validate configuration
uv run python prefect/orchestrate/deploy_flows.py --validate
```

### Run Flows Manually
```bash
# Test flows directly
uv run python prefect/orchestrate/prefect_flows.py --flow spotify --limit 10
uv run python prefect/orchestrate/prefect_flows.py --flow etl --config-env testing

# Via Prefect CLI
prefect deployment run spotify-ingestion
prefect deployment run daily-etl
```

### Test Configuration
```bash
# Test configuration loading
uv run python prefect/orchestrate/flow_config.py

# Test individual components
uv run python -c "from extract_load.orchestrate.flow_config import get_flow_config; print(get_flow_config().to_dict())"
```

## Architecture Benefits

### Current Implementation:
- **Concurrent enrichment execution** (Spotify + MBZ + Geographic in parallel)
- **Atomic task components** with base classes eliminating duplication
- **Dynamic configuration** with environment-specific settings
- **Modular subflows** handling single responsibilities

### Performance Characteristics:
- **Faster execution** through concurrency
- **Reduced code duplication** through DRY principles
- **Better reliability** through atomic tasks and granular retries
- **Enhanced debugging** through task-level visibility

## Getting Started

### Initial Setup:
1. **Configure environments** in your `.env` file
2. **Deploy flows** using the deployment script
3. **Monitor execution** in Prefect UI
4. **Adjust configuration** as needed for your environment

## Troubleshooting

### Common Issues

**"Cannot import new flows"**
- Ensure all processor classes are properly imported
- Check that your Python path includes the project root

**"Configuration validation failed"**  
- Verify environment variables in your `.env` file
- Use `--validate` flag to check configuration

**"Subflow execution failed"**
- Check individual task logs in Prefect UI
- Tasks are atomic and can be debugged independently

**"Performance not improved"**
- Verify concurrent execution in flow run logs
- Check that independent tasks are running in parallel

### Debugging Tips

1. **Task-Level Logs**: Each atomic task provides detailed logging
2. **Subflow Status**: Monitor subflow execution in Prefect UI
3. **Configuration**: Test config with `flow_config.py`
4. **Direct Execution**: Run flows directly for debugging

## Development Workflow

1. **Modify components** in respective files (atomic_tasks.py, subflows.py, etc.)
2. **Test configuration** with different environments
3. **Deploy flows** with deployment script
4. **Monitor performance** in Prefect UI
5. **Review execution metrics** and adjust as needed

---

For more details, see the main project [README.md](../../README.md) and [PREFECT_RESTRUCTURE_SPEC.md](PREFECT_RESTRUCTURE_SPEC.md).
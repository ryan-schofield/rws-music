# Music Tracker

A modern, open-source music tracking and analytics platform built with DuckDB, Polars, n8n, and Streamlit for cost-effective music listening analytics. Optimized for local-only deployment on Synology NAS with 2GB RAM.

## Overview

This project provides a complete data pipeline for ingesting, processing, and analyzing personal music listening data from Spotify. It creates a dimensional data warehouse with comprehensive reporting capabilities, all running in Docker containers for easy deployment.

## Architecture

### Technology Stack

- **🐍 Python 3.11**: Core application language with uv package management
- **⚡️ uv**: Ultra-fast Python package manager and dependency resolver
- **📊 DuckDB**: Analytical database for efficient data storage and querying
- **⚡ Polars**: High-performance DataFrame library for data processing
- **🔄 n8n**: Workflow orchestration and scheduling
- **🐳 Docker**: Containerized deployment with Docker Compose
- **🛠️ dbt**: Data transformation and modeling
- **🎨 Streamlit**: Interactive data visualization and analytics dashboards

### System Components

The application consists of several containerized services:

#### Core Services
- **Data Pipeline Container**: Main application with Python flows, dbt models, and data processing
  - Built with uv for fast dependency resolution and virtual environment management
  - Contains all Python dependencies defined in `pyproject.toml` and locked in `uv.lock`
- **n8n**: Workflow orchestration and monitoring UI (port 5678)
  - Executes data workflows, scheduling, and monitoring
  - Node-based visual workflow editor with REST API
  - Manages both Spotify ingestion and daily ETL workflows

#### Reporting & Database Services
- **DuckDB**: Analytical database for data warehousing and analytics (in-app file storage)
- **Streamlit**: Interactive analytics dashboards with real-time data visualization (port 8501)

### Data Architecture

The system implements a dimensional data warehouse using a star schema:

#### Data Sources
- **Spotify Web API**: Recently played tracks, artist metadata, album information
- **MusicBrainz API**: Genre classifications and music metadata enrichment
- **Geographic APIs**: Location data for artist origins

#### Data Storage Structure
```
data/
├── music_tracker.duckdb          # Main analytical database
├── reporting.duckdb              # Reporting-specific views
├── raw/                          # Raw API responses
├── processed/                    # Cleaned and enriched data
├── cache/                        # API response caching
└── src/                          # Parquet files for dbt processing
```

#### Dimensional Model
- **Fact Tables**: Track plays with listening timestamps and metrics
- **Dimension Tables**: Artists, tracks, albums, dates, times, genres, locations
- **Business Logic**: Popularity groupings, listening patterns, genre analytics

## Data Pipeline

### 1. Data Ingestion (`flows/ingest/`)
- **Spotify API Integration**: Fetches recently played tracks with full metadata
- **Rate Limiting**: Respects API limits with intelligent backoff strategies
- **Data Caching**: Stores raw responses to minimize API calls

### 2. Data Enrichment (`flows/enrich/`)
- **Spotify Metadata**: Enriches tracks with detailed artist and album information
- **MusicBrainz Integration**: Adds genre classifications and music metadata
- **Geographic Processing**: Maps artist locations to coordinates and regions
- **Concurrent Processing**: Parallel execution for performance optimization

### 3. Data Processing
- **Polars DataFrames**: High-performance data transformations
- **Deduplication**: Intelligent handling of duplicate tracks
- **Data Quality**: Comprehensive validation and cleansing rules
- **Parquet Storage**: Efficient columnar format for analytics

### 4. Data Transformation (`dbt/`)
- **Star Schema**: Dimensional modeling with fact and dimension tables
- **Surrogate Keys**: Consistent key management across dimensions
- **Business Metrics**: Pre-calculated aggregations and KPIs
- **Data Quality Tests**: Automated validation of data integrity

### 5. Orchestration (n8n Workflows)
- **CLI Wrappers** (`flows/cli/`): Standalone Python scripts callable by n8n tasks
- **Daily ETL Workflow**: Comprehensive pipeline with concurrent execution (runs daily at 2 AM UTC)
- **Spotify Ingestion Workflow**: Frequent data collection from Spotify API (runs every 30 minutes)
- **Monitoring**: n8n workflow execution history and logs
- **Version Control**: Exported workflow JSON files in `n8n-workflows/` directory

### 6. Reporting & Analytics (Streamlit App)
- **Interactive Dashboards**: Real-time analytics dashboards with filtering and exploration
- **Time-Series Analysis**: Track listening patterns over time
- **Artist & Genre Analytics**: Deep dives into artist popularity and genre preferences
- **Geographic Analysis**: Explore music by artist origin locations
- **Multi-Page Interface**: Organized analytics pages accessible from sidebar navigation

## Configuration

### Environment Variables
Create a `.env` file with the following required variables (copy from `.env.example`):

```bash
# Spotify API Credentials (Required)
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SPOTIFY_REFRESH_TOKEN=your_spotify_refresh_token

# n8n Configuration (Required)
N8N_HOST=localhost
TIMEZONE=UTC

# Optional Configuration
ENVIRONMENT=development  # or production
DUCKDB_PATH=//home/runner/workspace/data/music_tracker.duckdb
LOG_LEVEL=INFO
```

### Workflow Configuration
The system supports environment-specific configuration through CLI scripts and n8n workflow definitions:

- **CLI Scripts** (`flows/cli/`): Configure retry logic, timeouts, and parameters
- **n8n Workflows**: Define scheduling (cron expressions), dependencies, and error handling
- **Development**: No processing limits, verbose logging
- **Testing**: Small data limits for fast execution via `--limit` parameters
- **Production**: Optimized batch sizes and API limits

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Spotify Developer Account (for API credentials)
- 2GB RAM minimum (optimized for Synology NAS)

### Local Development Setup

1. **Clone and Configure**:
```bash
git clone <repository-url>
cd rws-music
cp .env.example .env  # Edit with your credentials
```

2. **Start Services**:
```bash
docker compose up -d
```

3. **Access Applications**:
- n8n UI: http://localhost:5678
- Streamlit Analytics: http://localhost:8501

4. **Deploy n8n Workflows**:
Deploy workflows using the CLI deployment script:
```bash
python flows/cli/deploy_n8n_workflows.py --action deploy
```
Monitor execution in the n8n UI.

### Data Flow Execution

1. **Spotify Ingestion**: Runs every 30 minutes to collect new tracks
2. **Daily ETL**: Processes and enriches data, runs dbt transformations
3. **Reporting**: Streamlit dashboards connect to DuckDB for real-time analytics

## Development

### Project Structure
```
├── flows/                    # Data processing flows and n8n CLI wrappers
│   ├── cli/                 # CLI wrapper scripts for n8n workflow execution
│   ├── ingest/              # Data ingestion from APIs
│   ├── enrich/              # Data enrichment and processing
│   ├── load/                # Data loading utilities
│   └── orchestrate/         # Legacy Prefect flows (deprecated)
├── dbt/                     # Data transformation models
│   ├── models/              # dbt SQL models (staging, intermediate, marts)
│   ├── macros/              # Reusable SQL macros
│   └── seeds/               # Reference data
├── streamlit/               # Interactive analytics dashboards
│   ├── app.py               # Main Streamlit application
│   ├── config.py            # Configuration settings
│   ├── pages/               # Multi-page dashboard definitions
│   └── utils/               # Database connection and utilities
├── data/                    # Data storage and caching
├── n8n-workflows/           # n8n workflow JSON exports (version control)
│   ├── utils/               # Export/import utility scripts
│   └── subflows/            # Workflow JSON definitions
├── docs/                    # Documentation
├── scripts/                 # Utility scripts
├── pyproject.toml           # Python project configuration and dependencies
├── uv.lock                  # Locked dependency versions for reproducible builds
├── docker-compose.yml       # Multi-container application orchestration
└── Dockerfile              # Container image definition with uv integration
```

### Package Management with uv

The project uses **uv** as the Python package manager for several key advantages:

- **Speed**: 10-100x faster than pip for dependency resolution and installation
- **Reliability**: Deterministic dependency resolution with `uv.lock` for reproducible builds
- **Docker Integration**: Efficient layer caching and smaller container images
- **Development Workflow**: Seamless virtual environment management

#### Key uv Files:
- **`pyproject.toml`**: Project metadata, dependencies, and configuration
- **`uv.lock`**: Exact dependency versions and hashes for reproducible installations
- **Dockerfile**: Uses `uv sync --frozen` for fast, reliable container builds

### Key Dependencies
- **dbt-duckdb**: dbt adapter for DuckDB integration
- **polars**: High-performance DataFrame operations
- **httpx**: Async HTTP client for API calls
- **musicbrainzngs**: MusicBrainz API client
- **pydantic**: Data validation and settings management
- **n8n** (runs in Docker container, not a Python dependency)

## Deployment

### Local Development
Use Docker Compose for local development and testing with uv handling all Python dependencies.

### Deployment

The application is designed for simple local deployment using Docker Compose:

#### Local Deployment
- **Docker Compose**: Simple multi-container orchestration
- **Local Access**: Services accessible on localhost ports
- **Easy Setup**: Single command to start all services

#### Deployment Process:
1. **Configure**: Set environment variables in `.env` file (copy from `.env.example`)
2. **Start**: `docker compose up -d` to launch all services
3. **Access**: Applications available at localhost ports

#### Synology NAS Deployment (Recommended)
For Synology NAS deployment using Container Manager (optimized for 2GB RAM):

**Complete Deployment Guide**: See [SYNOLOGY_DEPLOYMENT_GUIDE.md](SYNOLOGY_DEPLOYMENT_GUIDE.md) for comprehensive step-by-step instructions including screenshots, troubleshooting, and performance tuning.

**Quick Start**:

1. **Prepare NAS**:
   - Install Container Manager from Package Center
   - Create shared folder: `/volume1/docker/music-tracker`
   - Set proper permissions for your user account

2. **Transfer Files**:
   - Copy entire project to `/volume1/docker/music-tracker`
   - Ensure all subdirectories are preserved

3. **Container Manager Setup**:
   - Open Container Manager > Project > Create from docker-compose.yml
   - Browse to `/volume1/docker/music-tracker/docker-compose.yml`
   - Configure environment variables (copy from `.env.example`)
   - Set volume mappings to persistent storage locations

4. **Memory Configuration** (Post-Prefect Migration):
   - n8n: 300MB limit (workflow orchestration)
   - Streamlit: 200MB limit (analytics dashboards)
   - Data Pipeline: 400MB limit
   - **Total**: ~900MB (700MB freed from Prefect removal)

5. **Deploy Workflows**:
   - Access n8n UI: `http://<synology-ip>:5678`
   - Run deployment script: `python flows/cli/deploy_n8n_workflows.py --action deploy`
   - Workflows will be created and can be monitored in n8n UI

6. **Access Applications**:
   - Streamlit Analytics: `http://<synology-ip>:8501` (LAN only)
   - n8n Workflows: `http://<synology-ip>:5678` (LAN only)
   - No external DNS or SSL required for local deployment

7. **Monitoring & Maintenance**:
   - Use Synology Resource Monitor for real-time tracking
   - Set up Container Manager notifications for alerts
   - Configure Hyper Backup for automated data protection
   - Regularly check memory usage stays under 1.5GB
   - Monitor Streamlit app responsiveness and reconnect to DuckDB as needed

**Storage Optimization**:
- Use SSD cache for DuckDB files if available
- Implement backup strategy for `data/music_tracker.duckdb`
- Configure log rotation in Synology Log Center

**Optional Reverse Proxy**:
- Set up Synology Reverse Proxy for custom domains on LAN
- Configure `analytics.yourdomain.local` and `workflows.yourdomain.local`
- Update hosts files on client devices or use Synology DNS Server

**Performance Tips**:
- Monitor memory usage during initial data loads
- Streamlit caches queries automatically for better responsiveness
- Use wired network connection for stability
- Regularly compact DuckDB database for optimal performance
- Monitor n8n workflow execution history for job performance
- Restart Streamlit container if experiencing connection issues to DuckDB

## Monitoring & Observability

- **n8n UI**: Workflow monitoring, execution history, and scheduling
- **Streamlit Dashboards**: Real-time data visualization and exploration
- **CLI Output**: JSON logs from each task execution
- **Performance Metrics**: Execution timing and resource usage captured in n8n
- **Health Checks**: Container health monitoring
- **Error Handling**: Configurable retry logic and error notifications per task

## Resource Requirements

- **Memory**: 2GB RAM minimum (optimized for Synology NAS, typical usage ~900MB after Prefect removal)
- **Storage**: 1GB+ for Docker images and data files
- **CPU**: 2+ cores recommended for smooth operation
- **Network**: Local LAN access only (no external/internet exposure required)
- **Internet**: Required for API calls to Spotify and MusicBrainz during data processing

## Contributing

This project follows standard Python development practices:
- **Code Quality**: Black formatting, structured logging
- **Testing**: pytest with async support
- **Documentation**: Comprehensive inline documentation
- **Configuration**: Environment-based settings management

## Notes

```bash
docker exec -it music-tracker-streamlit bash # bash in container
sudo grep -R "8501" /volume1/@appconf/ # check for orphaned port refs
```
# Music Tracker

A modern, open-source music tracking and analytics platform built with DuckDB, Polars, and Metabase for cost-effective music listening analytics. Optimized for local-only deployment on Synology NAS with 2GB RAM.

## Overview

This project provides a complete data pipeline for ingesting, processing, and analyzing personal music listening data from Spotify. It creates a dimensional data warehouse with comprehensive reporting capabilities, all running in Docker containers for easy deployment.

## Architecture

### Technology Stack

- **🐍 Python 3.11**: Core application language with uv package management
- **⚡️ uv**: Ultra-fast Python package manager and dependency resolver
- **📊 DuckDB**: Analytical database for efficient data storage and querying
- **⚡ Polars**: High-performance DataFrame library for data processing
- **🔄 Prefect**: Workflow orchestration and scheduling
- **📈 Metabase**: Business intelligence and reporting platform
- **🐳 Docker**: Containerized deployment with Docker Compose
- **🛠️ dbt**: Data transformation and modeling
- **🐳 Docker**: Containerized deployment with Docker Compose

### System Components

The application consists of several containerized services:

#### Core Services
- **Data Pipeline Container**: Main application with Python flows, dbt models, and data processing
  - Built with uv for fast dependency resolution and virtual environment management
  - Contains all Python dependencies defined in `pyproject.toml` and locked in `uv.lock`
- **Prefect Server**: Workflow orchestration and monitoring UI (port 4200)
- **Prefect Worker**: Executes scheduled flows and tasks
- **Prefect Deployer**: Automatically deploys flows on startup

#### Reporting & Database Services
- **DuckDB**: Analytical database for data warehousing and analytics (in-app file storage)
- **Metabase**: Business intelligence platform (port 3000, local-only access)
- **H2 Database (Metabase)**: Embedded file-based metadata storage (no separate container)
- **PostgreSQL (Prefect)**: Workflow state and history storage (optimized for 2GB RAM)

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

### 5. Orchestration (`flows/orchestrate/`)
- **Daily ETL Flow**: Comprehensive pipeline with concurrent execution
- **Spotify Ingestion Flow**: Frequent data collection from Spotify API
- **Monitoring**: Performance metrics and alerting
- **Configuration Management**: Environment-specific settings

## Configuration

### Environment Variables
Create a `.env` file with the following required variables (copy from `.env.example`):

```bash
# Spotify API Credentials (Required)
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

# Database Passwords (Required)
PREFECT_DB_PASSWORD=another_secure_password

# Optional Configuration
ENVIRONMENT=development  # or production
DUCKDB_PATH=/app/data/music_tracker.duckdb
LOG_LEVEL=INFO
```

**Note**: Metabase now uses embedded H2 database - no separate database password needed.

### Flow Configuration
The system supports environment-specific configuration through `flows/orchestrate/flow_config.py`:

- **Development**: No processing limits, verbose logging
- **Testing**: Small data limits for fast execution
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
- Prefect UI: http://localhost:4200
- Metabase: http://localhost:3000

4. **Run Initial Data Load**:
The Prefect deployer will automatically deploy flows. Monitor execution in the Prefect UI.

### Data Flow Execution

1. **Spotify Ingestion**: Runs every 30 minutes to collect new tracks
2. **Daily ETL**: Processes and enriches data, runs dbt transformations
3. **Reporting**: Metabase connects to DuckDB for real-time analytics

## Development

### Project Structure
```
├── flows/                    # Prefect workflows and data processing
│   ├── ingest/              # Data ingestion from APIs
│   ├── enrich/              # Data enrichment and processing
│   ├── load/                # Data loading utilities
│   └── orchestrate/         # Flow coordination and scheduling
├── dbt/                     # Data transformation models
│   ├── models/              # dbt SQL models (staging, intermediate, marts)
│   ├── macros/              # Reusable SQL macros
│   └── seeds/               # Reference data
├── data/                    # Data storage and caching
├── docs/                    # Documentation
├── scripts/                 # Utility scripts
├── terraform/               # Historical cloud deployment (no longer used)
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
- **prefect**: Workflow orchestration framework
- **httpx**: Async HTTP client for API calls
- **musicbrainzngs**: MusicBrainz API client
- **pydantic**: Data validation and settings management

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

4. **Memory Configuration**:
   - Prefect Server: 300-400MB limit
   - Prefect Worker: 200-300MB limit
   - Prefect PostgreSQL: 128-200MB limit
   - Metabase: 400-512MB limit with JVM heap constraints
   - Data Pipeline: 250-400MB limit
   - **Total**: ~1,300-1,800MB (within 2GB budget)

5. **Access Applications**:
   - Metabase: `http://<synology-ip>:3000` (LAN only)
   - Prefect UI: `http://<synology-ip>:4200` (LAN only)
   - No external DNS or SSL required for local deployment

6. **Monitoring & Maintenance**:
   - Use Synology Resource Monitor for real-time tracking
   - Set up Container Manager notifications for alerts
   - Configure Hyper Backup for automated data protection
   - Regularly check memory usage stays under 1.8GB

**Storage Optimization**:
- Use SSD cache for DuckDB files if available
- Implement backup strategy for `data/music_tracker.duckdb`
- Configure log rotation in Synology Log Center

**Optional Reverse Proxy**:
- Set up Synology Reverse Proxy for custom domains on LAN
- Configure `metabase.yourdomain.local` and `prefect.yourdomain.local`
- Update hosts files on client devices or use Synology DNS Server

**Performance Tips**:
- Monitor memory usage during initial data loads
- Adjust Metabase JVM heap size if queries are slow
- Use wired network connection for stability
- Regularly compact DuckDB database for optimal performance

## Monitoring & Observability

- **Prefect UI**: Workflow monitoring, logs, and scheduling
- **Structured Logging**: JSON logs with correlation IDs
- **Performance Metrics**: Execution timing and resource usage
- **Health Checks**: Container health monitoring
- **Alerting**: Configurable notifications for failures

## Resource Requirements

- **Memory**: 2GB RAM minimum (optimized for Synology NAS, tested under 1.8GB usage)
- **Storage**: 1GB+ for Docker images and data files
- **CPU**: 2+ cores recommended for smooth operation
- **Network**: Local LAN access only (no external/internet exposure required)

## Contributing

This project follows standard Python development practices:
- **Code Quality**: Black formatting, structured logging
- **Testing**: pytest with async support
- **Documentation**: Comprehensive inline documentation
- **Configuration**: Environment-based settings management

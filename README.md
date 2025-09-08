# Music Tracker

A modern, open-source music tracking and analytics platform built with DuckDB, Polars, and Metabase for cost-effective music listening analytics.

## Overview

This project provides a complete data pipeline for ingesting, processing, and analyzing personal music listening data from Spotify. It creates a dimensional data warehouse with comprehensive reporting capabilities, all running in Docker containers for easy deployment.

## Architecture

### Technology Stack

- **🐍 Python 3.11**: Core application language with uv package management
- **� uv**: Ultra-fast Python package manager and dependency resolver
- **�📊 DuckDB**: Analytical database for efficient data storage and querying
- **⚡ Polars**: High-performance DataFrame library for data processing
- **🔄 Prefect**: Workflow orchestration and scheduling
- **📈 Metabase**: Business intelligence and reporting platform
- **🐳 Docker**: Containerized deployment with Docker Compose
- **🛠️ dbt**: Data transformation and modeling
- **🏗️ Terraform**: Infrastructure as Code for cloud deployment

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
- **DuckDB**: Analytical database for data warehousing and analytics
- **Metabase**: Business intelligence platform (port 3000)
- **PostgreSQL (Metabase)**: Metadata storage for Metabase
- **PostgreSQL (Prefect)**: Workflow state and history storage

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
Create a `.env` file with the following required variables:

```bash
# Spotify API Credentials
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

# Database Passwords
METABASE_DB_PASSWORD=secure_password_here
PREFECT_DB_PASSWORD=another_secure_password

# Optional Configuration
ENVIRONMENT=development  # or production
DUCKDB_PATH=/app/data/music_tracker.duckdb
LOG_LEVEL=INFO
```

### Flow Configuration
The system supports environment-specific configuration through `flows/orchestrate/flow_config.py`:

- **Development**: No processing limits, verbose logging
- **Testing**: Small data limits for fast execution
- **Production**: Optimized batch sizes and API limits

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Spotify Developer Account (for API credentials)
- 4GB+ RAM recommended

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
├── terraform/               # Infrastructure as Code for cloud deployment
│   └── phase1-lightsail/    # AWS Lightsail deployment configuration
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

### Production Deployment with Terraform

The project includes comprehensive **Terraform** Infrastructure as Code for AWS deployment:

#### Terraform Configuration (`terraform/phase1-lightsail/`)
- **`main.tf`**: Provider configuration and module orchestration
- **`lightsail.tf`**: AWS Lightsail instance configuration with Docker setup
- **`dns.tf`**: Domain and DNS management for public access
- **`variables.tf`**: Configurable deployment parameters
- **`outputs.tf`**: Important deployment information (IP addresses, domains)
- **`user_data.sh`**: Automated server setup script for Docker and application deployment

#### Infrastructure Features:
- **AWS Lightsail Instance**: Cost-effective VPS with predictable pricing
- **Automated Setup**: User data scripts handle Docker installation and application deployment
- **DNS Management**: Configurable domain setup for public Metabase access
- **Security**: Proper firewall configuration and SSH key management
- **Monitoring**: Instance health monitoring and alerting

#### Deployment Process:
1. **Configure**: Set variables in `terraform.tfvars`
2. **Plan**: `terraform plan` to review infrastructure changes
3. **Deploy**: `terraform apply` to provision AWS resources
4. **Access**: Automated DNS setup for public dashboard access

#### Cost Structure:
- **Infrastructure**: $5-15/month (AWS Lightsail instances)
- **Predictable Pricing**: No surprise charges from compute or storage usage
- **Scalability**: Easy instance upgrades through Terraform configuration updates

## Monitoring & Observability

- **Prefect UI**: Workflow monitoring, logs, and scheduling
- **Structured Logging**: JSON logs with correlation IDs
- **Performance Metrics**: Execution timing and resource usage
- **Health Checks**: Container health monitoring
- **Alerting**: Configurable notifications for failures

## Cost Analysis

- **Infrastructure**: $5-15/month (AWS Lightsail)
- **API Costs**: Free (Spotify Web API, MusicBrainz)
- **Software**: $0 (fully open-source stack)
- **Scalability**: Linear cost scaling with infrastructure needs

## Contributing

This project follows standard Python development practices:
- **Code Quality**: Black formatting, structured logging
- **Testing**: pytest with async support
- **Documentation**: Comprehensive inline documentation
- **Configuration**: Environment-based settings management

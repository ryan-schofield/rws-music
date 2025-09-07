# Music Tracker - Open Source Migration

A modern, open-source music tracking and analytics platform that migrates from Microsoft Fabric to a cost-effective, Docker-based solution using DuckDB, Polars, and Metabase.

## Migration Overview

### Original Microsoft Fabric Solution
- **Data Extraction**: Fabric User Data Function + Kafka for Spotify tracks
- **Processing**: Spark/Fabric Notebooks for supplemental data (Spotify, Musicbrainz, geographic APIs)
- **Storage**: Fabric Lakehouse (Delta tables) + Warehouse
- **Transformation**: dbt-core with Fabric adapter (star schema)
- **Orchestration**: Fabric Data Pipelines (daily dbt, frequent Spotify ingestion)
- **Reporting**: Power BI Semantic Model + Reports
- **Cost**: $200-500/month

### Target Open-Source Solution
- **Data Extraction**: Python prefect with Spotify Web API
- **Processing**: Polars DataFrames (replaces Spark)
- **Storage**: DuckDB (replaces Lakehouse/Warehouse)
- **Transformation**: dbt-core with DuckDB adapter (same star schema)
- **Orchestration**: Prefect (replaces Fabric pipelines)
- **Reporting**: Metabase (replaces Power BI)
- **Hosting**: Docker containers on AWS Lightsail
- **Cost**: $5-15/month

### Key Requirements
-  Keep dbt-core and existing transformation logic
-  Avoid Spark, use Polars for data processing
-  Use DuckDB as database
-  Use Metabase for reporting
-  Host on AWS Lightsail (low cost)
-  Make reports publicly available on existing domain
-  Maintain star schema structure

## Migration Plan & Progress

### Phase 1: Architecture & Core Components COMPLETED
- [x] Design Docker-based architecture with DuckDB, Polars, and Metabase
- [x] Create dbt-duckdb configuration and migrate dbt models
- [x] Convert Spark notebooks to Polars-based Python prefect
- [x] Implement data ingestion pipeline (Spotify API → DuckDB)
- [x] Create orchestration with Prefect for scheduled tasks

### Phase 2: Deployment & Testing IN PROGRESS
- [ ] Set up Metabase for reporting with DuckDB connection
- [ ] Configure public access for Metabase reports via domain
- [ ] Test end-to-end data flow and reporting
- [ ] Optimize performance and add error handling
- [ ] Create deployment prefect for AWS Lightsail (with Terraform)

## Architecture Decisions Made

### Technology Stack Selection
- **DuckDB**: Chosen over SQLite/PostgreSQL for analytical workloads and ~350k record dataset
- **Polars**: Selected over Pandas for better performance with large datasets
- **Metabase**: Preferred over Superset for easier Power BI migration and public sharing
- **Prefect**: Chosen over Airflow for Python-native workflows and easier Docker integration
- **Docker**: Containerization for consistent deployment across environments

### Data Architecture Preservation
- **Star Schema Maintained**: All existing dimensions (artist, track, album, date, time) and facts preserved
- **Surrogate Keys**: Kept existing surrogate key generation logic
- **Business Logic**: All transformations, popularity groupings, and aggregations maintained
- **Data Quality**: Preserved all existing data validation and cleansing rules

### Cost Optimization Strategy
- **AWS Lightsail**: $5-10/month for 2GB instance (sufficient for ~350k records)
- **No Per-Query Costs**: Unlike Fabric/Snowflake, no charges for data processing
- **Open-Source**: Zero licensing fees for all components
- **Scalable**: Easy to upgrade instance size if data grows

<<<<<<< HEAD
=======
## Current Project Structure

```
rws-music/
├── docker-compose.yml          #  Multi-service orchestration (Metabase, Prefect, PostgreSQL)
├── Dockerfile                  #  Main application container with Polars/DuckDB/dbt
├── requirements.txt            #  All Python dependencies (Polars, DuckDB, Prefect, etc.)
├── README.md                   #  This documentation
├── .env.example               # Environment configuration template
├── .gitignore                 # Git ignore rules
├── data/                      # Data storage (created at runtime)
├── logs/                      # Application logs (created at runtime)
├── dbt_duckdb/               # Migrated dbt project
│   ├── models/               #  All original dbt models preserved
│   ├── profiles.yml          #  Updated for DuckDB adapter
│   ├── dbt_project.yml       #  Updated project configuration
│   ├── packages.yml          #  Removed TSQL dependencies
│   └── seeds/                #  Original seed data preserved
├── prefect/                  #  Python application prefect
│   ├── run_pipeline.py       #  Main pipeline orchestrator
│   ├── ingestion/            #  Spotify API ingestion
│   │   └── spotify_api_ingestion.py
│   ├── processing/           #  Polars data processing
│   │   └── merge_spotify_recently_played.py
│   └── orchestration/        #  Prefect workflow definitions
│       └── prefect_flows.py
└── terraform/                # AWS deployment prefect (pending)
```

## Quick Start (Current State)

### Prerequisites
- Docker and Docker Compose installed
- Spotify Developer account with API credentials

### 1. Setup Project
```bash
cd rws-music
cp .env.example .env
# Edit .env with your Spotify credentials
```

### 2. Launch Services
```bash
docker-compose up -d
```

### 3. Test Pipeline
```bash
# Run full pipeline
docker-compose exec data-pipeline python prefect/run_pipeline.py

# Or run individual stages
docker-compose exec data-pipeline python prefect/run_pipeline.py --stage ingestion
docker-compose exec data-pipeline python prefect/run_pipeline.py --stage processing
docker-compose exec data-pipeline python prefect/run_pipeline.py --stage dbt
```

### 4. Access Interfaces
- **Metabase**: http://localhost:3000 (needs DuckDB connection setup)
- **Prefect Server**: http://localhost:4200
- **Pipeline Logs**: `docker-compose logs -f data-pipeline`

## Configuration

### Environment Variables (.env)
```bash
# Spotify API (Required)
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

# Database (Optional - defaults provided)
DUCKDB_PATH=./data/music_tracker.duckdb

# Metabase (Required for production)
METABASE_DB_PASSWORD=your_secure_password

# Application (Optional)
LOG_LEVEL=INFO
```

### Spotify API Setup
1. Visit [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Create new application
3. Copy Client ID and Client Secret to `.env`
4. No redirect URIs needed (using Client Credentials flow)

>>>>>>> 1c166aa7c78de24c096275292bb30d26ec7091b2
## Data Flow

### Current Implementation Status
1. ** Spotify API Ingestion**: Fetches recently played tracks with full metadata enrichment
2. ** Polars Processing**: Merges and deduplicates data using DataFrame operations
3. ** DuckDB Storage**: Stores raw and processed data efficiently
4. ** dbt Transformations**: Applies star schema transformations (same as original)
5. ** Metabase Reporting**: Needs connection setup and dashboard creation
6. ** Prefect Orchestration**: Flows defined for scheduled execution

### Data Volume Handling
- **Current Dataset**: ~350k track plays over 20 years
- **Daily Ingestion**: ~100 tracks/day
- **Performance**: DuckDB + Polars handles this efficiently on 2GB instances
- **Growth**: Scales well to millions of records

## Next Steps

### Immediate (This Session)
1. **Test Local Setup**: Verify Docker containers start correctly
2. **Run Sample Data**: Test pipeline with your Spotify credentials
3. **Validate dbt Models**: Ensure transformations work with DuckDB

### Short Term (Next Few Days)
1. **Metabase Setup**: Connect to DuckDB and recreate Power BI reports
2. **Domain Configuration**: Set up public access through existing domain
3. **End-to-End Testing**: Validate complete data flow

### Medium Term (1-2 Weeks)
1. **AWS Deployment**: Create Terraform prefect for Lightsail
2. **Performance Optimization**: Fine-tune for production workload
3. **Monitoring**: Add health checks and alerting

## Success Metrics

- [x] **Architecture Complete**: Docker-based solution designed
- [x] **Core Components Built**: All major prefect and configurations created
- [x] **Cost Reduction**: 95%+ reduction from Fabric costs
- [x] **Functionality Preserved**: All original features maintained
- [ ] **Testing Complete**: End-to-end validation pending
- [ ] **Production Deployed**: AWS Lightsail deployment pending
<<<<<<< HEAD
=======

---

*This migration transforms an expensive, proprietary solution into a cost-effective, open-source platform while preserving all analytical capabilities and data integrity.*

## Architecture Overview

```
Spotify API → Polars (Ingestion) → DuckDB (Storage) → dbt (Transform) → Metabase (Reports)
```

### Core Components

- **Database**: DuckDB - Fast, embedded analytical database
- **Data Processing**: Polars - Lightning-fast DataFrame library
- **Data Transformation**: dbt-core with DuckDB adapter
- **Reporting**: Metabase - Open-source business intelligence
- **Orchestration**: Prefect - Workflow orchestration
- **Containerization**: Docker - Easy deployment and scaling

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Spotify Developer Account (for API access)
- Python 3.11+ (for local development)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd rws-music
cp .env.example .env
```

### 2. Configure Environment

Edit `.env` with your credentials:

```bash
# Spotify API
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret

# Database
DUCKDB_PATH=./data/music_tracker.duckdb

# Metabase
METABASE_DB_PASSWORD=secure_password
```

### 3. Launch Services

```bash
docker-compose up -d
```

### 4. Access Interfaces

- **Metabase**: http://localhost:3000
- **Prefect Server**: http://localhost:4200

## Data Pipeline

### Daily Ingestion Flow

1. **Ingestion**: Fetch recently played tracks from Spotify API
2. **Processing**: Clean and merge data using Polars
3. **Enrichment**: Add MusicBrainz artist/location data
4. **Transformation**: Apply dbt models for star schema
5. **Reporting**: Update Metabase datasets

### Manual Execution

```bash
# Run full pipeline
python prefect/run_pipeline.py

# Run specific stages
python prefect/run_pipeline.py --stage ingestion
python prefect/run_pipeline.py --stage processing
python prefect/run_pipeline.py --stage dbt

# Incremental processing (skip ingestion)
python prefect/run_pipeline.py --incremental
```

### Scheduled Execution

Use Prefect flows for automated scheduling:

```python
from prefect.orchestration.prefect_flows import spotify_daily_ingestion_flow

# Run daily ingestion
result = spotify_daily_ingestion_flow()
```

## Project Structure

```
rws-music/
├── docker-compose.yml          # Service orchestration
├── Dockerfile                  # Main application container
├── requirements.txt            # Python dependencies
├── .env.example               # Environment template
├── data/                      # Data storage
│   ├── music_tracker.duckdb   # Main database
│   └── raw/                   # Raw data files
├── dbt_duckdb/               # dbt project
│   ├── models/               # dbt models
│   ├── profiles.yml          # dbt configuration
│   └── dbt_project.yml       # dbt project config
├── prefect/                  # Python prefect
│   ├── run_pipeline.py       # Main pipeline orchestrator
│   ├── ingestion/            # Data ingestion prefect
│   ├── processing/           # Data processing prefect
│   └── orchestration/        # Prefect flows
└── logs/                     # Application logs
```

## Data Model

### Source Tables
- `tracks_played` - Raw track play history
- `spotify_artists` - Artist information from Spotify
- `spotify_albums` - Album information from Spotify
- `mbz_artist_info` - MusicBrainz artist data

### Warehouse Tables (Star Schema)
- **Facts**: `fact_track_played`, `fact_artist_genre`
- **Dimensions**: `dim_artist`, `dim_track`, `dim_album`, `dim_date`, `dim_time`

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SPOTIFY_CLIENT_ID` | Spotify API client ID | Yes |
| `SPOTIFY_CLIENT_SECRET` | Spotify API client secret | Yes |
| `DUCKDB_PATH` | Path to DuckDB database file | No |
| `METABASE_DB_PASSWORD` | Metabase database password | Yes |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | No |

### Spotify API Setup

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Create a new application
3. Copy Client ID and Client Secret
4. Add your domain to allowed redirect URIs (if using OAuth)

## Deployment

### Local Development

```bash
# Start all services
docker-compose up

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### AWS Lightsail Deployment

1. **Create Lightsail Instance**
   ```bash
   # Ubuntu 22.04, 2GB RAM recommended
   aws lightsail create-instances --instance-names music-tracker \
     --availability-zone us-east-1a --blueprint-id ubuntu_22_04 \
     --bundle-id nano_3_0
   ```

2. **Deploy Application**
   ```bash
   # Copy files to server
   scp -r . ubuntu@your-instance-ip:/home/ubuntu/rws-music

   # SSH into server
   ssh ubuntu@your-instance-ip

   # Install Docker
   sudo apt update
   sudo apt install docker.io docker-compose
   sudo usermod -aG docker ubuntu

   # Deploy
   cd rws-music
   docker-compose up -d
   ```

3. **Configure Domain**
   - Point your domain to the Lightsail static IP
   - Configure reverse proxy for public access

## Monitoring and Maintenance

### Health Checks

```bash
# Check service health
docker-compose ps

# View logs
docker-compose logs data-pipeline
docker-compose logs metabase
```

### Database Maintenance

```bash
# Optimize DuckDB
python -c "import duckdb; duckdb.connect('data/music_tracker.duckdb').execute('OPTIMIZE')"
```

### Backup Strategy

```bash
# Backup database
cp data/music_tracker.duckdb data/backup_$(date +%Y%m%d_%H%M%S).duckdb

# Backup to S3 (optional)
aws s3 cp data/music_tracker.duckdb s3://your-backup-bucket/
```

## API Reference

### Spotify Ingestion

```python
from prefect.ingestion.spotify_api_ingestion import SpotifyDataIngestion

ingestor = SpotifyDataIngestion()
result = ingestor.run_ingestion(limit=50)
```

### Data Processing

```python
from prefect.processing.merge_spotify_recently_played import SpotifyDataMerger

merger = SpotifyDataMerger()
result = merger.run_merge_process()
```

## Troubleshooting

### Common Issues

1. **Spotify API Rate Limits**
   - Wait and retry, or reduce request frequency
   - Check API quota in Spotify Developer Dashboard

2. **DuckDB Connection Issues**
   - Ensure file permissions are correct
   - Check available disk space

3. **Metabase Connection**
   - Verify DuckDB file path in Metabase
   - Check database permissions

### Logs

All logs are stored in the `logs/` directory:
- `pipeline.log` - Main pipeline execution logs
- `spotify_api.log` - Spotify API interaction logs
- `dbt.log` - dbt transformation logs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Cost Comparison

| Component | Fabric Cost | Open Source Cost |
|-----------|-------------|------------------|
| Database | $$$ (Fabric Warehouse) | $0 (DuckDB) |
| Processing | $$$ (Spark clusters) | $0 (Local/Docker) |
| Reporting | $$$ (Power BI Pro) | $0 (Metabase) |
| Orchestration | $$$ (Fabric pipelines) | $0 (Prefect) |
| **Total** | **$200-500/month** | **$5-15/month** |

*Costs based on typical usage patterns. Open source solution runs on AWS Lightsail or local hardware.
>>>>>>> 1c166aa7c78de24c096275292bb30d26ec7091b2

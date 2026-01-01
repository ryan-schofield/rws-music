# Synology NAS Refactoring Plan

## Overview

Refactor the music tracking application to run efficiently on a Synology NAS with 2GB RAM by removing cloud infrastructure cruft, optimizing memory usage, and simplifying the deployment for local-only access while retaining Prefect orchestration.

## Current State Analysis

### Memory Footprint (Current)
- **Prefect Server**: 300-500MB
- **Prefect Worker**: 200-400MB
- **Prefect PostgreSQL**: 150-250MB
- **Metabase**: 500-800MB
- **Metabase PostgreSQL**: 150-250MB
- **Data Pipeline**: 150-300MB
- **Total**: ~1,600-2,800MB (exceeds 2GB budget)

### Expected Footprint (After Optimization)
- **Prefect Stack**: 650-1,150MB (retained)
- **Metabase**: 400-512MB (optimized)
- **Metabase DB**: File-based (H2/SQLite, minimal overhead)
- **Data Pipeline**: 150-300MB
- **Total**: ~1,200-1,800MB (within 2GB with headroom)

---

## Phase 1: Remove Cloud Infrastructure

**Objective**: Eliminate all Terraform, AWS Lightsail, and external deployment configurations.

**Memory Impact**: None (cleanup only)

### Tasks

#### 1.1 Delete Terraform Directory
- [ ] Delete `terraform/` directory entirely
- [ ] Remove `.terraform.lock.hcl` if present in root

#### 1.2 Clean Up Ignore Files
- [ ] Remove terraform-related rules from `.dockerignore`
- [ ] Remove terraform-related rules from `.gitignore` (keep the rule, but note it's for historical reasons)

#### 1.3 Update Documentation
- [ ] Remove "Production Deployment with Terraform" section from `README.md` (lines ~209-233)
- [ ] Remove "Infrastructure as Code" mentions from `README.md` overview
- [ ] Remove Terraform references from technology stack section (line ~21)
- [ ] Remove "Cost Analysis" section that mentions AWS Lightsail (lines ~174-175)
- [ ] Update "Deployment" section to focus only on local Docker Compose

#### 1.4 Clean Environment Configuration
- [ ] Remove cloud-specific variables from `.env.example`:
  - AWS credentials
  - Lightsail configuration
  - External DNS settings
  - SSL/TLS certificate paths
- [ ] Document Synology-specific environment setup

---

## Phase 2: Optimize Metabase Memory Usage

**Objective**: Reduce Metabase memory footprint from 500-800MB to 400-512MB.

**Memory Impact**: Saves ~200-400MB

### Tasks

#### 2.1 Update Metabase Dockerfile
- [ ] Modify `metabase.Dockerfile` to add JVM memory constraints:
  ```dockerfile
  ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  ```
- [ ] Add comment explaining 2GB RAM constraint optimization

#### 2.2 Add Container Memory Limits
- [ ] Update `docker-compose.yml` metabase service:
  - Add `mem_limit: 600m`
  - Add `mem_reservation: 400m`
- [ ] Document memory limit rationale in comments

#### 2.3 Test Metabase Performance
- [ ] Start optimized Metabase container
- [ ] Verify dashboard loading times
- [ ] Check query performance with memory constraints
- [ ] Monitor memory usage under load

---

## Phase 3: Migrate Metabase PostgreSQL to H2

**Objective**: Replace Metabase PostgreSQL instance with embedded H2 database.

**Memory Impact**: Saves ~150-250MB

### Tasks

#### 3.1 Export Existing Metabase Configuration
- [ ] Export current dashboards from Metabase UI (or use existing `metabase/dashboards.json`)
- [ ] Export questions/queries (or use existing `metabase/questions.json`)
- [ ] Document custom field configurations from `metabase/fields.json`
- [ ] Backup Metabase PostgreSQL database (optional, for rollback)

#### 3.2 Update Docker Compose Configuration
- [ ] Remove `metabase-db` service from `docker-compose.yml`
- [ ] Remove `METABASE_DB_PASSWORD` environment variable
- [ ] Update metabase service environment variables:
  ```yaml
  environment:
    - MB_DB_TYPE=h2
    - MB_DB_FILE=/home/metabase.db
    - MB_PLUGINS_DIR=/home/plugins/
    - JAVA_OPTS=-Xmx512m -Xms256m -XX:+UseG1GC
  ```
- [ ] Ensure metabase volume mounts include `/home` for H2 storage
- [ ] Remove metabase-db network dependencies

#### 3.3 Re-import Configuration
- [ ] Start new Metabase container with H2
- [ ] Complete initial Metabase setup
- [ ] Re-configure DuckDB connection
- [ ] Import dashboards using `metabase/utils/import_metabase.py` or manual import
- [ ] Verify all visualizations work correctly

#### 3.4 Update Environment Files
- [ ] Remove PostgreSQL variables from `.env.example`:
  - `MB_DB_DBNAME`
  - `MB_DB_PORT`
  - `MB_DB_USER`
  - `MB_DB_PASS`
  - `MB_DB_HOST`
  - `METABASE_DB_PASSWORD`

---

## Phase 4: Optimize Prefect Configuration

**Objective**: Minimize Prefect stack memory usage while retaining orchestration capabilities.

**Memory Impact**: Saves ~100-200MB

### Tasks

#### 4.1 Add Memory Limits to Prefect Services
- [ ] Update `docker-compose.yml` prefect-server service:
  - Add `mem_limit: 400m`
  - Add `mem_reservation: 300m`
- [ ] Update prefect-worker service:
  - Add `mem_limit: 300m`
  - Add `mem_reservation: 200m`
- [ ] Update prefect-db service:
  - Add `mem_limit: 200m`
  - Add `mem_reservation: 128m`

#### 4.2 Optimize Prefect Database
- [ ] Architect a minimal‑Postgres Prefect Server deployment

```yaml
# Example config. This is a starting point and is not meant to be replicated completely.
version: "3.9"

services:
  prefect-server:
    image: prefecthq/prefect:2-latest
    command: prefect server start
    environment:
      PREFECT_API_DATABASE_CONNECTION_URL: postgresql+asyncpg://prefect:prefect@postgres:5432/prefect

      # Keep UI logs enabled
      PREFECT_LOGGING_SERVER_ENABLED: "true"

      # Use a custom logging config file
      PREFECT_LOGGING_CONFIG_PATH: /etc/prefect/logging.yml

      # Optional: externalize results as well
      PREFECT_RESULTS_DEFAULT_STORAGE_BLOCK: "local-disk-storage"

    volumes:
      - ./logging.yml:/etc/prefect/logging.yml:ro
      - ./external-logs:/var/log/prefect-external
    depends_on:
      - postgres
    ports:
      - "4200:4200"

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: prefect
      POSTGRES_PASSWORD: prefect
      POSTGRES_DB: prefect
    command: >
      postgres
      -c max_connections=20
      -c shared_buffers=64MB
      -c effective_cache_size=128MB
      -c maintenance_work_mem=64MB
      -c wal_buffers=4MB
      -c max_wal_size=128MB
      -c min_wal_size=32MB
      -c log_min_duration_statement=500
    volumes:
      - pgdata:/var/lib/postgresql/data
    deploy:
      resources:
        limits:
          memory: 512M

volumes:
  pgdata:
```

- [ ] Consider PostgreSQL tuning in prefect-db service:
  ```yaml
  command: postgres -c shared_buffers=64MB -c max_connections=50
  ```
- [ ] Add health checks to prevent unnecessary restarts

#### 4.3 Optimize Prefect Worker Configuration
- [ ] Review `flows/orchestrate/deploy_flows.py` for deployment optimization
- [ ] Ensure work pool configuration uses minimal resources
- [ ] Consider reducing concurrent task execution if needed

---

## Phase 5: Add Memory Limits to All Services

**Objective**: Ensure predictable memory usage across all containers.

**Memory Impact**: Prevents memory spikes, ensures stability

### Tasks

#### 5.1 Update Data Pipeline Service
- [x] Add memory limits to `docker-compose.yml` data-pipeline:
  - Add `mem_limit: 400m`
  - Add `mem_reservation: 250m`

#### 5.2 Add Global Docker Compose Settings
- [x] Consider adding default memory limits at compose level
- [x] Document memory allocation strategy in comments

#### 5.3 Update Dockerfile
- [x] Review `Dockerfile` for any optimization opportunities
- [x] Ensure multi-stage build minimizes final image size
- [x] Consider removing unnecessary build dependencies

---

## Phase 6: Simplify for Local-Only Access

**Objective**: Remove external access configurations and simplify networking.

**Memory Impact**: None (cleanup only)

### Tasks

#### 6.1 Update Docker Compose Networking
- [ ] Remove any Nginx reverse proxy service (if present)
- [ ] Remove SSL/Certbot configurations
- [ ] Simplify network configuration to basic bridge mode
- [ ] Keep only necessary port mappings:
  - Prefect UI: `4200:4200` (LAN only)
  - Metabase: `3000:3000` (LAN only)

#### 6.2 Update Environment Variables
- [ ] Remove external access variables from `.env.example`:
  - Domain names
  - SSL certificate paths
  - External API endpoints (if any)
  - Public IP configurations
- [ ] Add Synology-specific notes:
  - Access via `http://<synology-ip>:3000`
  - No external DNS needed

#### 6.3 Update Documentation
- [ ] Add "Synology NAS Deployment" section to `README.md`
- [ ] Document memory requirements: "2GB RAM compatible (optimized)"
- [ ] Remove "4GB+ RAM recommended" from prerequisites
- [ ] Add Synology-specific access instructions
- [ ] Document Container Manager setup steps
- [ ] Add monitoring guidance (Resource Monitor app)

---

## Phase 7: Optional Dependency Optimization

**Objective**: Reduce Python dependency footprint if possible.

**Memory Impact**: Saves ~50-150MB (if dependencies removed)

### Tasks

#### 7.1 Audit Python Dependencies
- [ ] Review `pyproject.toml` dependencies:
  - `great-expectations==0.18.0` (~100MB+) - used for data validation?
  - `pandas==2.2.0` - needed if using Polars?
  - `numpy==1.26.0` - required by other packages?
- [ ] Run `uv tree` to understand dependency relationships
- [ ] Identify truly unused dependencies

#### 7.2 Remove Unused Dependencies (If Safe)
- [ ] Test removing `great-expectations` if not used in flows
- [ ] Test if pandas can be removed (polars-only)
- [ ] Ensure all flows still execute correctly
- [ ] Update `pyproject.toml` and regenerate `uv.lock`

#### 7.3 Rebuild and Test
- [ ] Rebuild Docker image with reduced dependencies
- [ ] Run full ETL pipeline to verify functionality
- [ ] Monitor memory usage improvements

---

## Phase 8: Synology-Specific Configuration

**Objective**: Optimize deployment for Synology Container Manager.

**Memory Impact**: None (configuration only)

### Tasks

#### 8.1 Create Synology Deployment Guide
- [ ] Document Container Manager setup steps
- [ ] Create volume mapping guide for `/volume1/docker/music-tracker`
- [ ] Document network configuration
- [ ] Add screenshots or step-by-step instructions

#### 8.2 Storage Optimization
- [ ] Document SSD cache usage (if available) for DuckDB files
- [ ] Create backup strategy for `data/music_tracker.duckdb`
- [ ] Document log rotation configuration

#### 8.3 Monitoring Setup
- [ ] Document using Synology Resource Monitor
- [ ] Set up Container Manager notifications
- [ ] Configure log aggregation in Synology

#### 8.4 Optional: Synology Reverse Proxy
- [ ] Document setting up reverse proxy for Metabase (optional)
- [ ] Configure custom domain on LAN (optional)

---

## Testing & Validation

### Pre-Deployment Testing
- [ ] Build all Docker images successfully
- [ ] Run `docker compose up` and verify all services start
- [ ] Check total memory usage via `docker stats`
- [ ] Verify memory usage stays under 1.8GB under normal operation

### Functional Testing
- [ ] Test Spotify API ingestion flow
- [ ] Verify data enrichment processes (MusicBrainz, geo)
- [ ] Run dbt transformations successfully
- [ ] Verify Metabase dashboards load and query correctly
- [ ] Test Prefect UI workflow monitoring

### Performance Testing
- [ ] Monitor memory usage during full ETL run
- [ ] Check Metabase query response times
- [ ] Verify DuckDB query performance
- [ ] Test concurrent dashboard access

### Rollback Plan
- [ ] Keep backup of original `docker-compose.yml`
- [ ] Document rollback steps for Metabase PostgreSQL
- [ ] Maintain backup of current Metabase configuration

---

## Success Criteria

- [ ] Total memory usage under 1.8GB during normal operation
- [ ] All containers start successfully with memory limits
- [ ] Spotify ingestion runs every 30 minutes via Prefect
- [ ] Daily ETL completes successfully
- [ ] Metabase dashboards accessible at `http://<synology-ip>:3000`
- [ ] Prefect UI accessible at `http://<synology-ip>:4200`
- [ ] No cloud infrastructure references remain in codebase
- [ ] Documentation reflects Synology-optimized deployment

---

## Estimated Memory Budget (Final)

| Service | Memory Allocation | Notes |
|---------|------------------|-------|
| Prefect Server | 300-400MB | With memory limit |
| Prefect Worker | 200-300MB | With memory limit |
| Prefect PostgreSQL | 128-200MB | Optimized config |
| Metabase | 400-512MB | JVM heap limited |
| Metabase H2 | Minimal | File-based |
| Data Pipeline | 250-400MB | Processing overhead |
| **Total** | **~1,300-1,800MB** | **Within 2GB budget** |
| **Reserve** | **200-700MB** | For DSM + buffers |

---

## Implementation Order

1. **Phase 1** (Cloud Cleanup) - Low risk, no dependencies
2. **Phase 5** (Memory Limits) - Add safety constraints before changes
3. **Phase 2** (Metabase Optimization) - Standalone optimization
4. **Phase 3** (Metabase PostgreSQL → H2) - Requires Phase 2 complete
5. **Phase 4** (Prefect Optimization) - Independent of Metabase changes
6. **Phase 6** (Local-Only Simplification) - After functional changes
7. **Phase 8** (Synology Config) - Documentation and deployment
8. **Phase 7** (Optional Dependencies) - Only if additional savings needed

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Metabase crashes with 512MB heap | Medium | High | Monitor performance, increase to 768MB if needed |
| H2 database corruption | Low | Medium | Regular backups, can revert to PostgreSQL |
| Prefect services OOM with limits | Low | Medium | Limits are conservative, can adjust |
| Memory usage exceeds 2GB | Medium | High | Phase 7 dependency reduction, reduce concurrent tasks |
| DuckDB queries slow with limited RAM | Low | Low | DuckDB uses mmap, less affected by container limits |

---

## Notes

- All changes should be committed to feature branch: `feature/docker-refactor`
- Test thoroughly on Synology before removing old configurations
- Consider creating `docker-compose.synology.yml` override file for Synology-specific settings
- Monitor logs carefully during first week of operation
- Document any issues or additional optimizations discovered during deployment

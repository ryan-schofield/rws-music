# Prefect to n8n Migration Plan

**Project**: Music Tracker  
**Date**: January 2, 2026  
**Objective**: Replace Prefect orchestration with n8n while preserving all Python data processing code and dbt transformations

## Overview

This plan replaces Prefect with n8n for workflow orchestration. The migration will be executed as a big-bang replacement, maintaining the existing two-workflow architecture (Daily ETL + Spotify Ingestion) while reducing infrastructure memory footprint from ~900MB (Prefect) to ~300MB (n8n).

### Key Principles

- **Preserve Python Code**: All existing processor classes, API clients, and data handling logic remain unchanged
- **Maintain Data Flow**: Same scheduling, task dependencies, and data passing patterns
- **API-First Workflows**: Define n8n workflows programmatically via API, not UI
- **Version Control**: Export workflow JSON to Git for change tracking
- **Simplified Infrastructure**: Single-instance n8n replaces multi-service Prefect setup

---

## Phase 1: Infrastructure Setup

### Task 1.1: Add n8n Docker Service

**Objective**: Add n8n container to docker-compose.yml and configure for local deployment

**Actions**:
1. Add n8n service definition to `docker-compose.yml`:
   ```yaml
   n8n:
     image: n8nio/n8n:latest
     container_name: music-tracker-n8n
     ports:
       - "5678:5678"
     volumes:
       - n8n-data:/home/node/.n8n
       - ./data:/app/data
       - ./flows:/app/flows
       - ./dbt:/app/dbt
       - ./logs:/app/logs
       - ./n8n-workflows:/app/n8n-workflows
     environment:
       - N8N_BASIC_AUTH_ACTIVE=true
       - N8N_BASIC_AUTH_USER=${N8N_USER}
       - N8N_BASIC_AUTH_PASSWORD=${N8N_PASSWORD}
       - N8N_HOST=0.0.0.0
       - N8N_PORT=5678
       - N8N_PROTOCOL=http
       - WEBHOOK_URL=http://localhost:5678/
       - GENERIC_TIMEZONE=America/Denver
       - EXECUTIONS_DATA_PRUNE=true
       - EXECUTIONS_DATA_MAX_AGE=168
       - N8N_LOG_LEVEL=info
     env_file: .env
     networks:
       - music-tracker
     restart: unless-stopped
     mem_limit: 300m
     mem_reservation: 200m
   ```

2. Add n8n data volume to volumes section:
   ```yaml
   volumes:
     n8n-data:
     metabase-data:
   ```

3. Add n8n environment variables to `.env` file:
   ```bash
   # n8n Configuration
   N8N_USER=admin
   N8N_PASSWORD=<generate-secure-password>
   ```

**Deliverables**:
- Updated `docker-compose.yml` with n8n service
- Updated `.env` with n8n credentials
- n8n accessible at http://localhost:5678

**Dependencies**: None

---

### Task 1.2: Remove Prefect Services

**Objective**: Remove all Prefect infrastructure from docker-compose.yml to free ~900MB memory

**Actions**:
1. Remove the following services from `docker-compose.yml`:
   - `prefect-server` (400MB)
   - `prefect-worker` (300MB)
   - `prefect-deployer` (minimal)
   - `prefect-db` (200MB)

2. Remove Prefect volumes:
   - `prefect-data`
   - `prefect-db-data`

3. Remove Prefect environment variables from `.env`:
   - `PREFECT_DB_PASSWORD`
   - `PREFECT_API_URL`
   - `PREFECT_WORK_POOL_NAME`

4. Update `data-pipeline` service dependencies:
   - Remove `depends_on: metabase` (no longer needed)
   - Container can remain for future use or be removed if not needed

**Deliverables**:
- Cleaned `docker-compose.yml` without Prefect services
- ~900MB memory freed for other services or headroom

**Dependencies**: Task 1.1 (ensure n8n is working before removing Prefect)

---

### Task 1.3: Create Workflow Version Control Structure

**Objective**: Set up directory structure for n8n workflow JSON exports

**Actions**:
1. Create directory structure:
   ```
   n8n-workflows/
   ├── README.md
   ├── daily_etl_workflow.json
   ├── spotify_ingestion_workflow.json
   ├── subflows/
   │   ├── data_preparation_workflow.json
   │   ├── enrichment_coordination_workflow.json
   │   ├── spotify_enrichment_workflow.json
   │   ├── musicbrainz_enrichment_workflow.json
   │   └── geographic_enrichment_workflow.json
   └── utils/
       ├── export_workflows.py
       └── import_workflows.py
   ```

2. Create `n8n-workflows/README.md` documenting:
   - Purpose of each workflow
   - How to import workflows via API
   - How to export workflows after UI changes
   - Workflow IDs and their purposes

3. Create `n8n-workflows/utils/export_workflows.py`:
   - Script to export all workflows from n8n API to JSON files
   - Takes n8n credentials from environment
   - Saves to `n8n-workflows/` directory with pretty-printed JSON
   - Can be run manually or via cron

4. Create `n8n-workflows/utils/import_workflows.py`:
   - Script to import workflows from JSON files to n8n
   - Creates or updates workflows based on name matching
   - Returns workflow IDs for reference

**Deliverables**:
- `n8n-workflows/` directory structure
- Export/import utility scripts
- Documentation for workflow management

**Dependencies**: Task 1.1 (n8n must be running)

---

## Phase 2: Python CLI Wrapper Development

### Task 2.1: Create CLI Module Structure

**Objective**: Set up new CLI module for standalone Python scripts callable by n8n

**Actions**:
1. Create new directory: `flows/cli/`

2. Create `flows/cli/__init__.py`:
   ```python
   """CLI wrappers for n8n workflow execution."""
   __version__ = "1.0.0"
   ```

3. Create `flows/cli/base.py`:
   - Base class `CLICommand` with common functionality:
     - Argument parsing
     - JSON output formatting
     - Error handling and logging
     - Exit code management (0=success, 1=error, 2=partial success)
   - Standardized result format:
     ```python
     {
       "status": "success|error|partial_success|no_updates",
       "message": "Human-readable message",
       "data": {...},
       "metrics": {...},
       "errors": [],
       "timestamp": "ISO 8601 datetime"
     }
     ```

4. Create `flows/cli/utils.py`:
   - Helper functions for:
     - Environment variable loading
     - Path resolution
     - FlowConfig initialization from environment
     - Logger setup

**Deliverables**:
- `flows/cli/` module with base classes
- Standardized CLI interface and output format
- Reusable utilities for all CLI commands

**Dependencies**: None (uses existing code)

---

### Task 2.2: Create Spotify Ingestion CLI

**Objective**: Wrap Spotify API ingestion in standalone CLI script

**Actions**:
1. Create `flows/cli/ingest_spotify.py`:
   - Import existing `SpotifyAPIIngestion` from `flows/ingest/spotify_api_ingestion.py`
   - Accept CLI arguments:
     - `--limit`: Number of tracks to fetch (default 50)
     - `--config-env`: Environment name (development/testing/production)
   - Call existing ingestion logic
   - Output JSON result to stdout
   - Example usage: `python flows/cli/ingest_spotify.py --limit 50`

2. Implement retry logic (3 attempts, 60s delay between attempts)

3. Set timeout: 300 seconds (5 minutes)

4. Error handling:
   - Catch API exceptions
   - Return structured error in JSON
   - Exit with code 1 on failure

**Deliverables**:
- `flows/cli/ingest_spotify.py` script
- Callable from command line with JSON output
- Maintains existing functionality from Prefect task

**Dependencies**: Task 2.1

---

### Task 2.3: Create Data Preparation CLIs

**Objective**: Wrap data loading and validation tasks in CLI scripts

**Actions**:
1. Create `flows/cli/load_raw_tracks.py`:
   - Import existing `append_tracks` from `flows/load/append_tracks.py`
   - Accept CLI arguments:
     - `--source-dir`: Raw JSON directory (default: `data/raw/recently_played/detail`)
     - `--target-table`: Target parquet table (default: `tracks_played`)
   - Output JSON result with:
     - Number of files processed
     - Number of records loaded
     - Any errors encountered

2. Create `flows/cli/validate_data.py`:
   - Check data quality:
     - Verify parquet files exist
     - Check for required columns
     - Validate record counts
   - Accept CLI arguments:
     - `--table`: Table name to validate
     - `--min-records`: Minimum expected records (optional)
   - Output JSON result with validation status

**Deliverables**:
- `flows/cli/load_raw_tracks.py`
- `flows/cli/validate_data.py`
- Both scripts output standardized JSON

**Dependencies**: Task 2.1

---

### Task 2.4: Create Spotify Enrichment CLIs

**Objective**: Wrap Spotify enrichment tasks in CLI scripts

**Actions**:
1. Create `flows/cli/enrich_spotify_artists.py`:
   - Import `SpotifyProcessor` from `flows/enrich/spotify_processor.py`
   - Accept CLI arguments:
     - `--limit`: Max artists to process (default from config)
   - Call `processor.enrich_artists(limit)`
   - Output JSON result

2. Create `flows/cli/enrich_spotify_albums.py`:
   - Similar to artists, calls `processor.enrich_albums(limit)`

3. Create `flows/cli/update_mbids.py`:
   - Calls `processor.update_mbids_from_musicbrainz()`
   - Updates Spotify artist records with MusicBrainz IDs

4. Configure retry: 3 attempts, 60s delay for all scripts

5. Set timeout: 600 seconds (10 minutes) for each script

**Deliverables**:
- Three CLI scripts for Spotify enrichment
- Maintain parallel execution capability (can be called concurrently)
- JSON output with enrichment metrics

**Dependencies**: Task 2.1

---

### Task 2.5: Create MusicBrainz Enrichment CLIs

**Objective**: Wrap MusicBrainz enrichment pipeline in CLI scripts

**Actions**:
1. Create `flows/cli/discover_mbz_artists.py`:
   - Import `MusicBrainzProcessor` from `flows/enrich/musicbrainz_processor.py`
   - Call `processor.discover_artists_needing_enrichment()`
   - Output list of artists needing enrichment

2. Create `flows/cli/fetch_mbz_artists.py`:
   - Call `processor.fetch_artist_data(limit, max_workers)`
   - Accept CLI arguments:
     - `--limit`: Max artists to fetch
     - `--workers`: Concurrent API calls (default 5)

3. Create `flows/cli/parse_mbz_data.py`:
   - Call `processor.parse_json_files()`
   - Processes raw JSON to structured data

4. Create `flows/cli/process_mbz_hierarchy.py`:
   - Call `processor.process_area_hierarchy()`
   - Builds geographic hierarchy from area data

5. Configure retry: 3 attempts for fetch, 2 for processing

6. Set timeouts: 900s for fetch, 600s for others

**Deliverables**:
- Four CLI scripts for MusicBrainz pipeline
- Sequential execution maintained (discovery → fetch → parse → hierarchy)
- JSON output at each stage

**Dependencies**: Task 2.1

---

### Task 2.6: Create Geographic Enrichment CLI

**Objective**: Wrap geographic processing in CLI script

**Actions**:
1. Create `flows/cli/enrich_geography.py`:
   - Import `GeographicProcessor` from `flows/enrich/geo_processor.py`
   - Call `processor.process_coordinates_and_continents()`
   - Accept CLI arguments:
     - `--max-workers`: Concurrent processing threads
   - Output JSON result with:
     - Number of locations processed
     - Coordinates added
     - Continents mapped

2. Configure retry: 0 retries (fail fast for data integrity)

3. Set timeout: 1800 seconds (30 minutes)

4. Note: Processor has internal parallelization, so CLI is single invocation

**Deliverables**:
- `flows/cli/enrich_geography.py` script
- No retry logic (critical task)
- JSON output with geographic enrichment metrics

**Dependencies**: Task 2.1

---

### Task 2.7: Create dbt Transformation CLI

**Objective**: Wrap dbt execution in CLI script with output capture

**Actions**:
1. Create `flows/cli/run_dbt.py`:
   - Accept CLI arguments:
     - `--command`: dbt command to run (default: `build`)
     - `--project-dir`: dbt project directory (default: `/app/dbt`)
     - `--profiles-dir`: dbt profiles directory (default: `/app/dbt`)
   - Execute commands:
     ```bash
     uv run dbt deps --profiles-dir {profiles_dir} --project-dir {project_dir}
     uv run dbt {command} --profiles-dir {profiles_dir} --project-dir {project_dir}
     ```
   - Capture stdout/stderr (n8n Execute Command node will show this)
   - Output JSON result with:
     - dbt command exit code
     - Summary of models built (parse from output)
     - Execution time
     - Log file paths

2. Set timeout: 1200 seconds (20 minutes)

3. Configure retry: 2 attempts, 30s delay

4. Note: dbt logs are already written to `dbt/logs/` directory and will be accessible after execution

**Deliverables**:
- `flows/cli/run_dbt.py` script
- dbt output captured in JSON and available via stdout
- Existing dbt log files preserved

**Dependencies**: Task 2.1

---

### Task 2.8: Create Workflow Test Script

**Objective**: Manual test script to verify all CLI wrappers work independently

**Actions**:
1. Create `flows/cli/test_all_commands.py`:
   - Sequentially runs each CLI command with test parameters
   - Verifies JSON output format
   - Checks exit codes
   - Reports any failures

2. Not automated testing, just a developer utility for validation

3. Document usage in `flows/cli/README.md`

**Deliverables**:
- Test utility script
- Documentation for manual verification

**Dependencies**: Tasks 2.2-2.7 (all CLI scripts completed)

---

## Phase 3: n8n Workflow Creation

### Task 3.1: Create Workflow Deployment Script

**Objective**: Python script to programmatically create all n8n workflows via API

**Actions**:
1. Create `flows/cli/deploy_n8n_workflows.py`:
   - Reads n8n credentials from environment
   - Connects to n8n API (http://localhost:5678)
   - Creates workflows using workflow definitions
   - Returns workflow IDs
   - Exports workflows to JSON files in `n8n-workflows/`

2. Workflow creation order:
   - Sub-workflows first (no dependencies)
   - Main workflows last (reference sub-workflow IDs)

3. Handle workflow updates:
   - Check if workflow exists by name
   - Update if exists, create if not
   - Preserve workflow IDs in exports

4. Error handling:
   - Verify n8n is accessible
   - Validate workflow JSON structure
   - Report creation failures

**Deliverables**:
- `flows/cli/deploy_n8n_workflows.py` script
- Can be run to create/update all workflows
- Exports workflow definitions to version control

**Dependencies**: Task 1.3 (version control structure)

---

### Task 3.2: Define Spotify Ingestion Workflow

**Objective**: Create n8n workflow for Spotify data ingestion

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure**:
   - **Trigger Node**: Schedule Trigger
     - Cron: `*/30 * * * *` (every 30 minutes)
     - Timezone: UTC
   
   - **Execute Command Node**: Ingest Spotify
     - Command: `cd /app && uv run python flows/cli/ingest_spotify.py --limit 50`
     - Working directory: `/app`
     - Timeout: 300000ms (5 minutes)
     - Retry: 3 attempts
     - Retry interval: 60000ms (60 seconds)
     - Environment variables: Pass through from n8n container
   
   - **Function Node**: Parse Output
     - Parse JSON from stdout
     - Extract status field
   
   - **Switch Node**: Check Status
     - Route based on status field:
       - `success` → Success branch
       - `error` → Error branch
       - `no_updates` → Success branch (no action needed)
   
   - **Error Workflow Node**: On Failure
     - Trigger error handling workflow
     - Send notification (email/webhook)

2. Export workflow JSON to `n8n-workflows/spotify_ingestion_workflow.json`

**Deliverables**:
- Spotify ingestion workflow definition
- Workflow created via API on deployment
- JSON exported to version control

**Dependencies**: Task 3.1, Task 2.2 (CLI script)

---

### Task 3.3: Define Data Preparation Sub-Workflow

**Objective**: Create sub-workflow for loading and validating raw data

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure** (no schedule trigger - called by parent):
   
   - **Webhook/Execute Workflow Trigger**: Start
   
   - **Execute Command Node**: Load Raw Tracks
     - Command: `cd /app && uv run python flows/cli/load_raw_tracks.py`
     - Timeout: 300000ms (5 minutes)
     - Retry: 2 attempts, 30s delay
   
   - **Function Node**: Parse Load Result
   
   - **Switch Node**: Check Load Status
     - If error → exit with error
     - If success → continue
   
   - **Execute Command Node**: Validate Data
     - Command: `cd /app && uv run python flows/cli/validate_data.py --table tracks_played`
     - Timeout: 60000ms (1 minute)
     - Retry: 1 attempt, 10s delay
   
   - **Function Node**: Parse Validation Result
   
   - **Function Node**: Combine Results
     - Merge load + validation results
     - Return as workflow output

2. Export to `n8n-workflows/subflows/data_preparation_workflow.json`

**Deliverables**:
- Data preparation sub-workflow
- Sequential execution: load → validate
- JSON exported to version control

**Dependencies**: Task 3.1, Task 2.3 (CLI scripts)

---

### Task 3.4: Define Spotify Enrichment Sub-Workflow

**Objective**: Create sub-workflow for Spotify data enrichment

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure**:
   
   - **Webhook/Execute Workflow Trigger**: Start
   
   - **Split Into Branches Node**: Parallel Execution
     - Branch 1: Enrich Artists
     - Branch 2: Enrich Albums
   
   - **Execute Command Node (Branch 1)**: Enrich Artists
     - Command: `cd /app && uv run python flows/cli/enrich_spotify_artists.py --limit 100`
     - Timeout: 600000ms (10 minutes)
     - Retry: 3 attempts, 60s delay
   
   - **Execute Command Node (Branch 2)**: Enrich Albums
     - Command: `cd /app && uv run python flows/cli/enrich_spotify_albums.py --limit 100`
     - Timeout: 600000ms (10 minutes)
     - Retry: 3 attempts, 60s delay
   
   - **Wait Node**: Wait for Both Branches
   
   - **Execute Command Node**: Update MBIDs
     - Command: `cd /app && uv run python flows/cli/update_mbids.py`
     - Timeout: 300000ms (5 minutes)
     - Retry: 2 attempts, 30s delay
     - Depends on: Both artist and album enrichment complete
   
   - **Function Node**: Combine Results
     - Merge all three results
     - Return enrichment summary

2. Export to `n8n-workflows/subflows/spotify_enrichment_workflow.json`

**Deliverables**:
- Spotify enrichment sub-workflow
- Parallel execution of artist + album enrichment
- Sequential MBID update after both complete
- JSON exported to version control

**Dependencies**: Task 3.1, Task 2.4 (CLI scripts)

---

### Task 3.5: Define MusicBrainz Enrichment Sub-Workflow

**Objective**: Create sub-workflow for MusicBrainz data enrichment

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure** (sequential execution):
   
   - **Webhook/Execute Workflow Trigger**: Start
   
   - **Execute Command Node**: Discover Artists
     - Command: `cd /app && uv run python flows/cli/discover_mbz_artists.py`
     - Timeout: 300000ms (5 minutes)
     - Retry: 2 attempts, 30s delay
   
   - **Function Node**: Parse Discovery Result
   
   - **Switch Node**: Check Discovery
     - If no artists found → exit with success (no work needed)
     - If error → exit with error
     - If artists found → continue
   
   - **Execute Command Node**: Fetch Artist Data
     - Command: `cd /app && uv run python flows/cli/fetch_mbz_artists.py --limit 100 --workers 5`
     - Timeout: 900000ms (15 minutes)
     - Retry: 3 attempts, 60s delay
   
   - **Execute Command Node**: Parse JSON Data
     - Command: `cd /app && uv run python flows/cli/parse_mbz_data.py`
     - Timeout: 600000ms (10 minutes)
     - Retry: 2 attempts, 30s delay
   
   - **Execute Command Node**: Process Hierarchy
     - Command: `cd /app && uv run python flows/cli/process_mbz_hierarchy.py`
     - Timeout: 600000ms (10 minutes)
     - Retry: 2 attempts, 30s delay
   
   - **Function Node**: Combine Results
     - Return enrichment summary

2. Export to `n8n-workflows/subflows/musicbrainz_enrichment_workflow.json`

**Deliverables**:
- MusicBrainz enrichment sub-workflow
- Sequential execution with early exit if no work needed
- JSON exported to version control

**Dependencies**: Task 3.1, Task 2.5 (CLI scripts)

---

### Task 3.6: Define Geographic Enrichment Sub-Workflow

**Objective**: Create sub-workflow for geographic data enrichment

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure**:
   
   - **Webhook/Execute Workflow Trigger**: Start
   
   - **Execute Command Node**: Enrich Geography
     - Command: `cd /app && uv run python flows/cli/enrich_geography.py --max-workers 10`
     - Timeout: 1800000ms (30 minutes)
     - Retry: 0 attempts (fail fast for data integrity)
   
   - **Function Node**: Parse Result
     - Return geographic enrichment summary

2. Export to `n8n-workflows/subflows/geographic_enrichment_workflow.json`

3. Note: Single task workflow, but kept as sub-workflow for consistency

**Deliverables**:
- Geographic enrichment sub-workflow
- No retry logic (critical data task)
- JSON exported to version control

**Dependencies**: Task 3.1, Task 2.6 (CLI script)

---

### Task 3.7: Define Enrichment Coordination Sub-Workflow

**Objective**: Create sub-workflow to orchestrate all three enrichment workflows in parallel

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure**:
   
   - **Webhook/Execute Workflow Trigger**: Start
   
   - **Split Into Branches Node**: Parallel Execution
     - Branch 1: Spotify Enrichment
     - Branch 2: MusicBrainz Enrichment
     - Branch 3: Geographic Enrichment
   
   - **Execute Workflow Node (Branch 1)**: Call Spotify Enrichment
     - Workflow: `spotify_enrichment_workflow`
     - Wait for completion: true
     - Capture errors: true
   
   - **Execute Workflow Node (Branch 2)**: Call MusicBrainz Enrichment
     - Workflow: `musicbrainz_enrichment_workflow`
     - Wait for completion: true
     - Capture errors: true
   
   - **Execute Workflow Node (Branch 3)**: Call Geographic Enrichment
     - Workflow: `geographic_enrichment_workflow`
     - Wait for completion: true
     - Capture errors: true
   
   - **Wait Node**: Wait for All Branches
   
   - **Function Node**: Aggregate Results
     - Collect results from all three workflows
     - Check if at least one succeeded
     - Return combined status:
       - All success → `success`
       - At least one success → `partial_success`
       - All failed → `error`
   
   - **Switch Node**: Check Combined Status
     - If `error` → exit with error
     - If `success` or `partial_success` → continue (acceptable for pipeline)

2. Export to `n8n-workflows/subflows/enrichment_coordination_workflow.json`

**Deliverables**:
- Enrichment coordination sub-workflow
- Parallel execution of 3 enrichment workflows
- Continues if at least one enrichment succeeds
- JSON exported to version control

**Dependencies**: Task 3.1, Tasks 3.4-3.6 (sub-workflows must exist first)

---

### Task 3.8: Define dbt Transformation Sub-Workflow

**Objective**: Create sub-workflow for dbt data transformations

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure**:
   
   - **Webhook/Execute Workflow Trigger**: Start
   
   - **Execute Command Node**: Run dbt
     - Command: `cd /app && uv run python flows/cli/run_dbt.py --command build`
     - Timeout: 1200000ms (20 minutes)
     - Retry: 2 attempts, 30s delay
     - Capture stdout: true (dbt output will be visible in n8n execution log)
   
   - **Function Node**: Parse dbt Result
     - Extract exit code, summary, log paths
     - Return dbt execution summary
   
   - **Switch Node**: Check dbt Status
     - If error → exit with error
     - If success → continue
   
   - **Execute Command Node**: Update Reporting Views (Optional)
     - Command: `cd /app && uv run python flows/cli/update_reporting.py` (if exists)
     - Timeout: 300000ms (5 minutes)
     - Retry: 1 attempt, 10s delay
   
   - **Function Node**: Combine Results

2. Export to `n8n-workflows/subflows/transformation_workflow.json`

3. Note: dbt logs written to `dbt/logs/` are accessible after execution for troubleshooting

**Deliverables**:
- dbt transformation sub-workflow
- dbt output captured in execution log
- JSON exported to version control

**Dependencies**: Task 3.1, Task 2.7 (CLI script)

---

### Task 3.9: Define Daily ETL Main Workflow

**Objective**: Create main workflow orchestrating full daily ETL pipeline

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure**:
   
   - **Trigger Node**: Schedule Trigger
     - Cron: `0 2 * * *` (daily at 2:00 AM)
     - Timezone: `America/Denver`
   
   - **Function Node**: Initialize Execution
     - Create execution context
     - Log start time
   
   - **Execute Workflow Node**: Stage 1 - Data Preparation
     - Workflow: `data_preparation_workflow`
     - Wait for completion: true
   
   - **Function Node**: Check Stage 1 Result
   
   - **Switch Node**: Stage 1 Status
     - If error → exit with error
     - If success → continue to Stage 2
   
   - **Execute Workflow Node**: Stage 2 - Enrichment Coordination
     - Workflow: `enrichment_coordination_workflow`
     - Wait for completion: true
   
   - **Function Node**: Check Stage 2 Result
   
   - **Switch Node**: Stage 2 Status
     - If error → exit with error
     - If success or partial_success → continue to Stage 3
   
   - **Execute Workflow Node**: Stage 3 - Transformations
     - Workflow: `transformation_workflow`
     - Wait for completion: true
   
   - **Function Node**: Check Stage 3 Result
   
   - **Function Node**: Aggregate Pipeline Results
     - Combine all stage results
     - Calculate total execution time
     - Return comprehensive pipeline summary
   
   - **Switch Node**: Final Status
     - If error → trigger error workflow
     - If success → log success
   
   - **Error Workflow Node**: On Any Failure
     - Send notification
     - Log detailed error information

2. Export to `n8n-workflows/daily_etl_workflow.json`

**Deliverables**:
- Daily ETL main workflow
- Three sequential stages with sub-workflow calls
- Comprehensive result aggregation
- JSON exported to version control

**Dependencies**: Task 3.1, Tasks 3.3, 3.7, 3.8 (all sub-workflows must exist)

---

### Task 3.10: Create Error Notification Workflow

**Objective**: Reusable workflow for error notifications

**Actions**:
1. Add workflow definition to `deploy_n8n_workflows.py`:

   **Workflow Structure**:
   
   - **Error Trigger**: Triggered by workflow errors
   
   - **Function Node**: Format Error Message
     - Extract workflow name, error details, timestamp
     - Create formatted error message
   
   - **Switch Node**: Notification Method
     - Based on severity or environment
   
   - **Email Node** (if configured):
     - Send error email to admin
     - Include error details and execution link
   
   - **Webhook Node** (alternative):
     - POST error details to external webhook
     - For integration with monitoring systems

2. Export to `n8n-workflows/error_notification_workflow.json`

3. Note: Email/webhook credentials configured in n8n credentials store

**Deliverables**:
- Error notification workflow
- Reusable across all workflows
- JSON exported to version control

**Dependencies**: Task 3.1

---

## Phase 4: Configuration and Documentation

### Task 4.1: Update Docker Compose Memory Allocations

**Objective**: Document and adjust memory limits reflecting Prefect removal

**Actions**:
1. Update memory allocation comments in `docker-compose.yml`:
   ```yaml
   # Memory allocation strategy (Updated Post-Prefect Migration):
   # - n8n: 300MB (workflow orchestration)
   # - Metabase: 600MB (JVM with 512MB heap)
   # - Data Pipeline: 400MB (can be increased to 600MB if needed)
   # - Total: ~1.3GB (700MB freed from Prefect removal, available for scaling)
   ```

2. Consider increasing `data-pipeline` memory if needed:
   - Current: 400MB limit, 250MB reservation
   - Possible: 600MB limit, 400MB reservation (using freed memory)

3. Update comments for n8n service with actual usage notes after deployment

**Deliverables**:
- Updated `docker-compose.yml` memory comments
- Optional: Increased data-pipeline limits

**Dependencies**: Tasks 1.1-1.2 (infrastructure changes complete)

---

### Task 4.2: Update .env.example File

**Objective**: Add n8n configuration to example environment file

**Actions**:
1. Create or update `.env.example`:
   ```bash
   # Spotify API Credentials (Required)
   SPOTIFY_CLIENT_ID=your_spotify_client_id
   SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

   # n8n Configuration (Required)
   N8N_USER=admin
   N8N_PASSWORD=generate_secure_password_here

   # Optional Configuration
   ENVIRONMENT=development
   DUCKDB_PATH=/app/data/music_tracker.duckdb
   LOG_LEVEL=INFO
   ```

2. Remove Prefect-related variables:
   - `PREFECT_DB_PASSWORD`
   - `PREFECT_API_URL`
   - `PREFECT_WORK_POOL_NAME`

3. Add comments explaining n8n configuration

**Deliverables**:
- Updated `.env.example` with n8n variables
- Removed Prefect variables

**Dependencies**: None

---

### Task 4.3: Update README.md

**Objective**: Replace Prefect documentation with n8n information

**Actions**:
1. Update "Architecture" section:
   - Replace "Prefect" references with "n8n"
   - Update service descriptions:
     - Remove: Prefect Server, Prefect Worker, Prefect Deployer, PostgreSQL (Prefect)
     - Add: n8n (workflow orchestration and monitoring UI on port 5678)

2. Update "System Components" section:
   - Update port information: n8n on 5678
   - Remove Prefect components
   - Update memory allocation summary

3. Update "Data Pipeline" section:
   - Keep existing process descriptions
   - Update orchestration description to reference n8n instead of Prefect

4. Update "Configuration" → "Environment Variables":
   - Replace Prefect variables with n8n variables

5. Update "Getting Started" → "Access Applications":
   - Replace: `Prefect UI: http://localhost:4200`
   - With: `n8n UI: http://localhost:5678`

6. Update "Getting Started" → "Run Initial Data Load":
   - Replace: "The Prefect deployer will automatically deploy flows"
   - With: "Workflows are deployed automatically via `flows/cli/deploy_n8n_workflows.py` or can be imported from `n8n-workflows/` directory"

7. Update "Development" → "Project Structure":
   - Update `flows/` descriptions
   - Add: `flows/cli/` - CLI wrappers for n8n workflow execution
   - Add: `n8n-workflows/` - n8n workflow definitions (JSON)
   - Remove: `flows/orchestrate/deploy_flows.py` reference (or note it's deprecated)

8. Update "Development" → "Key Dependencies":
   - Remove: `prefect`
   - Add: Note that n8n runs in separate container (not a Python dependency)

9. Update "Deployment" → "Synology NAS Deployment":
   - Update Container Manager instructions
   - Remove Prefect services from memory configuration
   - Add n8n service setup instructions
   - Update memory totals

10. Update "Resource Requirements":
    - Memory: Update from ~1.9GB to ~1.3GB with 700MB freed
    - Add note about n8n access: http://<synology-ip>:5678

**Deliverables**:
- Comprehensive README.md updates
- All Prefect references replaced with n8n
- Accurate memory and resource documentation

**Dependencies**: All previous tasks (full picture of changes needed)

---

### Task 4.4: Update SYNOLOGY_DEPLOYMENT_GUIDE.md

**Objective**: Update Synology-specific deployment guide with n8n instructions

**Actions**:
1. Update service listing:
   - Replace Prefect services with n8n
   - Update memory allocations

2. Update "Container Manager Setup" section:
   - Add instructions for n8n container configuration
   - Remove Prefect container instructions
   - Update port mappings

3. Update "Access Applications" section:
   - Replace Prefect UI URL with n8n UI URL
   - Add n8n credential information

4. Update troubleshooting section:
   - Remove Prefect-specific issues
   - Add common n8n issues:
     - Workflow execution failures
     - Authentication problems
     - Volume permission issues

5. Update memory monitoring section:
   - Reflect new memory allocation strategy
   - Update expected memory usage totals

**Deliverables**:
- Updated SYNOLOGY_DEPLOYMENT_GUIDE.md
- Synology-specific n8n instructions

**Dependencies**: Task 4.3 (README updates)

---

### Task 4.5: Create n8n Operations Documentation

**Objective**: Document how to use and maintain n8n workflows

**Actions**:
1. Create `docs/N8N_OPERATIONS.md`:

   **Contents**:
   - **Accessing n8n UI**: How to log in, basic navigation
   - **Workflow Management**:
     - How to view execution history
     - How to manually trigger workflows
     - How to view logs and output
   - **Workflow Updates**:
     - How to modify workflows via UI
     - How to export modified workflows to Git
     - How to import workflows from Git
   - **Monitoring**:
     - How to check workflow execution status
     - How to set up error notifications
     - How to view execution metrics
   - **Troubleshooting**:
     - Common workflow failures and solutions
     - How to check CLI script logs
     - How to manually run CLI scripts for testing
     - How to check dbt logs after transformation failures
   - **CLI Reference**:
     - List of all CLI scripts with usage examples
     - Parameter descriptions
     - Output format documentation

2. Add quick reference commands for common operations

**Deliverables**:
- Comprehensive n8n operations guide
- Troubleshooting documentation
- CLI script reference

**Dependencies**: All Phase 3 tasks (workflows must be defined)

---

### Task 4.6: Update pyproject.toml

**Objective**: Remove Prefect dependencies and add any new CLI-related dependencies

**Actions**:
1. Update `pyproject.toml`:
   - Remove: `prefect==3.4.0`
   - Remove: `prefect-docker==0.6.0`
   - Add any new dependencies for CLI scripts (if needed):
     - `click` or `typer` (if using for CLI argument parsing)
     - `httpx` (already present, may be used for n8n API calls)

2. Update project description if it references Prefect

3. Run `uv lock` to update `uv.lock` with new dependencies

**Deliverables**:
- Updated `pyproject.toml` without Prefect
- Updated `uv.lock`

**Dependencies**: None (can be done early)

---

## Phase 5: Migration Execution

### Task 5.1: Pre-Migration Backup

**Objective**: Backup all data and configurations before migration

**Actions**:
1. Stop all Docker containers:
   ```bash
   docker compose down
   ```

2. Backup data directory:
   ```bash
   # On Synology or local
   tar -czf music-tracker-backup-$(date +%Y%m%d).tar.gz data/
   ```

3. Backup current docker-compose.yml:
   ```bash
   cp docker-compose.yml docker-compose.yml.prefect-backup
   ```

4. Export current Prefect flow metadata (optional, for reference):
   - Access Prefect UI and document current schedules
   - Take screenshots of workflow structure

5. Document current Prefect deployment IDs and schedules

**Deliverables**:
- Data backup archive
- Configuration backup
- Documentation of current state

**Dependencies**: None (first step of execution)

---

### Task 5.2: Deploy Infrastructure Changes

**Objective**: Apply all docker-compose.yml changes and start n8n

**Actions**:
1. Apply all changes from Phase 1:
   - Updated `docker-compose.yml` with n8n service
   - Removed Prefect services
   - Updated `.env` with n8n credentials

2. Build and start containers:
   ```bash
   docker compose build
   docker compose up -d
   ```

3. Verify n8n is running:
   ```bash
   docker compose logs n8n
   curl http://localhost:5678
   ```

4. Verify other services (metabase, data-pipeline) are running:
   ```bash
   docker compose ps
   ```

5. Access n8n UI and complete initial setup:
   - Navigate to http://localhost:5678
   - Log in with credentials from `.env`
   - Verify UI is accessible

**Deliverables**:
- n8n running and accessible
- All non-Prefect services operational
- n8n UI accessible and configured

**Dependencies**: Task 5.1 (backup complete), Phase 1 tasks (changes ready)

---

### Task 5.3: Deploy CLI Scripts

**Objective**: Copy all CLI scripts to data-pipeline container and verify functionality

**Actions**:
1. Verify `flows/cli/` directory is mounted in data-pipeline container:
   ```bash
   docker compose exec data-pipeline ls -la /app/flows/cli
   ```

2. Test each CLI script individually (manual verification):
   ```bash
   # Test Spotify ingestion
   docker compose exec data-pipeline uv run python flows/cli/ingest_spotify.py --limit 10
   
   # Test data loading
   docker compose exec data-pipeline uv run python flows/cli/load_raw_tracks.py
   
   # Test validation
   docker compose exec data-pipeline uv run python flows/cli/validate_data.py --table tracks_played
   
   # Continue for all CLI scripts...
   ```

3. Verify JSON output format from each script

4. Verify exit codes (0 for success, 1 for error)

5. Document any issues found and fix before proceeding

**Deliverables**:
- All CLI scripts verified working
- JSON output validated
- Any issues resolved

**Dependencies**: Task 5.2 (containers running), Phase 2 tasks (CLI scripts ready)

---

### Task 5.4: Deploy n8n Workflows

**Objective**: Create all workflows in n8n using deployment script

**Actions**:
1. Run workflow deployment script:
   ```bash
   docker compose exec data-pipeline uv run python flows/cli/deploy_n8n_workflows.py
   ```

2. Verify workflows created in n8n UI:
   - Navigate to http://localhost:5678
   - Check "Workflows" page
   - Verify all workflows present:
     - spotify_ingestion_workflow
     - daily_etl_workflow
     - data_preparation_workflow
     - enrichment_coordination_workflow
     - spotify_enrichment_workflow
     - musicbrainz_enrichment_workflow
     - geographic_enrichment_workflow
     - transformation_workflow
     - error_notification_workflow

3. Verify workflow JSON exported to `n8n-workflows/` directory

4. Check workflow schedules are configured correctly:
   - Spotify Ingestion: Every 30 minutes
   - Daily ETL: Daily at 2 AM America/Denver

5. Manually activate workflows in n8n UI:
   - Toggle "Active" switch for each workflow

**Deliverables**:
- All workflows created in n8n
- Workflows active and scheduled
- Workflow JSON files in version control

**Dependencies**: Task 5.3 (CLI scripts working), Phase 3 tasks (workflow definitions ready)

---

### Task 5.5: Test Individual Workflows

**Objective**: Manually test each workflow execution before relying on schedules

**Actions**:
1. Test Spotify Ingestion workflow:
   - Manually trigger in n8n UI
   - Monitor execution in real-time
   - Verify JSON output in execution log
   - Check data written to `data/raw/recently_played/detail/`

2. Test Data Preparation sub-workflow:
   - Manually trigger
   - Verify tracks loaded to parquet
   - Check validation passes

3. Test Spotify Enrichment sub-workflow:
   - Manually trigger
   - Verify parallel execution of artist/album enrichment
   - Check MBID updates

4. Test MusicBrainz Enrichment sub-workflow:
   - Manually trigger
   - Verify sequential execution
   - Check data written to appropriate parquet files

5. Test Geographic Enrichment sub-workflow:
   - Manually trigger
   - Verify geographic data processed

6. Test Enrichment Coordination sub-workflow:
   - Manually trigger
   - Verify all three enrichment workflows run in parallel
   - Test partial success scenario (if possible)

7. Test Transformation sub-workflow:
   - Manually trigger
   - Monitor dbt output in execution log
   - Verify dbt models built successfully
   - Check `dbt/logs/` for detailed logs

8. Test Daily ETL main workflow:
   - Manually trigger
   - Monitor all stages executing sequentially
   - Verify complete pipeline success
   - Check final aggregated results

9. Test Error Notification workflow:
   - Trigger a workflow with intentional error
   - Verify error notification sent

**Deliverables**:
- All workflows tested and working
- Execution logs reviewed
- Any issues documented and fixed

**Dependencies**: Task 5.4 (workflows deployed)

---

### Task 5.6: Monitor Scheduled Executions

**Objective**: Verify scheduled workflows run correctly without manual intervention

**Actions**:
1. Monitor Spotify Ingestion workflow:
   - Wait for next 30-minute interval
   - Check n8n execution history
   - Verify successful execution
   - Monitor for next 2-3 automatic executions

2. Monitor Daily ETL workflow:
   - Wait for next scheduled run (2 AM America/Denver)
   - Check n8n execution history
   - Review full pipeline execution
   - Verify all stages complete successfully
   - Check final data in DuckDB

3. Monitor memory usage during executions:
   - Use `docker stats` to check container memory
   - Verify staying within allocated limits
   - Document peak memory usage

4. Check for any errors or warnings in execution logs

5. Monitor for 3-5 days to ensure stability

**Deliverables**:
- Scheduled workflows running automatically
- Memory usage within limits
- No recurring errors
- Stable operation confirmed

**Dependencies**: Task 5.5 (manual tests pass)

---

### Task 5.7: Validate Data Consistency

**Objective**: Ensure data pipeline produces same results as Prefect implementation

**Actions**:
1. Compare recent data with historical data:
   - Check record counts in key tables
   - Verify data quality metrics
   - Compare enrichment coverage

2. Run dbt tests:
   ```bash
   docker compose exec data-pipeline bash -c "cd /app/dbt && uv run dbt test"
   ```

3. Verify Metabase dashboards show correct data:
   - Access Metabase UI
   - Check all dashboards render correctly
   - Verify recent data appears

4. Compare execution times:
   - Document n8n execution times
   - Compare with historical Prefect execution times
   - Note any significant differences

5. Verify all enrichment sources working:
   - Spotify API enrichment
   - MusicBrainz API enrichment
   - Geographic processing

**Deliverables**:
- Data consistency verified
- dbt tests passing
- Metabase dashboards working
- Execution time documentation

**Dependencies**: Task 5.6 (several scheduled runs complete)

---

### Task 5.8: Clean Up Prefect Artifacts

**Objective**: Remove Prefect-related files and code no longer needed

**Actions**:
1. Remove or archive Prefect deployment script:
   ```bash
   # Option 1: Remove
   rm flows/orchestrate/deploy_flows.py
   
   # Option 2: Archive
   mkdir -p archive/prefect
   mv flows/orchestrate/deploy_flows.py archive/prefect/
   ```

2. Archive other Prefect-specific files:
   - `flows/orchestrate/flows/` (Prefect flow definitions)
   - `flows/orchestrate/subflows/` (Prefect subflow definitions)
   - `flows/orchestrate/atomic_tasks.py` (Prefect task definitions)
   - `flows/orchestrate/base_tasks.py` (Prefect base classes)

3. Keep but mark as deprecated:
   - `flows/orchestrate/flow_config.py` (still used by CLI scripts)
   - `flows/orchestrate/monitoring.py` (optionally used by CLI)

4. Remove Prefect backup docker-compose.yml after confirming success:
   ```bash
   rm docker-compose.yml.prefect-backup
   ```

5. Update `.gitignore` if needed to exclude Prefect artifacts

6. Commit all changes to Git:
   ```bash
   git add .
   git commit -m "Migrate from Prefect to n8n orchestration"
   git push
   ```

**Deliverables**:
- Prefect files removed or archived
- Repository cleaned up
- Changes committed to Git

**Dependencies**: Task 5.7 (migration validated as successful)

---

## Phase 6: Post-Migration

### Task 6.1: Update Synology NAS Deployment

**Objective**: Deploy migrated system to production Synology NAS environment

**Actions**:
1. Follow same steps as Phase 5 on Synology NAS:
   - Backup current production data
   - Stop existing containers
   - Pull latest code from Git
   - Update `.env` with production credentials
   - Build and start containers
   - Deploy workflows
   - Test and monitor

2. Update Container Manager:
   - Remove Prefect containers
   - Add n8n container
   - Verify memory allocations

3. Monitor resource usage on NAS:
   - Use Synology Resource Monitor
   - Verify memory stays under 2GB
   - Check CPU usage during workflows

4. Document any NAS-specific configurations or issues

**Deliverables**:
- Production Synology deployment updated
- n8n running on NAS
- Resource usage verified

**Dependencies**: Task 5.8 (local migration complete and stable)

---

### Task 6.2: Performance Optimization

**Objective**: Optimize workflow execution and resource usage based on monitoring data

**Actions**:
1. Review n8n execution metrics:
   - Identify slow workflows or tasks
   - Check for timeout issues
   - Look for retry patterns indicating problems

2. Adjust workflow configurations:
   - Optimize timeouts based on actual execution times
   - Adjust retry counts if needed
   - Tune parallelization settings

3. Optimize memory allocations:
   - Increase limits for bottleneck containers
   - Decrease limits for over-allocated containers
   - Ensure staying within 2GB total on NAS

4. Optimize CLI script performance:
   - Add caching where appropriate
   - Optimize batch sizes for API calls
   - Tune concurrent worker counts

5. Update workflow definitions with optimizations:
   - Run deployment script to update workflows
   - Export updated JSON to version control

**Deliverables**:
- Performance optimization recommendations
- Updated workflow configurations
- Optimized memory allocations

**Dependencies**: Task 6.1 (production deployment complete), 1-2 weeks of monitoring data

---

### Task 6.3: Create Monitoring Dashboard (Optional)

**Objective**: Set up external monitoring for n8n workflows

**Actions**:
1. Options for monitoring:
   - **Option A**: Use n8n's built-in execution history and metrics
   - **Option B**: Export metrics to external system (Prometheus, Grafana)
   - **Option C**: Custom dashboard using n8n API

2. If implementing external monitoring:
   - Set up n8n API access
   - Create script to poll execution history
   - Store metrics in time-series database
   - Create Grafana dashboard

3. Set up alerting:
   - Configure error notifications
   - Set up monitoring for workflow failures
   - Create alerts for performance degradation

4. Document monitoring setup in `docs/N8N_OPERATIONS.md`

**Deliverables**:
- Monitoring solution implemented (if desired)
- Alerting configured
- Documentation updated

**Dependencies**: Task 6.1 (production deployment complete)

---

### Task 6.4: Final Documentation Review

**Objective**: Review and finalize all documentation for completeness and accuracy

**Actions**:
1. Review all documentation files:
   - README.md
   - SYNOLOGY_DEPLOYMENT_GUIDE.md
   - docs/N8N_OPERATIONS.md
   - n8n-workflows/README.md

2. Verify accuracy of:
   - Port numbers
   - Memory allocations
   - Command examples
   - Workflow descriptions
   - Troubleshooting steps

3. Add migration notes:
   - Document migration date
   - Note any lessons learned
   - Document any deviations from plan

4. Create migration summary document:
   - Before/after comparison
   - Benefits achieved
   - Challenges encountered
   - Future improvement opportunities

5. Update any diagrams or architecture documentation

**Deliverables**:
- All documentation reviewed and finalized
- Migration summary document
- Updated architecture diagrams if applicable

**Dependencies**: All other tasks (final step)

---

## Success Criteria

The migration will be considered successful when:

1. **Infrastructure**:
   - n8n running stably in Docker on Synology NAS
   - Prefect services completely removed
   - Memory usage under 2GB total

2. **Workflows**:
   - All workflows executing on schedule
   - Spotify Ingestion: Every 30 minutes
   - Daily ETL: Daily at 2 AM America/Denver
   - No recurring errors in execution logs

3. **Data Pipeline**:
   - All Python CLI scripts functioning correctly
   - Data quality maintained (same as Prefect implementation)
   - dbt transformations running successfully
   - Metabase dashboards showing current data

4. **Monitoring**:
   - n8n execution history accessible
   - Error notifications working
   - Performance metrics within acceptable range

5. **Documentation**:
   - All documentation updated and accurate
   - Team can operate and troubleshoot n8n workflows
   - Workflow JSON versioned in Git

6. **Stability**:
   - System running for 1-2 weeks without intervention
   - No data loss or quality issues
   - Resource usage stable and predictable

---

## Risk Mitigation

### High Risk: Data Loss During Migration
- **Mitigation**: Task 5.1 ensures complete backup before any changes
- **Recovery**: Restore from backup and investigate issue

### Medium Risk: Workflow Execution Failures
- **Mitigation**: Task 5.3 tests all CLI scripts independently before workflow testing
- **Recovery**: Debug CLI scripts individually, update and redeploy workflows

### Medium Risk: Memory Exhaustion on 2GB NAS
- **Mitigation**: Conservative memory limits with monitoring; n8n uses less memory than Prefect
- **Recovery**: Adjust memory allocations, reduce concurrent executions

### Low Risk: Schedule Configuration Issues
- **Mitigation**: Careful testing of cron expressions and timezone settings
- **Recovery**: Update workflow schedules in n8n UI or via API

### Low Risk: API Rate Limiting
- **Mitigation**: CLI scripts maintain same rate limiting logic as Prefect tasks
- **Recovery**: Adjust retry delays and batch sizes if needed

---

## Timeline Estimate

- **Phase 1** (Infrastructure Setup): 2-3 hours
- **Phase 2** (Python CLI Wrappers): 8-12 hours
- **Phase 3** (n8n Workflow Creation): 12-16 hours
- **Phase 4** (Configuration and Documentation): 4-6 hours
- **Phase 5** (Migration Execution): 6-8 hours + 3-5 days monitoring
- **Phase 6** (Post-Migration): 4-6 hours + ongoing optimization

**Total Development Time**: 36-51 hours  
**Total Timeline**: 2-3 weeks (including monitoring periods)

---

## Next Steps

1. Review this plan and provide feedback or request clarifications
2. Set up development environment for testing (if not using production directly)
3. Begin Phase 1: Infrastructure Setup
4. Execute tasks sequentially, marking as complete in project tracking
5. Monitor progress and adjust plan as needed based on discoveries

---

## Questions or Clarifications

Before beginning implementation, confirm:

1. Are there any additional workflows or scheduled tasks beyond Daily ETL and Spotify Ingestion?
2. Should we implement gradual rollout (test environment first) or proceed directly to production?
3. Are there any specific monitoring or alerting requirements beyond basic error notifications?
4. Should we implement workflow execution metrics export for long-term trend analysis?
5. Are there any Synology-specific constraints or configurations we should be aware of?

---

*End of Migration Plan*

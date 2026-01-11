# CLI Module for n8n Workflow Integration

This directory contains CLI wrappers for all data processing tasks, enabling standalone invocation from n8n workflows.

## Overview

The CLI module provides command-line interfaces for all Python processors, replacing Prefect's task-based approach with direct script execution. Each CLI:

- Accepts command-line arguments
- Handles retries with exponential backoff (60s delay between attempts)
- Enforces timeouts
- Returns JSON output for n8n integration
- Implements standard error handling

## Architecture

### Base Classes

- **`base.py`**: `CLICommand` abstract base class with common functionality
  - Retry logic (configurable attempts)
  - Timeout enforcement
  - Error handling
  - JSON output formatting

- **`utils.py`**: Utility functions
  - Environment variable validation
  - Path validation
  - Metrics formatting

## CLI Commands

### Data Ingestion

#### `ingest_spotify.py`
Ingests recently played tracks from Spotify API.

```bash
python flows/cli/ingest_spotify.py --limit 50
```

**Options:**
- `--limit`: Maximum tracks to ingest per batch (default: None)

**Timeout:** 300s (5 minutes)
**Retries:** 3

---

### Data Loading

#### `load_raw_tracks.py`
Loads raw JSON track data into structured format.

```bash
python flows/cli/load_raw_tracks.py
```

**Timeout:** 300s (5 minutes)
**Retries:** 1

#### `validate_data.py`
Validates data quality and integrity of loaded data.

```bash
python flows/cli/validate_data.py
```

**Timeout:** 300s (5 minutes)
**Retries:** 0 (fail fast)

---

### Spotify Enrichment

#### `enrich_spotify_artists.py`
Enriches missing artist data from Spotify API.

```bash
python flows/cli/enrich_spotify_artists.py --limit 50
```

**Options:**
- `--limit`: Maximum artists to enrich (default: None)

**Timeout:** 600s (10 minutes)
**Retries:** 3

#### `enrich_spotify_albums.py`
Enriches missing album data from Spotify API.

```bash
python flows/cli/enrich_spotify_albums.py --limit 50
```

**Options:**
- `--limit`: Maximum albums to enrich (default: None)

**Timeout:** 600s (10 minutes)
**Retries:** 3

#### `update_mbids.py`
Updates Spotify artist records with MusicBrainz IDs.

```bash
python flows/cli/update_mbids.py
```

**Timeout:** 600s (10 minutes)
**Retries:** 3

---

### MusicBrainz Enrichment

The MusicBrainz enrichment pipeline consists of 4 sequential steps:

#### `discover_mbz_artists.py`
Discovers artists that need MusicBrainz enrichment.

```bash
python flows/cli/discover_mbz_artists.py
```

**Timeout:** 600s (10 minutes)
**Retries:** 2

#### `fetch_mbz_artists.py`
Fetches artist data from MusicBrainz API.

```bash
python flows/cli/fetch_mbz_artists.py --limit 100 --workers 5
```

**Options:**
- `--limit`: Maximum artists to fetch (default: None)
- `--workers`: Number of parallel workers (default: 5)

**Timeout:** 900s (15 minutes)
**Retries:** 3

#### `parse_mbz_data.py`
Processes raw JSON data from MusicBrainz API into structured format.

```bash
python flows/cli/parse_mbz_data.py
```

**Timeout:** 600s (10 minutes)
**Retries:** 2

#### `process_mbz_hierarchy.py`
Builds geographic hierarchy from MusicBrainz area data.

```bash
python flows/cli/process_mbz_hierarchy.py
```

**Timeout:** 600s (10 minutes)
**Retries:** 2

---

### Geographic Enrichment

#### `enrich_geography.py`
Enriches geographic data with continent mapping and latitude/longitude.

```bash
python flows/cli/enrich_geography.py
```

**Timeout:** 1800s (30 minutes)
**Retries:** 0 (fail fast - data integrity critical)

Note: Processor has internal parallelization, so CLI is single invocation.

---

### Data Transformation

#### `run_dbt.py`
Runs dbt models to transform raw data into reporting-ready tables.

```bash
python flows/cli/run_dbt.py --select models/reporting --full-refresh
```

**Options:**
- `--select`: dbt selector for models to run (default: None)
- `--exclude`: dbt models to exclude (default: None)
- `--full-refresh`: Force full refresh of all models

**Timeout:** 1200s (20 minutes)
**Retries:** 2

---

## Output Format

All CLI commands return JSON output with the following structure:

```json
{
  "status": "success|error|no_updates",
  "message": "Human-readable status message",
  "data": { /* command-specific data */ },
  "errors": ["error message 1", "error message 2"]
}
```

### Status Codes

- **`success`** (exit code 0): Command completed successfully
- **`error`** (exit code 1): Command failed
- **`no_updates`** (exit code 0): No data to process (not a failure)

### Examples

Success:
```json
{
  "status": "success",
  "message": "Ingested 42 tracks",
  "data": {"tracks_count": 42, "duration": 5.2},
  "errors": null
}
```

Error:
```json
{
  "status": "error",
  "message": "Spotify API request failed",
  "data": null,
  "errors": ["401 Unauthorized", "Check SPOTIFY_CLIENT_ID"]
}
```

No updates:
```json
{
  "status": "no_updates",
  "message": "No artists need MusicBrainz enrichment",
  "data": null,
  "errors": null
}
```

---

## Testing

### Manual Testing

Run all CLI commands with test parameters to verify functionality:

```bash
python flows/cli/test_all_commands.py
```

This script:
- Runs each CLI command sequentially
- Uses minimal test parameters (e.g., `--limit 5`)
- Verifies JSON output format
- Reports pass/fail for each command
- **Not** automated testing, just developer validation

---

## Error Handling

### Retry Logic

Commands with retries will:
1. Execute immediately
2. On failure, wait 60 seconds
3. Retry the command
4. Repeat until max attempts reached
5. Exit with code 1 if all attempts fail

Example with 3 retries:
```
Attempt 1/3: Failed
Waiting 60s...
Attempt 2/3: Failed
Waiting 60s...
Attempt 3/3: Failed
Exiting with code 1
```

### Timeout Enforcement

Commands have a timeout in seconds. If execution exceeds this duration:
- A warning is logged
- Task may continue (Python doesn't have hard process-level timeouts)
- Tasks should check elapsed time and exit gracefully

---

## Integration with n8n

These CLI commands are designed for n8n HTTP Request nodes:

```json
{
  "method": "GET",
  "url": "http://localhost:5678/webhook/spotify-ingestion",
  "parameters": {
    "limit": 50
  }
}
```

Each command returns JSON output that n8n can:
- Check status in conditionals
- Extract metrics for logging
- Pass to downstream nodes
- Store in database

---

## Development

### Adding New Commands

1. Create new file `flows/cli/new_command.py`
2. Implement class extending `CLICommand`:
   ```python
   class NewCommandCLI(CLICommand):
       def __init__(self):
           super().__init__(
               name="new_command",
               timeout=300,
               retries=1,
           )
       
       def execute(self, **kwargs):
           try:
               # Your code here
               return self.success_result("Done", data={...})
           except Exception as e:
               return self.error_result(str(e), errors=[str(e)])
   ```

3. Add `main()` function for CLI entry point
4. Add to `test_all_commands.py` test list

### Logging

All commands log to stdout/stderr with standard format:
```
2024-01-02 12:34:56,789 - cli.command_name - INFO - Starting command
```

Configure via environment variable:
```bash
export LOG_LEVEL=DEBUG
python flows/cli/command.py
```

---

## Troubleshooting

### Command times out

- Increase timeout in CLI class initialization
- Check n8n logs for resource constraints
- Consider parallelizing task (use `--workers` for applicable commands)

### Command fails with JSON parse error

- Ensure command is outputting valid JSON to stdout
- Check stderr for error messages
- Run command directly to debug

### Missing environment variables

- Ensure `.env` file is loaded before running
- Export variables: `export SPOTIFY_CLIENT_ID=...`
- Check command logs for which variables are missing

---

## References

- [n8n Webhook Documentation](https://docs.n8n.io/code-examples/webhooks/)
- [n8n HTTP Request Node](https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base-httprequest/)
- Prefect to n8n Migration Plan: `PREFECT_TO_N8N_MIGRATION_PLAN.md`

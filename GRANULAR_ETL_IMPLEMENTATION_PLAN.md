# Granular ETL Implementation Plan

## Overview

Convert the 6 remaining Daily ETL workflow steps into n8n sub-workflows following the Spotify enrichment pattern. This will improve memory efficiency, error recovery, and progress visibility.

**Estimated Total Effort:** 34 hours

## Implementation Strategy

- **2 Granular Multi-Step Workflows** (API-heavy, benefit from batching)
- **4 Simple Sub-Workflows** (fast operations, single-step wrappers)
- **1 Inline Code** (Update MBIDs - already optimal)

---

## Task 1: Granular MBZ Artist Enrichment Workflow

**Priority:** HIGH (API-heavy, long-running)  
**Estimated Effort:** 10 hours  
**Pattern:** Spotify enrichment (multi-step granular)

### Background
- Current: Single CLI command processes all artists sequentially
- Problem: 2 API calls per artist + 0.5s rate limit = ~50 minutes for 100 artists
- Solution: Break into batches for better progress tracking and resumability

### Implementation Steps

#### 1.1: Add DuckDB Query Method
**File:** `flows/enrich/utils/duckdb_queries.py`

Add method after existing `get_missing_spotify_artists`:

```python
def get_missing_mbz_artists(self, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Find artists needing MusicBrainz enrichment using DuckDB.
    
    Returns artists with ISRCs that don't have MBZ data yet.
    Filters to last 48 hours of play data.
    """
    query = """
    SELECT DISTINCT
        tp.artist_id,
        tp.artist,
        FIRST(tp.track_isrc) as track_isrc
    FROM tracks_played tp
    LEFT JOIN mbz_artist_info mbz ON tp.artist_id = mbz.spotify_id
    WHERE mbz.spotify_id IS NULL
      AND tp.track_isrc IS NOT NULL
      AND tp.artist_id IS NOT NULL
      AND tp.played_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
    GROUP BY tp.artist_id, tp.artist
    ORDER BY tp.artist
    """
    if limit:
        query += f" LIMIT {limit}"
    return self.execute_query(query)

def get_mbz_artists_batch(
    self, batch_size: int = 10, offset: int = 0
) -> pl.DataFrame:
    """
    Get a batch of missing MBZ artists for processing.
    
    Args:
        batch_size: Number of artists to return (default 10 for rate limiting)
        offset: Starting offset for pagination
        
    Returns:
        DataFrame with artist_id, artist, and track_isrc columns
    """
    query = f"""
    SELECT DISTINCT
        tp.artist_id,
        tp.artist,
        FIRST(tp.track_isrc) as track_isrc
    FROM tracks_played tp
    LEFT JOIN mbz_artist_info mbz ON tp.artist_id = mbz.spotify_id
    WHERE mbz.spotify_id IS NULL
      AND tp.track_isrc IS NOT NULL
      AND tp.artist_id IS NOT NULL
      AND tp.played_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
    GROUP BY tp.artist_id, tp.artist
    ORDER BY tp.artist
    LIMIT {batch_size} OFFSET {offset}
    """
    return self.execute_query(query)
```

Update `get_missing_count` method to include MBZ artists:

```python
def get_missing_count(self, entity_type: str = "artists") -> int:
    """
    Get count of missing entities efficiently.
    
    Args:
        entity_type: 'artists', 'albums', or 'mbz_artists'
    """
    if entity_type == "artists":
        # existing Spotify artists query
    elif entity_type == "albums":
        # existing albums query
    elif entity_type == "mbz_artists":
        query = """
        SELECT COUNT(DISTINCT tp.artist_id) as count
        FROM tracks_played tp
        LEFT JOIN mbz_artist_info mbz ON tp.artist_id = mbz.spotify_id
        WHERE mbz.spotify_id IS NULL
          AND tp.track_isrc IS NOT NULL
          AND tp.artist_id IS NOT NULL
          AND tp.played_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
        """
    # rest of method
```

#### 1.2: Create Granular CLI Commands
**New File:** `flows/cli/enrich_mbz_artists_granular.py`

```python
#!/usr/bin/env python3
"""
CLI commands for granular MusicBrainz artist enrichment tasks.

These commands break down the enrichment process into distinct steps:
1. Identify missing MBZ artists (DuckDB query)
2. Fetch artist batch from MusicBrainz API
3. Track failed lookups

This design is optimized for n8n workflows with better resumability.
"""

class IdentifyMissingMBZArtistsCLI(CLICommand):
    """Identify artists that need MusicBrainz enrichment."""
    
    def execute(self, limit: int = None, batch_size: int = 10, **kwargs):
        """
        Identify missing MBZ artists and return batching information.
        
        Args:
            limit: Maximum total artists to process
            batch_size: Size of each batch (default 10 for rate limiting)
        """
        # Use DuckDBQueryEngine.get_missing_count("mbz_artists")
        # Calculate batches
        # Return batch plan similar to Spotify pattern

class FetchMBZArtistBatchCLI(CLICommand):
    """Fetch a batch of artist data from MusicBrainz API."""
    
    def execute(self, batch_index: int = 0, batch_size: int = 10, 
                offset: int = None, **kwargs):
        """
        Fetch MBZ data for a specific batch of artists.
        
        For each artist:
        1. Lookup MBID using ISRC
        2. If found, fetch full artist data
        3. Write JSON to cache directory
        4. Track failures
        
        Returns:
            Result with fetched count and failures
        """
        # Get batch using DuckDBQueryEngine.get_mbz_artists_batch()
        # For each artist: call MusicBrainzClient methods
        # Respect rate limits (0.5s sleep)
        # Write JSON files
        # Collect failures

class TrackMBZFailuresCLI(CLICommand):
    """Track artists that failed MusicBrainz lookup."""
    
    def execute(self, failed_artists: List[Dict], **kwargs):
        """
        Write failed artist lookups to tracking table.
        
        Args:
            failed_artists: List of artist dicts that failed lookup
        """
        # Create DataFrame from failures
        # Write to mbz_artist_not_found table
```

**Key Differences from Spotify:**
- Smaller batch size (10 vs 50) due to rate limiting
- Two API calls per artist (ISRC lookup + artist data)
- File-based caching (JSON files) vs direct parquet writes
- Failure tracking as separate step

#### 1.3: Create n8n Workflow
**New File:** `n8n-workflows/MBZ Artist Enrichment.json`

**Workflow Structure:**
```
1. "When Executed by Another Workflow" trigger
2. "Identify Missing MBZ Artists" - Code node
3. "Split Into Batches" - SplitOut node
4. "Fetch MBZ Artist Batch" - Code node (runOnceForEachItem)
5. "Track Failures" - Code node (runOnceForEachItem, only if failures)
```

**Node Configuration:**
- Batch size: 10 artists (due to rate limits)
- Mode: runOnceForEachItem for Fetch and Track nodes
- Use _item["json"] syntax
- Parallel execution of batches

---

## Task 2: Simple MBZ Sub-Workflows

**Priority:** MEDIUM (quick wins)  
**Estimated Effort:** 6 hours (2h each × 3 workflows)  
**Pattern:** Simple wrapper with single code node

### 2.1: MBZ Artist Discovery Workflow
**New File:** `n8n-workflows/MBZ Artist Discovery.json`

**Workflow Structure:**
```
1. "When Executed by Another Workflow" trigger
2. "Discover MBZ Artists" - Code node
   - Calls: DiscoverMBZArtistsCLI().execute()
   - Returns: Count and metadata
```

**Background:**
- Fast operation (< 1 minute)
- No API calls, pure data processing
- Returns discovery metadata only
- No batching needed

### 2.2: MBZ Data Parsing Workflow
**New File:** `n8n-workflows/MBZ Data Parsing.json`

**Workflow Structure:**
```
1. "When Executed by Another Workflow" trigger
2. "Parse MBZ Data" - Code node
   - Calls: ParseMBZDataCLI().execute()
   - Returns: Processing metrics
```

**Background:**
- Processes cached JSON files from fetch step
- Complex schema reconciliation (better as single operation)
- File system operations (moving processed files)
- Volume limited by upstream fetch step

### 2.3: MBZ Hierarchy Processing Workflow
**New File:** `n8n-workflows/MBZ Hierarchy Processing.json`

**Workflow Structure:**
```
1. "When Executed by Another Workflow" trigger
2. "Process MBZ Hierarchy" - Code node
   - Calls: ProcessMBZHierarchyCLI().execute()
   - Returns: Processing metrics
```

**Background:**
- Recursive API calls to build area hierarchy
- Complex state management (visited tracking)
- Difficult to split into independent batches
- Low volume (10-50 areas typical)
- Runtime usually < 10 minutes

---

## Task 3: Granular Geography Coordinate Enrichment

**Priority:** HIGH (API-heavy, parallelizable)  
**Estimated Effort:** 12 hours  
**Pattern:** Multi-step granular for coordinates

### Background
Geography enrichment has 3 internal steps:
1. **Continents** - Fast, uses pycountry library, no API
2. **Geocoding params** - Fast, string concatenation
3. **Coordinates** - Slow, OpenWeather API calls for each city

**Strategy:** Split into two workflows:
- Geography Base (continents + params) - Simple wrapper
- Geography Coordinates (API calls) - Granular batching

### Implementation Steps

#### 3.1: Add DuckDB Query Method
**File:** `flows/enrich/utils/duckdb_queries.py`

Add after MBZ methods:

```python
def get_cities_needing_coordinates(self, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Find cities that need coordinate lookup using DuckDB.
    
    Returns cities with geocoding params that don't have coordinates yet.
    """
    query = """
    SELECT DISTINCT
        ah.params,
        ah.city_name,
        ah.country_code,
        ah.country_name
    FROM mbz_area_hierarchy ah
    LEFT JOIN cities_with_lat_long c ON ah.params = c.params
    WHERE ah.params IS NOT NULL
      AND ah.params != ''
      AND c.params IS NULL
    ORDER BY ah.city_name
    """
    if limit:
        query += f" LIMIT {limit}"
    return self.execute_query(query)

def get_cities_batch(
    self, batch_size: int = 50, offset: int = 0
) -> pl.DataFrame:
    """
    Get a batch of cities needing coordinate lookup.
    
    Args:
        batch_size: Number of cities to return (default 50)
        offset: Starting offset for pagination
        
    Returns:
        DataFrame with params, city_name, country_code, country_name columns
    """
    query = f"""
    SELECT DISTINCT
        ah.params,
        ah.city_name,
        ah.country_code,
        ah.country_name
    FROM mbz_area_hierarchy ah
    LEFT JOIN cities_with_lat_long c ON ah.params = c.params
    WHERE ah.params IS NOT NULL
      AND ah.params != ''
      AND c.params IS NULL
    ORDER BY ah.city_name
    LIMIT {batch_size} OFFSET {offset}
    """
    return self.execute_query(query)
```

Update `get_missing_count` to include cities:

```python
elif entity_type == "cities":
    query = """
    SELECT COUNT(DISTINCT ah.params) as count
    FROM mbz_area_hierarchy ah
    LEFT JOIN cities_with_lat_long c ON ah.params = c.params
    WHERE ah.params IS NOT NULL
      AND ah.params != ''
      AND c.params IS NULL
    """
```

#### 3.2: Split Geography Processor
**File:** `flows/enrich/geo_processor.py`

Extract coordinate enrichment logic to allow separate calling:

```python
def enrich_base(self) -> Dict[str, Any]:
    """
    Add continent and geocoding params only (no API calls).
    
    Steps:
    1. Enrich continents (pycountry)
    2. Add geocoding params
    
    Fast operations, no external APIs.
    """
    # Call existing enrich_continents()
    # Call existing add_geocoding_params()
    # Return combined results

def enrich_coordinates_batch(self, city_params: List[str]) -> Dict[str, Any]:
    """
    Enrich coordinates for a specific batch of cities.
    
    Args:
        city_params: List of geocoding param strings
        
    Returns:
        Result with coordinate data
    """
    # Extract from existing enrich_coordinates()
    # Process only the provided city_params
    # Return coordinate data for writing
```

#### 3.3: Create Granular CLI Commands
**New File:** `flows/cli/enrich_geography_coordinates_granular.py`

```python
#!/usr/bin/env python3
"""
CLI commands for granular geography coordinate enrichment.

Breaks coordinate lookup into batches for better memory management
and progress tracking.
"""

class IdentifyCitiesNeedingCoordinatesCLI(CLICommand):
    """Identify cities that need coordinate lookup."""
    
    def execute(self, limit: int = None, batch_size: int = 50, **kwargs):
        """
        Identify cities needing coordinates and return batch plan.
        
        Uses DuckDB for memory-efficient querying.
        """
        # Use DuckDBQueryEngine.get_missing_count("cities")
        # Calculate batches
        # Return batch plan

class FetchCoordinateBatchCLI(CLICommand):
    """Fetch coordinates for a batch of cities."""
    
    def execute(self, batch_index: int = 0, batch_size: int = 50,
                offset: int = None, **kwargs):
        """
        Fetch coordinates from OpenWeather API for a batch.
        
        Returns:
            Result with coordinate data
        """
        # Get batch using DuckDBQueryEngine.get_cities_batch()
        # Call OpenWeatherGeoClient for each city
        # Handle rate limits
        # Return coordinate data

class WriteCoordinateDataCLI(CLICommand):
    """Write coordinate data to parquet table."""
    
    def execute(self, coordinate_data: List[Dict], **kwargs):
        """
        Write fetched coordinates to cities_with_lat_long table.
        """
        # Create DataFrame
        # Write using ParquetDataWriter
```

#### 3.4: Create Simple Geography Base Wrapper
**New File:** `flows/cli/enrich_geography_base.py`

```python
#!/usr/bin/env python3
"""
CLI wrapper for geography base enrichment (continents + params).

Fast operations with no API calls.
"""

class EnrichGeographyBaseCLI(CLICommand):
    """Add continent and geocoding params to area hierarchy."""
    
    def execute(self, **kwargs):
        """
        Execute base geography enrichment.
        
        Steps:
        1. Add continent info (pycountry)
        2. Add geocoding params
        """
        # Call GeographicProcessor.enrich_base()
```

#### 3.5: Create n8n Workflows

**New File:** `n8n-workflows/Geography Base Enrichment.json`
```
1. "When Executed by Another Workflow" trigger
2. "Enrich Geography Base" - Code node
   - Calls: EnrichGeographyBaseCLI().execute()
   - Returns: Metrics for continents + params
```

**New File:** `n8n-workflows/Geography Coordinate Enrichment.json`
```
1. "When Executed by Another Workflow" trigger
2. "Identify Cities Needing Coordinates" - Code node
3. "Split Into Batches" - SplitOut node
4. "Fetch Coordinate Batch" - Code node (runOnceForEachItem)
5. "Write Coordinate Data" - Code node (runOnceForEachItem)
```

---

## Task 4: Update Daily ETL Workflow

**File:** `n8n-workflows/Daily ETL.json`

### Current Inline Nodes to Replace

Replace these inline code nodes with executeWorkflow calls:

1. **Discover MBZ Artists** → Call 'MBZ Artist Discovery'
2. **Fetch MBZ Artists** → Call 'MBZ Artist Enrichment' (granular)
3. **Parse MBZ Data** → Call 'MBZ Data Parsing'
4. **Process MBZ Hierarchy** → Call 'MBZ Hierarchy Processing'
5. **Enrich Geography** → Sequential calls:
   - Call 'Geography Base Enrichment'
   - Call 'Geography Coordinate Enrichment'
6. **Update MBIDs** → Keep as inline code (already optimal)

### Updated Workflow Structure

```
1. Schedule Trigger (daily 1am)

2. [PARALLEL] Call 'Spotify Artist Enrichment'
3. [PARALLEL] Call 'Spotify Album Enrichment'
4. Merge Spotify Outputs

5. Call 'MBZ Artist Discovery'
6. Call 'MBZ Artist Enrichment' (granular)
7. Call 'MBZ Data Parsing'
8. Call 'MBZ Hierarchy Processing'

9. Call 'Geography Base Enrichment'
10. Call 'Geography Coordinate Enrichment' (granular)

11. Update MBIDs (inline code node)
    - Pure data join operation
    - Fast, no API calls
    - Keep as-is

12. Run dbt Build (existing)
```

### Dependencies & Order

**Critical Path:**
- Spotify enrichment must complete before MBZ Discovery
- MBZ steps must run in sequence (Discovery → Fetch → Parse → Hierarchy)
- Geography Base must complete before Geography Coordinates
- Update MBIDs must run after all enrichment
- dbt Build must be last

**Parallelization:**
- Spotify Artist + Album run in parallel (existing)
- MBZ Fetch batches run in parallel (within workflow)
- Geography Coordinate batches run in parallel (within workflow)

---

## Task 5: Update Imports and References

### Files to Update

**`flows/cli/__init__.py`** - Add new CLI commands:
```python
from .enrich_mbz_artists_granular import (
    IdentifyMissingMBZArtistsCLI,
    FetchMBZArtistBatchCLI,
    TrackMBZFailuresCLI,
)
from .enrich_geography_base import EnrichGeographyBaseCLI
from .enrich_geography_coordinates_granular import (
    IdentifyCitiesNeedingCoordinatesCLI,
    FetchCoordinateBatchCLI,
    WriteCoordinateDataCLI,
)
```

**`flows/enrich/utils/__init__.py`** - Export query engine if needed

---

## Task 6: Testing Plan

### Unit Tests

**Test DuckDB Queries:**
- Test `get_missing_mbz_artists()` with various data states
- Test `get_mbz_artists_batch()` pagination
- Test `get_cities_needing_coordinates()` filtering
- Test `get_cities_batch()` pagination

**Test Granular CLI Commands:**
- Mock DuckDB queries
- Mock API clients
- Test batch processing logic
- Test error handling

### Integration Tests

**Test Individual Workflows:**
1. Run MBZ Artist Enrichment with test data
2. Run Geography Coordinate Enrichment with test data
3. Verify data written correctly
4. Check batch state management

**Test Full Daily ETL:**
1. Run complete workflow end-to-end
2. Verify all sub-workflows called correctly
3. Check data integrity at each step
4. Verify dbt build runs successfully

### Manual Testing Checklist

- [ ] DuckDB queries return expected results
- [ ] Batch sizes respect rate limits
- [ ] Progress tracking works across batches
- [ ] Failed batches can be retried
- [ ] Memory usage stays low (< 200MB per workflow)
- [ ] All data writes use merge mode correctly
- [ ] Workflow execution time acceptable
- [ ] Error messages are clear and actionable

---

## Task 7: Documentation Updates

### Files to Create/Update

**`flows/cli/README.md`** - Update with:
- New granular MBZ commands
- New granular Geography commands
- Usage examples
- Batch size recommendations

**`n8n-workflows/README.md`** - Add:
- MBZ workflow descriptions
- Geography workflow descriptions
- Dependency diagrams
- Troubleshooting guide

**Main README.md** - Update:
- Architecture diagram
- Workflow descriptions
- Memory optimization notes

---

## Success Metrics

### Performance
- ✅ Memory usage < 200MB per workflow (90% reduction from 500MB)
- ✅ MBZ fetch processes 100 artists in < 60 minutes
- ✅ Geography coordinates processes 100 cities in < 15 minutes

### Reliability
- ✅ Failed batches can be retried independently
- ✅ Progress visible per batch in n8n UI
- ✅ No data loss on workflow failure

### Maintainability
- ✅ Consistent patterns across all granular workflows
- ✅ Clear error messages
- ✅ Comprehensive logging
- ✅ Documentation complete

---

## Implementation Order

### Phase 1: Infrastructure (4 hours)
1. Add DuckDB query methods (all 3 entity types)
2. Update imports
3. Create base CLI structure

### Phase 2: MBZ Enrichment (10 hours)
1. Create `enrich_mbz_artists_granular.py`
2. Create MBZ Artist Enrichment workflow
3. Create 3 simple MBZ workflows
4. Test MBZ flows

### Phase 3: Geography Enrichment (12 hours)
1. Split geography processor methods
2. Create `enrich_geography_base.py`
3. Create `enrich_geography_coordinates_granular.py`
4. Create 2 geography workflows
5. Test geography flows

### Phase 4: Integration (4 hours)
1. Update Daily ETL workflow
2. Test end-to-end flow
3. Verify dependencies

### Phase 5: Documentation (2 hours)
1. Update all README files
2. Add troubleshooting guides
3. Document batch size recommendations

### Phase 6: Testing (2 hours)
1. Run full test suite
2. Manual testing
3. Performance validation

**Total: 34 hours**

---

## Risk Mitigation

### Potential Issues

1. **Rate Limiting**
   - Risk: API rate limits more aggressive than expected
   - Mitigation: Conservative batch sizes (10 for MBZ, 50 for geography)
   - Fallback: Add configurable sleep durations

2. **DuckDB Table Registration**
   - Risk: Tables not found in task runner environment
   - Mitigation: Absolute paths, existence checks
   - Fallback: Load full tables as backup

3. **Batch State Management**
   - Risk: Batch tracking gets out of sync
   - Mitigation: Use proven pattern from Spotify workflows
   - Fallback: Add reset command

4. **Schema Compatibility**
   - Risk: New writes don't match existing schemas
   - Mitigation: Check existing table schemas before implementation
   - Fallback: Schema reconciliation in ParquetDataWriter

5. **Workflow Dependencies**
   - Risk: Parallel execution causes race conditions
   - Mitigation: Clear sequential ordering in Daily ETL
   - Fallback: Add explicit wait nodes

---

## Rollback Plan

If issues arise during implementation:

1. **Keep existing workflows functional** - Don't modify until new workflows tested
2. **Parallel deployment** - Run old and new workflows side-by-side initially
3. **Feature flags** - Add ability to switch between old/new in Daily ETL
4. **Data validation** - Compare outputs of old vs new approaches
5. **Quick revert** - Keep old workflow nodes disabled but present in Daily ETL

---

## Next Steps

1. Review and approve this plan
2. Create feature branch: `feature/granular-etl-workflows`
3. Start with Phase 1 (Infrastructure)
4. Implement one workflow at a time
5. Test thoroughly before moving to next
6. Deploy to production incrementally

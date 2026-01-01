# Metabase PostgreSQL to H2 Migration Guide

This guide provides step-by-step instructions for migrating Metabase from PostgreSQL to H2 embedded database as part of the Synology NAS optimization.

## Overview

The migration process involves:
1. Exporting existing Metabase configuration
2. Updating Docker configuration to use H2
3. Starting new Metabase container with H2
4. Re-importing configuration into the new instance

## Prerequisites

- Docker and Docker Compose installed
- Existing Metabase configuration files (already present in this project)
- Backup of your current Metabase data (recommended)

## Step 1: Export Existing Configuration (Already Completed)

The Metabase configuration has already been exported and is available in the following files:

- `metabase/questions.json` - All questions/queries
- `metabase/dashboards.json` - Dashboard definitions
- `metabase/dashboard_cards.json` - Dashboard card layouts
- `metabase/tables.json` - Database table metadata
- `metabase/fields.json` - Field definitions
- `metabase/query_snippets.json` - Query snippets

## Step 2: Update Configuration (Completed)

The Docker configuration has been updated to use H2 database:

### Changes Made:

1. **docker-compose.yml**:
   - Removed `metabase-db` PostgreSQL service
   - Updated Metabase service environment variables:
     ```yaml
     environment:
       - MB_DB_TYPE=h2
       - MB_DB_FILE=/home/metabase.db
       - MB_PLUGINS_DIR=/home/plugins/
       - JAVA_OPTS=-Xmx512m -Xms256m -XX:+UseG1GC
     ```
   - Removed `depends_on: metabase-db`
   - Removed PostgreSQL-related environment variables

2. **.env.example**:
   - Removed `METABASE_DB_PASSWORD` variable
   - Added documentation about H2 database configuration

## Step 3: Start New Metabase Container

1. **Stop existing containers** (if running):
   ```bash
   docker compose down
   ```

2. **Remove old PostgreSQL data** (optional, but recommended for clean migration):
   ```bash
   docker volume rm rws-music_metabase-db-data
   ```

3. **Start new Metabase with H2**:
   ```bash
   docker compose up -d metabase
   ```

4. **Wait for Metabase to initialize**:
   - This may take a few minutes as it creates the new H2 database
   - Monitor logs with: `docker logs -f music-tracker-metabase`
   - Wait for the message: "Metabase Initialization COMPLETE"

## Step 4: Complete Initial Setup

1. **Access Metabase UI**: Open `http://localhost:3000` in your browser
2. **Complete setup wizard**:
   - Create admin user (use same credentials as before if desired)
   - Skip sample data
   - Set up DuckDB connection to your music data

## Step 5: Re-import Configuration

Use the provided import script to restore your dashboards and questions:

```bash
# Set required environment variables
cp .env.example .env
# Edit .env to set METABASE_API_KEY (get this from Metabase admin settings)

# Run the import script
cd metabase/utils
python import_metabase.py
```

### Import Process Details:

1. **Query Snippets**: Imported first as they may be referenced by questions
2. **Questions**: Imported with table and field reference updates
3. **Dashboards**: Imported after questions are available
4. **Dashboard Cards**: Linked to the newly imported questions

### Troubleshooting:

- **API Key Issues**: Ensure `METABASE_API_KEY` is set correctly in your `.env` file
- **Connection Issues**: Verify Metabase is running and accessible at `http://localhost:3000`
- **Reference Errors**: The import script automatically handles table/field ID mapping

## Step 6: Verify Migration

1. **Check dashboards**: All dashboards should be visible and functional
2. **Test queries**: Run a few sample queries to ensure data connectivity
3. **Verify visualizations**: Check that charts and graphs display correctly
4. **Monitor memory usage**: Should be significantly reduced (400-512MB vs 500-800MB)

## Step 7: Update DuckDB Connection

If your dashboards use DuckDB data, ensure the connection is properly configured:

1. Go to Metabase Admin > Databases
2. Edit the DuckDB connection
3. Verify the path points to your music data: `/data/music_tracker.duckdb`
4. Test the connection

## Rollback Plan

If issues arise, you can rollback:

1. **Restore docker-compose.yml** from backup
2. **Restore .env.example** from backup  
3. **Start original containers**:
   ```bash
   docker compose up -d
   ```

## Expected Memory Savings

- **Before**: Metabase + PostgreSQL = 500-800MB + 150-250MB = 650-1050MB
- **After**: Metabase with H2 = 400-512MB (H2 overhead is minimal)
- **Savings**: ~150-500MB depending on usage patterns

## Post-Migration Notes

- **H2 Database Location**: Stored in `/home/metabase.db` within the `metabase-data` volume
- **Backups**: Regularly backup the `metabase-data` volume
- **Performance**: H2 should provide comparable performance for this workload
- **Scalability**: H2 is suitable for single-user/small-team usage

## Support

If you encounter issues during migration:

1. Check Metabase logs: `docker logs music-tracker-metabase`
2. Review import script output for errors
3. Consult the Metabase documentation for H2-specific configuration
4. Consider temporary memory limit adjustments if needed
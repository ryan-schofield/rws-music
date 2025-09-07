#!/bin/bash
# Music Tracker Restore Script
# This script restores DuckDB database and PostgreSQL databases from backups

set -e

# Configuration
BACKUP_DIR="/opt/music-tracker/backups"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to show usage
usage() {
    echo "Usage: $0 <backup_date>"
    echo "Example: $0 20250107_143000"
    echo ""
    echo "Available backups:"
    ls -la "$BACKUP_DIR" | grep -E "\.(duckdb|sql)\.gz$" | awk '{print $9}' | sed 's/.*_\([0-9_]*\)\..*\.gz/\1/' | sort -u
    exit 1
}

# Check if backup date is provided
if [ $# -eq 0 ]; then
    usage
fi

BACKUP_DATE=$1

log "Starting restore process for backup date: $BACKUP_DATE"

# Stop the application first
log "Stopping Music Tracker application..."
systemctl stop music-tracker || docker-compose -f /opt/music-tracker/docker-compose.prod.yml down

# Restore DuckDB database
if [ -f "$BACKUP_DIR/music_tracker_${BACKUP_DATE}.duckdb.gz" ]; then
    log "Restoring DuckDB database..."
    
    # Backup current database if it exists
    if [ -f "/opt/music-tracker/data/music_tracker.duckdb" ]; then
        mv "/opt/music-tracker/data/music_tracker.duckdb" "/opt/music-tracker/data/music_tracker.duckdb.bak.$(date +%s)"
        log "Current database backed up"
    fi
    
    # Restore from backup
    gunzip -c "$BACKUP_DIR/music_tracker_${BACKUP_DATE}.duckdb.gz" > "/opt/music-tracker/data/music_tracker.duckdb"
    chown ubuntu:ubuntu "/opt/music-tracker/data/music_tracker.duckdb"
    log "DuckDB database restored successfully"
else
    log "Warning: DuckDB backup not found for date $BACKUP_DATE"
fi

# Start databases first
log "Starting database containers..."
cd /opt/music-tracker
docker-compose -f docker-compose.prod.yml up -d metabase-db prefect-db

# Wait for databases to be ready
log "Waiting for databases to be ready..."
sleep 30

# Restore PostgreSQL databases
if [ -f "$BACKUP_DIR/metabase_${BACKUP_DATE}.sql.gz" ]; then
    log "Restoring Metabase database..."
    
    # Drop and recreate database
    docker exec music-tracker-metabase-db psql -U metabase -c "DROP DATABASE IF EXISTS metabase;"
    docker exec music-tracker-metabase-db psql -U metabase -c "CREATE DATABASE metabase;"
    
    # Restore from backup
    gunzip -c "$BACKUP_DIR/metabase_${BACKUP_DATE}.sql.gz" | docker exec -i music-tracker-metabase-db psql -U metabase -d metabase
    log "Metabase database restored successfully"
else
    log "Warning: Metabase backup not found for date $BACKUP_DATE"
fi

if [ -f "$BACKUP_DIR/prefect_${BACKUP_DATE}.sql.gz" ]; then
    log "Restoring Prefect database..."
    
    # Drop and recreate database
    docker exec music-tracker-prefect-db psql -U prefect -c "DROP DATABASE IF EXISTS prefect;"
    docker exec music-tracker-prefect-db psql -U prefect -c "CREATE DATABASE prefect;"
    
    # Restore from backup
    gunzip -c "$BACKUP_DIR/prefect_${BACKUP_DATE}.sql.gz" | docker exec -i music-tracker-prefect-db psql -U prefect -d prefect
    log "Prefect database restored successfully"
else
    log "Warning: Prefect backup not found for date $BACKUP_DATE"
fi

# Restore logs if available
if [ -f "$BACKUP_DIR/logs_${BACKUP_DATE}.tar.gz" ]; then
    log "Restoring application logs..."
    mkdir -p /opt/music-tracker/logs
    tar -xzf "$BACKUP_DIR/logs_${BACKUP_DATE}.tar.gz" -C /opt/music-tracker/logs/
    chown -R ubuntu:ubuntu /opt/music-tracker/logs
    log "Logs restored successfully"
fi

# Start the full application
log "Starting Music Tracker application..."
systemctl start music-tracker || docker-compose -f /opt/music-tracker/docker-compose.prod.yml up -d

log "Restore process completed successfully!"
log "Please verify that all services are running:"
log "- Check containers: docker ps"
log "- Check logs: docker-compose -f /opt/music-tracker/docker-compose.prod.yml logs"
log "- Access Metabase: http://$(curl -s ifconfig.me):3000"
log "- Access Prefect: http://$(curl -s ifconfig.me):4200"
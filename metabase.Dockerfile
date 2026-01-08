# Use Debian-based image with necessary system libraries for DuckDB support
# Alpine base image doesn't have glibc/libstdc++ needed by DuckDB Java driver
FROM debian:bookworm-slim

# Install Java and system dependencies required by DuckDB Java driver
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    libstdc++6 \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create Metabase directories
RUN mkdir -p /metabase /home && \
    chmod 777 /home /metabase

# Create plugins directory with proper permissions
RUN mkdir -p /metabase/plugins && chmod 777 /metabase/plugins

# Download Metabase JAR
RUN curl -L --retry 3 --retry-delay 2 -o /metabase/metabase.jar https://downloads.metabase.com/v0.49.13/metabase.jar && \
    chmod 644 /metabase/metabase.jar

# Download and install DuckDB driver
RUN curl -L --retry 3 --retry-delay 2 -o /metabase/plugins/duckdb.metabase-driver.jar https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.0/duckdb.metabase-driver.jar && \
    chmod 644 /metabase/plugins/duckdb.metabase-driver.jar

# Set Metabase environment variables
ENV MB_PLUGINS_DIR=/metabase/plugins
ENV MB_DB_FILE=/home/metabase.db
ENV JAVA_OPTS="-Xmx480m -Xms256m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+DisableExplicitGC"

# Optimization flags for minimal resource usage
ENV MB_ANALYTICS_ENABLED=false
ENV MB_SEND_ANONYMOUS_USAGE_STATS=false
ENV MB_DB_AUTODETECT_REQUIRES_PASSWORD=false
ENV MB_QUERY_CACHE_MAX_KB=10000
ENV MB_QUERY_DEFAULT_CACHE_TTL_MS=300000
ENV REDISSON_ENABLED=false

# Expose Metabase port
EXPOSE 3000

# Start Metabase (runs as root to ensure write permissions)
CMD ["java", "-jar", "/metabase/metabase.jar"]

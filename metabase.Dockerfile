FROM metabase/metabase:latest

ENV MB_PLUGINS_DIR=/plugins/
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+DisableExplicitGC"

# Optimization flags for minimal resource usage
ENV MB_ANALYTICS_ENABLED=false
ENV MB_SEND_ANONYMOUS_USAGE_STATS=false
ENV MB_DB_AUTODETECT_REQUIRES_PASSWORD=false
ENV MB_QUERY_CACHE_MAX_KB=10000
ENV MB_QUERY_DEFAULT_CACHE_TTL_MS=300000
ENV REDISSON_ENABLED=false

# Create plugins directory
RUN mkdir -p $MB_PLUGINS_DIR

# Download and install DuckDB driver with retry logic (H2 is built-in)
RUN curl -L --retry 3 --retry-delay 2 -o $MB_PLUGINS_DIR/duckdb.metabase-driver.jar https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.0/duckdb.metabase-driver.jar && \
    chmod 744 $MB_PLUGINS_DIR/duckdb.metabase-driver.jar

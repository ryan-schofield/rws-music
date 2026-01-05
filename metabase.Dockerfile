FROM metabase/metabase:latest

ENV MB_PLUGINS_DIR=/plugins/

# Download and install DuckDB driver with retry logic
RUN curl -L --retry 3 --retry-delay 2 -o /plugins/duckdb.metabase-driver.jar https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.0/duckdb.metabase-driver.jar && \
    chmod 744 /plugins/duckdb.metabase-driver.jar

# Optimize JVM memory usage for 2GB RAM Synology NAS
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

FROM eclipse-temurin:21-jdk-jammy

ENV MB_PLUGINS_DIR=/home/plugins/

# Download latest Metabase
ADD https://downloads.metabase.com/v0.53.7/metabase.jar /home

# Download DuckDB driver
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.4.1/duckdb.metabase-driver.jar /home/plugins/

# Set permissions
RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar

# Optimize JVM memory usage for 2GB RAM Synology NAS
# Limit heap to 512MB max, 256MB initial with G1 garbage collector
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

CMD java $JAVA_OPTS -jar /home/metabase.jar
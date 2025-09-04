FROM openjdk:21-buster

ENV MB_PLUGINS_DIR=/home/plugins/

# Download latest Metabase
ADD https://downloads.metabase.com/v0.53.7/metabase.jar /home

# Download DuckDB driver
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.4.1/duckdb.metabase-driver.jar /home/plugins/

# Set permissions
RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar

CMD ["java", "-jar", "/home/metabase.jar"]
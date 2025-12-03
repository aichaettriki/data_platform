# UPDATE THIS TAG to match your Dev environment exactly
FROM metabase/metabase:v0.56.13
# (Note: If v0.56.13 doesn't exist on DockerHub, double check your dev version. 
# It might be v0.46.13 or v0.50.13. Use the exact tag you see in your Dev 'About' screen).

WORKDIR /plugins

# Download Spark Driver
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.5.0/spark-sql_2.12-3.5.0.jar /plugins/spark-sql.jar

USER root
RUN chmod 644 /plugins/spark-sql.jar
USER 2000

ENV MB_PLUGINS_DIR=/plugins
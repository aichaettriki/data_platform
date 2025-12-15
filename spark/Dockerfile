FROM apache/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y python3 python3-pip curl wget

# Fix permissions
RUN mkdir -p /opt/spark/work && chown -R 1001:1001 /opt/spark

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Copy configs
COPY jobs/ /opt/spark/jobs/
COPY notebooks/ /opt/spark/notebooks/
COPY conf/ /opt/spark/conf/

# -------------------------------------------------------
# ADD JARS
# -------------------------------------------------------

# 1. Postgres Driver
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/

# 2. Hadoop AWS
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# 3. JMX Exporter
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar /opt/jmx_exporter/jmx_prometheus_javaagent.jar
COPY jmx_exporter/config.yaml /opt/jmx_exporter/config.yaml
RUN chmod -R 755 /opt/jmx_exporter

# -------------------------------------------------------
# 4. DOWNLOAD SPARK-ATLAS CONNECTOR (Pre-built by Cloudera)
# -------------------------------------------------------
RUN curl -fL -o /opt/spark/jars/spark-atlas-connector-assembly.jar \
    "https://repository.cloudera.com/artifactory/cloudera-repos/com/hortonworks/spark/spark-atlas-connector-assembly/3.5.1.1.23.17218.0-1/spark-atlas-connector-assembly-3.5.1.1.23.17218.0-1.jar"

# -------------------------------------------------------
# 5. ATLAS CLIENT DEPENDENCIES
# -------------------------------------------------------
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-client-v2/2.3.0/atlas-client-v2-2.3.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-common/2.3.0/atlas-common-2.3.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-notification/2.3.0/atlas-notification-2.3.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-intg/2.3.0/atlas-intg-2.3.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/sun/jersey/jersey-client/1.19/jersey-client-1.19.jar /opt/spark/jars/

# -------------------------------------------------------
# 6. FIX: COMMONS DEPENDENCIES
# -------------------------------------------------------
ADD https://repo1.maven.org/maven2/commons-configuration/commons-configuration/1.10/commons-configuration-1.10.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar /opt/spark/jars/

# -------------------------------------------------------
# 7. FIX: KAFKA CLIENT DEPENDENCIES (Missing Link)
# -------------------------------------------------------
# Spark 3.5 uses Kafka Clients 3.4.1
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
# Adding lz4 just in case (usually in Spark, but safer to have)
ADD https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar /opt/spark/jars/

# Final permissions
RUN chmod 644 /opt/spark/jars/*.jar

WORKDIR /opt/spark
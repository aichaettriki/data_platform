FROM apache/spark:3.5.1

USER root

# 1. Install System Dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# 2. Setup Work Directory and Permissions
RUN mkdir -p /opt/spark/work && \
    mkdir -p /opt/spark/conf && \
    mkdir -p /opt/spark/jars && \
    chown -R 1001:1001 /opt/spark

# 3. Install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# --- JAR SECTION ---
WORKDIR /opt/spark/jars

# A. OpenLineage (The Multi-Version Agent Bundle)
# This artifact 'openlineage-spark' is the fat-jar that works for Spark 3.5
RUN curl -fL -o openlineage-spark.jar \
    https://repo1.maven.org/maven2/io/openlineage/openlineage-spark/1.8.0/openlineage-spark-1.8.0.jar

# B. Database & Storage
RUN curl -fL -o postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -fL -o hadoop-aws.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o aws-sdk-bundle.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# C. Atlas Connector
RUN curl -fL -o spark-atlas-connector.jar \
    "https://repository.cloudera.com/artifactory/cloudera-repos/com/hortonworks/spark/spark-atlas-connector-assembly/3.5.1.1.23.17218.0-1/spark-atlas-connector-assembly-3.5.1.1.23.17218.0-1.jar"

# D. Dependencies for Atlas
RUN curl -fL -o kafka-clients.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fL -o commons-configuration.jar https://repo1.maven.org/maven2/commons-configuration/commons-configuration/1.10/commons-configuration-1.10.jar && \
    curl -fL -o jackson-module-scala.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/module/jackson-module-scala_2.12/2.15.2/jackson-module-scala_2.12-2.15.2.jar

# Final permissions
RUN chmod 644 /opt/spark/jars/*.jar
WORKDIR /opt/spark
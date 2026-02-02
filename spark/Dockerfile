FROM apache/spark:3.5.1

USER root

# 1. Install System Dependencies (Minimaliste pour la prod)
RUN apt-get update && apt-get install -y python3-pip curl procps && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Setup Work Directory
# Note : L'image apache/spark utilise l'UID 185 par défaut, pas 1001.
RUN mkdir -p /opt/spark/work /opt/spark/conf /opt/spark/jars && \
    chown -R 185:185 /opt/spark

# 3. Install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# --- JAR SECTION ---
WORKDIR /opt/spark/jars

# A. OpenLineage (The Multi-Version Agent Bundle)

RUN curl -fL -o openlineage-spark.jar \
    https://repo1.maven.org/maven2/io/openlineage/openlineage-spark/1.8.0/openlineage-spark-1.8.0.jar


# B. Database & Storage
RUN curl -fL -o postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -fL -o hadoop-aws.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o aws-sdk-bundle.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 /opt/spark/jars/*.jar

# Revenir à l'utilisateur Spark par défaut pour la sécurité
USER 185

WORKDIR /opt/spark
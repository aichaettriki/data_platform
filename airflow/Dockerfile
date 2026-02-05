FROM apache/airflow:2.7.3-python3.8

USER root

# Déclaration des ARGs pour que l'installation pip fonctionne
ARG AIRFLOW_VERSION=2.7.3
ARG PYTHON_VERSION=3.8

# 1. Installation de Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk wget curl && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Installation de Spark 3.5.1
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar xvf spark-3.5.1-bin-hadoop3.tgz -C /opt && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

# 3. Variables d'environnement
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

# 4. Ajout des Jars (URLs vérifiées)
WORKDIR /opt/spark/jars
RUN curl -fL -O https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -fL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    # Utilisation du JAR Multi-Version 1.8.0 (URL stable et confirmée)
    curl -fL -o openlineage-spark.jar https://repo1.maven.org/maven2/io/openlineage/openlineage-spark/1.8.0/openlineage-spark-1.8.0.jar && \
    chmod 644 /opt/spark/jars/*.jar

# 5. Retour à l'utilisateur Airflow
USER airflow
WORKDIR /opt/airflow

# 6. Installation des dépendances Python avec Contraintes
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
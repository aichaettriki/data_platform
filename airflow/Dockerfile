FROM apache/airflow:2.6.3-python3.9
USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl && \
    wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar xvf spark-3.5.1-bin-hadoop3.tgz -C /opt && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz && \
    apt-get clean

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:${PATH}"

# 2. Add Jars
RUN mkdir -p /opt/spark/jars

WORKDIR /opt/spark/jars
RUN curl -O https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fL -o spark-atlas-connector-assembly.jar "https://repository.cloudera.com/artifactory/cloudera-repos/com/hortonworks/spark/spark-atlas-connector-assembly/3.5.1.1.23.17218.0-1/spark-atlas-connector-assembly-3.5.1.1.23.17218.0-1.jar" && \
    curl -O https://repo1.maven.org/maven2/io/openlineage/openlineage-spark-3.5_2.12/1.15.0/openlineage-spark-3.5_2.12-1.15.0.jar

RUN chmod 644 /opt/spark/jars/*.jar
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
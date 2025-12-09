FROM apache/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y python3 python3-pip curl
# Fix permissions
RUN mkdir -p /opt/spark/work && chown -R 1001:1001 /opt/spark

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
COPY jobs/ /opt/spark/jobs/
COPY notebooks/ /opt/spark/notebooks/
COPY conf/ /opt/spark/conf/

# Add Hadoop S3 dependency for MinIO
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
# Add driver postgresql
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/

# Add JMX Prometheus Exporter for metrics
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar /opt/jmx_exporter/jmx_prometheus_javaagent.jar
COPY jmx_exporter/config.yaml /opt/jmx_exporter/config.yaml
RUN chmod -R 755 /opt/jmx_exporter

# ADD USER 1001
WORKDIR /opt/spark

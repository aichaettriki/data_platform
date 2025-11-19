FROM apache/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y python3 python3-pip
# Fix permissions
RUN mkdir -p /opt/spark/work && chown -R 1001:1001 /opt/spark

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
COPY jobs/ /opt/spark/jobs/
COPY notebooks/ /opt/spark/notebooks/
# Add Hadoop S3 dependency for MinIO
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
# Add driver postgresql
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/

# USER 1001
WORKDIR /opt/spark


FROM apache/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y python3 python3-pip
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
COPY jobs/ /opt/spark/jobs/
COPY notebooks/ /opt/spark/notebooks/
WORKDIR /opt/spark
USER 1001

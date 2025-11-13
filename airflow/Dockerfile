FROM apache/airflow:2.7.1-python3.11

COPY requirements.txt /opt/airflow/requirements.txt

# Installer Airflow + dépendances en utilisant le fichier de contraintes
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
COPY dags/ /opt/airflow/dags/
WORKDIR /opt/airflow    
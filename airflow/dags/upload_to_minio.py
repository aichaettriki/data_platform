import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio


LOCAL_FILE_PATH = "/data/raw/test.csv"


BUCKET_NAME = "raw"
OBJECT_NAME = "test.csv"


def upload_to_minio():
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    # Crée le bucket s'il n'existe pas
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    # Upload du fichier
    client.fput_object(BUCKET_NAME, OBJECT_NAME, LOCAL_FILE_PATH)
    print(f"Fichier {LOCAL_FILE_PATH} uploadé dans {BUCKET_NAME}/{OBJECT_NAME}")


with DAG(
    "upload_to_minio_dag",
    start_date=datetime(2025, 11, 11),
    schedule_interval=None,
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio
    )
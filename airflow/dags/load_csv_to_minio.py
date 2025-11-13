from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import os

# ---------- Config ----------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "raw"   # zone cible

LOCAL_FILE_PATH = "/opt/airflow/data/clients.csv"  # fichier sur le conteneur
MINIO_FILE_PATH = "clients.csv"  # nom du fichier dans MinIO


def upload_to_minio():
    # connexion à MinIO
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # créer le bucket s’il n’existe pas encore
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    # upload du fichier
    client.fput_object(
        bucket_name=BUCKET,
        object_name=MINIO_FILE_PATH,
        file_path=LOCAL_FILE_PATH,
    )
    print(f"✅ Fichier {LOCAL_FILE_PATH} chargé dans bucket '{BUCKET}'.")


# ---------- DAG ----------
with DAG(
    dag_id="load_csv_to_minio",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["minio", "csv", "etl"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_csv_to_minio",
        python_callable=upload_to_minio,
    )

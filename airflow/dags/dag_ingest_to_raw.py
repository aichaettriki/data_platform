from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from minio import Minio
from dotenv import load_dotenv
from common.dag_helpers import RAW_BUCKET
from common.minio_utils import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_PASSWORD,
    )

LOCAL_INPUT_DIR = "/opt/airflow/data"

def upload_files_to_raw(**context):
    execution_date = context["ds"]  # yyyy-mm-dd
    year, month, day = execution_date.split("-")

    # On nettoie le nom du bucket pour enlever s3a:// ou s3://
    bucket_name = RAW_BUCKET.replace("s3a://", "").replace("s3://", "")

    client = Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

    # Vérifier si le bucket existe, sinon le créer (optionnel mais recommandé)
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Parcourir récursivement tous les fichiers dans LOCAL_INPUT_DIR
    for root, dirs, files in os.walk(LOCAL_INPUT_DIR):
        for filename in files:
            local_path = os.path.join(root, filename)
            
            # Calculer le chemin relatif par rapport à LOCAL_INPUT_DIR
            relative_path = os.path.relpath(local_path, LOCAL_INPUT_DIR)
            
            # Construire le chemin dans MinIO avec date de chargement 
            object_path = f"{year}/{month}/{relative_path}"

            client.fput_object(
                bucket_name=bucket_name,  # Utilisation du nom nettoyé
                object_name=object_path,
                file_path=local_path
            )

            print(f"✅ Uploaded {relative_path} → s3a://{bucket_name}/{object_path}")

with DAG(
    dag_id="Ingest_to_Raw",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["raw", "INS"],
) as dag:

    upload_to_raw = PythonOperator(
        task_id="upload_local_files_to_raw",
        python_callable=upload_files_to_raw,
        provide_context=True,
    )
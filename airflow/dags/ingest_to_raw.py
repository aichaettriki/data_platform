from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from minio import Minio
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

# 🔧 Config
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER", required=True)
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD", required=True)

LOCAL_INPUT_DIR = "/opt/airflow/data"
RAW_BUCKET = "01-raw"
SOURCE = "INS"

def upload_files_to_raw(**context):
    execution_date = context["ds"]  # yyyy-mm-dd
    year, month, day = execution_date.split("-")

    client = Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Parcourir récursivement tous les fichiers dans LOCAL_INPUT_DIR
    for root, dirs, files in os.walk(LOCAL_INPUT_DIR):
        for filename in files:
            local_path = os.path.join(root, filename)
            
            # Calculer le chemin relatif par rapport à LOCAL_INPUT_DIR
            relative_path = os.path.relpath(local_path, LOCAL_INPUT_DIR)
            
            # Construire le chemin dans MinIO avec date de chargement 
            object_path = f"{year}/{month}/{relative_path}"

            client.fput_object(
                bucket_name=RAW_BUCKET,
                object_name=object_path,
                file_path=local_path
            )

            print(f"✅ Uploaded {relative_path} → s3a://{RAW_BUCKET}/{object_path}")


with DAG(
    dag_id="ingest_ins_raw",
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
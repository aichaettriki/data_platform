
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd
import requests
import io
import os
from loguru import logger

# Pour charger les variables d'environnement du .env
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

# ------------------------------
# ⚙️ Variables de configuration
# ------------------------------
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER")
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD")
API_URL = get_env_var("API_URL")
BUCKET_NAME = "raw"


def fetch_api_and_upload():
    """
    1️⃣ Récupère les données depuis une API publique
    2️⃣ Convertit en CSV
    3️⃣ Upload dans MinIO (bucket raw)
    """
    try:
        logger.info(f"Fetching data from API: {API_URL}")
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        logger.info(f"Fetched {len(df)} records from API")

        # Convertir en CSV (en mémoire)
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        csv_stream = io.BytesIO(csv_bytes)

        # Connexion à MinIO
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        # Vérifie ou crée le bucket
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            logger.info(f"Created bucket: {BUCKET_NAME}")

        # Nom du fichier (avec timestamp)
        file_name = f"api_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Upload
        client.put_object(
            BUCKET_NAME,
            file_name,
            csv_stream,
            length=len(csv_bytes),
            content_type="text/csv",
        )

        logger.info(f"✅ Uploaded file to MinIO: {BUCKET_NAME}/{file_name}")
    except Exception as e:
        logger.error(f"❌ Error in fetch_api_and_upload: {e}")
        raise


# ------------------------------
# 🗓️ DAG Definition
# ------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}


with DAG(
    dag_id="ingest_api_to_minio",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ingestion", "minio", "api"],
) as dag:
    extract_task = PythonOperator(
        task_id="fetch_api_and_upload",
        python_callable=fetch_api_and_upload,
    )

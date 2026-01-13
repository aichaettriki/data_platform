
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER")
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD")
MINIO_HOST = get_env_var("MINIO_HOST")
MINIO_API_PORT = get_env_var("MINIO_API_PORT")

BUCKET_RAW = "01-raw"
LOCAL_FILE_PATH = "/opt/airflow/data/equipe2.csv"
RAW_OBJECT = "equipe1.csv"

def get_minio_client():
    return Minio(
        endpoint=f"{MINIO_HOST}:{MINIO_API_PORT}", # juste host:port, sans http://
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

def upload_to_raw():
    try:
        client = get_minio_client()
        if not client.bucket_exists(BUCKET_RAW):
            client.make_bucket(BUCKET_RAW)
        client.fput_object(BUCKET_RAW, RAW_OBJECT, LOCAL_FILE_PATH)
        print("📌 Upload RAW terminé.")
    except Exception as e:
        print(f"❌ Erreur upload_to_raw: {e}")
        raise

with DAG(
    dag_id="etl_csv_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task_upload_raw = PythonOperator(
        task_id="upload_to_raw",
        python_callable=upload_to_raw,
    )
    task_trigger_spark = TriggerDagRunOperator(
        task_id="trigger_spark_pipeline",
        trigger_dag_id="etl_csv_spark_pipeline_v2",  # DAG à déclencher
        wait_for_completion=True,
        poke_interval=10,
    )
    task_upload_raw >> task_trigger_spark

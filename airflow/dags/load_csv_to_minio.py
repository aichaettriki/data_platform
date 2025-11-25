from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from minio import Minio
import os
 
# ---------- CONFIG ----------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
 
BUCKET_RAW = "raw"
LOCAL_FILE_PATH = "/opt/airflow/data/equipe.csv"
RAW_OBJECT = "equipe1.csv"
 
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
 
def upload_to_raw():
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)
    client.fput_object(BUCKET_RAW, RAW_OBJECT, LOCAL_FILE_PATH)
    print("📌 Upload RAW terminé.")
 
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
        trigger_dag_id="etl_csv_spark_pipeline_v2",
    )
 
    task_upload_raw >> task_trigger_spark
 
 
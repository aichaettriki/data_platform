"""
DAG Airflow - Refined Layer Builder (Spark + MinIO)
 
Orchestration du job :
refined_job.py <year> <month> <day>
 
Étapes :
1. Lancement du Spark job Refined
2. Monitoring
"""
 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from dotenv import load_dotenv
 
# =========================================================
# Chargement des variables d’environnement
# =========================================================
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))
 
def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
 
# ===============================
# Configuration générale
# ===============================
 
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
 
# ===============================
# Variables d'environnement
# ===============================
 
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_ROOT_USER = get_env_var("MINIO_ROOT_USER", required=True)
MINIO_PASSWORD = get_env_var("MINIO_ROOT_PASSWORD", required=True)
 
SPARK_APP_PATH = "/opt/spark/jobs/refined_job.py"
 
# ===============================
# Définition du DAG
# ===============================
 
with DAG(
    dag_id="03-REF__transformed_to_refined_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "minio", "refined", "medallion"],
) as dag:
 
    # Paramètres dynamiques depuis Airflow
    year = "{{ ds.split('-')[0] }}"
    month = "{{ ds.split('-')[1] }}"
    day = "{{ ds.split('-')[2] }}"
 
    # ===============================
    # Task : Spark Submit Refined
    # ===============================
 
    run_refined_job = SparkSubmitOperator(
        task_id="run_refined_spark_job",
        application=SPARK_APP_PATH,
        name="refined_layer_builder",
        conn_id="spark_standalone",
        verbose=True,
        application_args=[year, month, day],
        conf={
            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g",
            "spark.sql.shuffle.partitions": "200",
        },
        env_vars={
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "MINIO_ROOT_USER": MINIO_ROOT_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
        },
    )
 
    run_refined_job
 
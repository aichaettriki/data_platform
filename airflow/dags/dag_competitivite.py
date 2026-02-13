"""
DAG Airflow - Competitive Scores Processing (Spark + MinIO)

Orchestration du job :
spark_competitif_processor.py <year> <month> <day>

Étapes :
1. Lancement du Spark job
2. Monitoring
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from minio import Minio
from datetime import datetime, timezone, timedelta
from minio.deleteobjects import DeleteObject
import logging
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

# Variables Airflow (optionnel)
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_ROOT_USER = get_env_var("MINIO_ROOT_USER", required=True)
MINIO_PASSWORD = get_env_var("MINIO_ROOT_PASSWORD", required=True)

SPARK_APP_PATH = "/opt/spark/jobs/competitivite_processing.py"


# ===============================
# Définition du DAG
# ===============================

with DAG(
    dag_id="Transform_Competitivite",
    default_args=default_args,
    schedule_interval=None,   # ou None pour manuel
    catchup=False,
    max_active_runs=1,
    tags=["spark", "minio", "itceq"],
) as dag:

    # Paramètres dynamiques (date Airflow)
    year = "{{ ds.split('-')[0] }}"
    month = "{{ ds.split('-')[1] }}"
    day = "{{ ds.split('-')[2] }}"

    # ===============================
    # Task : Spark Submit
    # ===============================
    run_spark_job = SparkSubmitOperator(
    task_id="run_competitif_scores_spark_job",
    application=SPARK_APP_PATH,
    name="competitif_scores_processor",
    conn_id="spark_standalone",
    verbose=True,
    application_args=[year, month, day],
    conf={
        "spark.dynamicAllocation.enabled": "false",
        "spark.sql.shuffle.partitions": "4",
    },
    env_vars={
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
    },
    retries=2,  # retry si erreur temporaire
    retry_delay=timedelta(minutes=5),
    do_xcom_push=False,  # évite surcharge XCom en prod
    
)
    
    trigger_cleanup = TriggerDagRunOperator(
    task_id='trigger_cleanup_minio',
    trigger_dag_id='Cleanup_Minio',  # ton DAG de nettoyage
    wait_for_completion=True,               # True si tu veux attendre la fin
)
    run_spark_job >> trigger_cleanup
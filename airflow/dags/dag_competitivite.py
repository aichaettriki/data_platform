"""
DAG Airflow — Competitive Scores Processing (Spark + MinIO)
Orchestrates: competitivite_processing.py --year X --month Y --day Z
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.dag_helpers import create_zip_task, create_cleanup_task, SPARK_COMMON_ZIP, make_spark_conf, RAW_BUCKET
from common.minio_utils import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_PASSWORD,
    )


SPARK_APP_PATH = "/opt/spark/jobs/competitivite_processing.py"

# ──────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ──────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "start_date":        datetime(2024, 1, 1),
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}



# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="Transform_Competitivite",
    default_args=default_args,
    description="ETL Spark — scores de compétitivité ITCEQ",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["ITCEQ", "competitivite", "Spark", "ETL"],
) as dag:
    zip_common = create_zip_task(dag)
    # ── Spark job ─────────────────────────────────────────────────────────────
    run_spark_job = SparkSubmitOperator(
        task_id="run_competitif_scores_spark_job",
        application=SPARK_APP_PATH,
        conn_id="spark_standalone",
        name="competitif_scores_processor",
        verbose=True,
        do_xcom_push=False,
        application_args=[
            "--year",  "{{ ds.split('-')[0] }}",
            "--month", "{{ ds.split('-')[1] }}",
            "--day",   "{{ ds.split('-')[2] }}",
        ],

        env_vars={
            "MINIO_ENDPOINT":      MINIO_ENDPOINT,
            "MINIO_ROOT_USER":     MINIO_ROOT_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
        },

        conf=make_spark_conf(),  
        py_files=SPARK_COMMON_ZIP,
    )

    cleanup = create_cleanup_task(dag, source_bucket=RAW_BUCKET, triggered_by="Transform_Competitivite")

    zip_common >> run_spark_job >> cleanup
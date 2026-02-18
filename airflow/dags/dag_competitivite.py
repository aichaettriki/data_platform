"""
DAG Airflow — Competitive Scores Processing (Spark + MinIO)
Orchestrates: competitivite_processing.py --year X --month Y --day Z
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ──────────────────────────────────────────────────────────────────────────────
# ENV VARS  — injectées par Docker Compose / Airflow Connections
# Pas de dotenv en production (le scheduler Airflow ne charge pas .env)
# ──────────────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Required env var missing: {name}")
    return value


MINIO_ENDPOINT = _require_env("MINIO_ENDPOINT")
MINIO_USER     = _require_env("MINIO_ROOT_USER")
MINIO_PASSWORD = _require_env("MINIO_ROOT_PASSWORD")

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
# SPARK CONF
# ──────────────────────────────────────────────────────────────────────────────
SPARK_CONF = {
    "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access":      "true",
    "spark.hadoop.fs.s3a.endpoint":               MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":             MINIO_USER,
    "spark.hadoop.fs.s3a.secret.key":             MINIO_PASSWORD,
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.sources.partitionOverwriteMode":   "dynamic",
    "spark.sql.adaptive.enabled":                 "true",
    "spark.dynamicAllocation.enabled":            "false",
    "spark.sql.shuffle.partitions":               "4",
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

    # ── Spark job ─────────────────────────────────────────────────────────────
    run_spark_job = SparkSubmitOperator(
        task_id="run_competitif_scores_spark_job",
        application=SPARK_APP_PATH,
        conn_id="spark_standalone",
        name="competitif_scores_processor",
        verbose=True,
        do_xcom_push=False,

        # ── FIX PRINCIPAL : flags nommés pour argparse ────────────────────────
        # Avant : application_args=[year, month, day]   → args positionnels
        #         → erreur "the following arguments are required: --year …"
        # Après : flags explicites alignés sur argparse du job Python
        application_args=[
            "--year",  "{{ ds.split('-')[0] }}",
            "--month", "{{ ds.split('-')[1] }}",
            "--day",   "{{ ds.split('-')[2] }}",
        ],

        env_vars={
            "MINIO_ENDPOINT":      MINIO_ENDPOINT,
            "MINIO_ROOT_USER":     MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
        },

        conf=SPARK_CONF,
    )

    # ── Cleanup trigger ───────────────────────────────────────────────────────
    trigger_cleanup = TriggerDagRunOperator(
        task_id="trigger_cleanup_minio",
        trigger_dag_id="Cleanup_Minio",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        conf={"triggered_by": "Transform_Competitivite"},
    )

    run_spark_job >> trigger_cleanup
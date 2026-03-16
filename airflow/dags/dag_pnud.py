"""
DAG : 02-TRANS_PNUD
Orchestration du job Spark de transformation HDR UNDP.
Lit depuis 01-raw, écrit dans 02-transformed (partitionné par year).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from typing import List, Optional

from common.dag_helpers import (
    create_zip_task,
    create_cleanup_task,
    SPARK_COMMON_ZIP,
    make_spark_conf,
    RAW_BUCKET,
    TRANSFORMED_BUCKET,
)
from common.minio_utils import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_PASSWORD,
)

# ─── Constantes ────────────────────────────────────────────────────────────────
SPARK_JOB_PATH = "/opt/spark/jobs/etl_pnud.py"

# ─── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="02-TRANS_PNUD",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["transformed", "PNUD", "hdr", "spark"],
) as dag:

    # ── Tâche 1 : zip du module common ───────────────────────────────────────
    zip_common = create_zip_task(dag)

    # ── Tâche 2 : job Spark ───────────────────────────────────────────────────
    spark_transform = SparkSubmitOperator(
        task_id="spark_hdr_transform",
        application=SPARK_JOB_PATH,
        conn_id="spark_standalone",
        verbose=True,
        do_xcom_push=False,
        # Les args suivent la convention --year/--month/--day du projet
        application_args=[
            "--year",  "{{ ds.split('-')[0] }}",
            "--month", "{{ ds.split('-')[1] }}",
            "--day",   "{{ ds.split('-')[2] }}",
        ],
        # Credentials MinIO passés en variables d'environnement
        env_vars={
            "MINIO_ENDPOINT":      MINIO_ENDPOINT,
            "MINIO_ROOT_USER":     MINIO_ROOT_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
        },
        conf=make_spark_conf(),
        py_files=SPARK_COMMON_ZIP,
    )

    # ── Tâche 3 : nettoyage du zip ────────────────────────────────────────────
    cleanup = create_cleanup_task(
    dag,
    source_bucket="02-transformed",   # 👈 pas RAW_BUCKET
    triggered_by="Transform_PNUD",
    target_folder="PNUD/hdi",     # 👈 dossier dans ce bucket
)
    zip_common >> spark_transform >> cleanup
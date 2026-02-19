from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.dag_helpers import create_zip_task, create_cleanup_task, SPARK_COMMON_ZIP, make_spark_conf



RAW_BUCKET = os.getenv("RAW_BUCKET", "s3a://01-raw")
TRANSFORMED_BUCKET = os.getenv("TRANSFORMED_BUCKET", "s3a://02-transformed")

SPARK_SCRIPT_PATH = "/opt/spark/jobs/tre_processing.py"
SPARK_PY_FILES = "/opt/spark/common.zip"

# ──────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ──────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner":             "data_team",
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
    dag_id="Transform_TRE",
    default_args=default_args,
    description="ETL Spark pour les fichiers économiques INS/TRE",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["INS", "TRE", "Spark", "ETL"],
) as dag:
    zip_common = create_zip_task(dag)
    transform_tre = SparkSubmitOperator(
        task_id="transform_excel_to_parquet",
        application=SPARK_SCRIPT_PATH,
        conn_id="spark_standalone",
        name="ins_tre_etl",
        conf=make_spark_conf(),  
        py_files=SPARK_COMMON_ZIP,

        application_args=[
            "--raw-bucket",    RAW_BUCKET,
            "--target-bucket", TRANSFORMED_BUCKET,
            "--target-folder", "INS/TRE",
        ],
        verbose=True,
    )

    cleanup = create_cleanup_task(dag, source_bucket=RAW_BUCKET, triggered_by="Transform_TRE")

    zip_common >> transform_tre >> cleanup
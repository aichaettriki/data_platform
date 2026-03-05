from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from common.dag_helpers import create_zip_task, create_cleanup_task, SPARK_COMMON_ZIP, make_spark_conf


# ==========================================================
# ENV VARIABLES
# ==========================================================

RAW_BUCKET = os.getenv("RAW_BUCKET", "s3a://01-raw")
TRANSFORMED_BUCKET = os.getenv("TRANSFORMED_BUCKET", "s3a://02-transformed")

SPARK_SCRIPT_PATH = "/opt/spark/jobs/tre_processing.py"


# ==========================================================
# DEFAULT ARGS
# ==========================================================

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ==========================================================
# DAG
# ==========================================================

with DAG(
    dag_id="Transform_TRE",
    default_args=default_args,
    description="ETL Spark TRE INS",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["INS", "TRE", "Spark", "ETL"],
) as dag:

    # ZIP common Python utilities
    zip_common = create_zip_task(dag)

    # ------------------------------------------------------
    # Spark ETL Task
    # ------------------------------------------------------

    transform_tre = SparkSubmitOperator(
        task_id="transform_excel_to_parquet",
        application=SPARK_SCRIPT_PATH,
        conn_id="spark_standalone",
        name="ins_tre_etl",

        # Global infra config (MinIO + Spark tuning)
        conf=make_spark_conf(),

        # Shared Python utilities
        py_files=SPARK_COMMON_ZIP,

        application_args=[
            "--raw-bucket", RAW_BUCKET,
            "--target-bucket", TRANSFORMED_BUCKET,
            "--target-folder", "INS/TRE",
        ],

        verbose=True,
    )

    # ------------------------------------------------------
    # Cleanup Task
    # ------------------------------------------------------

    cleanup = create_cleanup_task(
        dag,
        source_bucket=RAW_BUCKET,
        triggered_by="Transform_TRE"
    )

    # ======================================================
    # Pipeline Order
    # ======================================================

    zip_common >> transform_tre >> cleanup
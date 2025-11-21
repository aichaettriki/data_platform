from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minio_csv_processing",
    default_args=default_args,
    description="A DAG to run PySpark job with MinIO",
    start_date=datetime(2025, 11, 21),
    schedule_interval=None,  # Set cron here if needed
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="run_minio_csv_job",
        application="/opt/spark/jobs/test_gen.py",
        name="minio_csv_processing",
        conn_id="spark_default",  # Airflow Spark connection
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        execution_timeout=timedelta(minutes=60),
        # conf={"spark.master": "local[*]"},
    )

    spark_job

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark-marquez",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1️⃣ Task: Ingest CSV from MinIO
    task_read_raw = SparkSubmitOperator(
        task_id="read_raw_csv",
        application="/opt/spark/jobs/etl_job.py",
        name="read_raw_csv",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["ingest"]
    )

    # 2️⃣ Task: Clean and deduplicate
    task_clean_transformed = SparkSubmitOperator(
        task_id="clean_and_transformed",
        application="/opt/spark/jobs/etl_job.py",
        name="clean_and_transformed",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["clean"]
    )

    # 3️⃣ Task: Refine (add fullname and timestamp)
    task_add_timestamp = SparkSubmitOperator(
        task_id="add_timestamp_refined",
        application="/opt/spark/jobs/etl_job.py",
        name="add_timestamp_refined",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["refine"]
    )

    # Définir l’ordre d’exécution
    task_read_raw >> task_clean_transformed >> task_add_timestamp

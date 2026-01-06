from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="ins_api_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # exécution manuelle, tu peux changer si nécessaire
    catchup=False,
    tags=["ins", "spark", "minio"]
) as dag:

    fetch_ins_task = SparkSubmitOperator(
        task_id="fetch_ins_and_store_raw",
        application="/opt/spark/jobs/etl_ins_api.py",
        name="fetch_ins_and_store_raw",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
    )

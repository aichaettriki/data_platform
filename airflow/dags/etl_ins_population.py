from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="etl_ins_dimension_regions",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    load_regions_dimension = SparkSubmitOperator(
        task_id="load_regions_dimension",
        application="/opt/spark/jobs/etl_dimension_regions.py",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        name="etl_dimension_regions"
    )

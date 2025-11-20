from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="etl_csv_spark_pipeline_v2",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1️⃣ read csv
    task_read_raw = SparkSubmitOperator(
        task_id="read_raw_csv",
        application="/opt/spark/jobs/etl_equipes.py",
        name="read_raw_csv",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["read_raw_csv"]
    )

    # 2️⃣ clean and transform
    task_clean_transformed = SparkSubmitOperator(
        task_id="clean_and_transformed",
        application="/opt/spark/jobs/etl_equipes.py",
        name="clean_and_transformed",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["clean_and_transformed"]
    )
    print("----------------> TRANSFORMED OK.")

    # 3️⃣ add timestamp refined
    task_add_timestamp = SparkSubmitOperator(
        task_id="add_timestamp_refined",
        application="/opt/spark/jobs/etl_equipes.py",
        name="add_timestamp_refined",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["add_timestamp_refined"]
    )
    print("----------------> REFINED OK.")
    # 4️⃣ save to postgresql
    task_write_postgres = SparkSubmitOperator(
        task_id="write_postgres",
        application="/opt/spark/jobs/etl_equipes.py",
        name="write_postgres",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3",
        application_args=["write_postgres"]
    )

    task_read_raw >> task_clean_transformed >> task_add_timestamp >> task_write_postgres

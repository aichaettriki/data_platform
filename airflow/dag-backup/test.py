
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

SPARK_CONN_ID = get_env_var("SPARK_CONN_ID")
SPARK_JOB_PATH = get_env_var("SPARK_JOB_PATH_EQUIPE")
SPARK_PACKAGES = get_env_var("SPARK_PACKAGES")
SPARK_PG_PACKAGE = get_env_var("SPARK_PG_PACKAGE")

with DAG(
    dag_id="etl_csv_spark_pipeline_v2",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # 1️⃣ read csv
    task_read_raw = SparkSubmitOperator(
        task_id="read_raw_csv",
        application=SPARK_JOB_PATH,
        name="read_raw_csv",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        application_args=["read_raw_csv"]
    )
    # 2️⃣ clean and transform
    task_clean_transformed = SparkSubmitOperator(
        task_id="clean_and_transformed",
        application=SPARK_JOB_PATH,
        name="clean_and_transformed",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        application_args=["clean_and_transformed"]
    )
    # 3️⃣ add timestamp refined
    task_add_timestamp = SparkSubmitOperator(
        task_id="add_timestamp_refined",
        application=SPARK_JOB_PATH,
        name="add_timestamp_refined",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        application_args=["add_timestamp_refined"]
    )
    # 4️⃣ save to postgresql
    task_write_postgres = SparkSubmitOperator(
        task_id="write_postgres",
        application=SPARK_JOB_PATH,
        name="write_postgres",
        conn_id=SPARK_CONN_ID,
        packages=f"{SPARK_PACKAGES},{SPARK_PG_PACKAGE}",
        application_args=["write_postgres"]
    )
    task_read_raw >> task_clean_transformed >> task_add_timestamp >> task_write_postgres

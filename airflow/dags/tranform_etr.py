from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))


def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

# Variables MinIO et job
MINIO_ENDPOINT = "minio:9000"  
MINIO_USER = get_env_var("MINIO_ROOT_USER", required=True)
MINIO_PASSWORD = get_env_var("MINIO_ROOT_PASSWORD", required=True)


# Define Buckets
RAW_BUCKET = "s3a://01-raw"
TRANSFORMED_BUCKET = "s3a://transformed"

# Path to the Spark script
SPARK_SCRIPT_PATH = "/opt/spark/jobs/etl_tre.py"

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    dag_id='ins_tre_etl_pipeline1',
    default_args=default_args,
    description='ETL for INS TRE Economic Files',
    schedule_interval='@daily',
    catchup=False,
    tags=['INS', 'TRE', 'Spark']
) as dag:

    # Task: Submit Spark Job
    transform_tre_task = SparkSubmitOperator(
        task_id='transform_excel_to_clean',
        application=SPARK_SCRIPT_PATH,
        conn_id='spark_standalone', # Ensure this connection exists in Airflow
        name='ins_tre_job',
        
        # IMPORTANT: Add the Excel Jar dependency
        packages='com.crealytics:spark-excel_2.12:3.3.1_0.18.7',
        
        # Pass the action name as argument
        application_args=['transform_tre'],
        
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        verbose=True
    )
    trigger_cleanup = TriggerDagRunOperator(
    task_id='trigger_cleanup_minio',
    trigger_dag_id='cleanup_minio_folders',  # ton DAG de nettoyage
    wait_for_completion=True,               # True si tu veux attendre la fin
)

    transform_tre_task >> trigger_cleanup
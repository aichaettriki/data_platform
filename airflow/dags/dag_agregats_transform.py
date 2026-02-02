from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
from dotenv import load_dotenv

# =========================================================
# Chargement des variables d’environnement
# =========================================================
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_USER = get_env_var("MINIO_ROOT_USER", required=True)
MINIO_PASSWORD = get_env_var("MINIO_ROOT_PASSWORD", required=True)

JOB_PATH = "/opt/spark/jobs/transform_aggregats_job.py"

# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="transform_aggregats_spark",
    start_date=datetime(2026, 1, 27),
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "minio", "parquet"],
) as dag:

    transform_spark = SparkSubmitOperator(
        task_id="transform_aggregats",
        application=JOB_PATH,
        conn_id="spark_standalone",
        verbose=True,
        packages="com.crealytics:spark-excel_2.12:3.5.1_0.20.4",
        conf={
            # =============================
            # MinIO / S3A
            # =============================
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

            "spark.hadoop.fs.s3a.access.key": MINIO_USER,
            "spark.hadoop.fs.s3a.secret.key": MINIO_PASSWORD,
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

            # =============================
            # Committer STABLE (OBLIGATOIRE)
            # =============================
            "spark.sql.sources.commitProtocolClass":
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "1",

            # =============================
            # Optimisation
            # =============================
            "spark.sql.shuffle.partitions": "8",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        },
    )

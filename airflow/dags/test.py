# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from minio import Minio
 
# MINIO_ENDPOINT = "minio:9000"
# MINIO_ACCESS_KEY = "minio"
# MINIO_SECRET_KEY = "minio123"
 
# BUCKET_RAW = "raw"
# LOCAL_FILE_PATH = "/opt/airflow/data/equipe.csv"
# RAW_OBJECT = "equipe1.csv"
 
# def get_minio_client():
#     return Minio(
#         MINIO_ENDPOINT,
#         access_key=MINIO_ACCESS_KEY,
#         secret_key=MINIO_SECRET_KEY,
#         secure=False,
#     )
 
# def upload_to_raw():
#     client = get_minio_client()
#     if not client.bucket_exists(BUCKET_RAW):
#         client.make_bucket(BUCKET_RAW)
#     client.fput_object(BUCKET_RAW, RAW_OBJECT, LOCAL_FILE_PATH)
#     print("📌 Upload RAW terminé.")
 
# with DAG(
#     dag_id="test_test",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
 
#     task_upload_raw = PythonOperator(
#         task_id="upload_to_raw",
#         python_callable=upload_to_raw,
#     )
 
#     task_upload_raw 


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
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["read_raw_csv"]
    )

    # 2️⃣ clean and transform
    task_clean_transformed = SparkSubmitOperator(
        task_id="clean_and_transformed",
        application="/opt/spark/jobs/etl_equipes.py",
        name="clean_and_transformed",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["clean_and_transformed"]
    )
    print("----------------> TRANSFORMED OK.")

    # 3️⃣ add timestamp refined
    task_add_timestamp = SparkSubmitOperator(
        task_id="add_timestamp_refined",
        application="/opt/spark/jobs/etl_equipes.py",
        name="add_timestamp_refined",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=["add_timestamp_refined"]
    )
    print("----------------> REFINED OK.")
    # 4️⃣ save to postgresql
    task_write_postgres = SparkSubmitOperator(
        task_id="write_postgres",
        application="/opt/spark/jobs/etl_equipes.py",
        name="write_postgres",
        conn_id="spark_standalone",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3",
        application_args=["write_postgres"]
    )

    task_read_raw >> task_clean_transformed >> task_add_timestamp >> task_write_postgres
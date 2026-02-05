from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="transform_agregat_ins_api",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["INS", "SPARK", "Agregat"],
) as dag:

    enrich_ins = SparkSubmitOperator(
        task_id="spark_ins_enrich",
        application="/opt/spark/jobs/etl_agregat.py",
        conn_id="spark_standalone",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        },
    )

    enrich_ins

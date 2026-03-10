from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from common.dag_helpers import make_spark_conf, create_zip_task, create_cleanup_task, SPARK_COMMON_ZIP, RAW_BUCKET
 
with DAG(
    dag_id="02-TRANS__worldbank_countries",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["WORLD_BANK", "SPARK", "Countries"],
) as dag:
 
    zip_common = create_zip_task(dag)
 
    transform_countries = SparkSubmitOperator(
        task_id="spark_worldbank_countries",
        application="/opt/spark/jobs/etl_worldbank_countries.py",
        conn_id="spark_standalone",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf=make_spark_conf(),
        py_files=SPARK_COMMON_ZIP,
        application_args=[
            "--raw-bucket",    RAW_BUCKET,
            "--target-bucket", "s3a://02-transformed",
            "--target-folder", "WORLD_BANK/Countries",
        ],
    )
 
    cleanup = create_cleanup_task(dag, source_bucket=RAW_BUCKET, triggered_by="transform_worldbank_countries")
 
    zip_common >> transform_countries >> cleanup
 
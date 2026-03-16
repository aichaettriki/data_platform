from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from common.dag_helpers import make_spark_conf, create_zip_task, create_cleanup_task, SPARK_COMMON_ZIP, RAW_BUCKET

with DAG(
    dag_id="02-TRANS__world_bank_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["WORLD_BANK", "SPARK"],
) as dag:

    zip_common = create_zip_task(dag)

    transform_world_bank = SparkSubmitOperator(
        task_id="spark_world_bank_transform",
        application="/opt/spark/jobs/transform_WORLD_BANK_api.py",
        conn_id="spark_standalone",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf=make_spark_conf(),
        py_files=SPARK_COMMON_ZIP,
    )

    cleanup = create_cleanup_task(
    dag,
    source_bucket="02-transformed",   # 👈 pas RAW_BUCKET
    triggered_by="transform_world_bank_api",
    target_folder="WORLD_BANK",     # 👈 dossier dans ce bucket
)
    zip_common >> transform_world_bank >> cleanup
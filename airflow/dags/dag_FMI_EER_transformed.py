from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from common.dag_helpers import make_spark_conf, create_zip_task, SPARK_COMMON_ZIP
from common.dag_helpers import create_zip_task, create_cleanup_task, SPARK_COMMON_ZIP, make_spark_conf, RAW_BUCKET
 
with DAG(
    dag_id="02-TRANS__imf_data",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["IMF","SPARK"],
) as dag:
 
    zip_common = create_zip_task(dag)
 
    transform_imf = SparkSubmitOperator(
        task_id="spark_imf_transform",
        application="/opt/spark/jobs/etl_IMF_EER.py",
        conn_id="spark_standalone",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf=make_spark_conf(),
        py_files=SPARK_COMMON_ZIP,
    )

    cleanup = create_cleanup_task(
    dag,
    source_bucket="02-transformed",   # 👈 pas RAW_BUCKET
    triggered_by="spark_imf_transform",
    target_folder="FMI",     # 👈 dossier dans ce bucket
)
    zip_common >> transform_imf >> cleanup
 
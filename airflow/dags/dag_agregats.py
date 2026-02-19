from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from common.dag_helpers import create_zip_task, create_cleanup_task, RAW_BUCKET    
from common.dag_helpers import make_spark_conf


JOB_PATH = "/opt/spark/jobs/agregats_processing.py"

# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="Transform_Agregats",
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=["spark", "minio", "parquet"],
) as dag:
    zip_common = create_zip_task(dag)
    transform_spark = SparkSubmitOperator(
        task_id="transform_aggregats",
        application=JOB_PATH,
        conn_id="spark_standalone",
        verbose=True,
        packages="com.crealytics:spark-excel_2.12:3.5.1_0.20.4",
        conf=make_spark_conf(),
    )
    cleanup = create_cleanup_task(dag, source_bucket=RAW_BUCKET, triggered_by="Transform_Agregats")

    zip_common >> transform_spark >> cleanup

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from common.dag_helpers import make_spark_conf,create_cleanup_task, RAW_BUCKET
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
        conf = make_spark_conf(),
    )
    cleanup = create_cleanup_task(dag, source_bucket=RAW_BUCKET, triggered_by="transform_agregat_ins_api")
    enrich_ins >> cleanup 


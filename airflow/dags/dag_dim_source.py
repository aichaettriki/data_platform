from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from common.dag_helpers import create_zip_task, create_cleanup_task, SPARK_COMMON_ZIP, make_spark_conf
 
with DAG(dag_id="03-REFINED__DIM_SOURCE", start_date=datetime(2024, 1, 1), schedule_interval=None, tags=["REFINED"]) as dag:
    zip_common = create_zip_task(dag)
    run_job = SparkSubmitOperator(
        task_id="process_dim_source",
        application="/opt/spark/jobs/dim_source.py",
        conn_id="spark_standalone",
        conf=make_spark_conf(),
        py_files=SPARK_COMMON_ZIP
    )
    cleanup = create_cleanup_task(dag, "03-refined", "Dim_Source", "dimensions/dim_source")
    zip_common >> run_job >> cleanup
 
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

OPENLINEAGE_URL = "http://openmetadata-server:8585/api/v1/lineage"

spark_lineage_conf = {
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": OPENLINEAGE_URL,
    "spark.openlineage.namespace": "itceq_spark_jobs",
    "spark.openlineage.parentJobName": "{{ dag.dag_id }}",
    "spark.openlineage.parentRunId": "{{ run_id }}"
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="spark_openlineage_demo",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_task = SparkSubmitOperator(
        task_id="spark_lineage_demo",
        application="/opt/spark/jobs/etl_equipes.py",  # Exemple de job Spark
        name="spark_lineage_demo",
        conn_id="spark_standalone",
        conf=spark_lineage_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3",
        application_args=["read_raw_csv"]
    )

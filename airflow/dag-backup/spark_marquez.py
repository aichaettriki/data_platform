from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os


# On définit la version de l'agent OpenLineage (compatible Spark 3.5)
OPENLINEAGE_JAR_PACKAGE = "io.openlineage:openlineage-spark-3.5:1.13.0"

OPENLINEAGE_CONF = {
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    # On supprime les extraClassPath manuels qui sont risqués
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": "http://marquez:5000",
    "spark.openlineage.namespace": "itceq_prod",
    "spark.openlineage.appName": "{{ task.task_id }}", 
    "spark.openlineage.jobName.appendActionName": "false",
    "spark.openlineage.jobName.appendDatasetName": "false",
    "spark.openlineage.parentJobName": "Pipeline_Production_Equipe",
    "spark.openlineage.facets.disabled": "spark_unknown",
    "spark.openlineage.transport.timeout": "120000"
}

with DAG(
    dag_id="spark-marquez",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1️⃣ Task: Ingest
    task_read_raw = SparkSubmitOperator(
        task_id="read_raw_csv",
        application="/opt/spark/jobs/etl_job.py",
        conn_id="spark_standalone",
        conf=OPENLINEAGE_CONF,
        packages=OPENLINEAGE_JAR_PACKAGE, 
        application_args=["ingest"] # L'argument pour le script
    )

    # 2️⃣ Task: Clean
    task_clean_transformed = SparkSubmitOperator(
        task_id="clean_and_transformed",
        application="/opt/spark/jobs/etl_job.py",
        conn_id="spark_standalone",
        conf=OPENLINEAGE_CONF,
        packages=OPENLINEAGE_JAR_PACKAGE, 
        application_args=["clean"]
    )

    # 3️⃣ Task: Refine
    task_add_timestamp = SparkSubmitOperator(
        task_id="add_timestamp_refined",
        application="/opt/spark/jobs/etl_job.py",
        conn_id="spark_standalone",
        conf=OPENLINEAGE_CONF,
        packages=OPENLINEAGE_JAR_PACKAGE, 
        application_args=["refine"]
    )

    # 4️⃣ Task: Write
    task_write_postgres = SparkSubmitOperator(
        task_id="write_to_postgres",
        application="/opt/spark/jobs/etl_job.py",
        conn_id="spark_standalone",
        conf=OPENLINEAGE_CONF,
        packages=f"{OPENLINEAGE_JAR_PACKAGE},org.postgresql:postgresql:42.5.1",
        application_args=["write_postgres"]
    )

    task_read_raw >> task_clean_transformed >> task_add_timestamp >> task_write_postgres
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="etl_csv_spark_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_etl = SparkSubmitOperator(
    task_id="spark_etl_equipes",
    application="/opt/spark/jobs/etl_equipes.py",
    conn_id="spark_default",
    packages="org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3",    
    conf={
            "spark.master": "spark://spark-master:7077", 
            "spark.submit.deployMode": "client" # pour le débogage
        }    
    )


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from atlas_helper import push_dag_to_atlas
from pyspark.sql import SparkSession

def spark_job():
    spark = SparkSession.builder \
        .appName("ETL Job") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Read raw data from MinIO
    df = spark.read.csv("s3a://raw/my_data.csv", header=True)

    # Transform
    df_transformed = df.filter(df['value'] > 100)

    # Write transformed data
    df_transformed.write.parquet("s3a://transformed/my_data_transformed.parquet", mode='overwrite')

    spark.stop()

def register_dag():
    push_dag_to_atlas("spark_etl_dag", "ETL DAG example")

with DAG(
    dag_id="spark_etl_dag",
    start_date=datetime(2025, 11, 25),
    schedule_interval="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="run_spark_job",
        python_callable=spark_job
    )

    t2 = PythonOperator(
        task_id="register_dag_atlas",
        python_callable=register_dag
    )

    t2 >> t1

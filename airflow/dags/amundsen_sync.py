from airflow import DAG
from airflow.operators.python import PythonOperator
from databuilder.job.job import DefaultJob
from databuilder.publisher.atlas_publisher import AtlasPublisher
from datetime import datetime

def sync_amundsen():
    # Configure job to read from Atlas
    job_config = {
        'publisher.atlas.rest.address': 'http://atlas:21000',
        'publisher.atlas.entity_type': 'Process',
        'publisher.atlas.neo4j.endpoints': 'bolt://neo4j:7687',
    }
    job = DefaultJob(conf=job_config, task=AtlasPublisher())
    job.launch()

with DAG(
    dag_id="amundsen_sync_dag",
    start_date=datetime(2025, 11, 25),
    schedule_interval="@daily",
    catchup=False
) as dag:
    sync_task = PythonOperator(
        task_id="sync_amundsen",
        python_callable=sync_amundsen
    )

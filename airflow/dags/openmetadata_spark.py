from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openmetadata_spark_lineage_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",  # every hour
    catchup=False,
    tags=["openmetadata", "spark", "lineage"],
) as dag:

    ingest_spark_lineage = BashOperator(
        task_id="ingest_spark_lineage",
        bash_command="""
        docker exec openmetadata-ingestion \
        metadata ingest -c /ingestion/spark-lineage.yaml
        """,
    )

    ingest_spark_lineage

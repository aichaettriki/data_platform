from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="00-MASTER__GLOBAL_PIPELINE",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["MASTER", "GLOBAL", "ORCHESTRATION"],
) as dag:

    trigger_ingest_raw = TriggerDagRunOperator(
        task_id="trigger_ingest_raw",
        trigger_dag_id="00-MASTER__INGEST_RAW",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,  # important pour rejouer proprement
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="00-TRANS__MASTER",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    # Orchestration globale
    trigger_ingest_raw >> trigger_transform
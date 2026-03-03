"""
DAG Orchestrateur — Lance tous les DAGs de transformation séquentiellement
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="00-TRANS__MASTER",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["MASTER", "TRANSFORM", "ORCHESTRATION"],
) as dag:

    trigger_world_bank = TriggerDagRunOperator(
        task_id="trigger_world_bank",
        trigger_dag_id="02-TRANS__world_bank_data",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    trigger_agregat = TriggerDagRunOperator(
        task_id="trigger_agregat",
        trigger_dag_id="02-TRANS__agregat",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    trigger_competitivite = TriggerDagRunOperator(
        task_id="trigger_competitivite",
        trigger_dag_id="02-TRANS__Competitivite",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    trigger_tre = TriggerDagRunOperator(
        task_id="trigger_tre",
        trigger_dag_id="02-TRANS__TRE",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Exécution séquentielle
    trigger_world_bank >> trigger_agregat >> trigger_competitivite >> trigger_tre
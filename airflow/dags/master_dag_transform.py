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
    trigger_pnud = TriggerDagRunOperator(
        task_id="trigger_pnud",
        trigger_dag_id="02-TRANS_PNUD",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )
    trigger_worldbank_countries = TriggerDagRunOperator(
        task_id="trigger_worldbank_countries",
        trigger_dag_id="02-TRANS__worldbank_countries",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )
    trigger_fmi = TriggerDagRunOperator(
        task_id="trigger_fmi",
        trigger_dag_id="02-TRANS__imf_data",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )


    # Exécution séquentielle
    trigger_world_bank >> trigger_agregat >> trigger_competitivite >> trigger_tre >> trigger_pnud >> trigger_worldbank_countries >> trigger_fmi
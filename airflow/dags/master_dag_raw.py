from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="00-MASTER__INGEST_RAW",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["MASTER", "ORCHESTRATION"],
) as dag:

    ingest_INS_sources = TriggerDagRunOperator(
        task_id="ingest_ins_sources",
        trigger_dag_id="01-ING__ingest_all_ins_sources",
        wait_for_completion=True,  # maintenant le master attend que ce DAG se termine
        poke_interval=30,          # vérifie toutes les 60s si le DAG enfant est fini
    )
    

    ingest_INS_dimensions = TriggerDagRunOperator(
        task_id="ingest_ins_dimensions",
        trigger_dag_id="01-ING__ingest_all_ins_dimensions",
        wait_for_completion=True,
        poke_interval=30,
    )

    ingest_WorldBank_data = TriggerDagRunOperator(
        task_id="ingest_WorldBank_data",
        trigger_dag_id="01-ING__ingest_worldbank_indicators",
        wait_for_completion=True,
        poke_interval=30,
    )
    ingest_WorldBank_country_data = TriggerDagRunOperator(
        task_id="ingest_WorldBank_country_data",
        trigger_dag_id="01-ING__ingest_worldbank_countries",
        wait_for_completion=True,
        poke_interval=30,
    )
    ingest_PNUD_data = TriggerDagRunOperator(
        task_id="ingest_PNUD_data",
        trigger_dag_id="01-ING__PNUD_HDI_to_Raw",
        wait_for_completion=True,
        poke_interval=30,
    )
    ingest_from_data_folder = TriggerDagRunOperator(
        task_id="ingest_from_data_folder",
        trigger_dag_id="01-ING__Ingest_data_to_Raw",
        wait_for_completion=True,
        poke_interval=30,
    )



    # ORCHESTRATION ORDER
    ingest_from_data_folder >> ingest_PNUD_data >> ingest_WorldBank_country_data >> ingest_WorldBank_data >> ingest_INS_sources >> ingest_INS_dimensions
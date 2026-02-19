# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime

# from check_raw_consistency import check_ins_raw_full_consistency

# with DAG(
#     dag_id="check_ins_raw_full_consistency",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["INS", "CHECK", "RAW", "AUDIT"],
# ) as dag:

#     check = PythonOperator(
#         task_id="check_raw_full_consistency",
#         python_callable=check_ins_raw_full_consistency ,
#         provide_context=True,
#     )

#     check

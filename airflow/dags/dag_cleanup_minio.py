from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from common.minio_utils import (
    find_all_temporary_folders,
    delete_all_temporary_folders,
    find_all_temp_write_folders,
    delete_all_temp_write_folders,
    find_all_marker_files,
    delete_all_marker_files,
    find_spark_staging_folders,
    delete_spark_staging_folders,
    )      


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# =========================================================
# DAG Airflow
# =========================================================
with DAG(
    dag_id='99-Tech__Cleanup_Minio',
    default_args=default_args,
    description='Nettoie TOUS les dossiers _temporary et _temp_write dans TOUS les buckets MinIO',
    schedule_interval='0 2 * * 0',  # Dimanches à 2h
    catchup=False,
    tags=['cleanup', 'minio', 'maintenance', 'global'],
) as dag:
    
    # _temporary
    find_temporary = PythonOperator(
        task_id='find_temporary_folders',
        python_callable=find_all_temporary_folders,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    
    cleanup_temporary = PythonOperator(
        task_id='delete_all_temporary_folders',
        python_callable=delete_all_temporary_folders,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )
    
    # _temp_write
    find_temp_write = PythonOperator(
        task_id='find_temp_write_folders',
        python_callable=find_all_temp_write_folders,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    
    cleanup_temp_write = PythonOperator(
        task_id='delete_all_temp_write_folders',
        python_callable=delete_all_temp_write_folders,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )
    find_marker_files = PythonOperator(
    task_id='find_marker_files',
    python_callable=find_all_marker_files,
    provide_context=True,
    execution_timeout=timedelta(minutes=30),
    trigger_rule='all_done', 
)

    cleanup_marker_files = PythonOperator(
    task_id='delete_all_marker_files',
    python_callable=delete_all_marker_files,
    provide_context=True,
    execution_timeout=timedelta(hours=2),
    trigger_rule='all_done',
)
    
    find_spark_staging = PythonOperator(
        task_id='find_spark_staging_folders',
        python_callable=find_spark_staging_folders,
        provide_context=True,
    )

    cleanup_spark_staging = PythonOperator(
        task_id='delete_spark_staging_folders',
        python_callable=delete_spark_staging_folders,
        provide_context=True,
    )


    
    # Dépendances
    find_temporary >> cleanup_temporary  >> find_temp_write >> cleanup_temp_write >> find_marker_files >> cleanup_marker_files >> find_spark_staging >> cleanup_spark_staging

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


def _get_scope(**context):
    """
    Lit target_bucket et target_folder depuis le conf du DagRun.
    Si le DAG est lancé manuellement sans conf → None (scan complet).
    """
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    target_bucket = conf.get('source_bucket')   # nom de la clé envoyée par les DAGs appelants
    target_folder = conf.get('target_folder')
    triggered_by  = conf.get('triggered_by', 'manual')

    print(f"🚀 Cleanup déclenché par : {triggered_by}")
    print(f"📦 target_bucket : {target_bucket or '(tous)'}")
    print(f"📁 target_folder : {target_folder or '(tous)'}")

    # On pousse le scope en XCom pour que toutes les tâches le lisent
    context['ti'].xcom_push(key='target_bucket', value=target_bucket)
    context['ti'].xcom_push(key='target_folder', value=target_folder)


def _wrap(func):
    """Injecte target_bucket / target_folder depuis XCom avant d'appeler la fonction."""
    def wrapper(**context):
        ti = context['ti']
        target_bucket = ti.xcom_pull(task_ids='read_conf', key='target_bucket')
        target_folder = ti.xcom_pull(task_ids='read_conf', key='target_folder')
        return func(target_bucket=target_bucket, target_folder=target_folder, **context)
    wrapper.__name__ = func.__name__
    return wrapper


# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id='99-Tech__Cleanup_Minio',
    default_args=default_args,
    description='Nettoie les dossiers temporaires MinIO (scope optionnel via conf)',
    schedule_interval='0 2 * * 0',  # Dimanches à 2h — scan complet
    catchup=False,
    tags=['cleanup', 'minio', 'maintenance', 'global'],
) as dag:

    # Tâche 0 : lecture du conf (bucket / dossier cibles)
    read_conf = PythonOperator(
        task_id='read_conf',
        python_callable=_get_scope,
        provide_context=True,
    )

    # _temporary
    find_temporary = PythonOperator(
        task_id='find_temporary_folders',
        python_callable=_wrap(find_all_temporary_folders),
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    cleanup_temporary = PythonOperator(
        task_id='delete_all_temporary_folders',
        python_callable=_wrap(delete_all_temporary_folders),
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )

    # _temp_write
    find_temp_write = PythonOperator(
        task_id='find_temp_write_folders',
        python_callable=_wrap(find_all_temp_write_folders),
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    cleanup_temp_write = PythonOperator(
        task_id='delete_all_temp_write_folders',
        python_callable=_wrap(delete_all_temp_write_folders),
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )

    # marker files
    find_marker = PythonOperator(
        task_id='find_marker_files',
        python_callable=_wrap(find_all_marker_files),
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
        trigger_rule='all_done',
    )
    cleanup_marker = PythonOperator(
        task_id='delete_all_marker_files',
        python_callable=_wrap(delete_all_marker_files),
        provide_context=True,
        execution_timeout=timedelta(hours=2),
        trigger_rule='all_done',
    )

    # spark-staging
    find_staging = PythonOperator(
        task_id='find_spark_staging_folders',
        python_callable=_wrap(find_spark_staging_folders),
        provide_context=True,
    )
    cleanup_staging = PythonOperator(
        task_id='delete_spark_staging_folders',
        python_callable=_wrap(delete_spark_staging_folders),
        provide_context=True,
    )

    # Dépendances
    (
        read_conf
        >> find_temporary >> cleanup_temporary
        >> find_temp_write >> cleanup_temp_write
        >> find_marker >> cleanup_marker
        >> find_staging >> cleanup_staging
    )
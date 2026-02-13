from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv

# =========================================================
# Chargement des variables d'environnement
# =========================================================
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_ROOT_USER = get_env_var("MINIO_ROOT_USER", required=True)
MINIO_PASSWORD = get_env_var("MINIO_ROOT_PASSWORD", required=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =========================================================
# Client MinIO
# =========================================================
def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

# =========================================================
# Fonctions de recherche et suppression
# =========================================================
def find_all_temporary_folders(**context):
    """Trouve tous les dossiers _temporary dans tous les buckets"""
    
    s3_client = get_minio_client()
    print(f"🔍 Recherche de TOUS les dossiers '_temporary' dans MinIO")
    
    buckets = [b['Name'] for b in s3_client.list_buckets()['Buckets']]
    temporary_folders = []
    
    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")
        found_paths = set()
        paginator = s3_client.get_paginator('list_object_versions')
        
        for page in paginator.paginate(Bucket=bucket_name):
            # Versions
            for obj in page.get('Versions', []):
                key = obj['Key']
                if '_temporary' in key:
                    temp_path = key.split('_temporary')[0] + '_temporary/'
                    if temp_path not in found_paths:
                        found_paths.add(temp_path)
                        temporary_folders.append({'bucket': bucket_name, 'path': temp_path})
                        print(f"   ✅ Trouvé: {temp_path}")
            # Delete markers
            for marker in page.get('DeleteMarkers', []):
                key = marker['Key']
                if '_temporary' in key:
                    temp_path = key.split('_temporary')[0] + '_temporary/'
                    if temp_path not in found_paths:
                        found_paths.add(temp_path)
                        temporary_folders.append({'bucket': bucket_name, 'path': temp_path})
                        print(f"   ✅ Trouvé (delete marker): {temp_path}")
    
    print(f"\n🎯 Résumé: {len(temporary_folders)} dossier(s) '_temporary' trouvé(s)")
    context['ti'].xcom_push(key='temporary_folders', value=temporary_folders)
    return temporary_folders

def delete_all_temporary_folders(**context):
    """Supprime tous les dossiers _temporary"""
    
    s3_client = get_minio_client()
    temporary_folders = context['ti'].xcom_pull(task_ids='find_temporary_folders', key='temporary_folders')
    
    if not temporary_folders:
        print("ℹ️ Aucun dossier '_temporary' à supprimer")
        return 0
    
    total_deleted = 0
    for idx, item in enumerate(temporary_folders, 1):
        bucket_name = item['bucket']
        prefix = item['path']
        print(f"\n[{idx}/{len(temporary_folders)}] Suppression: {bucket_name}/{prefix}")
        paginator = s3_client.get_paginator('list_object_versions')
        delete_count = 0
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            objects_to_delete = [{'Key': v['Key'], 'VersionId': v['VersionId']} for v in page.get('Versions', [])]
            objects_to_delete += [{'Key': m['Key'], 'VersionId': m['VersionId']} for m in page.get('DeleteMarkers', [])]
            
            for i in range(0, len(objects_to_delete), 1000):
                chunk = objects_to_delete[i:i+1000]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': chunk})
                delete_count += len(chunk)
        
        total_deleted += delete_count
        print(f"   ✅ {delete_count} objets/versions supprimés dans {prefix}")
    
    print(f"\n🎉 TOTAL: {total_deleted} objets/versions '_temporary' supprimés")
    context['ti'].xcom_push(key='total_deleted_temporary', value=total_deleted)
    return total_deleted

# =========================================================
# Fonctions pour _temp_write
# =========================================================
def find_all_temp_write_folders(**context):
    """Trouve tous les dossiers avec suffixe '_temp_write'"""
    
    s3_client = get_minio_client()
    print(f"🔍 Recherche de TOUS les dossiers '_temp_write'")
    
    buckets = [b['Name'] for b in s3_client.list_buckets()['Buckets']]
    temp_write_folders = []
    
    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")
        found_paths = set()
        paginator = s3_client.get_paginator('list_object_versions')
        
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Versions', []):
                key = obj['Key']
                if key.endswith('_temp_write') or '_temp_write/' in key:
                    temp_path = key.split('_temp_write')[0] + '_temp_write/'
                    if temp_path not in found_paths:
                        found_paths.add(temp_path)
                        temp_write_folders.append({'bucket': bucket_name, 'path': temp_path})
                        print(f"   ✅ Trouvé: {temp_path}")
            for marker in page.get('DeleteMarkers', []):
                key = marker['Key']
                if key.endswith('_temp_write') or '_temp_write/' in key:
                    temp_path = key.split('_temp_write')[0] + '_temp_write/'
                    if temp_path not in found_paths:
                        found_paths.add(temp_path)
                        temp_write_folders.append({'bucket': bucket_name, 'path': temp_path})
                        print(f"   ✅ Trouvé (delete marker): {temp_path}")
    
    print(f"\n🎯 Résumé: {len(temp_write_folders)} dossier(s) '_temp_write' trouvé(s)")
    context['ti'].xcom_push(key='temp_write_folders', value=temp_write_folders)
    return temp_write_folders

def delete_all_temp_write_folders(**context):
    """Supprime tous les dossiers avec suffixe '_temp_write'"""
    
    s3_client = get_minio_client()
    temp_write_folders = context['ti'].xcom_pull(task_ids='find_temp_write_folders', key='temp_write_folders')
    
    if not temp_write_folders:
        print("ℹ️ Aucun dossier '_temp_write' à supprimer")
        return 0
    
    total_deleted = 0
    for idx, item in enumerate(temp_write_folders, 1):
        bucket_name = item['bucket']
        prefix = item['path']
        print(f"\n[{idx}/{len(temp_write_folders)}] Suppression: {bucket_name}/{prefix}")
        paginator = s3_client.get_paginator('list_object_versions')
        delete_count = 0
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            objects_to_delete = [{'Key': v['Key'], 'VersionId': v['VersionId']} for v in page.get('Versions', [])]
            objects_to_delete += [{'Key': m['Key'], 'VersionId': m['VersionId']} for m in page.get('DeleteMarkers', [])]
            
            for i in range(0, len(objects_to_delete), 1000):
                chunk = objects_to_delete[i:i+1000]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': chunk})
                delete_count += len(chunk)
        
        total_deleted += delete_count
        print(f"   ✅ {delete_count} objets/versions supprimés dans {prefix}")
    
    print(f"\n🎉 TOTAL: {total_deleted} objets/versions '_temp_write' supprimés")
    context['ti'].xcom_push(key='total_deleted_temp_write', value=total_deleted)
    return total_deleted


# =========================================================
# Fonctions pour fichiers _SUCCESS, _COPYING_, etc.
# =========================================================
def find_all_marker_files(**context):
    """Trouve tous les fichiers _SUCCESS, _COPYING_ dans tous les buckets"""
    
    s3_client = get_minio_client()
    marker_suffixes = ['_SUCCESS', '_COPYING_']  # Ajouter d'autres si nécessaire
    print(f"🔍 Recherche de fichiers marqueurs: {marker_suffixes}")
    
    buckets = [b['Name'] for b in s3_client.list_buckets()['Buckets']]
    marker_files = []
    
    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")
        paginator = s3_client.get_paginator('list_object_versions')
        
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Versions', []):
                key = obj['Key']
                if any(key.endswith(suf) for suf in marker_suffixes):
                    marker_files.append({'bucket': bucket_name, 'key': key, 'version_id': obj['VersionId']})
                    print(f"   ✅ Trouvé: {key}")
            for marker in page.get('DeleteMarkers', []):
                key = marker['Key']
                if any(key.endswith(suf) for suf in marker_suffixes):
                    marker_files.append({'bucket': bucket_name, 'key': key, 'version_id': marker['VersionId']})
                    print(f"   ✅ Trouvé (delete marker): {key}")
    
    print(f"\n🎯 Résumé: {len(marker_files)} fichiers marqueurs trouvés")
    context['ti'].xcom_push(key='marker_files', value=marker_files)
    return marker_files

def delete_all_marker_files(**context):
    """Supprime tous les fichiers _SUCCESS, _COPYING_, etc."""
    
    s3_client = get_minio_client()
    marker_files = context['ti'].xcom_pull(task_ids='find_marker_files', key='marker_files')
    
    if not marker_files:
        print("ℹ️ Aucun fichier marqueur à supprimer")
        return 0
    
    total_deleted = 0
    for idx, item in enumerate(marker_files, 1):
        bucket_name = item['bucket']
        key = item['key']
        version_id = item['version_id']
        
        print(f"\n[{idx}/{len(marker_files)}] Suppression: {bucket_name}/{key}")
        s3_client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        total_deleted += 1
    
    print(f"\n🎉 TOTAL: {total_deleted} fichiers marqueurs supprimés")
    context['ti'].xcom_push(key='total_deleted_marker_files', value=total_deleted)
    return total_deleted

# =========================================================
# Fonctions pour .spark-staging
# =========================================================
def find_spark_staging_folders(**context):
    """Trouve tous les dossiers .spark-staging dans tous les buckets"""

    s3_client = get_minio_client()
    print("🔍 Recherche des dossiers '.spark-staging-'")

    buckets = [b['Name'] for b in s3_client.list_buckets()['Buckets']]
    staging_folders = []

    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")
        found_paths = set()
        paginator = s3_client.get_paginator('list_object_versions')

        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Versions', []):
                key = obj['Key']
                if '.spark-staging-' in key:
                    path = key.split('.spark-staging-')[0] + '.spark-staging-'
                    if path not in found_paths:
                        found_paths.add(path)
                        staging_folders.append({'bucket': bucket_name, 'path': path})
                        print(f"   ✅ Trouvé: {path}")

            for marker in page.get('DeleteMarkers', []):
                key = marker['Key']
                if '.spark-staging-' in key:
                    path = key.split('.spark-staging-')[0] + '.spark-staging-'
                    if path not in found_paths:
                        found_paths.add(path)
                        staging_folders.append({'bucket': bucket_name, 'path': path})
                        print(f"   ✅ Trouvé (delete marker): {path}")

    context['ti'].xcom_push(key='spark_staging_folders', value=staging_folders)
    return staging_folders

def delete_spark_staging_folders(**context):
    """Supprime tous les dossiers .spark-staging"""

    s3_client = get_minio_client()
    staging_folders = context['ti'].xcom_pull(
        task_ids='find_spark_staging_folders',
        key='spark_staging_folders'
    )

    if not staging_folders:
        print("ℹ️ Aucun dossier '.spark-staging' à supprimer")
        return 0

    total_deleted = 0

    for idx, item in enumerate(staging_folders, 1):
        bucket_name = item['bucket']
        prefix = item['path']

        print(f"\n[{idx}/{len(staging_folders)}] Suppression: {bucket_name}/{prefix}")

        paginator = s3_client.get_paginator('list_object_versions')
        delete_count = 0

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):

            objects_to_delete = [
                {'Key': v['Key'], 'VersionId': v['VersionId']}
                for v in page.get('Versions', [])
            ]

            objects_to_delete += [
                {'Key': m['Key'], 'VersionId': m['VersionId']}
                for m in page.get('DeleteMarkers', [])
            ]

            for i in range(0, len(objects_to_delete), 1000):
                chunk = objects_to_delete[i:i+1000]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': chunk})
                delete_count += len(chunk)

        total_deleted += delete_count
        print(f"   ✅ {delete_count} supprimés")

    context['ti'].xcom_push(key='total_deleted_spark_staging', value=total_deleted)
    return total_deleted

# =========================================================
# DAG Airflow
# =========================================================
with DAG(
    dag_id='Cleanup_Minio',
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

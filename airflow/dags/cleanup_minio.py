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

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def find_all_temporary_folders(**context):
    """Trouve tous les dossiers _temporary dans tous les buckets"""
    
    s3_client = get_minio_client()
    
    print(f"🔍 Recherche de TOUS les dossiers '_temporary' dans MinIO")
    print(f"🔗 Endpoint: {MINIO_ENDPOINT}\n")
    
    # Lister tous les buckets
    buckets_response = s3_client.list_buckets()
    buckets = [b['Name'] for b in buckets_response['Buckets']]
    print(f"📦 {len(buckets)} bucket(s) trouvé(s): {', '.join(buckets)}\n")
    
    temporary_folders = []
    
    # Pour chaque bucket
    for bucket_name in buckets:
        print(f"\n🔎 Analyse du bucket: {bucket_name}")
        print("-" * 60)
        
        try:
            # Lister avec versions pour voir TOUS les objets
            paginator = s3_client.get_paginator('list_object_versions')
            
            found_paths = set()
            sample_keys = []
            
            for page in paginator.paginate(Bucket=bucket_name):
                
                # Analyser les Versions
                if 'Versions' in page:
                    for obj in page['Versions']:
                        key = obj['Key']
                        
                        # Garder quelques exemples pour debug
                        if len(sample_keys) < 5:
                            sample_keys.append(key)
                        
                        # Chercher '_temporary' dans le chemin (avec ou sans slash)
                        if '_temporary' in key:
                            # Extraire le chemin parent du dossier _temporary
                            parts = key.split('_temporary')
                            if len(parts) > 1:
                                # Reconstruire le chemin jusqu'à _temporary/
                                temp_path = parts[0] + '_temporary/'
                                
                                if temp_path not in found_paths:
                                    found_paths.add(temp_path)
                                    temporary_folders.append({
                                        'bucket': bucket_name,
                                        'path': temp_path
                                    })
                                    print(f"   ✅ Trouvé: {temp_path}")
                                    print(f"      Exemple de fichier: {key}")
                
                # Analyser aussi les DeleteMarkers
                if 'DeleteMarkers' in page:
                    for marker in page['DeleteMarkers']:
                        key = marker['Key']
                        
                        if '_temporary' in key:
                            parts = key.split('_temporary')
                            if len(parts) > 1:
                                temp_path = parts[0] + '_temporary/'
                                
                                if temp_path not in found_paths:
                                    found_paths.add(temp_path)
                                    temporary_folders.append({
                                        'bucket': bucket_name,
                                        'path': temp_path
                                    })
                                    print(f"   ✅ Trouvé (delete marker): {temp_path}")
            
            if not found_paths:
                print(f"   ℹ️  Aucun dossier '_temporary' trouvé")
                if sample_keys:
                    print(f"   📝 Exemples de chemins dans ce bucket:")
                    for sample in sample_keys[:5]:
                        print(f"      - {sample}")
            
        except Exception as e:
            print(f"   ⚠️  Erreur lors de l'analyse: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "=" * 60)
    print(f"🎯 Résumé: {len(temporary_folders)} dossier(s) '_temporary' trouvé(s)")
    print("=" * 60)
    
    for item in temporary_folders:
        print(f"   📁 {item['bucket']}/{item['path']}")
    
    context['ti'].xcom_push(key='temporary_folders', value=temporary_folders)
    
    return temporary_folders

def delete_all_temporary_folders(**context):
    """Supprime tous les dossiers _temporary trouvés"""
    
    s3_client = get_minio_client()
    
    temporary_folders = context['ti'].xcom_pull(
        task_ids='find_temporary_folders',
        key='temporary_folders'
    )
    
    if not temporary_folders:
        print("ℹ️  Aucun dossier '_temporary' à supprimer")
        return 0
    
    print(f"🗑️  Début de la suppression de {len(temporary_folders)} dossier(s)\n")
    
    total_deleted = 0
    summary = []
    
    for idx, item in enumerate(temporary_folders, 1):
        bucket_name = item['bucket']
        prefix = item['path']
        
        print(f"\n{'=' * 60}")
        print(f"[{idx}/{len(temporary_folders)}] Bucket: {bucket_name}")
        print(f"Path: {prefix}")
        print('-' * 60)
        
        paginator = s3_client.get_paginator('list_object_versions')
        delete_count = 0
        batch_count = 0
        
        try:
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                objects_to_delete = []
                
                # Versions actuelles
                if 'Versions' in page:
                    for version in page['Versions']:
                        objects_to_delete.append({
                            'Key': version['Key'],
                            'VersionId': version['VersionId']
                        })
                
                # Delete markers
                if 'DeleteMarkers' in page:
                    for marker in page['DeleteMarkers']:
                        objects_to_delete.append({
                            'Key': marker['Key'],
                            'VersionId': marker['VersionId']
                        })
                
                # Supprimer par batch (max 1000)
                if objects_to_delete:
                    # Diviser en chunks de 1000 max
                    for i in range(0, len(objects_to_delete), 1000):
                        chunk = objects_to_delete[i:i+1000]
                        
                        response = s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': chunk}
                        )
                        
                        batch_count += 1
                        delete_count += len(chunk)
                        
                        print(f"   ✅ Batch {batch_count}: {len(chunk)} objets supprimés (total: {delete_count})")
                        
                        if 'Errors' in response:
                            for error in response['Errors']:
                                print(f"   ⚠️  Erreur: {error.get('Key', 'unknown')} - {error.get('Message', 'unknown error')}")
            
            total_deleted += delete_count
            
            summary.append({
                'bucket': bucket_name,
                'path': prefix,
                'deleted': delete_count,
                'batches': batch_count,
                'status': 'success'
            })
            
            print(f"✅ Terminé: {delete_count} objets/versions supprimés en {batch_count} batch(es)")
            
        except Exception as e:
            print(f"❌ Erreur: {str(e)}")
            import traceback
            traceback.print_exc()
            
            summary.append({
                'bucket': bucket_name,
                'path': prefix,
                'deleted': delete_count,
                'status': 'error',
                'error': str(e)
            })
    
    # Afficher le résumé final
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ FINAL")
    print("=" * 60)
    
    for item in summary:
        status_icon = "✅" if item['status'] == 'success' else "❌"
        print(f"\n{status_icon} {item['bucket']}/{item['path']}")
        print(f"   Supprimés: {item['deleted']} objets/versions")
        if item['status'] == 'error':
            print(f"   Erreur: {item.get('error', 'Unknown')}")
    
    print(f"\n🎉 TOTAL: {total_deleted} objets/versions supprimés dans {len(temporary_folders)} dossier(s)")
    print("=" * 60)
    
    context['ti'].xcom_push(key='total_deleted', value=total_deleted)
    context['ti'].xcom_push(key='summary', value=summary)
    
    return total_deleted

with DAG(
    'cleanup_all_temporary_folders',
    default_args=default_args,
    description='Nettoie TOUS les dossiers _temporary dans TOUS les buckets MinIO',
    schedule_interval='0 2 * * 0',  # Dimanches à 2h
    catchup=False,
    tags=['cleanup', 'minio', 'maintenance', 'global'],
) as dag:
    
    find_folders = PythonOperator(
        task_id='find_temporary_folders',
        python_callable=find_all_temporary_folders,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    
    cleanup_folders = PythonOperator(
        task_id='delete_all_temporary_folders',
        python_callable=delete_all_temporary_folders,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )
    
    find_folders >> cleanup_folders
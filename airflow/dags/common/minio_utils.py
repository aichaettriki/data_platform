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
# Helpers
# =========================================================
def _resolve_buckets(s3_client, target_bucket=None):
    """Retourne la liste des buckets à scanner (tous ou un seul)."""
    if target_bucket:
        print(f"📦 Scope limité au bucket: {target_bucket}")
        return [target_bucket]
    return [b['Name'] for b in s3_client.list_buckets()['Buckets']]


def _paginate_versions(s3_client, bucket_name, prefix=None):
    """Itère sur toutes les pages de list_object_versions avec préfixe optionnel."""
    paginator = s3_client.get_paginator('list_object_versions')
    kwargs = {'Bucket': bucket_name}
    if prefix:
        kwargs['Prefix'] = prefix
        print(f"📁 Scope limité au dossier: {prefix}")
    yield from paginator.paginate(**kwargs)


def _bulk_delete(s3_client, bucket_name, objects_to_delete):
    """Supprime une liste d'objets par chunks de 1000."""
    deleted = 0
    for i in range(0, len(objects_to_delete), 1000):
        chunk = objects_to_delete[i:i + 1000]
        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': chunk})
        deleted += len(chunk)
    return deleted


# =========================================================
# Fonctions de recherche et suppression — _temporary
# =========================================================
def find_all_temporary_folders(target_bucket=None, target_folder=None, **context):
    """
    Trouve tous les dossiers _temporary.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la recherche à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    scope = f"bucket='{target_bucket}'" if target_bucket else "tous les buckets"
    scope += f", dossier='{target_folder}'" if target_folder else ""
    print(f"🔍 Recherche de '_temporary' [{scope}]")

    buckets = _resolve_buckets(s3_client, target_bucket)
    temporary_folders = []

    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")
        found_paths = set()

        for page in _paginate_versions(s3_client, bucket_name, target_folder):
            for obj in page.get('Versions', []) + page.get('DeleteMarkers', []):
                key = obj['Key']
                if '_temporary' in key:
                    temp_path = key.split('_temporary')[0] + '_temporary/'
                    if temp_path not in found_paths:
                        found_paths.add(temp_path)
                        temporary_folders.append({'bucket': bucket_name, 'path': temp_path})
                        label = "(delete marker)" if 'ETag' not in obj else ""
                        print(f"   ✅ Trouvé {label}: {temp_path}")

    print(f"\n🎯 Résumé: {len(temporary_folders)} dossier(s) '_temporary' trouvé(s)")
    context['ti'].xcom_push(key='temporary_folders', value=temporary_folders)
    return temporary_folders


def delete_all_temporary_folders(target_bucket=None, target_folder=None, **context):
    """
    Supprime tous les dossiers _temporary.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la suppression à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    temporary_folders = context['ti'].xcom_pull(task_ids='find_temporary_folders', key='temporary_folders')

    if target_bucket:
        temporary_folders = [f for f in temporary_folders if f['bucket'] == target_bucket]
    if target_folder:
        temporary_folders = [f for f in temporary_folders if f['path'].startswith(target_folder)]

    if not temporary_folders:
        print("ℹ️ Aucun dossier '_temporary' à supprimer")
        return 0

    total_deleted = 0
    for idx, item in enumerate(temporary_folders, 1):
        bucket_name, prefix = item['bucket'], item['path']
        print(f"\n[{idx}/{len(temporary_folders)}] Suppression: {bucket_name}/{prefix}")
        objects_to_delete = []

        for page in _paginate_versions(s3_client, bucket_name, prefix):
            objects_to_delete += [{'Key': v['Key'], 'VersionId': v['VersionId']} for v in page.get('Versions', [])]
            objects_to_delete += [{'Key': m['Key'], 'VersionId': m['VersionId']} for m in page.get('DeleteMarkers', [])]

        delete_count = _bulk_delete(s3_client, bucket_name, objects_to_delete)
        total_deleted += delete_count
        print(f"   ✅ {delete_count} objets/versions supprimés dans {prefix}")

    print(f"\n🎉 TOTAL: {total_deleted} objets/versions '_temporary' supprimés")
    context['ti'].xcom_push(key='total_deleted_temporary', value=total_deleted)
    return total_deleted


# =========================================================
# Fonctions pour _temp_write
# =========================================================
def find_all_temp_write_folders(target_bucket=None, target_folder=None, **context):
    """
    Trouve tous les dossiers _temp_write.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la recherche à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    scope = f"bucket='{target_bucket}'" if target_bucket else "tous les buckets"
    scope += f", dossier='{target_folder}'" if target_folder else ""
    print(f"🔍 Recherche de '_temp_write' [{scope}]")

    buckets = _resolve_buckets(s3_client, target_bucket)
    temp_write_folders = []

    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")
        found_paths = set()

        for page in _paginate_versions(s3_client, bucket_name, target_folder):
            for obj in page.get('Versions', []) + page.get('DeleteMarkers', []):
                key = obj['Key']
                if key.endswith('_temp_write') or '_temp_write/' in key:
                    temp_path = key.split('_temp_write')[0] + '_temp_write/'
                    if temp_path not in found_paths:
                        found_paths.add(temp_path)
                        temp_write_folders.append({'bucket': bucket_name, 'path': temp_path})
                        label = "(delete marker)" if 'ETag' not in obj else ""
                        print(f"   ✅ Trouvé {label}: {temp_path}")

    print(f"\n🎯 Résumé: {len(temp_write_folders)} dossier(s) '_temp_write' trouvé(s)")
    context['ti'].xcom_push(key='temp_write_folders', value=temp_write_folders)
    return temp_write_folders


def delete_all_temp_write_folders(target_bucket=None, target_folder=None, **context):
    """
    Supprime tous les dossiers _temp_write.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la suppression à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    temp_write_folders = context['ti'].xcom_pull(task_ids='find_temp_write_folders', key='temp_write_folders')

    if target_bucket:
        temp_write_folders = [f for f in temp_write_folders if f['bucket'] == target_bucket]
    if target_folder:
        temp_write_folders = [f for f in temp_write_folders if f['path'].startswith(target_folder)]

    if not temp_write_folders:
        print("ℹ️ Aucun dossier '_temp_write' à supprimer")
        return 0

    total_deleted = 0
    for idx, item in enumerate(temp_write_folders, 1):
        bucket_name, prefix = item['bucket'], item['path']
        print(f"\n[{idx}/{len(temp_write_folders)}] Suppression: {bucket_name}/{prefix}")
        objects_to_delete = []

        for page in _paginate_versions(s3_client, bucket_name, prefix):
            objects_to_delete += [{'Key': v['Key'], 'VersionId': v['VersionId']} for v in page.get('Versions', [])]
            objects_to_delete += [{'Key': m['Key'], 'VersionId': m['VersionId']} for m in page.get('DeleteMarkers', [])]

        delete_count = _bulk_delete(s3_client, bucket_name, objects_to_delete)
        total_deleted += delete_count
        print(f"   ✅ {delete_count} objets/versions supprimés dans {prefix}")

    print(f"\n🎉 TOTAL: {total_deleted} objets/versions '_temp_write' supprimés")
    context['ti'].xcom_push(key='total_deleted_temp_write', value=total_deleted)
    return total_deleted


# =========================================================
# Fonctions pour fichiers _SUCCESS, _COPYING_, etc.
# =========================================================
def find_all_marker_files(target_bucket=None, target_folder=None, **context):
    """
    Trouve tous les fichiers _SUCCESS, _COPYING_, etc.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la recherche à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    marker_suffixes = ['_SUCCESS', '_COPYING_']
    scope = f"bucket='{target_bucket}'" if target_bucket else "tous les buckets"
    scope += f", dossier='{target_folder}'" if target_folder else ""
    print(f"🔍 Recherche de fichiers marqueurs {marker_suffixes} [{scope}]")

    buckets = _resolve_buckets(s3_client, target_bucket)
    marker_files = []

    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")

        for page in _paginate_versions(s3_client, bucket_name, target_folder):
            for obj in page.get('Versions', []) + page.get('DeleteMarkers', []):
                key = obj['Key']
                if any(key.endswith(suf) for suf in marker_suffixes):
                    marker_files.append({'bucket': bucket_name, 'key': key, 'version_id': obj['VersionId']})
                    label = "(delete marker)" if 'ETag' not in obj else ""
                    print(f"   ✅ Trouvé {label}: {key}")

    print(f"\n🎯 Résumé: {len(marker_files)} fichiers marqueurs trouvés")
    context['ti'].xcom_push(key='marker_files', value=marker_files)
    return marker_files


def delete_all_marker_files(target_bucket=None, target_folder=None, **context):
    """
    Supprime tous les fichiers _SUCCESS, _COPYING_, etc.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la suppression à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    marker_files = context['ti'].xcom_pull(task_ids='find_marker_files', key='marker_files')

    if target_bucket:
        marker_files = [f for f in marker_files if f['bucket'] == target_bucket]
    if target_folder:
        marker_files = [f for f in marker_files if f['key'].startswith(target_folder)]

    if not marker_files:
        print("ℹ️ Aucun fichier marqueur à supprimer")
        return 0

    total_deleted = 0
    for idx, item in enumerate(marker_files, 1):
        bucket_name, key, version_id = item['bucket'], item['key'], item['version_id']
        print(f"\n[{idx}/{len(marker_files)}] Suppression: {bucket_name}/{key}")
        s3_client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        total_deleted += 1

    print(f"\n🎉 TOTAL: {total_deleted} fichiers marqueurs supprimés")
    context['ti'].xcom_push(key='total_deleted_marker_files', value=total_deleted)
    return total_deleted


# =========================================================
# Fonctions pour .spark-staging
# =========================================================
def find_spark_staging_folders(target_bucket=None, target_folder=None, **context):
    """
    Trouve tous les dossiers .spark-staging-.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la recherche à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    scope = f"bucket='{target_bucket}'" if target_bucket else "tous les buckets"
    scope += f", dossier='{target_folder}'" if target_folder else ""
    print(f"🔍 Recherche de '.spark-staging-' [{scope}]")

    buckets = _resolve_buckets(s3_client, target_bucket)
    staging_folders = []

    for bucket_name in buckets:
        print(f"\n🔎 Bucket: {bucket_name}")
        found_paths = set()

        for page in _paginate_versions(s3_client, bucket_name, target_folder):
            for obj in page.get('Versions', []) + page.get('DeleteMarkers', []):
                key = obj['Key']
                if '.spark-staging-' in key:
                    path = key.split('.spark-staging-')[0] + '.spark-staging-'
                    if path not in found_paths:
                        found_paths.add(path)
                        staging_folders.append({'bucket': bucket_name, 'path': path})
                        label = "(delete marker)" if 'ETag' not in obj else ""
                        print(f"   ✅ Trouvé {label}: {path}")

    print(f"\n🎯 Résumé: {len(staging_folders)} dossier(s) '.spark-staging-' trouvé(s)")
    context['ti'].xcom_push(key='spark_staging_folders', value=staging_folders)
    return staging_folders


def delete_spark_staging_folders(target_bucket=None, target_folder=None, **context):
    """
    Supprime tous les dossiers .spark-staging-.

    Paramètres optionnels
    ---------------------
    target_bucket : str  — restreint la suppression à ce bucket
    target_folder : str  — préfixe de dossier (ex: 'raw/orders/')
    """
    s3_client = get_minio_client()
    staging_folders = context['ti'].xcom_pull(task_ids='find_spark_staging_folders', key='spark_staging_folders')

    if target_bucket:
        staging_folders = [f for f in staging_folders if f['bucket'] == target_bucket]
    if target_folder:
        staging_folders = [f for f in staging_folders if f['path'].startswith(target_folder)]

    if not staging_folders:
        print("ℹ️ Aucun dossier '.spark-staging' à supprimer")
        return 0

    total_deleted = 0
    for idx, item in enumerate(staging_folders, 1):
        bucket_name, prefix = item['bucket'], item['path']
        print(f"\n[{idx}/{len(staging_folders)}] Suppression: {bucket_name}/{prefix}")
        objects_to_delete = []

        for page in _paginate_versions(s3_client, bucket_name, prefix):
            objects_to_delete += [{'Key': v['Key'], 'VersionId': v['VersionId']} for v in page.get('Versions', [])]
            objects_to_delete += [{'Key': m['Key'], 'VersionId': m['VersionId']} for m in page.get('DeleteMarkers', [])]

        delete_count = _bulk_delete(s3_client, bucket_name, objects_to_delete)
        total_deleted += delete_count
        print(f"   ✅ {delete_count} supprimés")

    print(f"\n🎉 TOTAL: {total_deleted} dossiers '.spark-staging-' supprimés")
    context['ti'].xcom_push(key='total_deleted_spark_staging', value=total_deleted)
    return total_deleted
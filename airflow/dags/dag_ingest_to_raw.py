from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import shutil
from minio import Minio
from common.dag_helpers import RAW_BUCKET
from common.minio_utils import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_PASSWORD,
)

# =====================================================
# CONFIG
# =====================================================
LOCAL_INPUT_DIR = "/opt/airflow/data"
ARCHIVE_DIR = os.path.join(LOCAL_INPUT_DIR, "archive")

EXCLUDED_FOLDERS = ["archive", "liste_indicateurs"]

# =====================================================
# CLEAN EMPTY DIRECTORIES
# =====================================================
def clean_empty_dirs(base_path):
    """
    Supprime récursivement TOUS les dossiers vides
    (corrige le problème des dossiers parents non supprimés)
    """

    for root, dirs, files in os.walk(base_path, topdown=False):

        # ❌ ignorer archive et liste_indicateurs
        if any(x in root for x in ["archive", "liste_indicateurs"]):
            continue

        # ❌ ne pas supprimer le dossier racine
        if root == base_path:
            continue

        try:
            # 🔥 check réel du filesystem
            if len(os.listdir(root)) == 0:
                os.rmdir(root)
                print(f"🧹 Removed empty folder → {root}")

        except Exception as e:
            print(f"⚠️ Could not remove {root}: {e}")

# =====================================================
# MAIN FUNCTION
# =====================================================
def upload_files_to_raw(**context):

    execution_date = context["ds"]
    year, month, day = execution_date.split("-")

    # Nettoyage du nom du bucket
    bucket_name = RAW_BUCKET.replace("s3a://", "").replace("s3://", "")

    # Connexion MinIO
    client = Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

    # Création bucket si inexistant
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # =====================================================
    # 1️⃣ UPLOAD + ARCHIVE
    # =====================================================
    for root, dirs, files in os.walk(LOCAL_INPUT_DIR):

        # ✅ Exclusion propre des dossiers
        dirs[:] = [d for d in dirs if d not in EXCLUDED_FOLDERS]

        for filename in files:

            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, LOCAL_INPUT_DIR)

            object_path = f"{year}/{month}/{relative_path}"

            try:
                # ---------------------------
                # Upload vers MinIO
                # ---------------------------
                client.fput_object(
                    bucket_name=bucket_name,
                    object_name=object_path,
                    file_path=local_path
                )

                print(f"✅ Uploaded {relative_path} → s3a://{bucket_name}/{object_path}")

                # ---------------------------
                # Vérification
                # ---------------------------
                client.stat_object(bucket_name, object_path)
                print(f"✔️ Verified in MinIO: {object_path}")

                # ---------------------------
                # Archivage local
                # ---------------------------
                archive_path = os.path.join(ARCHIVE_DIR, relative_path)

                os.makedirs(os.path.dirname(archive_path), exist_ok=True)

                shutil.move(local_path, archive_path)

                print(f"📦 Archived → {archive_path}")

            except Exception as e:
                print(f"❌ Error processing {relative_path}: {e}")

    # =====================================================
    # 2️⃣ CLEAN DOSSIERS VIDES
    # =====================================================
    clean_empty_dirs(LOCAL_INPUT_DIR)

    print("🎯 Upload + Archive + Cleanup terminé")



import zipfile

def zip_archive(**context):
    """
    Zip uniquement les dossiers présents dans archive/
    puis les supprime après compression
    """

    timestamp = datetime.now().strftime("%Y%m%d%H%M")

    zip_filename = f"{timestamp}.zip"
    zip_path = os.path.join(ARCHIVE_DIR, zip_filename)

    print(f"🗜️ Creating ZIP → {zip_path}")

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:

        # 🔥 On parcourt UNIQUEMENT les dossiers racine dans archive
        for item in os.listdir(ARCHIVE_DIR):

            item_path = os.path.join(ARCHIVE_DIR, item)

            # ❌ ignorer les fichiers zip existants
            if item.endswith(".zip"):
                continue

            # ✅ traiter seulement les dossiers
            if os.path.isdir(item_path):

                for root, dirs, files in os.walk(item_path):
                    for file in files:

                        file_path = os.path.join(root, file)

                        # garder structure relative
                        arcname = os.path.relpath(file_path, ARCHIVE_DIR)

                        zipf.write(file_path, arcname)

                        print(f"➕ Added: {arcname}")

                # 🔥 SUPPRIMER LE DOSSIER APRÈS ZIP
                shutil.rmtree(item_path)
                print(f"🧹 Removed archived folder → {item_path}")

    print(f"✅ ZIP created → {zip_path}")
# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="01-ING__Ingest_data_to_Raw",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["raw", "INS"],
) as dag:

    upload_to_raw = PythonOperator(
        task_id="upload_local_files_to_raw",
        python_callable=upload_files_to_raw,
        provide_context=True,
    )

    zip_task = PythonOperator(
        task_id="zip_archive_files",
        python_callable=zip_archive,
        provide_context=True,
    )

    
    upload_to_raw >> zip_task
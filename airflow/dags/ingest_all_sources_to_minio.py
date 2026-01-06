import csv
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

# ============================
# 🔧 CONFIGURATION
# ============================
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "raw"

API_BASE_URL = "http://dataportal.ins.tn/WebApi/"

# ============================
# 🛠 UTILS
# ============================
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def post_xml(url, body):
    headers = {'Content-Type': 'application/xml'}
    try:
        response = requests.post(url, data=body, headers=headers, timeout=120) # Timeout long car GetData peut être lent
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"❌ Erreur HTTP : {e}")
        return None

# =========================================
# 1️⃣ ÉTAPE 1 : LISTER TOUTES LES SOURCES
# ========================================
def fetch_sources_metadata(**kwargs):
    print("📡 Récupération de la structure globale (GetStructure)...")
    xml_data = post_xml(f"{API_BASE_URL}GetStructure", "<QueryMessage></QueryMessage>")
    
    if not xml_data:
        raise Exception("Impossible de récupérer la structure.")

    root = ET.fromstring(xml_data)
    sources_list = []

    # On parcourt toutes les balises <Source>
    for source in root.findall(".//Source"):
        s_id = source.attrib.get('Id')
        s_name = source.attrib.get('Name')
        
        # Récupération de la période (StartYear / FinishYear)
        # Parfois la balise Period n'existe pas ou est vide, on met des valeurs par défaut
        period = source.find("Period")
        start_year = period.find("StartYear").text if period is not None and period.find("StartYear") is not None else "2000"
        finish_year = period.find("FinishYear").text if period is not None and period.find("FinishYear") is not None else str(datetime.now().year)

        sources_list.append({
            "id": s_id,
            "name": s_name,
            "start": start_year,
            "end": finish_year
        })

    print(f"✅ {len(sources_list)} sources identifiées.")
    return sources_list

# =================================================
# 2️⃣ ÉTAPE 2 : BOUCLE SUR LES SOURCES ET INGESTION
# =================================================
def ingest_all_datasets(**kwargs):
    ti = kwargs['ti']
    sources = ti.xcom_pull(task_ids='fetch_structure')
    
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    print(f"🚀 Début de l'ingestion pour {len(sources)} sources...")

    for i, source in enumerate(sources):
        s_id = source['id']
        s_name = source['name'].strip()

        # Nettoyage du nom pour pouvoir l'utiliser dans un chemin et un fichier
        safe_name = s_name.replace(" ", "_").replace("/", "_").replace("'", "").replace("(", "").replace(")", "")

        start = source['start']
        end = source['end']

        print(f"\n[{i+1}/{len(sources)}] Traitement : {s_id} - {safe_name} ({start}-{end})")

        body = f"""
        <QueryMessage SourceId='{s_id}'>
            <Period From='{start}' To='{end}' Frequency='Y'/>
            <DataWhere></DataWhere>
        </QueryMessage>
        """

        xml_resp = post_xml(f"{API_BASE_URL}GetData", body)
        
        if not xml_resp:
            print(f"⚠️ Pas de réponse pour {s_id}")
            continue

        try:
            root = ET.fromstring(xml_resp)
            sets = root.findall('Set')

            if not sets:
                print(f"⚠️ Aucun dataset pour {s_id}")
                continue

            # Génération CSV dynamique
            rows = []
            all_keys = set()

            for s in sets:
                row = s.attrib.copy()
                row['value'] = s.text
                rows.append(row)
                all_keys.update(row.keys())

            # Construction CSV en mémoire
            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=list(all_keys))
            writer.writeheader()
            writer.writerows(rows)

            csv_bytes = csv_buffer.getvalue().encode("utf-8")
            from io import BytesIO
            data_stream = BytesIO(csv_bytes)

            # ========================================================
            # 📌 STRUCTURE MINIO DEMANDÉE : ID - FolderName(filename)
            # ========================================================
            folder_name = f"{s_id}-{safe_name}"
            file_name = f"{safe_name}.csv"
            object_path = f"sourcesINS/{folder_name}/{file_name}"

            print(f"📤 Upload → {object_path}")

            client.put_object(
                BUCKET_NAME,
                object_path,
                data_stream,
                len(csv_bytes),
                content_type="text/csv"
            )

            print(f"✅ Upload réussi : {object_path}")

        except Exception as e:
            print(f"❌ Erreur traitement {s_id} : {e}")

# =====================================
# 📅 DÉFINITION DU DAG
# =====================================
with DAG(
    dag_id="ingest_all_ins_sources",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly", # On lance ça une fois par mois par exemple
    catchup=False,
    tags=['ins', 'minio', 'ingestion']
) as dag:

    # Tâche 1 : Lister les sources
    task_fetch_structure = PythonOperator(
        task_id="fetch_structure",
        python_callable=fetch_sources_metadata,
    )

    # Tâche 2 : Télécharger et uploader chaque source
    task_ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_all_datasets,
        provide_context=True,
    )

    task_fetch_structure >> task_ingest_data
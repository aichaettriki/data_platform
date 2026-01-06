import csv
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import pandas as pd
import unicodedata
import re
import time

# ============================
# 🔧 CONFIGURATION
# ============================
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_RAW = "raw"
BASE_URL = "http://dataportal.ins.tn/WebApi/"

# Dossier temporaire local
OUTPUT_FOLDER = "/opt/airflow/data/dimensionsINS/"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

# ============================
# 🧹 UTILS (Nettoyage)
# ============================
def clean_filename(text):
    """Nettoie les noms de fichiers pour MinIO (enlève accents et tabulations)"""
    if not text: return "unknown"
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^\w\s-]', '', text).strip().lower()
    return re.sub(r'[-\s]+', '_', text)

def post_xml(url, body_xml):
    headers = {"Content-Type": "application/xml"}
    try:
        response = requests.post(url, data=body_xml, headers=headers, timeout=60)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"   ⚠️ Erreur HTTP (Retry dans 2s) : {e}")
        time.sleep(2)
        response = requests.post(url, data=body_xml, headers=headers, timeout=60)
        response.raise_for_status()
        return response.text

# ============================
# 🧠 CŒUR DU SYSTÈME : PARSING RÉCURSIF
# ============================
def parse_recursive(element, parent_key=None, dim_id=None):
    """
    Cette fonction transforme n'importe quelle structure XML (plate ou arbre)
    en une liste plate de dictionnaires avec une colonne 'parent_key'.
    """
    # 1. On capture TOUS les attributs de la balise actuelle (Dynamique)
    # On met les clés en MAJUSCULES (KEY, NAME, UNIT...) pour standardiser
    row = {k.upper(): v for k, v in element.attrib.items()}
    
    # 2. On ajoute les infos de structure qu'on a calculées
    row['dimension_id'] = dim_id
    row['parent_key'] = parent_key  # <--- C'est ici qu'on gère la hiérarchie
    
    # On initialise la liste de résultats avec l'élément courant
    results = [row]
    
    # 3. On récupère la clé de l'élément courant (pour dire aux enfants qui est leur père)
    my_key = row.get('KEY')
    
    # 4. On cherche les enfants directs (./Element)
    children = element.findall("./Element")
    
    # 5. Si on trouve des enfants, on rappelle la fonction (Récursivité)
    for child in children:
        # On passe 'my_key' comme 'parent_key' pour l'enfant
        child_results = parse_recursive(child, parent_key=my_key, dim_id=dim_id)
        results.extend(child_results)
            
    return results

# ============================
# 🔽 1. LISTER LES DIMENSIONS
# ============================
def extract_dimensions():
    print("👉 Appel API GetStructure...")
    xml = post_xml(BASE_URL + "GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(xml)
    unique_dims = {}

    for source in root.findall(".//Source"):
        for dim in source.findall("./Dimensions/Dimension"):
            dim_id = dim.attrib.get("Id")
            if dim_id not in unique_dims:
                unique_dims[dim_id] = {
                    "id": dim_id,
                    "name": dim.attrib.get("Name", "Unknown")
                }
    
    dims = list(unique_dims.values())
    print(f"✔ {len(dims)} dimensions uniques identifiées.")
    return dims

# ============================
# 🔽 2. EXTRACTION ET SAUVEGARDE
# ============================
def extract_dimension_elements(**context):
    dimensions = extract_dimensions()
    output_files = []

    print(f"👉 Début du traitement récursif pour {len(dimensions)} dimensions...")

    for i, dim in enumerate(dimensions):
        dim_id = dim["id"]
        dim_name = dim["name"]
        
        # Nom de fichier propre
        safe_name = clean_filename(dim_name)
        file_name = f"{dim_id}_{safe_name}.csv"
        file_path = os.path.join(OUTPUT_FOLDER, file_name)

        print(f"[{i+1}/{len(dimensions)}] 🔎 {dim_id} ({dim_name})")

        body = f"<QueryMessage><DataWhere><DimensionId WithData='true'>{dim_id}</DimensionId></DataWhere></QueryMessage>"

        try:
            xml_resp = post_xml(BASE_URL + "GetDimensionElements", body)
            root = ET.fromstring(xml_resp)
            
            # On cherche le conteneur principal <Elements>
            elements_container = root.find("Elements")
            
            if elements_container is None:
                # Création d'un CSV vide avec header minimum si la dimension est vide
                print("   ⚠️ Vide (Pas de données).")
                with open(file_path, 'w') as f: f.write("dimension_id,KEY,NAME,parent_key\n")
                output_files.append(file_path)
                continue

            # --- Lancement de la Récursivité ---
            all_rows = []
            # On prend les éléments racines (ceux qui sont directement sous <Elements>)
            for top_element in elements_container.findall("./Element"):
                # Pour les racines, le parent est None
                all_rows.extend(parse_recursive(top_element, parent_key=None, dim_id=dim_id))

            if not all_rows:
                print("   ⚠️ Vide après parsing.")
                with open(file_path, 'w') as f: f.write("dimension_id,KEY,NAME,parent_key\n")
                output_files.append(file_path)
                continue

            # Conversion en DataFrame Pandas
            df = pd.DataFrame(all_rows)
            
            # Mise en ordre des colonnes (Plus lisible)
            # On force certaines colonnes au début, le reste suit
            first_cols = ['dimension_id', 'KEY', 'NAME', 'parent_key', 'C_UNIT_', 'ISO']
            existing_first = [c for c in first_cols if c in df.columns]
            other_cols = [c for c in df.columns if c not in first_cols]
            
            df = df[existing_first + other_cols]

            # Sauvegarde CSV
            df.to_csv(file_path, index=False)
            output_files.append(file_path)
            print(f"   ✅ Généré : {file_name} ({len(df)} lignes)")

        except Exception as e:
            print(f"   ❌ Erreur critique : {e}")

    return output_files

# ============================
# 🔽 3. UPLOAD MINIO
# ============================
def upload_to_minio(**context):
    file_paths = context["ti"].xcom_pull(task_ids="extract_ins_dimensions")
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)

    print(f"📦 Upload vers MinIO ({len(file_paths)} fichiers)...")

    for file_path in file_paths:
        filename = os.path.basename(file_path)
        object_name = f"dimensions/{filename}"
        
        try:
            client.fput_object(BUCKET_RAW, object_name, file_path)
        except Exception as e:
            print(f"   ❌ Erreur upload {filename} : {e}")
            
    print("✅ Pipeline terminé.")

# ============================
# 🚀 DAG
# ============================
with DAG(
    dag_id="etl_ins_dimensions_recursive",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id="extract_ins_dimensions",
        python_callable=extract_dimension_elements,
        provide_context=True,
    )

    task_upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        provide_context=True,
    )

    task_extract >> task_upload
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import csv
import io
from minio import Minio
import logging
import re
import pandas as pd
from sqlalchemy import create_engine
 
# =====================================================
# CONFIG
# =====================================================
API_BASE = "http://dataportal.ins.tn/WebApi/"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "01-raw"
 
# DATE_PATH = datetime.today().strftime("%Y/%m/%d")
today = datetime.today()
YEAR = today.strftime("%Y")
MONTH = today.strftime("%m")
 
DATE_PATH = f"{YEAR}/{MONTH}"
# =====================================================
# LOGGING
# =====================================================
log = logging.getLogger("INS_SOURCE_INGEST")
log.setLevel(logging.INFO)
 
# =====================================================
# HELPERS
# =====================================================
def sanitize(name):
    """Sanitize string pour un nom de fichier sûr"""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
 
def get_minio_client():
    """Retourne un client MinIO"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
 
def post_xml(endpoint, body):
    """Appel POST à l'API INS et parse XML"""
    r = requests.post(
        API_BASE + endpoint,
        data=body,
        headers={"Content-Type": "application/xml"},
        timeout=120,
    )
    r.raise_for_status()
    return ET.fromstring(r.text)
 
def read_minio_csv(client, path):
    try:
        response = client.get_object(BUCKET, path)
        text = response.read().decode("utf-8")
 
        # Auto-détection du séparateur
        return pd.read_csv(io.StringIO(text), sep=None, engine="python")
 
    except Exception as e:
        log.warning(f"⚠️ Could not read MinIO CSV at {path}: {e}")
        return pd.DataFrame()
 
 
def write_minio_csv(client, df, path):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, sep=",")
    csv_bytes = buffer.getvalue().encode("utf-8")
 
    data_stream = io.BytesIO(csv_bytes)
    data_stream.seek(0)
 
    client.put_object(
        BUCKET,
        path,
        data=data_stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )
 
    log.info(f"💾 Written → s3://{BUCKET}/{path}")
 
 
# =====================================================
# FONCTIONS SPECIFIQUES
# =====================================================
def fetch_ins_structure():
    """Récupère toutes les sources depuis l'API INS"""
    log.info("📡 Fetching INS structure")
    root = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
    sources = []
    for src in root.findall(".//Source"):
        sources.append({
            "id": src.attrib["Id"],
            "name": src.attrib["Name"],
            "start_year": int(src.find("./Period/StartYear").text),
            "end_year": int(src.find("./Period/FinishYear").text),
            "dimensions": [d.attrib["Id"] for d in src.findall("./Dimensions/Dimension")]
        })
    return sources
 
def fetch_api_data(source):
    """Récupère les données d'une source depuis l'API et retourne DataFrame"""
    body = f"""
    <QueryMessage SourceId='{source['id']}'>
        <Period From='{source['start_year']}' To='{source['end_year']}' Frequency='Y'/>
        <DataWhere></DataWhere>
    </QueryMessage>
    """
    data_root = post_xml("GetData", body)
    rows = []
    for s in data_root.findall(".//Set"):
        year = s.attrib.get("Period", "").replace("YEARS:", "")
        row = {"year": year}
        for dim in source["dimensions"]:
            row[f"{dim}_id"] = dim
            row[f"{dim}_key"] = s.attrib.get(dim, "")
        row["value"] = s.text.strip() if s.text else ""
        rows.append(row)
    return pd.DataFrame(rows)
 

# =====================================================
# TASK PRINCIPALE SNAPSHOT (SANS MERGE)
# =====================================================
def ingest_all_sources():

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    sources = fetch_ins_structure()

    for source in sources:
        log.info("=" * 80)
        log.info(f"🔎 SOURCE {source['id']} | {source['name']}")

        # ── 1️⃣ Récupération complète depuis API
        api_df = fetch_api_data(source)

        log.info(f"📊 Snapshot API récupéré : {len(api_df)} lignes")

        # ── 2️⃣ Path basé sur la date courante
        object_path = (
            f"{DATE_PATH}/INS/API-Sources/"
            f"{source['id']}-{sanitize(source['name'])}.csv"
        )

        # ── 3️⃣ Écriture directe (overwrite si même mois)
        write_minio_csv(client, api_df, object_path)

        log.info(f"💾 Snapshot écrit pour la source {source['id']}")

    log.info("🎉 Toutes les sources INS ont été ingérées (mode snapshot)")

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="01-ING__ingest_all_ins_sources",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["API", "INS", "RAW", "Sources"],
) as dag:
 
    PythonOperator(
        task_id="ingest_all_ins_sources",
        python_callable=ingest_all_sources,
    )



# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import requests
# import xml.etree.ElementTree as ET
# import csv
# import io
# from minio import Minio
# import logging
# import re
# import pandas as pd
# from sqlalchemy import create_engine
 
# # =====================================================
# # CONFIG
# # =====================================================
# API_BASE = "http://dataportal.ins.tn/WebApi/"
# MINIO_ENDPOINT = "minio:9000"
# MINIO_ACCESS_KEY = "minio"
# MINIO_SECRET_KEY = "minio123"
# BUCKET = "01-raw"
 
# # DATE_PATH = datetime.today().strftime("%Y/%m/%d")
# today = datetime.today()
# YEAR = today.strftime("%Y")
# MONTH = today.strftime("%m")
 
# DATE_PATH = f"{YEAR}/{MONTH}"
# # =====================================================
# # LOGGING
# # =====================================================
# log = logging.getLogger("INS_SOURCE_INGEST")
# log.setLevel(logging.INFO)
 
# # =====================================================
# # HELPERS
# # =====================================================
# def sanitize(name):
#     """Sanitize string pour un nom de fichier sûr"""
#     return re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
 
# def get_minio_client():
#     """Retourne un client MinIO"""
#     return Minio(
#         MINIO_ENDPOINT,
#         access_key=MINIO_ACCESS_KEY,
#         secret_key=MINIO_SECRET_KEY,
#         secure=False,
#     )
 
# def post_xml(endpoint, body):
#     """Appel POST à l'API INS et parse XML"""
#     r = requests.post(
#         API_BASE + endpoint,
#         data=body,
#         headers={"Content-Type": "application/xml"},
#         timeout=120,
#     )
#     r.raise_for_status()
#     return ET.fromstring(r.text)
 
# def read_minio_csv(client, path):
#     try:
#         response = client.get_object(BUCKET, path)
#         text = response.read().decode("utf-8")
 
#         # Auto-détection du séparateur
#         return pd.read_csv(io.StringIO(text), sep=None, engine="python")
 
#     except Exception as e:
#         log.warning(f"⚠️ Could not read MinIO CSV at {path}: {e}")
#         return pd.DataFrame()
 
 
# def write_minio_csv(client, df, path):
#     buffer = io.StringIO()
#     df.to_csv(buffer, index=False, sep=",")
#     csv_bytes = buffer.getvalue().encode("utf-8")
 
#     data_stream = io.BytesIO(csv_bytes)
#     data_stream.seek(0)
 
#     client.put_object(
#         BUCKET,
#         path,
#         data=data_stream,
#         length=len(csv_bytes),
#         content_type="text/csv",
#     )
 
#     log.info(f"💾 Written → s3://{BUCKET}/{path}")
 
 
# # =====================================================
# # FONCTIONS SPECIFIQUES
# # =====================================================
# def fetch_ins_structure():
#     """Récupère toutes les sources depuis l'API INS"""
#     log.info("📡 Fetching INS structure")
#     root = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
#     sources = []
#     for src in root.findall(".//Source"):
#         sources.append({
#             "id": src.attrib["Id"],
#             "name": src.attrib["Name"],
#             "start_year": int(src.find("./Period/StartYear").text),
#             "end_year": int(src.find("./Period/FinishYear").text),
#             "dimensions": [d.attrib["Id"] for d in src.findall("./Dimensions/Dimension")]
#         })
#     return sources
 
# def fetch_api_data(source):
#     """Récupère les données d'une source depuis l'API et retourne DataFrame"""
#     body = f"""
#     <QueryMessage SourceId='{source['id']}'>
#         <Period From='{source['start_year']}' To='{source['end_year']}' Frequency='Y'/>
#         <DataWhere></DataWhere>
#     </QueryMessage>
#     """
#     data_root = post_xml("GetData", body)
#     rows = []
#     for s in data_root.findall(".//Set"):
#         year = s.attrib.get("Period", "").replace("YEARS:", "")
#         row = {"year": year}
#         for dim in source["dimensions"]:
#             row[f"{dim}_id"] = dim
#             row[f"{dim}_key"] = s.attrib.get(dim, "")
#         row["value"] = s.text.strip() if s.text else ""
#         rows.append(row)
#     return pd.DataFrame(rows)
 
# def detect_new_rows(minio_df, api_df, dimensions):
#     """
#     Détecte les nouvelles lignes via SQL LEFT JOIN
#     Version robuste qui s'adapte aux colonnes réelles
#     """
 
#     if minio_df.empty:
#         return api_df.copy()
#     log.info(f"Colonnes API: {api_df.columns.tolist()}")
#     log.info(f"Colonnes MinIO: {minio_df.columns.tolist()}")
#     # Colonnes communes entre les deux DF
#     common_columns = list(set(api_df.columns).intersection(set(minio_df.columns)))
 
#     if not common_columns:
#         log.warning("⚠️ Aucune colonne commune entre API et MinIO.")
#         return api_df.copy()
 
#     engine = create_engine("sqlite:///:memory:")
 
#     minio_df.to_sql("minio_table", engine, index=False, if_exists="replace")
#     api_df.to_sql("api_table", engine, index=False, if_exists="replace")
 
#     # Construction dynamique de la jointure
#     join_conditions = [f"a.{col} = m.{col}" for col in common_columns]
#     join_sql = " AND ".join(join_conditions)
 
#     # On teste sur la première colonne commune
#     first_col = common_columns[0]
 
#     sql_query = f"""
#     SELECT a.*
#     FROM api_table a
#     LEFT JOIN minio_table m
#       ON {join_sql}
#     WHERE m.{first_col} IS NULL
#     """
 
#     new_rows_df = pd.read_sql(sql_query, engine)
 
#     return new_rows_df
 
# # =====================================================
# # TASK PRINCIPALE AVEC LOGS DETAILLES
# # =====================================================
# def ingest_all_sources():
#     client = get_minio_client()
#     if not client.bucket_exists(BUCKET):
#         client.make_bucket(BUCKET)
 
#     sources = fetch_ins_structure()
 
#     for source in sources:
#         log.info("=" * 80)
#         log.info(f"🔎 SOURCE {source['id']} | {source['name']}")
 
#         # Récupérer données API
#         api_df = fetch_api_data(source)
 
#         # Path MinIO
#         object_path = f"{DATE_PATH}/INS/API-Sources/{source['id']}-{sanitize(source['name'])}.csv"
#         minio_df = read_minio_csv(client, object_path)
 
#         # Logs sur nombre de lignes
#         log.info(f"📊 Lignes MinIO: {len(minio_df)}, Lignes API: {len(api_df)}")
 
#         # Détecter nouvelles lignes via SQL LEFT JOIN
#         new_rows_df = detect_new_rows(minio_df, api_df, source["dimensions"])
#         log.info(f"🟢 {len(new_rows_df)} nouvelles lignes à insérer pour la source {source['id']}:")
#         if not new_rows_df.empty:
#             # log.info(f"🟢 {len(new_rows_df)} nouvelles lignes à insérer pour la source {source['id']}:")
#             # for _, row in new_rows_df.iterrows():
#             #     log.info(f"    {row.to_dict()}")
 
#             # Append-only → concat avec l'ancien DF
#             updated_df = pd.concat([minio_df, new_rows_df], ignore_index=True)
#             write_minio_csv(client, updated_df, object_path)
#         else:
#             log.info(f"✅ Aucun ajout détecté pour la source {source['id']}")
 
#     log.info("🎉 Toutes les sources ont été ingérées et comparées avec succès")
 
# # =====================================================
# # DAG
# # =====================================================
# with DAG(
#     dag_id="01-ING__ingest_all_ins_sources",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["API", "INS", "RAW", "Sources"],
# ) as dag:
 
#     PythonOperator(
#         task_id="ingest_all_ins_sources",
#         python_callable=ingest_all_sources,
#     )

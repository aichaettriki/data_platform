from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import io
import logging
from minio import Minio
from minio.error import S3Error

# =====================================================
# CONFIG
# =====================================================
WORLD_BANK_BASE = "https://api.worldbank.org/v2/country/all/indicator"
MINIO_ENDPOINT  = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "01-raw"

today      = datetime.today()
YEAR       = today.strftime("%Y")
MONTH      = today.strftime("%m")
DATE_PATH  = f"{YEAR}/{MONTH}"

INDICATORS = {
    "NY.GDP.MKTP.CD": "PIB_USD_courants",
    "SP.POP.TOTL":    "Population_Totale",
    "HD.HCI.OVRL":    "Human_Capital_Index_HCI"
}

# Colonnes qui forment la CLÉ UNIQUE d'une ligne
KEY_COLS = ["country_iso3", "indicator_id", "year"]

# Colonnes à comparer pour détecter les changements (tout sauf la clé)
COMPARE_COLS = [
    "value", "country_name", "indicator_name",
    "unit", "obs_status", "decimal", "lastupdated"
]

# =====================================================
# LOGGING
# =====================================================
log = logging.getLogger("WORLD_BANK_INGEST")
log.setLevel(logging.INFO)

# =====================================================
# MINIO CLIENT
# =====================================================
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

# =====================================================
# WRITE CSV TO MINIO  (écrase le fichier existant)
# =====================================================
def write_minio_csv(client, df, path):
    buffer    = io.StringIO()
    df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")
    stream    = io.BytesIO(csv_bytes)
    stream.seek(0)
    client.put_object(
        BUCKET, path,
        data=stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    log.info(f"💾 Fichier écrit → s3://{BUCKET}/{DATE_PATH}/{path}  ({len(df)} lignes)")

# =====================================================
# READ CSV FROM MINIO  (retourne None si absent)
# =====================================================
def read_minio_csv(client, path):
    """
    Tente de lire le CSV existant dans MinIO.
    Retourne un DataFrame si le fichier existe, sinon None.
    """
    try:
        response = client.get_object(BUCKET, path)
        content  = response.read()
        df       = pd.read_csv(io.BytesIO(content), dtype=str)   # tout en str pour la comparaison
        log.info(f"📂 Fichier existant trouvé → s3://{BUCKET}/{path}  ({len(df)} lignes)")
        return df
    except S3Error as e:
        if e.code == "NoSuchKey":
            log.info(f"🆕 Aucun fichier existant pour → s3://{BUCKET}/{path}")
            return None
        raise

# =====================================================
# FETCH WORLD BANK DATA  (toutes les pages)
# =====================================================
def fetch_worldbank_data(indicator):
    base_url = f"{WORLD_BANK_BASE}/{indicator}"
    params   = {"format": "json", "per_page": 1000, "page": 1}

    log.info(f"📡 [{indicator}] Récupération page 1 pour détecter le nombre de pages...")
    resp     = requests.get(base_url, params=params, timeout=120)
    resp.raise_for_status()
    data     = resp.json()
    metadata = data[0]
    total_pages = metadata["pages"]
    log.info(f"📄 [{indicator}] Nombre total de pages : {total_pages}")

    all_rows = []
    for page in range(1, total_pages + 1):
        log.info(f"   ↳ Fetching page {page}/{total_pages}")
        params["page"] = page
        resp     = requests.get(base_url, params=params, timeout=120)
        resp.raise_for_status()
        json_data = resp.json()
        metadata  = json_data[0]
        rows      = json_data[1]
        if rows is None:
            continue
        for row in rows:
            all_rows.append({
                "page":           str(metadata["page"]),
                "pages":          str(metadata["pages"]),
                "per_page":       str(metadata["per_page"]),
                "total":          str(metadata["total"]),
                "sourceid":       str(metadata["sourceid"]),
                "lastupdated":    str(metadata["lastupdated"]),
                "indicator_id":   row["indicator"]["id"],
                "indicator_name": row["indicator"]["value"],
                "country_id":     row["country"]["id"],
                "country_name":   row["country"]["value"],
                "country_iso3":   str(row["countryiso3code"]),
                "year":           str(row["date"]),
                "value":          str(row["value"]),
                "unit":           str(row["unit"]),
                "obs_status":     str(row["obs_status"]),
                "decimal":        str(row["decimal"]),
            })

    df_api = pd.DataFrame(all_rows)
    log.info(f"✅ [{indicator}] Total lignes récupérées depuis l'API : {len(df_api)}")
    return df_api


# =====================================================
# MAIN INGEST FUNCTION (SIMPLE SNAPSHOT LOAD)
# =====================================================
def ingest_worldbank_indicator(indicator, folder_name):

    log.info("")
    log.info(f"{'='*60}")
    log.info(f"🚀 DÉMARRAGE INGESTION : {indicator} ({folder_name})")
    log.info(f"{'='*60}")

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info(f"🪣 Bucket '{BUCKET}' créé")

    # Chemin dynamique basé sur la date courante
    object_path = f"{DATE_PATH}/WORLD_BANK/{folder_name}/{indicator}.csv"

    # ── ÉTAPE 1 : Récupération API ─────────────────────
    df_api = fetch_worldbank_data(indicator)

    log.info(f"📦 Snapshot récupéré : {len(df_api)} lignes")

    # ── ÉTAPE 2 : Écriture directe (overwrite si existe déjà ce mois) ────
    write_minio_csv(client, df_api, object_path)

    log.info(f"🏁 SNAPSHOT INGESTION TERMINÉE : {indicator}")
    log.info(f"{'='*60}")

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="01-ING__ingest_worldbank_indicators",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["API", "WORLD_BANK", "RAW"],
) as dag:

    for indicator, folder in INDICATORS.items():
        PythonOperator(
            task_id=f"ingest_{indicator.replace('.', '_')}",
            python_callable=ingest_worldbank_indicator,
            op_kwargs={
                "indicator":   indicator,
                "folder_name": folder
            },
        )
        

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import requests
# import pandas as pd
# import io
# import logging
# from minio import Minio
# from minio.error import S3Error

# # =====================================================
# # CONFIG
# # =====================================================
# WORLD_BANK_BASE = "https://api.worldbank.org/v2/country/all/indicator"
# MINIO_ENDPOINT  = "minio:9000"
# MINIO_ACCESS_KEY = "minio"
# MINIO_SECRET_KEY = "minio123"
# BUCKET = "01-raw"

# today      = datetime.today()
# YEAR       = today.strftime("%Y")
# MONTH      = today.strftime("%m")
# DATE_PATH  = f"{YEAR}/{MONTH}"

# INDICATORS = {
#     "NY.GDP.MKTP.CD": "PIB_USD_courants",
#     "SP.POP.TOTL":    "Population_Totale",
#     "HD.HCI.OVRL":    "Human_Capital_Index_HCI"
# }

# # Colonnes qui forment la CLÉ UNIQUE d'une ligne
# KEY_COLS = ["country_iso3", "indicator_id", "year"]

# # Colonnes à comparer pour détecter les changements (tout sauf la clé)
# COMPARE_COLS = [
#     "value", "country_name", "indicator_name",
#     "unit", "obs_status", "decimal", "lastupdated"
# ]

# # =====================================================
# # LOGGING
# # =====================================================
# log = logging.getLogger("WORLD_BANK_INGEST")
# log.setLevel(logging.INFO)

# # =====================================================
# # MINIO CLIENT
# # =====================================================
# def get_minio_client():
#     return Minio(
#         MINIO_ENDPOINT,
#         access_key=MINIO_ACCESS_KEY,
#         secret_key=MINIO_SECRET_KEY,
#         secure=False,
#     )

# # =====================================================
# # WRITE CSV TO MINIO  (écrase le fichier existant)
# # =====================================================
# def write_minio_csv(client, df, path):
#     buffer    = io.StringIO()
#     df.to_csv(buffer, index=False)
#     csv_bytes = buffer.getvalue().encode("utf-8")
#     stream    = io.BytesIO(csv_bytes)
#     stream.seek(0)
#     client.put_object(
#         BUCKET, path,
#         data=stream,
#         length=len(csv_bytes),
#         content_type="text/csv",
#     )
#     log.info(f"💾 Fichier écrit → s3://{BUCKET}/{DATE_PATH}/{path}  ({len(df)} lignes)")

# # =====================================================
# # READ CSV FROM MINIO  (retourne None si absent)
# # =====================================================
# def read_minio_csv(client, path):
#     """
#     Tente de lire le CSV existant dans MinIO.
#     Retourne un DataFrame si le fichier existe, sinon None.
#     """
#     try:
#         response = client.get_object(BUCKET, path)
#         content  = response.read()
#         df       = pd.read_csv(io.BytesIO(content), dtype=str)   # tout en str pour la comparaison
#         log.info(f"📂 Fichier existant trouvé → s3://{BUCKET}/{path}  ({len(df)} lignes)")
#         return df
#     except S3Error as e:
#         if e.code == "NoSuchKey":
#             log.info(f"🆕 Aucun fichier existant pour → s3://{BUCKET}/{path}")
#             return None
#         raise

# # =====================================================
# # FETCH WORLD BANK DATA  (toutes les pages)
# # =====================================================
# def fetch_worldbank_data(indicator):
#     base_url = f"{WORLD_BANK_BASE}/{indicator}"
#     params   = {"format": "json", "per_page": 1000, "page": 1}

#     log.info(f"📡 [{indicator}] Récupération page 1 pour détecter le nombre de pages...")
#     resp     = requests.get(base_url, params=params, timeout=120)
#     resp.raise_for_status()
#     data     = resp.json()
#     metadata = data[0]
#     total_pages = metadata["pages"]
#     log.info(f"📄 [{indicator}] Nombre total de pages : {total_pages}")

#     all_rows = []
#     for page in range(1, total_pages + 1):
#         log.info(f"   ↳ Fetching page {page}/{total_pages}")
#         params["page"] = page
#         resp     = requests.get(base_url, params=params, timeout=120)
#         resp.raise_for_status()
#         json_data = resp.json()
#         metadata  = json_data[0]
#         rows      = json_data[1]
#         if rows is None:
#             continue
#         for row in rows:
#             all_rows.append({
#                 "page":           str(metadata["page"]),
#                 "pages":          str(metadata["pages"]),
#                 "per_page":       str(metadata["per_page"]),
#                 "total":          str(metadata["total"]),
#                 "sourceid":       str(metadata["sourceid"]),
#                 "lastupdated":    str(metadata["lastupdated"]),
#                 "indicator_id":   row["indicator"]["id"],
#                 "indicator_name": row["indicator"]["value"],
#                 "country_id":     row["country"]["id"],
#                 "country_name":   row["country"]["value"],
#                 "country_iso3":   str(row["countryiso3code"]),
#                 "year":           str(row["date"]),
#                 "value":          str(row["value"]),
#                 "unit":           str(row["unit"]),
#                 "obs_status":     str(row["obs_status"]),
#                 "decimal":        str(row["decimal"]),
#             })

#     df_api = pd.DataFrame(all_rows)
#     log.info(f"✅ [{indicator}] Total lignes récupérées depuis l'API : {len(df_api)}")
#     return df_api

# # =====================================================
# # SMART MERGE  (LEFT JOIN logique avec pandas)
# # =====================================================
# def compute_delta(df_api: pd.DataFrame, df_minio: pd.DataFrame) -> pd.DataFrame:
#     """
#     Compare df_api (source de vérité) avec df_minio (ce qu'on a déjà).
#     Retourne UNIQUEMENT les lignes à insérer :
#       - Nouvelles lignes (clé absente dans MinIO)
#       - Lignes modifiées (clé présente mais au moins 1 colonne COMPARE_COLS différente)
#     """
#     log.info("🔍 Début du calcul du delta (LEFT JOIN logique)...")
#     log.info(f"   API        : {len(df_api)} lignes")
#     log.info(f"   MinIO      : {len(df_minio)} lignes")

#     # ── ÉTAPE 1 : LEFT JOIN sur la clé composite ──────────────────────────
#     # On ajoute un suffixe aux colonnes MinIO pour les distinguer
#     df_joined = df_api.merge(
#         df_minio,
#         on=KEY_COLS,
#         how="left",
#         suffixes=("_api", "_minio")
#     )
#     log.info(f"   Après LEFT JOIN : {len(df_joined)} lignes")

#     # ── ÉTAPE 2 : Nouvelles lignes (aucune correspondance dans MinIO) ──────
#     # Si la clé n'existe pas dans MinIO, la 1ère colonne _minio sera NaN
#     first_compare_minio = f"{COMPARE_COLS[0]}_minio"
#     mask_new = df_joined[first_compare_minio].isna()
#     df_new   = df_api[mask_new.values].copy()

#     log.info(f"   🆕 Nouvelles lignes détectées : {len(df_new)}")
#     if len(df_new) > 0:
#         log.info(f"      Exemples :\n{df_new[KEY_COLS + ['value']].head(3).to_string(index=False)}")

#     # ── ÉTAPE 3 : Lignes modifiées (clé existe mais valeur(s) différente(s)) ─
#     mask_existing = ~mask_new
#     df_existing   = df_joined[mask_existing].copy()

#     # Pour chaque colonne à comparer, on vérifie si api != minio
#     change_mask = pd.Series([False] * len(df_existing), index=df_existing.index)
#     changed_details = {}

#     for col in COMPARE_COLS:
#         col_api   = f"{col}_api"
#         col_minio = f"{col}_minio"
#         if col_api in df_existing.columns and col_minio in df_existing.columns:
#             # IS DISTINCT FROM : gère les NaN/None correctement
#             col_changed = df_existing[col_api].fillna("__NULL__") != df_existing[col_minio].fillna("__NULL__")
#             nb_changed  = col_changed.sum()
#             if nb_changed > 0:
#                 changed_details[col] = nb_changed
#             change_mask = change_mask | col_changed

#     if changed_details:
#         log.info(f"   ✏️  Colonnes avec changements détectés :")
#         for col, nb in changed_details.items():
#             log.info(f"      → '{col}' : {nb} ligne(s) modifiée(s)")
#     else:
#         log.info("   ✅ Aucune modification détectée sur les lignes existantes")

#     # On récupère les lignes API originales qui ont changé (pas la version joinée)
#     changed_indices = df_existing[change_mask].index
#     df_modified     = df_api.loc[changed_indices].copy()

#     log.info(f"   ✏️  Lignes modifiées à insérer : {len(df_modified)}")
#     if len(df_modified) > 0:
#         log.info(f"      Exemples :\n{df_modified[KEY_COLS + ['value']].head(3).to_string(index=False)}")

#     # ── ÉTAPE 4 : Concaténation du delta ──────────────────────────────────
#     df_delta = pd.concat([df_new, df_modified], ignore_index=True)
#     log.info(f"   📦 Delta total à insérer : {len(df_delta)} lignes ({len(df_new)} nouvelles + {len(df_modified)} modifiées)")

#     return df_delta

# # =====================================================
# # MAIN INGEST FUNCTION
# # =====================================================
# def ingest_worldbank_indicator(indicator, folder_name):
#     log.info(f"")
#     log.info(f"{'='*60}")
#     log.info(f"🚀 DÉMARRAGE INGESTION : {indicator} ({folder_name})")
#     log.info(f"{'='*60}")

#     client = get_minio_client()
#     if not client.bucket_exists(BUCKET):
#         client.make_bucket(BUCKET)
#         log.info(f"🪣 Bucket '{BUCKET}' créé")

#     # Chemin du fichier dans MinIO (fixe, sans date pour le fichier principal)
#     object_path = f"{DATE_PATH}/WORLD_BANK/{folder_name}/{indicator}.csv"
#     # ── ÉTAPE 1 : Vérification existence fichier MinIO ────────────────────
#     log.info(f"🔎 Vérification du fichier existant → s3://{BUCKET}/{object_path}")
#     df_minio = read_minio_csv(client, object_path)

#     # ── ÉTAPE 2 : Récupération API ────────────────────────────────────────
#     df_api = fetch_worldbank_data(indicator)

#     # ── ÉTAPE 3 : Premier chargement ou delta ? ───────────────────────────
#     if df_minio is None:
#         log.info(f"")
#         log.info(f"📥 PREMIER CHARGEMENT détecté pour {indicator}")
#         log.info(f"   → Écriture directe de {len(df_api)} lignes dans MinIO")
#         write_minio_csv(client, df_api, object_path)

#     else:
#         log.info(f"")
#         log.info(f"🔄 CHARGEMENT DELTA détecté pour {indicator}")
#         log.info(f"   → Comparaison API vs MinIO en cours...")

#         df_delta = compute_delta(df_api, df_minio)

#         if len(df_delta) == 0:
#             log.info(f"✅ Aucun changement détecté. Fichier MinIO inchangé.")
#         else:
#             log.info(f"📝 Insertion de {len(df_delta)} nouvelles lignes dans MinIO (mode APPEND)")
#             # On concatène les anciennes lignes + le delta → INSERT, jamais UPDATE
#             df_final = pd.concat([df_minio, df_delta], ignore_index=True)
#             log.info(f"   Avant : {len(df_minio)} lignes")
#             log.info(f"   Delta : +{len(df_delta)} lignes")
#             log.info(f"   Après : {len(df_final)} lignes")
#             write_minio_csv(client, df_final, object_path)

#     log.info(f"")
#     log.info(f"🏁 INGESTION TERMINÉE : {indicator}")
#     log.info(f"{'='*60}")

# # =====================================================
# # DAG
# # =====================================================
# with DAG(
#     dag_id="01-ING__ingest_worldbank_indicators",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["API", "WORLD_BANK", "RAW"],
# ) as dag:

#     for indicator, folder in INDICATORS.items():
#         PythonOperator(
#             task_id=f"ingest_{indicator.replace('.', '_')}",
#             python_callable=ingest_worldbank_indicator,
#             op_kwargs={
#                 "indicator":   indicator,
#                 "folder_name": folder
#             },
#         )
        
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import requests
# import pandas as pd
# import io
# import logging
# from minio import Minio

# # =====================================================
# # CONFIG
# # =====================================================

# WORLD_BANK_BASE = "https://api.worldbank.org/v2/country/all/indicator"

# INDICATOR = "NY.GDP.MKTP.CD"   # GDP

# MINIO_ENDPOINT = "minio:9000"
# MINIO_ACCESS_KEY = "minio"
# MINIO_SECRET_KEY = "minio123"

# BUCKET = "01-raw"

# today = datetime.today()
# YEAR = today.strftime("%Y")
# MONTH = today.strftime("%m")

# DATE_PATH = f"{YEAR}/{MONTH}"
# INDICATORS = {
#     "NY.GDP.MKTP.CD": "PIB_USD_courants",
#     "SP.POP.TOTL": "Population_Totale",
#     "HD.HCI.OVRL": "Human_Capital_Index_HCI"
# }
# # =====================================================
# # LOGGING
# # =====================================================

# log = logging.getLogger("WORLD_BANK_INGEST")
# log.setLevel(logging.INFO)

# # =====================================================
# # MINIO CLIENT
# # =====================================================

# def get_minio_client():

#     return Minio(
#         MINIO_ENDPOINT,
#         access_key=MINIO_ACCESS_KEY,
#         secret_key=MINIO_SECRET_KEY,
#         secure=False,
#     )

# # =====================================================
# # READ FROM MINIO
# # =====================================================
# def read_minio_csv(client, path):

#     try:

#         response = client.get_object(BUCKET, path)

#         data = response.read().decode("utf-8")

#         df = pd.read_csv(io.StringIO(data))

#         log.info(f"📥 Existing file loaded → {len(df)} rows")

#         return df

#     except Exception:

#         log.info("📂 No existing file found → first ingestion")

#         return None

# def normalize_types(df):

#     df["indicator_id"] = df["indicator_id"].astype(str)
#     df["country_iso3"] = df["country_iso3"].astype(str)
#     df["year"] = df["year"].astype(str)

#     df["value"] = pd.to_numeric(df["value"], errors="coerce")

#     return df
# # =====================================================
# # JOIN 
# # =====================================================  
# import pandas as pd
# import sqlite3
# import logging

# log = logging.getLogger("WORLD_BANK_INGEST")
# log.setLevel(logging.INFO)
# import pandas as pd

# def detect_new_and_modified_rows(existing_df: pd.DataFrame, api_df: pd.DataFrame):
#     """
#     Compare les DataFrames existants et les nouvelles données de l'API,
#     et retourne :
#       - new_rows : les lignes complètement nouvelles
#       - modified_rows : les lignes existantes mais avec valeurs modifiées

#     Cette version corrige les problèmes de colonnes renommées après merge.
#     """
#     # Colonnes clés pour identifier une ligne unique (à adapter selon ton dataset)
#     key_cols = ['indicator_id', 'country_iso3', 'year']

#     # Merge existant vs API pour détecter les nouvelles ou modifiées
#     merged = existing_df.merge(
#         api_df,
#         on=key_cols,
#         how='outer',
#         indicator=True,
#         suffixes=('_existing', '_api')
#     )

#     # ----------- NOUVELLES LIGNES -----------
#     new_rows = merged[merged['_merge'] == 'right_only']
#     # Garder uniquement les colonnes API et renommer correctement
#     api_cols = [c for c in merged.columns if c.endswith('_api')]
#     new_rows = new_rows[api_cols]
#     new_rows.columns = [c.replace('_api', '') for c in new_rows.columns]

#     # ----------- LIGNES MODIFIÉES -----------
#     # Détecter les lignes présentes dans les deux mais avec au moins une valeur différente
#     mask_both = merged['_merge'] == 'both'

#     # On compare toutes les colonnes API vs existing
#     compare_cols = [c for c in merged.columns if c.endswith('_api')]
#     modified_mask = merged[mask_both][compare_cols].ne(
#         merged[mask_both][[c.replace('_api', '_existing') for c in compare_cols]]
#     ).any(axis=1)

#     modified_rows = merged[mask_both].loc[modified_mask, compare_cols]
#     modified_rows.columns = [c.replace('_api', '') for c in modified_rows.columns]

#     return new_rows, modified_rows

# # =====================================================
# # WRITE CSV TO MINIO
# # =====================================================

# def write_minio_csv(client, df, path):

#     buffer = io.StringIO()

#     df.to_csv(buffer, index=False)

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
# # FETCH WORLD BANK DATA
# # =====================================================

# def fetch_worldbank_data(indicator):

#     base_url = f"{WORLD_BANK_BASE}/{indicator}"

#     params = {
#         "format": "json",
#         "per_page": 1000,
#         "page": 1
#     }

#     log.info("📡 Fetching first page to detect number of pages")

#     response = requests.get(base_url, params=params, timeout=120)
#     response.raise_for_status()

#     data = response.json()

#     metadata = data[0]

#     total_pages = metadata["pages"]

#     log.info(f"Total pages detected: {total_pages}")

#     all_rows = []

#     for page in range(1, total_pages + 1):

#         log.info(f"Fetching page {page}/{total_pages}")

#         params["page"] = page

#         response = requests.get(base_url, params=params, timeout=120)
#         response.raise_for_status()

#         json_data = response.json()

#         metadata = json_data[0]
#         rows = json_data[1]

#         if rows is None:
#             continue

#         for row in rows:

#             all_rows.append({

#                 # metadata
#                 "page": metadata["page"],
#                 "pages": metadata["pages"],
#                 "per_page": metadata["per_page"],
#                 "total": metadata["total"],
#                 "sourceid": metadata["sourceid"],
#                 "lastupdated": metadata["lastupdated"],

#                 # indicator
#                 "indicator_id": row["indicator"]["id"],
#                 "indicator_name": row["indicator"]["value"],

#                 # country
#                 "country_id": row["country"]["id"],
#                 "country_name": row["country"]["value"],
#                 "country_iso3": row["countryiso3code"],

#                 # observation
#                 "year": row["date"],
#                 "value": row["value"],
#                 "unit": row["unit"],
#                 "obs_status": row["obs_status"],
#                 "decimal": row["decimal"]

#             })

#     df = pd.DataFrame(all_rows)

#     return df
# # =====================================================
# # MAIN INGEST FUNCTION
# # =====================================================
# def ingest_worldbank_indicator(indicator, folder_name):

#     log.info("====================================================")
#     log.info(f"🚀 START INGESTION → {indicator}")
#     log.info("====================================================")

#     client = get_minio_client()

#     if not client.bucket_exists(BUCKET):
#         client.make_bucket(BUCKET)

#     object_path = f"{DATE_PATH}/WORLD_BANK/{folder_name}/{indicator}.csv"

#     # -----------------------------
#     # FETCH API DATA
#     # -----------------------------

#     log.info("🌍 Fetching API data")
#     api_df = fetch_worldbank_data(indicator)
#     log.info(f"API rows fetched: {len(api_df)}")

#     # -----------------------------
#     # LOAD EXISTING DATA
#     # -----------------------------
#     existing_df = read_minio_csv(client, object_path)

#     # -----------------------------
#     # NORMALIZE TYPES
#     # -----------------------------
#     if existing_df is not None:
#         existing_df = normalize_types(existing_df)
#     api_df = normalize_types(api_df)

#     # -----------------------------
#     # FIRST LOAD
#     # -----------------------------

#     if existing_df is None:

#         log.info("📥 FIRST LOAD → writing full dataset")

#         write_minio_csv(client, api_df, object_path)

#         log.info(f"✅ {indicator} ingestion completed")

#         return

#     # -----------------------------
#     # DETECT CHANGES
#     # -----------------------------

#     new_rows, modified_rows = detect_new_and_modified_rows(existing_df, api_df)

#     rows_to_append = pd.concat([new_rows, modified_rows])

#     if len(rows_to_append) == 0:

#         log.info("✅ No new or modified rows detected")

#         return

#     log.info(f"📦 Rows to append: {len(rows_to_append)}")

#     log.info(f"📦 Rows to append (preview):")
#     log.info("\n%s", rows_to_append.head(5))  # max 5 lignes
#     # -----------------------------
#     # APPEND
#     # -----------------------------

#     final_df = pd.concat([existing_df, rows_to_append])

#     log.info(f"Final dataset size: {len(final_df)}")

#     write_minio_csv(client, final_df, object_path)

#     log.info("✅ Incremental ingestion completed")

# # =====================================================
# # DAG
# # =====================================================
# with DAG(

#     dag_id="ingest_worldbank_indicators",

#     start_date=datetime(2024, 1, 1),

#     schedule_interval=None,

#     catchup=False,

#     tags=["API", "WORLD_BANK", "RAW"],

# ) as dag:

#     for indicator, folder in INDICATORS.items():

#         PythonOperator(

#             task_id=f"ingest_{indicator.replace('.', '_')}",

#             python_callable=ingest_worldbank_indicator,

#             op_kwargs={
#                 "indicator": indicator,
#                 "folder_name": folder
#             },
#         )
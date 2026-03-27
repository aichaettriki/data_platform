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
WORLD_BANK_COUNTRIES_URL = "https://api.worldbank.org/v2/country"
MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET           = "01-raw"

today     = datetime.today()
YEAR      = today.strftime("%Y")
MONTH     = today.strftime("%m")
DATE_PATH = f"{YEAR}/{MONTH}"

# Chemins de sortie dans MinIO
OBJECT_PATH_RAW   = f"{DATE_PATH}/WORLD_BANK/Countries/worldbank_countries_raw.csv"
OBJECT_PATH_CLEAN = f"{DATE_PATH}/WORLD_BANK/Countries/worldbank_countries_clean.csv"

# =====================================================
# LOGGING
# =====================================================
log = logging.getLogger("WORLD_BANK_COUNTRIES_INGEST")
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
# WRITE CSV TO MINIO
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
    log.info(f"💾 Fichier écrit → s3://{BUCKET}/{path}  ({len(df)} lignes)")

# =====================================================
# FETCH ALL COUNTRIES (toutes les pages)
# =====================================================
def fetch_all_countries():
    params = {
        "format":   "json",
        "per_page": 300,
        "page":     1,
    }

    log.info("📡 Récupération page 1 pour détecter le nombre de pages...")
    resp = requests.get(WORLD_BANK_COUNTRIES_URL, params=params, timeout=60)
    resp.raise_for_status()
    data        = resp.json()
    metadata    = data[0]
    total_pages = metadata["pages"]
    total       = metadata["total"]
    log.info(f"📄 Total entrées : {total} | Nombre de pages : {total_pages}")

    all_rows = []

    for page in range(1, total_pages + 1):
        log.info(f"   ↳ Fetching page {page}/{total_pages}")
        params["page"] = page
        resp = requests.get(WORLD_BANK_COUNTRIES_URL, params=params, timeout=60)
        resp.raise_for_status()
        json_data = resp.json()
        countries = json_data[1]

        if countries is None:
            log.warning(f"   ⚠️  Page {page} vide, ignorée.")
            continue

        for c in countries:
            all_rows.append({
                "id":                c.get("id", ""),
                "iso2_code":         c.get("iso2Code", ""),
                "name":              c.get("name", ""),
                "region_id":         c.get("region", {}).get("id", ""),
                "region_iso2":       c.get("region", {}).get("iso2code", ""),
                "region_name":       c.get("region", {}).get("value", ""),
                # "admin_region_id":   c.get("adminregion", {}).get("id", ""),
                # "admin_region_name": c.get("adminregion", {}).get("value", ""),
                # "income_level_id":   c.get("incomeLevel", {}).get("id", ""),
                # "income_level_name": c.get("incomeLevel", {}).get("value", ""),
                # "lending_type_id":   c.get("lendingType", {}).get("id", ""),
                # "lending_type_name": c.get("lendingType", {}).get("value", ""),
                # "capital_city":      c.get("capitalCity", ""),
                # "longitude":         c.get("longitude", ""),
                # "latitude":          c.get("latitude", ""),
            })

    df = pd.DataFrame(all_rows)
    log.info(f"✅ Total lignes récupérées : {len(df)}")
    return df

# =====================================================
# MAIN INGEST FUNCTION
# =====================================================
def ingest_worldbank_countries():

    log.info("\n" + "="*60)
    log.info("🚀 DÉMARRAGE INGESTION : World Bank Countries")
    log.info("="*60)

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info(f"🪣 Bucket '{BUCKET}' créé")

    # ── Étape 1 : Fetch toutes les pages ───────────────
    df = fetch_all_countries()

    # ── Étape 2 : Stats avant écriture ────────────────
    total         = len(df)
    vrais_pays    = df[df["region_name"] != "Aggregates"]
    agregats      = df[df["region_name"] == "Aggregates"]

    log.info(f"📊 Total entrées      : {total}")
    log.info(f"🌍 Vrais pays         : {len(vrais_pays)}")
    log.info(f"🗂️  Agrégats/Régions  : {len(agregats)}")

    # ── Étape 3 : Écriture brute complète dans MinIO ──
    write_minio_csv(client, df, OBJECT_PATH_RAW)

    # ── Étape 4 : Écriture CSV filtré (vrais pays) ─────
    df_clean = vrais_pays[["id", "iso2_code", "name", "region_id", "region_iso2", "region_name"]]    
    write_minio_csv(client, df_clean, OBJECT_PATH_CLEAN)

    log.info(f"🏁 INGESTION TERMINÉE → CSV brut et CSV pays filtrés écrits")
    log.info("="*60)

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="01-ING__ingest_worldbank_countries",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["API", "WORLD_BANK", "RAW", "COUNTRIES"],
) as dag:

    PythonOperator(
        task_id="ingest_worldbank_countries",
        python_callable=ingest_worldbank_countries,
    )
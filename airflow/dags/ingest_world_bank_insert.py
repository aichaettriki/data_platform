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
WORLD_BANK_BASE  = "https://api.worldbank.org/v2/country/all/indicator"
MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET           = "01-raw"

INDICATORS = {
    "NY.GDP.MKTP.CD": "PIB_USD_courants",
    "SP.POP.TOTL":    "Population_Totale",
    "HD.HCI.OVRL":    "Human_Capital_Index_HCI"
}

KEY_COLS = ["country_id", "indicator_id", "year"]

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
# HELPER : nettoyage d'une valeur brute API
# =====================================================
def clean(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("none", "nan", "null") else s

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
# READ CSV FROM MINIO
# =====================================================
def read_minio_csv(client, path):
    try:
        response = client.get_object(BUCKET, path)
        content  = response.read()

        df = pd.read_csv(
            io.BytesIO(content),
            dtype=str,
            keep_default_na=False
        )

        df = df.replace({"None": "", "nan": "", "NaN": "", "null": ""})
        log.info(f"📂 Fichier existant trouvé → s3://{BUCKET}/{path} ({len(df)} lignes)")
        return df

    except S3Error as e:
        if e.code == "NoSuchKey":
            log.info(f"🆕 Aucun fichier existant pour → s3://{BUCKET}/{path}")
            return None
        raise

# =====================================================
# FETCH WORLD BANK DATA
# =====================================================
def fetch_worldbank_data(indicator):
    base_url = f"{WORLD_BANK_BASE}/{indicator}"
    params   = {"format": "json", "per_page": 1000, "page": 1}

    log.info(f"📡 [{indicator}] Récupération page 1 pour détecter le nombre de pages...")
    resp        = requests.get(base_url, params=params, timeout=120)
    resp.raise_for_status()
    data        = resp.json()
    metadata    = data[0]
    total_pages = metadata["pages"]
    log.info(f"📄 [{indicator}] Nombre total de pages : {total_pages}")

    all_rows = []
    for page in range(1, total_pages + 1):
        log.info(f"   ↳ Fetching page {page}/{total_pages}")
        params["page"] = page
        resp      = requests.get(base_url, params=params, timeout=120)
        resp.raise_for_status()
        json_data = resp.json()
        metadata  = json_data[0]
        rows      = json_data[1]
        if rows is None:
            continue
        for row in rows:
            all_rows.append({
                "page":           clean(metadata["page"]),
                "pages":          clean(metadata["pages"]),
                "per_page":       clean(metadata["per_page"]),
                "total":          clean(metadata["total"]),
                "sourceid":       clean(metadata["sourceid"]),
                "lastupdated":    clean(metadata["lastupdated"]),
                "indicator_id":   clean(row["indicator"]["id"]),
                "indicator_name": clean(row["indicator"]["value"]),
                "country_id":     clean(row["country"]["id"]),
                "country_name":   clean(row["country"]["value"]),
                "country_iso3":   clean(row["countryiso3code"]),
                "year":           clean(row["date"]),
                "value":          clean(row["value"]),
                "unit":           clean(row["unit"]),
                "obs_status":     clean(row["obs_status"]),
                "decimal":        clean(row["decimal"]),
            })

    df_api = pd.DataFrame(all_rows)
    log.info(f"✅ [{indicator}] Total lignes récupérées depuis l'API : {len(df_api)}")
    return df_api

# =====================================================
# COMPUTE DELTA avec logs détaillés
# =====================================================
def compute_delta(df_api: pd.DataFrame, df_minio: pd.DataFrame) -> pd.DataFrame:
    log.info("🔍 Début du calcul du delta (LEFT JOIN logique)...")
    log.info(f"   API        : {len(df_api)} lignes")
    log.info(f"   MinIO      : {len(df_minio)} lignes")

    df_api_reset   = df_api.reset_index(drop=True)
    df_minio_reset = df_minio.reset_index(drop=True)

    df_joined = df_api_reset.merge(
        df_minio_reset,
        on=KEY_COLS,
        how="left",
        suffixes=("_api", "_minio")
    )
    log.info(f"   Après LEFT JOIN : {len(df_joined)} lignes")

    first_minio_col = f"{COMPARE_COLS[0]}_minio"
    mask_new        = df_joined[first_minio_col].isna()
    df_new = df_api_reset[mask_new.values].copy()
    log.info(f"   🆕 Nouvelles lignes détectées : {len(df_new)}")
    if len(df_new) > 0:
        log.info(f"      Exemples :\n{df_new[KEY_COLS + ['value']].head(3).to_string(index=False)}")

    mask_existing = ~mask_new
    df_existing   = df_joined[mask_existing].copy()
    change_mask     = pd.Series([False] * len(df_existing), index=df_existing.index)
    changed_details = {}

    # Comparaison détaillée
    for col in COMPARE_COLS:
        col_api   = f"{col}_api"
        col_minio = f"{col}_minio"

        if col_api in df_existing.columns and col_minio in df_existing.columns:
            if df_existing[col_api].dtype == object:
                col_changed = df_existing[col_api].str.strip().str.lower() != df_existing[col_minio].str.strip().str.lower()
            else:
                col_changed = df_existing[col_api] != df_existing[col_minio]

            nb_changed  = int(col_changed.sum())
            if nb_changed > 0:
                changed_details[col] = nb_changed

                # Log des différences
                diffs = df_existing.loc[col_changed, [col_api, col_minio] + KEY_COLS]
                for _, row in diffs.iterrows():
                    log.info(f"      🔹 Diff {col} | API='{row[col_api]}' vs MINIO='{row[col_minio]}' | { {k: row[k] for k in KEY_COLS} }")

            change_mask = change_mask | col_changed

    if changed_details:
        log.info(f"   ✏️ Colonnes avec changements détectés : {changed_details}")
    else:
        log.info("   ✅ Aucune modification détectée sur les lignes existantes")

    df_modified = df_api_reset[mask_existing.values & change_mask.reindex(df_joined.index, fill_value=False).values].copy()
    log.info(f"   ✏️ Lignes modifiées à insérer : {len(df_modified)}")
    if len(df_modified) > 0:
        log.info(f"      Exemples :\n{df_modified[KEY_COLS + ['value']].head(3).to_string(index=False)}")

    df_delta = pd.concat([df_new, df_modified], ignore_index=True)
    log.info(f"   📦 Delta total : {len(df_delta)} lignes ({len(df_new)} nouvelles + {len(df_modified)} modifiées)")
    return df_delta

# =====================================================
# MAIN INGEST FUNCTION
# =====================================================
def ingest_worldbank_indicator(indicator, folder_name):
    log.info("\n" + "="*60)
    log.info(f"🚀 DÉMARRAGE INGESTION : {indicator} ({folder_name})")
    log.info("="*60)

    client = get_minio_client()
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info(f"🪣 Bucket '{BUCKET}' créé")

    object_path = f"WORLD_BANK/{folder_name}/{indicator}.csv"
    log.info(f"🔎 Vérification du fichier existant → s3://{BUCKET}/{object_path}")
    df_minio = read_minio_csv(client, object_path)
    df_api   = fetch_worldbank_data(indicator)
    # Remplacer les country_id vides par "UNKNOWN" pour éviter les deltas fantômes
    df_api["country_id"] = df_api["country_id"].replace("", "UNKNOWN")
    if df_minio is not None:
        df_minio["country_id"] = df_minio["country_id"].replace("", "UNKNOWN")

    if df_minio is None:
        log.info(f"\n📥 PREMIER CHARGEMENT pour {indicator} ({len(df_api)} lignes)")
        write_minio_csv(client, df_api, object_path)
    else:
        log.info(f"\n🔄 CHARGEMENT DELTA pour {indicator}")
        df_minio_latest = df_minio.drop_duplicates(subset=KEY_COLS, keep="last").reset_index(drop=True)
        log.info(f"   🔎 MinIO dédupliqué pour comparaison : {len(df_minio_latest)} lignes")

        df_delta = compute_delta(df_api, df_minio_latest)

        if len(df_delta) == 0:
            log.info(f"✅ Aucun changement détecté. Fichier MinIO inchangé.")
        else:
            log.info(f"📝 Insertion de {len(df_delta)} ligne(s) dans MinIO (mode APPEND)")
            df_final = pd.concat([df_minio, df_delta], ignore_index=True)
            log.info(f"   Avant : {len(df_minio)} lignes | Delta : +{len(df_delta)} | Après : {len(df_final)}")
            write_minio_csv(client, df_final, object_path)

    log.info("\n🏁 INGESTION TERMINÉE : {indicator}")
    log.info("="*60)

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="ingest_worldbank_indicators_insert",
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
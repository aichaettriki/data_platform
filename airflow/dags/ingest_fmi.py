from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import io
import logging
from minio import Minio

# =====================================================
# CONFIG
# =====================================================

IMF_BASE_URL = "https://www.imf.org/external/datamapper/api/v1"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

BUCKET = "01-raw"

today = datetime.today()
YEAR = today.strftime("%Y")
MONTH = today.strftime("%m")

DATE_PATH = f"{YEAR}/{MONTH}"

INDICATORS = {
    "ENEER": "Taux_de_change_effectif_nominal",
    "EREER": "Taux_de_change_effectif_reel"
}

# =====================================================
# LOGGING
# =====================================================

log = logging.getLogger("IMF_INGEST")
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

    buffer = io.StringIO()

    df.to_csv(buffer, index=False)

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

    log.info(f"Written → s3://{BUCKET}/{path}")

# =====================================================
# FETCH IMF DATA
# =====================================================

def fetch_imf_indicator(indicator):

    url = f"{IMF_BASE_URL}/{indicator}"

    log.info(f"Fetching IMF data → {indicator}")

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    data = response.json()

    values = data["values"][indicator]

    rows = []

    for country, years in values.items():

        for year, value in years.items():

            rows.append({

                "indicator": indicator,
                "country_iso3": country,
                "year": year,
                "value": value

            })

    df = pd.DataFrame(rows)

    return df

# =====================================================
# MAIN INGEST FUNCTION
# =====================================================

def ingest_imf_indicator(indicator, folder):

    log.info("======================================")
    log.info(f"START IMF INGESTION → {indicator}")
    log.info("======================================")

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    object_path = f"{DATE_PATH}/FMI/{folder}/{indicator}.csv"

    df = fetch_imf_indicator(indicator)

    log.info(f"Rows fetched: {len(df)}")

    write_minio_csv(client, df, object_path)

    log.info("IMF ingestion completed")

# =====================================================
# DAG
# =====================================================

with DAG(

    dag_id="01-ING__ingest_FMI_indicators",

    start_date=datetime(2024,1,1),

    schedule_interval=None,

    catchup=False,

    tags=["API","IMF","RAW"]

) as dag:

    for indicator, folder in INDICATORS.items():

        PythonOperator(

            task_id=f"ingest_{indicator}",

            python_callable=ingest_imf_indicator,

            op_kwargs={
                "indicator": indicator,
                "folder": folder
            },

        )
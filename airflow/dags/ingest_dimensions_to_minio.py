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
BASE_URL = "http://dataportal.ins.tn/WebApi/"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "01-raw"

DATE_PATH = datetime.today().strftime("%Y/%m")

# =====================================================
# LOGGING
# =====================================================
log = logging.getLogger("INS_DIMENSION_INGEST")
log.setLevel(logging.INFO)

# =====================================================
# HELPERS
# =====================================================

def sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def post_xml(endpoint, body):
    r = requests.post(
        BASE_URL + endpoint,
        data=body,
        headers={"Content-Type": "application/xml"},
        timeout=120,
    )
    r.raise_for_status()
    return ET.fromstring(r.text)


def read_minio_dimension_csv(client, path):
    try:
        response = client.get_object(BUCKET, path)
        text = response.read().decode("utf-8")
        return pd.read_csv(io.StringIO(text), sep=None, engine="python")
    except Exception:
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

def detect_new_dimension_rows(minio_df, api_df):

    log.info("--------------------------------------------------")
    log.info("📊 DEBUG DIMENSION COMPARISON")

    log.info(f"API rows: {len(api_df)}")
    log.info(f"MinIO rows: {len(minio_df)}")

    if minio_df.empty:
        log.info("🆕 MinIO vide → insertion complète")
        return api_df.copy()

    # On utilise uniquement la clé métier
    if "KEY" not in api_df.columns or "KEY" not in minio_df.columns:
        log.warning("⚠️ Pas de colonne KEY → insertion complète")
        return api_df.copy()

    engine = create_engine("sqlite:///:memory:")

    minio_df.to_sql("minio_table", engine, index=False, if_exists="replace")
    api_df.to_sql("api_table", engine, index=False, if_exists="replace")

    sql_query = """
    SELECT a.*
    FROM api_table a
    LEFT JOIN minio_table m
        ON a.dimension_id = m.dimension_id
       AND a.KEY = m.KEY
    WHERE m.KEY IS NULL
    """

    new_rows_df = pd.read_sql(sql_query, engine)

    log.info(f"🆕 Nouvelles lignes détectées: {len(new_rows_df)}")
    log.info("--------------------------------------------------")

    return new_rows_df


# =====================================================
# MAIN TASK
# =====================================================

def ingest_all_dimensions():

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    # -------------------------------------------------
    # 1. GET STRUCTURE
    # -------------------------------------------------
    log.info("📡 Calling GetStructure")
    structure_root = post_xml("GetStructure", "<QueryMessage></QueryMessage>")

    dimensions = {}

    for src in structure_root.findall(".//Source"):
        for dim in src.findall("./Dimensions/Dimension"):
            dim_id = dim.attrib.get("Id")
            dim_name = dim.attrib.get("Name")
            dimensions[dim_id] = dim_name

    log.info(f"✅ Total unique dimensions detected = {len(dimensions)}")

    # -------------------------------------------------
    # 2. LOOP ON DIMENSIONS
    # -------------------------------------------------
    for idx, (dim_id, dim_name) in enumerate(dimensions.items(), start=1):

        log.info("=" * 80)
        log.info(f"🔎 [{idx}] Processing dimension {dim_id} | {dim_name}")

        body = f"""
        <QueryMessage>
            <DataWhere>
                <DimensionId WithData='true'>{dim_id}</DimensionId>
            </DataWhere>
        </QueryMessage>
        """

        dim_root = post_xml("GetDimensionElements", body)

        # ATTRIBUTES dynamiques
        attributes = [
            attr.attrib["Id"]
            for attr in dim_root.findall("./Attributes/Attribute")
        ]

        rows = []

        for el in dim_root.findall(".//Element"):
            row = {
                "dimension_id": dim_id,
                "dimension_name": dim_name,
            }
            for attr in attributes:
                row[attr] = el.attrib.get(attr, "")
            rows.append(row)

        if not rows:
            log.warning(f"⚠️ No elements found for {dim_id}")
            continue

        headers = ["dimension_id", "dimension_name"] + attributes
        api_df = pd.DataFrame(rows, columns=headers)

        object_path = (
            f"2026/02/INS/Api-Dimensions/"
            # f"{dim_id}-{sanitize(dim_name)}/"
            f"{dim_id}-{sanitize(dim_name)}.csv"
        )

        # -------------------------------------------------
        # INCREMENTAL CHECK
        # -------------------------------------------------
        minio_df = read_minio_dimension_csv(client, object_path)

        new_rows_df = detect_new_dimension_rows(minio_df, api_df)

        if new_rows_df.empty:
            log.info(f"🟢 No new elements for {dim_id}")
            continue

        log.info(f"🆕 {len(new_rows_df)} nouvelles lignes détectées")

        final_df = pd.concat([minio_df, new_rows_df], ignore_index=True)

        write_minio_csv(client, final_df, object_path)

    log.info("🎉 INS Dimension ingestion completed successfully")


# =====================================================
# DAG
# =====================================================

with DAG(
    dag_id="ingest_all_ins_dimensions",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["INS", "RAW", "API", "DIMENSION"],
) as dag:

    ingest_dimensions = PythonOperator(
        task_id="ingest_all_ins_dimensions",
        python_callable=ingest_all_dimensions,
    )

    ingest_dimensions

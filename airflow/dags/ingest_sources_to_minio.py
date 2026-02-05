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

# =====================================================
# CONFIG
# =====================================================
BASE_URL = "http://dataportal.ins.tn/WebApi/"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "01-raw"

today = datetime.today()
DATE_PATH = today.strftime("%Y/%m/%d")

# =====================================================
# LOGGING
# =====================================================
log = logging.getLogger("INS_RAW_INGEST")
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
    return r.text

# =====================================================
# MAIN TASK
# =====================================================
def ingest_all_sources():

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    # -------------------------------------------------
    # 1. GET STRUCTURE
    # -------------------------------------------------
    log.info("📡 Calling GetStructure")
    structure_xml = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(structure_xml)

    sources = []

    for src in root.findall(".//Source"):
        src_id = src.attrib.get("Id")
        src_name = src.attrib.get("Name")

        period = src.find("./Period")
        start_year = int(period.findtext("StartYear"))
        finish_year = int(period.findtext("FinishYear"))

        sources.append({
            "id": src_id,
            "name": src_name,
            "start_year": start_year,
            "finish_year": finish_year,
        })

    log.info(f"✅ Total sources detected = {len(sources)}")

    for s in sources:
        log.info(
            f"📘 Source | {s['id']} | {s['name']} "
            f"({s['start_year']} → {s['finish_year']})"
        )

    # -------------------------------------------------
    # 2. LOOP ON SOURCES → GET DATA
    # -------------------------------------------------
    for src in sources:

        log.info("=" * 80)
        log.info(
            f"🚀 Processing source {src['id']} | {src['name']} "
            f"({src['start_year']} → {src['finish_year']})"
        )

        body = f"""
        <QueryMessage SourceId='{src['id']}'>
            <Period From='{src['start_year']}' To='{src['finish_year']}' Frequency='Y'/>
            <DataWhere></DataWhere>
        </QueryMessage>
        """

        xml = post_xml("GetData", body)
        root_data = ET.fromstring(xml)

        rows = []

        for s in root_data.findall(".//Set"):
            period = s.attrib.get("Period")

            for dim_id, element_key in s.attrib.items():
                if dim_id == "Period":
                    continue

                rows.append((
                    period,
                    dim_id,
                    element_key,
                    s.text,
                    src["id"],
                    src["name"],
                    src["start_year"],
                    src["finish_year"],
                ))

        log.info(f"📊 Rows collected = {len(rows)}")

        if not rows:
            log.warning(f"⚠️ No data for source {src['id']}")
            continue

        # -------------------------------------------------
        # 3. WRITE CSV TO MINIO
        # -------------------------------------------------
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow([
            "period",
            "dimension_id",
            "element_key",
            "value",
            "source_id",
            "source_name",
            "start_year",
            "finish_year",
        ])
        writer.writerows(rows)

        object_path = (
            f"{DATE_PATH}/INS/Sources/{src['id']}/"
            f"{src['id']}-{sanitize(src['name'])}.csv"
        )

        client.put_object(
            BUCKET,
            object_path,
            io.BytesIO(buffer.getvalue().encode()),
            length=len(buffer.getvalue()),
            content_type="text/csv",
        )

        log.info(f"💾 Written → s3://{BUCKET}/{object_path}")

    log.info("🎉 INS RAW ingestion completed successfully")

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="ingest_all_ins_sources",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["INS", "RAW", "API","SOURCES"],
) as dag:

    ingest_all = PythonOperator(
        task_id="ingest_all_ins_sources",
        python_callable=ingest_all_sources,
    )

    ingest_all

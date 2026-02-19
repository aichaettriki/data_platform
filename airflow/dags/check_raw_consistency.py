
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

# =========================
# CONFIG
# =========================
API_BASE = "http://dataportal.ins.tn/WebApi/"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "01-raw"

DATE_PATH = datetime.today().strftime("%Y/%m/%d")

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("INS_SOURCE_SYNC")

# =========================
# HELPERS
# =========================
def sanitize(name):
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
        API_BASE + endpoint,
        data=body,
        headers={"Content-Type": "application/xml"},
        timeout=120,
    )
    r.raise_for_status()
    return ET.fromstring(r.text)

def source_exists(client, src_id):
    prefix = f"{DATE_PATH}/INS/Sources/{src_id}/"
    return any(client.list_objects(BUCKET, prefix=prefix, recursive=True))

def read_csv_header(client, object_name):
    obj = client.get_object(BUCKET, object_name)

    try:
        # Lire seulement la première ligne
        first_line = obj.readline()
        header = first_line.decode("utf-8", errors="replace").strip()
        return set(header.split(","))
    finally:
        obj.close()
        obj.release_conn()


# =========================
# MAIN TASK
# =========================
def sync_sources():

    client = get_minio_client()
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    log.info("📡 Fetching INS structure")
    root = post_xml("GetStructure", "<QueryMessage></QueryMessage>")

    for src in root.findall(".//Source"):
        src_id = src.attrib["Id"]
        src_name = src.attrib["Name"]

        start_year = int(src.find("./Period/StartYear").text)
        end_year = int(src.find("./Period/FinishYear").text)

        log.info("=" * 80)
        log.info(f"🔍 SOURCE {src_id} | {src_name}")

        body = f"""
        <QueryMessage SourceId='{src_id}'>
            <Period From='{start_year}' To='{end_year}' Frequency='Y'/>
            <DataWhere></DataWhere>
        </QueryMessage>
        """

        data_root = post_xml("GetData", body)
        rows = []

        for s in data_root.findall(".//Set"):
            period = s.attrib.get("Period")
            for dim_id, key in s.attrib.items():
                if dim_id == "Period":
                    continue
                rows.append([
                    period,
                    dim_id,
                    key,
                    s.text,
                    src_id,
                    src_name,
                    start_year,
                    end_year,
                ])

        if not rows:
            log.warning("⚠️ No data")
            continue

        headers = [
            "period",
            "dimension_id",
            "element_key",
            "value",
            "source_id",
            "source_name",
            "start_year",
            "finish_year",
        ]

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(headers)
        writer.writerows(rows)

        csv_content = buffer.getvalue()
        object_path = (
            f"{DATE_PATH}/INS/Sources/{src_id}/"
            f"{src_id}-{sanitize(src_name)}.csv"
        )

        if not source_exists(client, src_id):
            log.info("🆕 Source not found → inserting")
            csv_bytes = buffer.getvalue().encode("utf-8")

            client.put_object(
                BUCKET,
                object_path,
                data=io.BytesIO(csv_bytes),   # ✅ même source
                length=len(csv_bytes),        # ✅ même taille
                content_type="text/csv; charset=utf-8",
            )


            log.info("✅ Inserted")

        else:
            log.info("🔁 Source exists → checking schema")
            existing = list(
                client.list_objects(
                    BUCKET,
                    prefix=f"{DATE_PATH}/INS/Sources/{src_id}/",
                    recursive=True,
                )
            )[0].object_name

            minio_cols = read_csv_header(client, existing)
            api_cols = set(headers)

            if api_cols - minio_cols:
                log.warning("🧩 Schema evolution detected → updating")
                csv_bytes = buffer.getvalue().encode("utf-8")

                client.put_object(
                    BUCKET,
                    object_path,
                    data=io.BytesIO(csv_bytes),   # ✅ même source
                    length=len(csv_bytes),        # ✅ même taille
                    content_type="text/csv; charset=utf-8",
                )


            else:
                log.info("✅ No change detected")

# =========================
# DAG
# =========================
with DAG(
    dag_id="ingest_all_ins_sources_with_check",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["API", "INS", "RAW", "Sources"],
) as dag:

    PythonOperator(
        task_id="ingest_all_ins_sources_with_check",
        python_callable=sync_sources,
    )

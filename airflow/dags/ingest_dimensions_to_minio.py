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
    return r.text

# =====================================================
# MAIN TASK
# =====================================================
def ingest_all_dimensions():

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    # -------------------------------------------------
    # 1. GET STRUCTURE → COLLECT DIMENSIONS
    # -------------------------------------------------
    log.info("📡 Calling GetStructure")
    structure_xml = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(structure_xml)

    dimensions = {}

    for src in root.findall(".//Source"):
        for dim in src.findall("./Dimensions/Dimension"):
            dim_id = dim.attrib.get("Id")
            dim_name = dim.attrib.get("Name")
            dimensions[dim_id] = dim_name

    total_dims = len(dimensions)
    log.info(f"✅ Total unique dimensions detected = {total_dims}")

    # -------------------------------------------------
    # 2. LOOP ON DIMENSIONS
    # -------------------------------------------------
    for idx, (dim_id, dim_name) in enumerate(dimensions.items(), start=1):

        log.info("=" * 80)
        log.info(
            f"🔎 [{idx}/{total_dims}] Loading dimension "
            f"{dim_id} | {dim_name}"
        )

        body = f"""
        <QueryMessage>
            <DataWhere>
                <DimensionId WithData='true'>{dim_id}</DimensionId>
            </DataWhere>
        </QueryMessage>
        """

        xml = post_xml("GetDimensionElements", body)
        root_dim = ET.fromstring(xml)

        # -------------------------------------------------
        # 3. ATTRIBUTES (DYNAMIC SCHEMA)
        # -------------------------------------------------
        attributes = [
            attr.attrib["Id"]
            for attr in root_dim.findall("./Attributes/Attribute")
        ]

        log.info(f"📘 Attributes detected = {len(attributes)}")

        # -------------------------------------------------
        # 4. ELEMENTS → ROWS
        # -------------------------------------------------
        rows = []

        for el in root_dim.findall(".//Element"):
            row = {
                "dimension_id": dim_id,
                "dimension_name": dim_name,
            }
            for attr in attributes:
                row[attr] = el.attrib.get(attr, "")
            rows.append(row)

        log.info(f"📊 Elements collected = {len(rows)}")

        if not rows:
            log.warning(f"⚠️ No elements for dimension {dim_id}")
            continue

        # -------------------------------------------------
        # 5. WRITE CSV TO MINIO
        # -------------------------------------------------
        headers = ["dimension_id", "dimension_name"] + attributes

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

        object_path = (
            f"{DATE_PATH}/INS/Dimension/{dim_id}-{sanitize(dim_name)}/"
            f"{dim_id}-{sanitize(dim_name)}.csv"
        )

        client.put_object(
            BUCKET,
            object_path,
            io.BytesIO(buffer.getvalue().encode()),
            length=len(buffer.getvalue()),
            content_type="text/csv",
        )

        log.info(f"💾 Written → s3://{BUCKET}/{object_path}")

    log.info("🎉 INS DIMENSION ingestion completed successfully")

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

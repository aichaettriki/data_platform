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
 
def get_last_two_levels(full_path: str) -> str:
    if not full_path:
        return ""

    parts = full_path.split("\\")

    if len(parts) >= 2:
        return "\\".join(parts[-2:])
    else:
        return full_path

def remove_first_level(full_path: str) -> str:
    if not full_path:
        return ""

    parts = full_path.split("\\")

    if len(parts) > 1:
        return "\\".join(parts[1:])  # 🔥 skip first level
    else:
        return full_path
 
def parse_fixed_dimension(element, parent_path="", parent_key=None, level=0):
    rows = []

    name = element.attrib.get("NAME", "")
    key = element.attrib.get("KEY", "")

    if parent_path:
        full_path = f"{parent_path}\\{name}"
    else:
        full_path = name

    rows.append({
        "dimension_id": "OBJ11288499",
        "key": key,
        "name": name,
        "parent_key": parent_key,
        "level": level,
        "full_path": remove_first_level(full_path),
        "short_fullname": get_last_two_levels(full_path),
    })

    for child in element.findall("Element"):
        rows.extend(
            parse_fixed_dimension(
                child,
                parent_path=full_path,
                parent_key=key,
                level=level + 1
            )
        )

    return rows

    
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
# MAIN TASK - SNAPSHOT ONLY
# =====================================================

def ingest_all_dimensions():

    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    # -------------------------------------------------
    # 1️⃣ GET STRUCTURE
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
    # 2️⃣ LOOP ON DIMENSIONS
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

        # Attributs dynamiques
        attributes = [
            attr.attrib["Id"]
            for attr in dim_root.findall("./Attributes/Attribute")
        ]
        # ==================================================
        # 🔥 CAS SPÉCIAL DIMENSION FIX
        # ==================================================
        if dim_id == "OBJ11288499":

            log.info("⚙️ Applying custom parser for OBJ11288499")

            rows = []

            for root_el in dim_root.findall("./Elements/Element"):
                rows.extend(parse_fixed_dimension(root_el))

            if not rows:
                log.warning(f"⚠️ No elements found for {dim_id}")
                continue

            api_df = pd.DataFrame(rows)

            # optionnel mais recommandé (homogénéité)
            api_df["dimension_name"] = dim_name

        else:
            # ==================================================
            # 🟢 CAS STANDARD
            # ==================================================
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


        # ✅ Path dynamique basé sur date
        object_path = (
            f"{DATE_PATH}/INS/Api-Dimensions/"
            f"{dim_id}-{sanitize(dim_name)}.csv"
        )

        write_minio_csv(client, api_df, object_path)

        log.info(f"💾 Snapshot dimension écrit → {dim_id}")

    log.info("🎉 INS Dimension ingestion completed successfully (snapshot mode)") 
# =====================================================
# DAG
# =====================================================
 
with DAG(
    dag_id="01-ING__ingest_all_ins_dimensions",
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

 
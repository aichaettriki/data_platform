from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import io
from minio import Minio
import logging

# =====================================================
# CONFIG
# =====================================================
BASE_URL = "http://dataportal.ins.tn/WebApi/"
DIM_ID = "OBJ11288499"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "01-raw"

DATE_PATH = datetime.today().strftime("%Y/%m")

# =====================================================
# LOGGING
# =====================================================
log = logging.getLogger("INS_DIMENSION_FIX")
log.setLevel(logging.INFO)

# =====================================================
# MINIO
# =====================================================
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

# =====================================================
# API CALL
# =====================================================
def post_xml(endpoint, body):
    r = requests.post(
        BASE_URL + endpoint,
        data=body,
        headers={"Content-Type": "application/xml"},
        timeout=120,
    )
    r.raise_for_status()
    return ET.fromstring(r.text)

# =====================================================
# 🌳 RECURSIVE PARSER (CORE LOGIC)
# =====================================================
def parse_element(element, parent_path="", parent_key=None, level=0):

    rows = []

    # ─────────────────────────────────────────────
    # CURRENT ELEMENT
    # ─────────────────────────────────────────────
    name = element.attrib.get("NAME", "")
    key = element.attrib.get("KEY", "")

    # 🔥 REBUILD FULL PATH (IMPORTANT)
    if parent_path:
        full_path = f"{parent_path}\\{name}"
    else:
        full_path = name

    # ─────────────────────────────────────────────
    # LOG STRUCTURE (TREE VIEW)
    # ─────────────────────────────────────────────
    log.info(f"{'  '*level}📂 Level {level}")
    log.info(f"{'  '*level}   ├─ NAME       : {name}")
    log.info(f"{'  '*level}   ├─ KEY        : {key}")
    log.info(f"{'  '*level}   ├─ PARENT_KEY : {parent_key}")
    log.info(f"{'  '*level}   ├─ FULL_PATH  : {full_path}")
    log.info(f"{'  '*level}   └─ SHORT_PATH : {get_last_two_levels(full_path)}")
    # ─────────────────────────────────────────────
    # STORE ROW
    # ─────────────────────────────────────────────
    rows.append({
        "dimension_id": DIM_ID,
        "key": key,
        "name": name,
        "parent_key": parent_key,
        "level": level,
        "full_path": full_path,
        "short_fullname": get_last_two_levels(full_path), 
    })

    # ─────────────────────────────────────────────
    # CHILDREN (RECURSION)
    # ─────────────────────────────────────────────
    for child in element.findall("Element"):
        rows.extend(
            parse_element(
                child,
                parent_path=full_path,
                parent_key=key,
                level=level + 1
            )
        )

    return rows

def get_last_two_levels(full_path: str) -> str:
    """
    Retourne les 2 derniers niveaux d'un chemin hiérarchique.

    Exemples :
    A\B\C → B\C
    A\B   → A\B
    A     → A
    """
    if not full_path:
        return ""

    parts = full_path.split("\\")

    if len(parts) >= 2:
        return "\\".join(parts[-2:])
    else:
        return full_path
# =====================================================
# MAIN TASK
# =====================================================
def ingest_fixed_dimension():

    log.info("📡 Fetching dimension from INS API")

    body = f"""
    <QueryMessage>
        <DataWhere>
            <DimensionId WithData='true'>{DIM_ID}</DimensionId>
        </DataWhere>
    </QueryMessage>
    """

    root = post_xml("GetDimensionElements", body)

    # ─────────────────────────────────────────────
    # ROOT ELEMENTS
    # ─────────────────────────────────────────────
    all_rows = []

    for root_el in root.findall("./Elements/Element"):
        all_rows.extend(parse_element(root_el))

    df = pd.DataFrame(all_rows)

    log.info(f"📊 Total elements parsed = {len(df)}")

    # =====================================================
    # SAVE TO MINIO
    # =====================================================
    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    path = f"{DATE_PATH}/INS/Fixed-Dimensions/{DIM_ID}.csv"

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    data = buffer.getvalue().encode("utf-8")

    client.put_object(
        BUCKET,
        path,
        data=io.BytesIO(data),
        length=len(data),
        content_type="text/csv",
    )

    log.info(f"💾 Dimension FIXED saved → s3://{BUCKET}/{path}")

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="01-ING__fix_dimension_OBJ11288499",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["INS", "DIMENSION", "FIX"],
) as dag:

    run = PythonOperator(
        task_id="fix_dimension_fullname",
        python_callable=ingest_fixed_dimension,
    )
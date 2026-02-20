from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import csv
import io
from minio import Minio
import re

# =====================================================
# CONFIG
# =====================================================
BASE_URL = "http://dataportal.ins.tn/WebApi/"
SOURCES = ["OBJ5259889", "OBJ11288479"]

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "01-raw"

today = datetime.today()
YEAR = today.strftime("%Y")
MONTH = today.strftime("%m")

BASE_PATH = f"{YEAR}/{MONTH}/INS"


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
# TASK 1 : INGEST SOURCES (DYNAMIQUE)
# =====================================================
def ingest_sources():
    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    print("📡 Calling GetStructure")
    structure_xml = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(structure_xml)

    sources = {}

    # 🔥 Récupération dynamique des métadonnées
    for src in root.findall(".//Source"):
        src_id = src.attrib.get("Id")

        if src_id not in SOURCES:
            continue

        full_name = src.attrib.get("FullName") or src.attrib.get("Name")

        start_year_el = src.find("./Period/StartYear")
        finish_year_el = src.find("./Period/FinishYear")

        start_year = start_year_el.text if start_year_el is not None else "1990"
        finish_year = finish_year_el.text if finish_year_el is not None else str(datetime.today().year)

        sources[src_id] = {
            "full_name": full_name,
            "start_year": start_year,
            "finish_year": finish_year,
        }

    print(f"✅ Sources detected: {sources}")

    # 🔥 Appel dynamique GetData
    for source_id, meta in sources.items():
        source_name = meta["full_name"]
        start_year = meta["start_year"]
        finish_year = meta["finish_year"]

        print(f"\n📡 GetData → {source_id} | {source_name}")
        print(f"📅 Period: {start_year} → {finish_year}")

        body = f"""
        <QueryMessage SourceId='{source_id}'>
            <Period From='{start_year}' To='{finish_year}' Frequency='Y'></Period>
            <DataWhere></DataWhere>
        </QueryMessage>
        """

        xml = post_xml("GetData", body)
        root_data = ET.fromstring(xml)

        rows = []

        for s in root_data.findall(".//Set"):
            period = s.attrib.get("Period", "")

            for dimension_id, element_key in s.attrib.items():
                if dimension_id == "Period":
                    continue

                rows.append((
                    period,
                    dimension_id,
                    element_key,
                    s.text.strip() if s.text else ""
                ))

        print(f"✅ Rows collected: {len(rows)}")

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["period", "dimension_id", "dimension_key", "value"])
        writer.writerows(rows)

        filename = f"{source_id}-{sanitize(source_name)}.csv"
        object_path = f"{BASE_PATH}/Source/Agregat/{filename}"

        data_bytes = buffer.getvalue().encode("utf-8")

        client.put_object(
            BUCKET,
            object_path,
            io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="text/csv",
        )

        print(f"📦 Written → s3://{BUCKET}/{object_path}")

# =====================================================
# TASK 2 : INGEST DIMENSIONS (FullName)
# =====================================================
def ingest_dimensions():
    client = get_minio_client()

    print("📡 Calling GetStructure")
    structure_xml = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(structure_xml)

    for src in root.findall(".//Source"):
        source_id = src.attrib.get("Id")

        if source_id not in SOURCES:
            continue

        source_name = src.attrib.get("FullName") or src.attrib.get("Name")

        print(f"\n📂 Source {source_id} | {source_name}")

        for dim in src.findall("./Dimensions/Dimension"):
            dim_id = dim.attrib.get("Id")
            dim_name = dim.attrib.get("FullName") or dim.attrib.get("Name")

            print(f"🔎 Dimension {dim_id} | {dim_name}")

            body = f"""
            <QueryMessage>
                <DataWhere>
                    <DimensionId WithData='true'>{dim_id}</DimensionId>
                </DataWhere>
            </QueryMessage>
            """

            xml = post_xml("GetDimensionElements", body)
            root_dim = ET.fromstring(xml)

            attribute_ids = [
                attr.attrib["Id"]
                for attr in root_dim.findall("./Attributes/Attribute")
            ]

            rows = []

            for el in root_dim.findall(".//Element"):
                row = {
                    "dimension_id": dim_id,
                    "dimension_name": dim_name,
                }

                for attr in attribute_ids:
                    row[attr] = el.attrib.get(attr, "")

                rows.append(row)

            print(f"✅ Elements: {len(rows)}")

            headers = ["dimension_id", "dimension_name"] + attribute_ids

            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

            filename = f"{dim_id}-{sanitize(dim_name)}.csv"
            object_path = f"{BASE_PATH}/Dimension/Agregat/{source_id}/{filename}"


            data_bytes = buffer.getvalue().encode("utf-8")

            client.put_object(
                BUCKET,
                object_path,
                io.BytesIO(data_bytes),
                length=len(data_bytes),
                content_type="text/csv",
            )

            print(f"📦 Written → s3://{BUCKET}/{object_path}")

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="ins_raw_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["INS", "RAW", "API"],
) as dag:

    ingest_sources_task = PythonOperator(
        task_id="ingest_ins_sources",
        python_callable=ingest_sources,
    )

    ingest_dimensions_task = PythonOperator(
        task_id="ingest_ins_dimensions",
        python_callable=ingest_dimensions,
    )

    ingest_sources_task >> ingest_dimensions_task

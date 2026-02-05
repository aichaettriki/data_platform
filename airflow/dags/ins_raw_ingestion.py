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
DATE_PATH = today.strftime("%Y/%m/%d")

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
        timeout=60,
    )
    r.raise_for_status()
    return r.text

# =====================================================
# TASK 1 : INGEST SOURCES
# =====================================================
def ingest_sources():
    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    print("📡 Calling GetStructure")
    structure_xml = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(structure_xml)

    sources = {}

    for src in root.findall(".//Source"):
        src_id = src.attrib.get("Id")
        src_name = src.attrib.get("Name")

        if src_id in SOURCES:
            sources[src_id] = src_name

    print(f"✅ Sources detected: {sources}")

    for source_id, source_name in sources.items():
        print(f"\n📡 GetData → {source_id} | {source_name}")

        body = f"""
        <QueryMessage SourceId='{source_id}'>
            <Period From='1997' To='2019' Frequency='Y'></Period>
            <DataWhere></DataWhere>
        </QueryMessage>
        """

        xml = post_xml("GetData", body)
        root = ET.fromstring(xml)

        rows = []
        for s in root.findall(".//Set"):
            period = s.attrib.get("Period")

            for dimension_id, element_key in s.attrib.items():
                if dimension_id == "Period":
                    continue

                rows.append((
                    period,
                    dimension_id,
                    element_key,
                    s.text
                ))


        print(f"✅ Rows collected: {len(rows)}")
        print("🔍 Preview:")
        for r in rows[:5]:
            print(r)

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["period", "indicator_id","indicator_key", "value"])
        writer.writerows(rows)

        filename = f"{source_id}-{sanitize(source_name)}.csv"
        object_path = f"{DATE_PATH}/INS/Source/Agregat/{filename}"

        client.put_object(
            BUCKET,
            object_path,
            io.BytesIO(buffer.getvalue().encode()),
            length=len(buffer.getvalue()),
            content_type="text/csv",
        )

        print(f"📦 Written → s3://{BUCKET}/{object_path}")

# =====================================================
# TASK 2 : INGEST DIMENSIONS PER SOURCE
# =====================================================
def ingest_dimensions():
    client = get_minio_client()

    print("📡 Calling GetStructure")
    structure_xml = post_xml("GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(structure_xml)

    for src in root.findall(".//Source"):
        source_id = src.attrib.get("Id")
        source_name = src.attrib.get("Name")

        if source_id not in SOURCES:
            continue

        print(f"\n📂 Source {source_id} | {source_name}")

        for dim in src.findall("./Dimensions/Dimension"):
            dim_id = dim.attrib.get("Id")
            dim_name = dim.attrib.get("Name")

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
            # 1. récupérer dynamiquement les attributs décrits par l'API
            attribute_ids = [
                attr.attrib["Id"]
                for attr in root_dim.findall("./Attributes/Attribute")
            ]

            # 2. construire les lignes (1 ligne = 1 Element)
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
            print("🔍 Preview:")
            for r in rows[:2]:
                print(r)

            # 3. header dynamique FINAL
            headers = ["dimension_id", "dimension_name"] + attribute_ids

            # 4. écriture CSV CORRECTE
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)


            filename = f"{dim_id}-{sanitize(dim_name)}.csv"
            object_path = (
                f"{DATE_PATH}/INS/Dimension/Agregat/{source_id}/{filename}"
            )

            client.put_object(
                BUCKET,
                object_path,
                io.BytesIO(buffer.getvalue().encode()),
                length=len(buffer.getvalue()),
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
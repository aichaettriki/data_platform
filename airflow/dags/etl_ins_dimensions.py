import csv
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio


# ============================
# 🔧 CONFIG MINIO
# ============================
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_RAW = "raw"
OUTPUT_FILE = "/opt/airflow/data/ins_dimensions.csv"
RAW_OBJECT = "ins_dimensions.csv"

# ============================
# 🔧 CONFIG API INS
# ============================
BASE_URL = "http://dataportal.ins.tn/WebApi/"


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

# ============================
# 🔽 1. Fonction pour appeler l'API INS
# ============================
def post_xml(url, body_xml):
    headers = {"Content-Type": "application/xml"}
    response = requests.post(url, data=body_xml, headers=headers)
    response.raise_for_status()
    return response.text


# ============================
# 🔽 2. Extraction de toutes les dimensions
# ============================
def extract_dimensions():
    print("👉 Appel API GetStructure...")

    structure_xml = post_xml(BASE_URL + "GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(structure_xml)

    dimensions_list = []

    for source in root.findall(".//Source"):
        source_id = source.attrib.get("Id")
        source_key = source.attrib.get("Key")
        source_name = source.attrib.get("Name")

        for dim in source.findall("./Dimensions/Dimension"):
            dim_id = dim.attrib.get("Id")
            dim_name = dim.attrib.get("Name")
            dim_key = dim.attrib.get("Key")
            dim_type = dim.attrib.get("DimensionType")

            dimensions_list.append({
                "source_id": source_id,
                "source_name": source_name,
                "source_key": source_key,
                "dimension_id": dim_id,
                "dimension_key": dim_key,
                "dimension_name": dim_name,
                "dimension_type": dim_type
            })

    print(f"✔ {len(dimensions_list)} dimensions récupérées")
    return dimensions_list


# ============================
# 🔽 3. Pour chaque dimension → récupérer ses éléments
# ============================
def extract_dimension_elements(**context):
    dimensions = extract_dimensions()

    print("👉 Extraction des éléments de chaque dimension...")

    final_rows = []

    for dim in dimensions:
        dim_id = dim["dimension_id"]

        body = f"""
        <QueryMessage>
            <DataWhere>
                <DimensionId WithData='true'>{dim_id}</DimensionId>
            </DataWhere>
        </QueryMessage>
        """

        try:
            xml_resp = post_xml(BASE_URL + "GetDimensionElements", body)
        except Exception as e:
            print(f"❌ Erreur sur dimension {dim_id}: {e}")
            continue

        root = ET.fromstring(xml_resp)

        for elem in root.findall(".//Element"):
            element_key = elem.attrib.get("KEY")
            element_name = elem.attrib.get("NAME")

            final_rows.append({
                "source_id": dim["source_id"],
                "source_name": dim["source_name"],
                "dimension_id": dim_id,
                "dimension_name": dim["dimension_name"],
                "element_key": element_key,
                "element_name": element_name
            })

    print(f"✔ Total éléments extraits : {len(final_rows)}")

    # Sauvegarde CSV
    print(f"📌 Écriture CSV → {OUTPUT_FILE}")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "source_id",
            "source_name",
            "dimension_id",
            "dimension_name",
            "element_key",
            "element_name",
        ])

        for row in final_rows:
            writer.writerow([
                row["source_id"],
                row["source_name"],
                row["dimension_id"],
                row["dimension_name"],
                row["element_key"],
                row["element_name"],
            ])

    print("✔ CSV dimensions + éléments généré.")

    return OUTPUT_FILE


# ============================
# 🔽 4. Upload CSV dans MinIO
# ============================
def upload_to_minio(**context):
    file_path = context["ti"].xcom_pull(task_ids="extract_ins_dimensions")

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)

    client.fput_object(BUCKET_RAW, RAW_OBJECT, file_path)

    print(f"📦 Upload CSV → MinIO bucket={BUCKET_RAW}/{RAW_OBJECT}")


# ============================
# 🚀 DAG Airflow
# ============================
with DAG(
    dag_id="etl_ins_dimensions_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id="extract_ins_dimensions",
        python_callable=extract_dimension_elements,
        provide_context=True,
    )

    task_upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        provide_context=True,
    )

    task_extract >> task_upload

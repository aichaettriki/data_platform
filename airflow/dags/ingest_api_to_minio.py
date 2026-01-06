import csv
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import os

# ============================
# 🔧 CONFIG MINIO
# ============================
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_RAW = "raw"

# dossier local Airflow
OUTPUT_FOLDER = "/opt/airflow/data/dimensions/"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

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
# 🔽 1. Fonction générique d'appel XML
# ============================
def post_xml(url, body_xml):
    headers = {"Content-Type": "application/xml"}
    response = requests.post(url, data=body_xml, headers=headers)
    response.raise_for_status()
    return response.text


# ============================
# 🔽 2. Extraction des dimensions uniques
# ============================
def extract_dimensions():
    print("👉 Appel API GetStructure...")

    xml = post_xml(BASE_URL + "GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(xml)

    unique_dims = {}

    # Parcours de toutes les sources
    for source in root.findall(".//Source"):
        for dim in source.findall("./Dimensions/Dimension"):

            dim_id = dim.attrib.get("Id")

            # éviter les doublons
            if dim_id not in unique_dims:
                unique_dims[dim_id] = {
                    "dimension_id": dim_id,
                    "dimension_name": dim.attrib.get("Name"),
                    "dimension_key": dim.attrib.get("Key"),
                    "dimension_type": dim.attrib.get("DimensionType"),
                }

    dimensions = list(unique_dims.values())
    print(f"✔ {len(dimensions)} dimensions uniques trouvées")

    return dimensions


# ============================
# 🔽 3. Extraire les éléments et générer 1 CSV par dimension
# ============================
def extract_dimension_elements(**context):

    dimensions = extract_dimensions()
    output_files = []

    print(f"👉 Extraction des éléments ({len(dimensions)} dimensions)…")

    for dim in dimensions:

        dim_id = dim["dimension_id"]
        dim_name = dim["dimension_name"]

        print(f"\n🔎 Dimension : {dim_id} ({dim_name})")

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
            print(f"❌ Erreur API dimension {dim_id}: {e}")
            continue

        root = ET.fromstring(xml_resp)
        elements = root.findall(".//Element")

        print(f"   ➕ {len(elements)} éléments trouvés")

        # Nettoyage du nom pour le fichier
        safe_name = dim_name.replace(" ", "_").replace("'", "")
        file_path = f"{OUTPUT_FOLDER}{dim_id}_{safe_name}.csv"

        # Écriture CSV par dimension
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["dimension_id", "dimension_name", "element_key", "element_name"])

            for elem in elements:
                writer.writerow([
                    dim_id,
                    dim_name,
                    elem.attrib.get("KEY"),
                    elem.attrib.get("NAME")
                ])

        print(f"   ✔ CSV généré : {file_path}")
        output_files.append(file_path)

    return output_files  # → pour upload


# ============================
# 🔽 4. Upload dans MinIO
# ============================
def upload_to_minio(**context):
    file_paths = context["ti"].xcom_pull(task_ids="extract_ins_dimensions")

    client = get_minio_client()
    folder = "dimensions"

    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)

    for file_path in file_paths:
        filename = os.path.basename(file_path)
        object_name = f"{folder}/{filename}"

        client.fput_object(BUCKET_RAW, object_name, file_path)

        print(f"📦 Upload → {object_name}")


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

# import csv
# import requests
# import xml.etree.ElementTree as ET
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from minio import Minio


# # ============================
# # 🔧 CONFIG MINIO
# # ============================
# MINIO_ENDPOINT = "minio:9000"
# MINIO_ACCESS_KEY = "minio"
# MINIO_SECRET_KEY = "minio123"
# BUCKET_RAW = "raw"
# OUTPUT_FILE = "/opt/airflow/data/ins_dimensions.csv"
# RAW_OBJECT = "ins_dimensions.csv"

# # ============================
# # 🔧 CONFIG API INS
# # ============================
# BASE_URL = "http://dataportal.ins.tn/WebApi/"


# def get_minio_client():
#     return Minio(
#         MINIO_ENDPOINT,
#         access_key=MINIO_ACCESS_KEY,
#         secret_key=MINIO_SECRET_KEY,
#         secure=False,
#     )

# # ============================
# # 🔽 1. Fonction pour appeler l'API INS
# # ============================
# def post_xml(url, body_xml):
#     headers = {"Content-Type": "application/xml"}
#     response = requests.post(url, data=body_xml, headers=headers)
#     response.raise_for_status()
#     return response.text


# # ============================
# # 🔽 2. Extraction de toutes les dimensions
# # ============================
# def extract_dimensions():
#     print("👉 Appel API GetStructure...")

#     structure_xml = post_xml(BASE_URL + "GetStructure", "<QueryMessage></QueryMessage>")
#     root = ET.fromstring(structure_xml)

#     dimensions_list = []
#                                 # toutes les sources
#     for source in root.findall(".//Source"):
#         source_id = source.attrib.get("Id")
#         source_key = source.attrib.get("Key")
#         source_name = source.attrib.get("Name")
# #               toutes les dimensions de tout le document source->dimensions->dimension
#         for dim in source.findall("./Dimensions/Dimension"):
#             dim_id = dim.attrib.get("Id")
#             dim_name = dim.attrib.get("Name")
#             dim_key = dim.attrib.get("Key")
#             dim_type = dim.attrib.get("DimensionType")

#             dimensions_list.append({
#                 "source_id": source_id,
#                 "source_name": source_name,
#                 "source_key": source_key,
#                 "dimension_id": dim_id,
#                 "dimension_key": dim_key,
#                 "dimension_name": dim_name,
#                 "dimension_type": dim_type
#             })
#     print(dimensions_list[:5])
#     print(f"✔ {len(dimensions_list)} dimensions récupérées")
#     return dimensions_list


# # ============================
# # 🔽 3. Pour chaque dimension → récupérer ses éléments
# # ============================
# def extract_dimension_elements(**context):
#     dimensions = extract_dimensions()
#     total_dims = len(dimensions)

#     print(f"👉 Début extraction des éléments ({total_dims} dimensions à traiter)…")

#     final_rows = []

#     for idx, dim in enumerate(dimensions, start=1):
#         dim_id = dim["dimension_id"]
#         dim_name = dim["dimension_name"]

#         print(f"\n🔎 [{idx}/{total_dims}] Dimension : {dim_id} ({dim_name})")

#         body = f"""
#         <QueryMessage>
#             <DataWhere>
#                 <DimensionId WithData='true'>{dim_id}</DimensionId>
#             </DataWhere>
#         </QueryMessage>
#         """

#         try:
#             xml_resp = post_xml(BASE_URL + "GetDimensionElements", body)
#             print(f"   ✔ Requête API OK (taille réponse = {len(xml_resp)} chars)")
#         except Exception as e:
#             print(f"   ❌ Erreur API sur dimension {dim_id} : {e}")
#             continue

#         try:
#             root = ET.fromstring(xml_resp)
#         except Exception as e:
#             print(f"   ❌ Erreur parsing XML : {e}")
#             print(f"   ⚠ XML reçu : {xml_resp[:500]}...")
#             continue

#         elements = root.findall(".//Element")
#         print(f"   ➕ {len(elements)} éléments trouvés")

#         for elem in elements:
#             element_key = elem.attrib.get("KEY")
#             element_name = elem.attrib.get("NAME")

#             final_rows.append({
#                 "source_id": dim["source_id"],
#                 "source_name": dim["source_name"],
#                 "dimension_id": dim_id,
#                 "dimension_name": dim_name,
#                 "element_key": element_key,
#                 "element_name": element_name
#             })

#     print(f"\n✔ Extraction terminée : {len(final_rows)} éléments totaux")

#     # Sauvegarde CSV
#     print(f"📌 Écriture CSV → {OUTPUT_FILE}")

#     with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerow([
#             "source_id",
#             "source_name",
#             "dimension_id",
#             "dimension_name",
#             "element_key",
#             "element_name",
#         ])

#         for row in final_rows:
#             writer.writerow([
#                 row["source_id"],
#                 row["source_name"],
#                 row["dimension_id"],
#                 row["dimension_name"],
#                 row["element_key"],
#                 row["element_name"],
#             ])

#     print("✔ CSV dimensions + éléments généré.")

#     return OUTPUT_FILE


# # ============================
# # 🔽 4. Upload CSV dans MinIO
# # ============================
# def upload_to_minio(**context):
#     file_path = context["ti"].xcom_pull(task_ids="extract_ins_dimensions")

#     client = get_minio_client()

#     if not client.bucket_exists(BUCKET_RAW):
#         client.make_bucket(BUCKET_RAW)

#     client.fput_object(BUCKET_RAW, RAW_OBJECT, file_path)

#     print(f"📦 Upload CSV → MinIO bucket={BUCKET_RAW}/{RAW_OBJECT}")


# # ============================
# # 🚀 DAG Airflow
# # ============================
# with DAG(
#     dag_id="etl_ins_dimensions_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     task_extract = PythonOperator(
#         task_id="extract_ins_dimensions",
#         python_callable=extract_dimension_elements,
#         provide_context=True,
#     )

#     task_upload = PythonOperator(
#         task_id="upload_to_minio",
#         python_callable=upload_to_minio,
#         provide_context=True,
#     )

#     task_extract >> task_upload

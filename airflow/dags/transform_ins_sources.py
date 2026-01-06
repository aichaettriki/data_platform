import pandas as pd
from io import BytesIO
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# -----------------------------------
# Config MINIO
# -----------------------------------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

BUCKET_RAW = "raw"
BUCKET_TRANSFORMED = "transformed"

RAW_SOURCES_PATH = "sourcesINS/"
RAW_DIM_PATH = "dimensions/"

# -----------------------------------
# MinIO client
# -----------------------------------
def get_minio():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

# -----------------------------------
# Utilitaire : télécharger CSV depuis MinIO
# -----------------------------------
def minio_read_csv(client, bucket, path):
    response = client.get_object(bucket, path)
    content = response.read()
    return pd.read_csv(BytesIO(content))

# -----------------------------------
# Construire mapping : dimensionId → {key:name}
# -----------------------------------
def load_dimension_mapping(client):
    value_mapping = {}     # dim_id → {key: label}
    name_mapping = {}      # dim_id → dim_name
    file_mapping = {}      # dim_id → chemin CSV

    # objects = client.list_objects(BUCKET_RAW, prefix=RAW_DIM_PATH, recursive=True)
    print("🔍 Recherche des dimensions dans MinIO :")
    objects = list(client.list_objects(BUCKET_RAW, prefix=RAW_DIM_PATH, recursive=True))

    print(f"➡️ {len(objects)} objets trouvés sous {BUCKET_RAW}/{RAW_DIM_PATH}")
    for obj in objects[:10]:
        print("   -", obj.object_name)

    for obj in objects:
        if not obj.object_name.endswith(".csv"):
            continue

        filename = obj.object_name.split("/")[-1]

        base = filename.replace(".csv", "")

        if "_" not in base:
            print(f"⚠️ Nom de fichier invalide : {filename}")
            continue

        dim_id, dim_name = base.split("_", 1)
        dim_name = dim_name.replace("_", " ")


        df = minio_read_csv(client, BUCKET_RAW, obj.object_name)

        if "element_key" not in df.columns or "element_name" not in df.columns:
            print(f"⚠️ Colonnes manquantes dans {filename}")
            continue

        value_mapping[dim_id] = dict(
            zip(df["element_key"].astype(str), df["element_name"].astype(str))
        )
        name_mapping[dim_id] = dim_name
        file_mapping[dim_id] = obj.object_name

        print(f"✔ Dimension chargée : {dim_id} → {dim_name} ({len(df)} valeurs)")

    print(f"\n✅ TOTAL dimensions chargées : {len(value_mapping)}")
    return value_mapping, name_mapping, file_mapping

# -----------------------------------
# Transformation principale
# -----------------------------------
def transform_sources(**kwargs):
    client = get_minio()

    value_mapping, name_mapping, file_mapping = load_dimension_mapping(client)


    objects = client.list_objects(BUCKET_RAW, prefix=RAW_SOURCES_PATH, recursive=True)

    for obj in objects:
        if not obj.object_name.endswith(".csv"):
            continue

        print(f"\n🔎 Traitement : {obj.object_name}")

        df = minio_read_csv(client, BUCKET_RAW, obj.object_name)
        # -------------------------
        # 📌 DÉTECTION DES DIMENSIONS
        # -------------------------
        found_dims = []

        for col in df.columns:
            if col in value_mapping:
                found_dims.append(col)

        if not found_dims:
            print("⚠️ Aucune dimension trouvée dans ce fichier.")
        else:
            print("📌 Dimensions trouvées :")
            for dim_id in found_dims:
                dim_name = name_mapping.get(dim_id, "Inconnu")
                dim_file = file_mapping.get(dim_id, "N/A")

                print(f"   - {dim_id} → {dim_name}")
                print(f"     📄 Fichier dimension : {dim_file}")

                try:
                    dim_df = minio_read_csv(client, BUCKET_RAW, dim_file)
                    print("     🔍 Aperçu (5 premières lignes) :")
                    print(dim_df.head(5))
                except Exception as e:
                    print(f"     ❌ Impossible de lire le fichier dimension : {e}")

        # -------------------------
        # 1️⃣ REMPLACER LES VALEURS
        # -------------------------
        for col in df.columns:
            if col in value_mapping:
                print(f"   ➜ Remplacement valeurs via {col}")
                df[col] = (
                    df[col]
                    .astype(str)
                    .map(value_mapping[col])
                    .fillna(df[col])
                )

        # -------------------------
        # 2️⃣ RENOMMER LES COLONNES
        # -------------------------
        new_columns = {}
        for col in df.columns:
            if col in name_mapping:
                new_columns[col] = f"{name_mapping[col]} ({col})"
            else:
                new_columns[col] = col

        df.rename(columns=new_columns, inplace=True)

        # -------------------------
        # 3️⃣ WRITE TRANSFORMED
        # -------------------------
        output_path = obj.object_name.replace(
            "sourcesINS/", "sourcesINS_transformed/"
        )

        csv_bytes = df.to_csv(index=False).encode("utf-8")

        client.put_object(
            BUCKET_TRANSFORMED,
            output_path,
            BytesIO(csv_bytes),
            len(csv_bytes),
            content_type="text/csv"
        )

        print(df.head(3))
        print(f"✔ Upload transformé → {output_path}")

# -----------------------------------
# DAG definition
# -----------------------------------
with DAG(
    dag_id="transform_ins_sources",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ins", "transform", "minio"]
) as dag:

    task_transform = PythonOperator(
        task_id="transform_sources_data",
        python_callable=transform_sources,
        provide_context=True,
    )


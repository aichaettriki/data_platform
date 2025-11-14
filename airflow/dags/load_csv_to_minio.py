from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import pandas as pd
import os
from io import BytesIO


# ---------- CONFIG ----------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

BUCKET_RAW = "raw"
BUCKET_TRANSFORMED = "transformed"
BUCKET_REFINED = "refined"

LOCAL_FILE_PATH = "/opt/airflow/data/clients.csv"
RAW_OBJECT = "clients.csv"
TRANSFORMED_OBJECT = "clients_cleaned.csv"
REFINED_OBJECT = "clients_refined.csv"



# ---------- CLIENT MINIO ----------
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )



# ---------- TASK 1 : Upload to RAW ----------
def upload_to_raw():
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)

    client.fput_object(
        BUCKET_RAW, RAW_OBJECT, LOCAL_FILE_PATH
    )

    print("📌 Upload RAW terminé.")



# ---------- TASK 2 : data cleaning : TRANSFORMED ----------
def clean_and_transform():
    client = get_minio_client()

    response = client.get_object(BUCKET_RAW, RAW_OBJECT)

    df = pd.read_csv(BytesIO(response.read()), sep=";")  # ← ICI

    print("\n====== Colonnes lues depuis RAW ======")
    print(df.columns.tolist())

    df = df.drop_duplicates()

    output = df.to_csv(index=False, sep=";").encode("utf-8")  # ← garder séparateur

    if not client.bucket_exists(BUCKET_TRANSFORMED):
        client.make_bucket(BUCKET_TRANSFORMED)

    client.put_object(
        bucket_name=BUCKET_TRANSFORMED,
        object_name=TRANSFORMED_OBJECT,
        data=BytesIO(output),
        length=len(output),
        content_type="text/csv",
    )

    print("----------------> TRANSFORMED OK.")


# ---------- TASK 3 : enrichissement : REFINED ----------
def refine_data():
    client = get_minio_client()

    print("lecture du fichier transforme depuis MinIO...")

    response = client.get_object(BUCKET_TRANSFORMED, TRANSFORMED_OBJECT)
    df = pd.read_csv(BytesIO(response.read()), sep=";")


    print("\n====== Colonnes AVANT normalisation ======")
    print(df.columns.tolist())

    # normalisation des noms de colonnes
    df.columns = df.columns.str.lower().str.strip()

    print("\n====== Colonnes APRES normalisation ======")
    print(df.columns.tolist())

    print("\n====== Aperçu du DataFrame ======")
    print(df.head())

    # verif existence colonnes
    required = ["nom", "prenom"]
    missing = [c for c in required if c not in df.columns]

    if missing:
        print(f"\n❌ ERREUR : Colonnes manquantes : {missing}")
        raise KeyError(f"Colonnes manquantes : {missing}")

    # ajout fullname
    df["fullname"] = df["nom"] + " " + df["prenom"]

    # ajout Timestamp
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n====== Colonnes finales ======")
    print(df.columns.tolist())

    # ipload to refined
    print(" upload du fichier enrichi to refined...")

    output = df.to_csv(index=False, sep=";").encode("utf-8")  # ← garder séparateur
    
    if not client.bucket_exists(BUCKET_REFINED):
        client.make_bucket(BUCKET_REFINED)
    
    client.put_object(
        bucket_name=BUCKET_REFINED,
        object_name=REFINED_OBJECT,
        data=BytesIO(output),
        length=len(output),
        content_type="text/csv",
    )

    print("-------------->>>> REFINED terminé.")



# ---------- DAG ----------
with DAG(
    dag_id="etl_csv_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["minio", "etl", "csv"],
) as dag:

    task_upload_raw = PythonOperator(
        task_id="upload_to_raw",
        python_callable=upload_to_raw,
    )

    task_transform = PythonOperator(
        task_id="clean_transform",
        python_callable=clean_and_transform,
    )

    task_refine = PythonOperator(
        task_id="refine_file",
        python_callable=refine_data,
    )

    task_upload_raw >> task_transform >> task_refine

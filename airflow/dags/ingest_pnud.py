from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from minio import Minio

from common.dag_helpers import RAW_BUCKET
from common.minio_utils import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_PASSWORD,
)

LOCAL_INPUT_DIR = "/opt/airflow/data"
BASE_PAGE = "https://hdr.undp.org/data-center/documentation-and-downloads"
LOCAL_FILE_NAME = "hdr_composite_indices.csv"


def find_latest_hdr_csv(**context):

    print("🔎 Searching HDR CSV link on UNDP website...")

    response = requests.get(BASE_PAGE)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    csv_link = None

    for link in soup.find_all("a", href=True):
        if "Composite_indices_complete_time_series.csv" in link["href"]:
            csv_link = urljoin(BASE_PAGE, link["href"])
            break

    if not csv_link:
        raise Exception("HDR CSV link not found")

    print(f"✅ Found CSV link: {csv_link}")

    context["ti"].xcom_push(key="csv_url", value=csv_link)


def convert_csv_to_utf8(**context):
    import pandas as pd

    file_path = os.path.join(LOCAL_INPUT_DIR, LOCAL_FILE_NAME)

    # Lire le CSV en ISO-8859-1 (latin1)
    df = pd.read_csv(file_path, encoding="ISO-8859-1")

    # Réécrire en UTF-8
    df.to_csv(file_path, index=False, encoding="utf-8")

    print(f"✅ Converted {file_path} to UTF-8")


def download_hdr_csv(**context):

    csv_url = context["ti"].xcom_pull(key="csv_url")

    os.makedirs(LOCAL_INPUT_DIR, exist_ok=True)

    file_path = os.path.join(LOCAL_INPUT_DIR, LOCAL_FILE_NAME)

    print(f"⬇ Downloading {csv_url}")

    response = requests.get(csv_url)
    response.raise_for_status()

    with open(file_path, "wb") as f:
        f.write(response.content)

    print(f"✅ File downloaded: {file_path}")


def upload_to_minio(**context):

    execution_date = context["ds"]
    year, month, day = execution_date.split("-")

    bucket_name = RAW_BUCKET.replace("s3a://", "").replace("s3://", "")

    client = Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    file_path = os.path.join(LOCAL_INPUT_DIR, LOCAL_FILE_NAME)

    object_path = f"{year}/{month}/PNUD/hdr/{LOCAL_FILE_NAME}"

    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_path,
        file_path=file_path
    )

    print(f"✅ Uploaded to s3a://{bucket_name}/{object_path}")


with DAG(
    dag_id="01-ING__UNDP_HDR_to_Raw",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["raw", "PNUD", "hdr"],
) as dag:

    find_csv_link = PythonOperator(
        task_id="find_hdr_csv_link",
        python_callable=find_latest_hdr_csv,
        provide_context=True,
    )

    download_csv = PythonOperator(
        task_id="download_hdr_csv",
        python_callable=download_hdr_csv,
        provide_context=True,
    )
    
    convert_csv = PythonOperator(
    task_id="convert_csv_to_utf8",
    python_callable=convert_csv_to_utf8,
    provide_context=True,
)
    upload_csv = PythonOperator(
        task_id="upload_hdr_to_raw",
        python_callable=upload_to_minio,
        provide_context=True,
    )

    find_csv_link >> download_csv >> convert_csv >> upload_csv
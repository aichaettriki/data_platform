import os
from io import BytesIO
from minio import Minio

class MinIOConfig:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket_raw = "01-raw"
        self.bucket_transformed = "02-transformed"

def get_minio_client(config: MinIOConfig) -> Minio:
    endpoint = config.endpoint.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=config.access_key,
                 secret_key=config.secret_key, secure=False)

def find_latest_excel(minio_client, bucket, prefix) -> str:
    objects = minio_client.list_objects(bucket, prefix=prefix, recursive=True)
    files = [
        obj.object_name for obj in objects
        if obj.object_name.endswith(('.xlsx', '.xls'))
        and not obj.object_name.split('/')[-1].startswith('~')
    ]
    if not files:
        raise FileNotFoundError(f"No Excel files found in {bucket}/{prefix}")
    return sorted(files, reverse=True)[0]

def read_excel_bytes(minio_client, bucket, key) -> BytesIO:
    response = minio_client.get_object(bucket, key)
    return BytesIO(response.read())
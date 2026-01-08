
import sys
import time
import uuid
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp
import os
from dotenv import load_dotenv

# --- LINEAGE IMPORTS ---
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, Dataset, RunState

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


MARQUEZ_URL = get_env_var("MARQUEZ_URL")
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER")
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD")
POSTGRES_URL = get_env_var("POSTGRES_URL")
POSTGRES_USER = get_env_var("POSTGRES_USER")
POSTGRES_PASSWORD = get_env_var("POSTGRES_PASSWORD")

def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs):
    """ 
    Sends Sequential metadata + Data Schema to Marquez.
    Fixed to avoid 422 Unprocessable Entity error.
    """
    try:
        client = OpenLineageClient(url=MARQUEZ_URL)
        
        # 1. Convert Spark Schema to OpenLineage Schema Facet format
        fields = []
        for field in spark_df.schema.fields:
            fields.append({
                "name": field.name,
                "type": str(field.dataType)
            })

        # 2. Build Facets with mandatory internal keys to satisfy Marquez validation
        # We use standard OpenLineage keys for 'schema' and 'documentation'
        dataset_facets = {
            "schema": {
                "_producer": " spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
                "fields": fields
            },
            "documentation": {
                "_producer": " spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json#",
                "description": f"Type: {trans_type} | {description}"
            }
        }
        
        run_id = str(uuid.uuid4())
        
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat() + "Z",
            run=Run(runId=run_id),
            job=Job(namespace="itceq_prod", name=step_name),
            inputs=[Dataset(namespace="itceq_prod", name=i) for i in inputs],
            outputs=[
                Dataset(
                    namespace="itceq_prod", 
                    name=o, 
                    facets=dataset_facets
                ) for o in outputs
            ],
            producer=" spark-producer"
        )
        
        client.emit(event)
        print(f"📡 Marquez Updated: {step_name} (Schema + Metadata sent)")
    except Exception as e:
        print(f"⚠️ Marquez metadata error: {e}")

# --- SPARK LOGIC ---

def create_spark_session(app_name="ETL_Equipe_Production"):
    return (SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate())

def task_1_ingest(spark):
    print("🚀 Task 1: Ingesting RAW data")
    src = "s3a://raw/equipe.csv"

    df = spark.read.csv(src, header=True, sep=";", inferSchema=True)

    emit_marquez_step(
        df,
        "01_Ingestion",
        "Read raw CSV from MinIO",
        "EXTRACT",
        [src],
        []
    )

    return df


def task_2_clean(df):
    print("🧹 Task 2: Cleaning data")
    dest = "s3a://transformed/cleaned_data"

    df_clean = df.dropDuplicates()

    df_clean.write.mode("overwrite").parquet(dest)

    emit_marquez_step(
        df_clean,
        "02_Cleaning",
        "Removed duplicate rows",
        "TRANSFORMATION",
        [],
        [dest]
    )

    return df_clean


def task_3_refine(df):
    print("📦 Task 3: Refining data")
    dest = "s3a://refined/final_output"

    df_final = (
        df
        .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
        .withColumn("processed_at", current_timestamp())
    )

    df_final.write.mode("overwrite").parquet(dest)

    emit_marquez_step(
        df_final,
        "03_Refining",
        "Created fullname and added timestamps",
        "TRANSFORMATION",
        [],
        [dest]
    )
    return df_final


def task_4_write_postgres(df, jdbc_url=POSTGRES_URL):
    print("💾 Task 4: Writing data to PostgreSQL")
    table_name = "equipe"

    # Écriture dans PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("overwrite") \
        .save()

    # --- Lineage Marquez ---
    input_dataset = "s3a://refined/final_output"
    output_dataset = f"postgresql://{POSTGRES_URL}/{table_name}"

    emit_marquez_step(
        df,
        "04_Write_Postgres",
        f"Loaded refined data into PostgreSQL table {table_name}",
        "LOAD",
        [input_dataset],     # ✅ input = output de la task 3
        [output_dataset]    # ✅ output = Postgres
    )

    return df



if __name__ == "__main__":
    spark = create_spark_session(app_name="ETL_Equipe_Production")

    # MinIO config
    h = spark._jsc.hadoopConfiguration()
    h.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    h.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    h.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    h.set("fs.s3a.path.style.access", "true")
    h.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    try:
        df_raw = task_1_ingest(spark)
        df_clean = task_2_clean(df_raw)
        df_refined = task_3_refine(df_clean)
        task_4_write_postgres(df_refined)
    finally:
        spark.stop()


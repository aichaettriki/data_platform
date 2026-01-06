import sys
import time
import uuid
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp

# --- LINEAGE IMPORTS ---
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, Dataset, RunState

def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs):
    """ 
    Sends Sequential metadata + Data Schema to Marquez.
    Fixed to avoid 422 Unprocessable Entity error.
    """
    try:
        client = OpenLineageClient(url="http://marquez:5000")
        
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
    print("🚀 Task 1: Ingesting...")
    src = "s3a://raw/equipe.csv"
    dest = "s3a://transformed/raw_data"
    
    df = spark.read.csv(src, header=True, sep=";", inferSchema=True)
    df.write.mode("overwrite").parquet(dest)
    
    emit_marquez_step(df, "01_Ingestion", "Ingested CSV from MinIO", "EXTRACT", [src], [dest])
    return dest

def task_2_clean(spark, input_path):
    print("🧹 Task 2: Cleaning...")
    dest = "s3a://transformed/cleaned_data"
    
    df = spark.read.parquet(input_path)
    df_clean = df.dropDuplicates()
    df_clean.write.mode("overwrite").parquet(dest)
    
    emit_marquez_step(df_clean, "02_Cleaning", "Removed duplicate rows", "DEDUPLICATION", [input_path], [dest])
    return dest

def task_3_refine(spark, input_path):
    print("📦 Task 3: Refining...")
    dest = "s3a://refined/final_output"
    
    df = spark.read.parquet(input_path)
    df_final = (df.withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
                  .withColumn("processed_at", current_timestamp()))
    
    df_final.write.mode("overwrite").parquet(dest)
    
    emit_marquez_step(df_final, "03_Refining", "Created fullname and added timestamps", "TRANSFORMATION", [input_path], [dest])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("step", help="Step to run: ingest | clean | refine")
    args = parser.parse_args()

    spark = create_spark_session(app_name="ETL_Equipe_Production")

    # MinIO configuration
    h = spark._jsc.hadoopConfiguration()
    h.set("fs.s3a.access.key", "minio")
    h.set("fs.s3a.secret.key", "minio123")
    h.set("fs.s3a.endpoint", "http://minio:9000")
    h.set("fs.s3a.path.style.access", "true")
    h.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    try:
        if args.step == "ingest":
            task_1_ingest(spark)
        elif args.step == "clean":
            path_1 = "s3a://transformed/raw_data"
            task_2_clean(spark, path_1)
        elif args.step == "refine":
            path_2 = "s3a://transformed/cleaned_data"
            task_3_refine(spark, path_2)
        else:
            print(f"⚠️ Unknown step {args.step}")
    finally:
        spark.stop()

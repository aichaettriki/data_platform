


# import sys
# import time
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, concat_ws, current_timestamp, year, month, dayofmonth

# def create_spark_session(app_name="ETL Equipe via Spark + MinIO"):
#     # 1. Create session without listeners first to avoid the Builder crash
#     spark = (SparkSession.builder
#         .appName(app_name)
#         .config("spark.sql.catalogImplementation", "in-memory")
#         # Marquez Config
#         .config("spark.openlineage.transport.type", "http")
#         .config("spark.openlineage.transport.url", "http://itceq-marquez:5000")
#         .config("spark.openlineage.namespace", "itceq_prod")
#         .getOrCreate())

#     # 2. MANUALLY register the OpenLineage listener (The "Late Registration" fix)
#     try:
#         sc = spark.sparkContext
#         gw = sc._gateway
#         listener = gw.jvm.io.openlineage.spark.agent.OpenLineageSparkListener()
#         sc._jsc.sc().addSparkListener(listener)
#         print("✅ OpenLineage Listener registered successfully.")
#     except Exception as e:
#         print(f"⚠️ Marquez Listener failed: {e}")

#     # 3. Configure Hadoop for MinIO
#     h = spark._jsc.hadoopConfiguration()
#     h.set("fs.s3a.access.key", "minio")
#     h.set("fs.s3a.secret.key", "minio123")
#     h.set("fs.s3a.endpoint", "http://itceq-minio:9000") # Use your container name
#     h.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     h.set("fs.s3a.path.style.access", "true")
#     h.set("fs.s3a.connection.ssl.enabled", "false")
    
#     return spark

# def read_raw_csv(spark, raw_path="s3a://raw/"):
#     print(f"--- Action: Reading CSV from {raw_path} ---")
#     df = spark.read.csv(raw_path, header=True, sep=";", inferSchema=True)
#     return df

# def clean_and_write(df, transformed_root_path="s3a://transformed", dataset_name="equipe_spark"):
#     print("--- Action: Cleaning and Writing to Transformed ---")
#     df_clean = df.dropDuplicates()

#     # Optimized date extraction for the path
#     now = datetime.now()
#     output_path = f"{transformed_root_path}/{dataset_name}/{now.year}/{now.month}/{now.day}/"

#     df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
#     return df_clean

# def add_timestamp_and_write(df_clean, refined_root_path="s3a://refined"):
#     print("--- Action: Adding Logic and Writing to Refined ---")
#     df_refined = (df_clean
#         .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
#         .withColumn("timestamp", current_timestamp()))

#     now = datetime.now()
#     output_path = f"{refined_root_path}/{now.year}/{now.month}/{now.day}/"

#     df_refined.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
#     return df_refined

# def write_to_postgres(df_refined):
#     print("--- Action: Writing to Postgres ---")
#     # Use the container name 'itceq-postgres' from your docker-compose
#     jdbc_url = "jdbc:postgresql://itceq-postgres:5432/marquez" 
    
#     df_refined.write \
#         .format("jdbc") \
#         .option("url", jdbc_url) \
#         .option("driver", "org.postgresql.Driver") \
#         .option("dbtable", "equipe_output") \
#         .option("user", "marquez") \
#         .option("password", "marquez") \
#         .mode("overwrite") \
#         .save()

# if __name__ == "__main__":
#     if len(sys.argv) < 2:
#         print("Usage: etl_job.py <action>")
#         sys.exit(1)

#     action = sys.argv[1]
#     spark = create_spark_session(f"ETL Equipe - {action}")

#     try:
#         if action == "read_raw_csv":
#             df = read_raw_csv(spark)
#             df.show()

#         elif action == "clean_and_transformed":
#             df = read_raw_csv(spark)
#             clean_and_write(df)

#         elif action == "add_timestamp_refined":
#             df = read_raw_csv(spark)
#             df_clean = clean_and_write(df)
#             add_timestamp_and_write(df_clean)

#         elif action == "full_pipeline":
#             # This is the best one for Marquez because it shows the WHOLE flow
#             df = read_raw_csv(spark)
#             df_clean = clean_and_write(df)
#             df_refined = add_timestamp_and_write(df_clean)
#             write_to_postgres(df_refined)

#         else:
#             print(f"Unknown action: {action}")
        
#         print("✅ Action Completed. Syncing with Marquez...")
#         time.sleep(10) # Important to let the listener send the final POST
#     finally:
#         spark.stop()


import sys
import time
import uuid
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
        client = OpenLineageClient(url="http://itceq-marquez:5000")
        
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
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
                "fields": fields
            },
            "documentation": {
                "_producer": "itceq-spark-producer",
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
            producer="itceq-spark-producer"
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
    dest = "s3a://transformed/raw_data.parquet"
    
    df = spark.read.csv(src, header=True, sep=";", inferSchema=True)
    df.write.mode("overwrite").parquet(dest)
    
    emit_marquez_step(df, "01_Ingestion", "Ingested CSV from MinIO", "EXTRACT", [src], [dest])
    return dest

def task_2_clean(spark, input_path):
    print("🧹 Task 2: Cleaning...")
    dest = "s3a://transformed/cleaned_data.parquet"
    
    df = spark.read.parquet(input_path)
    df_clean = df.dropDuplicates()
    df_clean.write.mode("overwrite").parquet(dest)
    
    emit_marquez_step(df_clean, "02_Cleaning", "Removed duplicate rows", "DEDUPLICATION", [input_path], [dest])
    return dest

def task_3_refine(spark, input_path):
    print("📦 Task 3: Refining...")
    dest = "s3a://refined/final_output.parquet"
    
    df = spark.read.parquet(input_path)
    df_final = (df.withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
                  .withColumn("processed_at", current_timestamp()))
    
    df_final.write.mode("overwrite").parquet(dest)
    
    emit_marquez_step(df_final, "03_Refining", "Created fullname and added timestamps", "TRANSFORMATION", [input_path], [dest])

if __name__ == "__main__":
    spark = create_spark_session()
    
    # MinIO credentials
    h = spark._jsc.hadoopConfiguration()
    h.set("fs.s3a.access.key", "minio")
    h.set("fs.s3a.secret.key", "minio123")
    h.set("fs.s3a.endpoint", "http://itceq-minio:9000")
    h.set("fs.s3a.path.style.access", "true")
    h.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    try:
        path_1 = task_1_ingest(spark)
        path_2 = task_2_clean(spark, path_1)
        task_3_refine(spark, path_2)
        print("✅ Pipeline Complete.")
    finally:
        spark.stop()
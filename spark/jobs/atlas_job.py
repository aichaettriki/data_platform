import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp

# def create_spark_session(app_name="ETL_Equipe_Production"):
#     return (SparkSession.builder
#         .appName(app_name)
#         .enableHiveSupport()
#         .config("spark.sql.catalogImplementation", "hive")
        
#         # --- 1. THE LISTENERS (Required for Arrows) ---
#         .config("spark.extraListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker")
#         .config("spark.sql.queryExecutionListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker")
        
#         # --- 2. THE FORCE FIXES (Ensures names match Atlas) ---
#         # Forces cluster name to match your atlas-application.properties
#         .config("spark.atlas.cluster.name", "itceq_prod") 
        
#         # Removes the 'app-2025...' prefix so the graph nodes can 'stitch' together
#         .config("spark.atlas.spark.app.id.as.qualified.name", "false")
        
#         # Points the listener to the Atlas REST API
#         .config("spark.atlas.rest.address", "http://atlas-server:21000")
        
#         # Enables detailed column tracking (The Gear Icons)
#         .config("spark.atlas.column.lineage.enabled", "true")
        
#         .getOrCreate())

def create_spark_session(app_name="ETL_Equipe_Production"):
    # Define the Atlas properties as a string
    atlas_flags = (
        "-Datlas.spark.app.id.as.qualified.name=false "
        "-Datlas.cluster.name=itceq_prod "
        "-Datlas.rest.address=http://atlas-server:21000"
    )

    return (SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.catalogImplementation", "hive")
        # 1. Listeners
        .config("spark.extraListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker")
        .config("spark.sql.queryExecutionListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker")
        
        # 2. FORCE THE JVM to ignore the app-id
        .config("spark.driver.extraJavaOptions", atlas_flags)
        .config("spark.executor.extraJavaOptions", atlas_flags)
        
        # 3. Standard Configs (as backup)
        .config("spark.atlas.cluster.name", "itceq_prod")
        .config("spark.atlas.spark.app.id.as.qualified.name", "false")
        .config("spark.atlas.rest.address", "http://atlas-server:21000")
        .getOrCreate())


def step_1_register_source(spark, raw_path="s3a://raw/"):
    """
    Identifies WHERE the data comes from.
    """
    print(f"--- Step 1: Connecting to Source {raw_path} ---")
    
    # We read from the file and write to a table. 
    # This creates a 'spark_process' entity (The first arrow in your graph)
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("sep", ";")
          .option("inferSchema", "true")
          .load(raw_path))
    
    table_name = "default.raw_source_data"
    df.write.mode("overwrite").saveAsTable(table_name)
    
    # Push Details to Atlas about the source
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'data_origin' = 'MinIO S3 bucket: raw',
            'ingestion_method' = 'Spark Dataframe Write',
            'file_format' = 'CSV',
            'description' = 'Raw employee data from production'
        )
    """)

def step_2_clean_data(spark):
    """
    Identifies WHAT was modified (Deduplication).
    """
    print("--- Step 2: Modification - Removing Duplicates ---")
    # Read from catalog to STITCH the graph
    df = spark.table("default.raw_source_data")
    df_clean = df.dropDuplicates()
    
    table_name = "default.cleaned_equipe_spark"
    df_clean.write.mode("overwrite").format("csv").option("header","true").saveAsTable(table_name)

    # Push Detailed Modification info to Atlas
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'action_taken' = 'Deduplication',
            'modification_detail' = 'Identified and removed identical rows',
            'source_node' = 'default.raw_source_data'
        )
    """)

def step_3_refine_data(spark):
    """
    Identifies the LOGIC used (Concatenation).
    """
    print("--- Step 3: Modification - Applying Business Logic ---")
    # Read from catalog to STITCH the graph
    df_clean = spark.table("default.cleaned_equipe_spark")
    
    df_refined = (df_clean
        .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
        .withColumn("processed_at", current_timestamp()))

    table_name = "default.refined_equipe_spark"
    df_refined.write.mode("overwrite").format("csv").option("header","true").saveAsTable(table_name)

    # Push Business Logic details to Atlas
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'transformation_logic' = 'concat_ws(" ", nom, prenom)',
            'added_columns' = 'fullname, processed_at'
        )
    """)

if __name__ == "__main__":
    spark = create_spark_session()
    
    # MinIO credentials
    h = spark._jsc.hadoopConfiguration()
    h.set("fs.s3a.endpoint", "http://minio:9000")
    h.set("fs.s3a.access.key", "minio")
    h.set("fs.s3a.secret.key", "minio123")
    h.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    h.set("fs.s3a.path.style.access", "true")

    try:
        # Note: Ensure the raw_path is a directory containing your CSVs
        step_1_register_source(spark)
        step_2_clean_data(spark)
        step_3_refine_data(spark)
        
        print("✅ Workflow Complete. Syncing Lineage with Atlas (Wait 60s)...")
        # Increased to 60s to ensure the background Kafka producer finishes
        time.sleep(60) 
    finally:
        spark.stop()


 
import os
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType
 
# Standard Schema for incoming data
TARGET_SCHEMA = StructType([
    StructField("periode", StringType(), True),
    StructField("variable", StringType(), True),
    StructField("version", StringType(), True),
    StructField("base", StringType(), True),
    StructField("valeur", DoubleType(), True),
    StructField("date_chargement", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("version_active", LongType(), True),
    StructField("pays", StringType(), True),
    StructField("pays_fr", StringType(), True),
    StructField("code_secteur_produit", StringType(), True),
    StructField("lib_secteur_produit", StringType(), True)
])
 
class MinIOConfig:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.bucket_transformed = "02-transformed"
        self.bucket_refined = "03-refined"
        # Local path for Excel sync
        self.local_data_path = os.getenv("DATA_PATH_local", "/mnt/local_data")
 
def create_spark(app_name):
    return (SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
       
        # --- STABILITY CONFIGS FOR MINIO ---
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate())
 
def get_excel_local_path(config: MinIOConfig) -> str:
    """Returns the full path to the dimension referentiel Excel file."""
    return os.path.join(config.local_data_path, "dim_variable_referentiel.xlsx")
 
def read_transformed(spark, config, sub_folder=""):
    """Reads data from transformed bucket. sub_folder can be 'WORLD_BANK/Countries/' etc."""
    path = f"s3a://{config.bucket_transformed}/{sub_folder}"
    df = spark.read.option("recursiveFileLookup", "true").parquet(path)
   
    # Clean N/A and cast types
    for field in TARGET_SCHEMA.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name,
                F.when(F.col(field.name) == "N/A", None)
                 .otherwise(F.col(field.name))
                 .cast(field.dataType))
    return df.filter(F.col("version_active") == 1)
 
def write_refined(df, folder_name, config):
    """Writes to refined bucket with a lineage breaker to prevent S3 errors."""
    path = f"s3a://{config.bucket_refined}/{folder_name}/"
   
    # Lineage breaker: Force computation before overwrite
    df_final = df.cache()
    df_final.count()
   
    logging.info(f"Overwriting refined data at: {path}")
    df_final.coalesce(1).write.mode("overwrite").parquet(path)
   
    df_final.unpersist()
    logging.info(f"Successfully wrote to {path}")
 
def get_tech_snapshot(df):
    """Extracts latest tech metadata from the dataframe."""
    row = df.select("date_chargement", "version_active").orderBy(F.col("date_chargement").desc()).limit(1).collect()
    if row:
        return {"date_chargement": row[0]["date_chargement"], "version_active": row[0]["version_active"]}
    return {"date_chargement": None, "version_active": 1}
 
def get_next_id(spark, config, folder_name, id_column):
    """Returns the next available ID by checking the existing Parquet file in Refined."""
    path = f"s3a://{config.bucket_refined}/{folder_name}/"
    try:
        existing = spark.read.parquet(path)
        max_val = existing.agg(F.max(id_column)).collect()[0][0]
        return (max_val or 0) + 1
    except Exception:
        # File doesn't exist yet (first run)
        return 1
 
# import os
# import logging
# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType
 
# # Initialize basic logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
# # Standard Schema for incoming data
# TARGET_SCHEMA = StructType([
#     StructField("periode", StringType(), True),
#     StructField("variable", StringType(), True),
#     StructField("version", StringType(), True),
#     StructField("base", StringType(), True),
#     StructField("valeur", DoubleType(), True),
#     StructField("date_chargement", TimestampType(), True),
#     StructField("source", StringType(), True),
#     StructField("version_active", LongType(), True),
#     StructField("pays", StringType(), True),
#     StructField("pays_fr", StringType(), True),
#     StructField("code_secteur_produit", StringType(), True),
#     StructField("lib_secteur_produit", StringType(), True)
# ])
 
# class MinIOConfig:
#     """Class to hold MinIO and Local environment configurations."""
#     def __init__(self):
#         self.endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
#         self.access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
#         self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
#         self.bucket_transformed = "02-transformed"
#         self.bucket_refined = "03-refined"
#         # Local path for Excel sync
#         self.local_data_path = os.getenv("DATA_PATH_local", "/mnt/local_data")
 
# def get_config():
#     """Helper function to return a config instance.
#     Fixes 'NameError: name get_config is not defined' in your jobs.
#     """
#     return MinIOConfig()
 
# def create_spark(app_name):
#     """Creates a Spark session with S3A and MinIO stability configurations."""
#     return (SparkSession.builder.appName(app_name)
#         .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
#         .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
#         .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
       
#         # --- STABILITY CONFIGS FOR MINIO ---
#         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
#         .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
#         .config("spark.hadoop.fs.s3a.fast.upload", "true")
#         .config("spark.sql.parquet.enableVectorizedReader", "true")
#         .getOrCreate())
 
# def get_excel_local_path(config: MinIOConfig) -> str:
#     """Returns the full path to the dimension referentiel Excel file."""
#     return os.path.join(config.local_data_path, "dim_variable_referentiel.xlsx")
 
# def read_transformed(spark, config, sub_folder=""):
#     """Reads data from transformed bucket. sub_folder can be 'WORLD_BANK/Countries/Axe_Referentiel/' etc."""
#     # Ensure path ends with slash if not empty
#     if sub_folder and not sub_folder.endswith('/'):
#         sub_folder += '/'
       
#     path = f"s3a://{config.bucket_transformed}/{sub_folder}"
#     logging.info(f"Reading transformed data from: {path}")
   
#     df = spark.read.option("recursiveFileLookup", "true").parquet(path)
   
#     # Clean N/A and cast types based on TARGET_SCHEMA
#     for field in TARGET_SCHEMA.fields:
#         if field.name in df.columns:
#             df = df.withColumn(field.name,
#                 F.when(F.col(field.name) == "N/A", None)
#                  .otherwise(F.col(field.name))
#                  .cast(field.dataType))
   
#     # Filtering for active versions is standard for Refined layer
#     if "version_active" in df.columns:
#         df = df.filter(F.col("version_active") == 1)
       
#     return df
 
# def write_refined(df, folder_name, config):
#     """Writes to refined bucket with a lineage breaker to prevent S3 overwrite errors."""
#     path = f"s3a://{config.bucket_refined}/{folder_name}/"
   
#     # Lineage breaker: Force computation and storage in memory before the overwrite starts
#     # This prevents the 'FileNotFound' error when Spark tries to read what it is currently deleting.
#     df_final = df.cache()
#     df_count = df_final.count()
#     logging.info(f"Preparing to write {df_count} rows to {path}")
   
#     logging.info(f"Overwriting refined data at: {path}")
#     df_final.coalesce(1).write.mode("overwrite").parquet(path)
   
#     df_final.unpersist()
#     logging.info(f"Successfully finished writing to {path}")
 
# def get_tech_snapshot(df):
#     """Extracts the latest loading date and active version metadata from a dataframe."""
#     try:
#         row = df.select("date_chargement", "version_active")\
#                 .orderBy(F.col("date_chargement").desc())\
#                 .limit(1).collect()
#         if row:
#             return {"date_chargement": row[0]["date_chargement"], "version_active": row[0]["version_active"]}
#     except Exception as e:
#         logging.warning(f"Could not extract tech snapshot: {e}")
       
#     return {"date_chargement": None, "version_active": 1}
 
# def get_next_id(spark, config, folder_name, id_column):
#     """Returns the next available ID by checking the existing Parquet file in the Refined bucket."""
#     path = f"s3a://{config.bucket_refined}/{folder_name}/"
#     try:
#         existing = spark.read.parquet(path)
#         max_val = existing.agg(F.max(id_column)).collect()[0][0]
#         return (int(max_val) if max_val is not None else 0) + 1
#     except Exception:
#         # If file doesn't exist (first run), start at ID 1
#         logging.info(f"No existing data found at {path}. Starting IDs from 1.")
#         return 1
 
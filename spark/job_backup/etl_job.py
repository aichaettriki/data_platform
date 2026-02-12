import sys
import os
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dotenv import load_dotenv

# --- CHARGEMENT DES VARIABLES (Ta méthode) ---
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

# Variables MinIO
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER")
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD")

# Variables Postgres (Indispensables pour la Task 4)
POSTGRES_URL = get_env_var("POSTGRES_URL")
POSTGRES_USER = get_env_var("POSTGRES_USER")
POSTGRES_PASSWORD = get_env_var("POSTGRES_PASSWORD")

# --- SPARK SESSION ---

def create_spark_session():
    """
    On laisse Airflow piloter le nom du Job via la config OpenLineage.
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Configuration Hadoop S3A pour MinIO
    h = spark._jsc.hadoopConfiguration()
    h.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    h.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    h.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    h.set("fs.s3a.path.style.access", "true")
    h.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return spark

# --- LOGIQUE DES TÂCHES ---

def task_1_ingest(spark):
    """ Étape 1 : RAW -> Landing Parquet """
    spark.sparkContext.setJobDescription("Action : Ingestion CSV Raw")
    
    # Schéma manuel pour éviter le job technique "inferSchema"
    schema = StructType([
        StructField("nom", StringType(), True),
        StructField("prenom", StringType(), True),
        StructField("number", IntegerType(), True)
    ])
    
    df = spark.read.csv("s3a://01-raw/equipe1.csv", header=True, sep=";", schema=schema)
    df.write.mode("overwrite").parquet("s3a://01-raw/step_1_ingested.parquet")

def task_2_clean(spark):
    """ Étape 2 : DÉDUPLICATION """
    spark.sparkContext.setJobDescription("Action : Suppression des doublons")
    
    df = spark.read.parquet("s3a://01-raw/step_1_ingested.parquet")
    df_clean = df.dropDuplicates()
    df_clean.write.mode("overwrite").parquet("s3a://02-transformed/step_2_dedup.parquet")

def task_3_refine(spark):
    """ Étape 3 : ENRICHISSEMENT (Column Lineage) """
    spark.sparkContext.setJobDescription("Action : Calcul Fullname et Horodatage")
    
    df = spark.read.parquet("s3a://02-transformed/step_2_dedup.parquet")
    df_final = df.withColumn("fullname", concat_ws(" ", col("nom"), col("prenom"))) \
                 .withColumn("processed_at", current_timestamp())
    
    df_final.write.mode("overwrite").parquet("s3a://03-refined/step_3_final.parquet")

def task_4_write_postgres(spark):
    """ Étape 4 : CHARGEMENT POSTGRES """
    spark.sparkContext.setJobDescription("Action : Export final vers PostgreSQL")
    
    df = spark.read.parquet("s3a://03-refined/step_3_final.parquet")
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "equipe") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("overwrite") \
        .save()

# --- POINT D'ENTRÉE ---

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: etl_job.py [ingest|clean|refine|write_postgres]")
        sys.exit(1)

    task_name = sys.argv[1]
    spark_sess = create_spark_session()

    try:
        if task_name == "ingest": task_1_ingest(spark_sess)
        elif task_name == "clean": task_2_clean(spark_sess)
        elif task_name == "refine": task_3_refine(spark_sess)
        elif task_name == "write_postgres": task_4_write_postgres(spark_sess)
        print(f"✅ Succès de la tâche : {task_name}")
    except Exception as e:
        print(f"❌ ERREUR lors de l'exécution de {task_name}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark_sess.stop()
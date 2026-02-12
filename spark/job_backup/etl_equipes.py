
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.functions import year, month, dayofmonth, current_timestamp
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value



MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER")
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD")
POSTGRES_URL = get_env_var("POSTGRES_URL")
POSTGRES_USER = get_env_var("POSTGRES_USER")
POSTGRES_PASSWORD = get_env_var("POSTGRES_PASSWORD")

def create_spark_session(app_name="ETL Equipe via Spark + MinIO"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoopConf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark


def read_raw_csv(spark, raw_path):
    raw_path = "s3a://01-raw/"
    df = spark.read.csv(raw_path, header=True, sep=";", inferSchema=True)
    df.show()
    print("-------->>>>>>>>>>>>>>> files :")
    for f in df.inputFiles():
        print(" -", f)
        df.printSchema()
        df.show()
    return df


def clean_and_write(df, transformed_root_path="s3a://02-transformed", dataset_name="equipe_spark"):
    now = current_timestamp()
    df_clean = df.dropDuplicates()
    # date extraction
    now = datetime.now()
    annee, mois, jour = now.year, now.month, now.day

    # ajouter le nom du dataset dans le chemin
    output_path = f"{transformed_root_path}/{dataset_name}/{annee}/{mois}/{jour}/"

    # Écriture
    df_clean.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    return df_clean

def add_timestamp_and_write(df_clean, refined_root_path="s3a://03-refined", dataset_name="equipe_spark"):
    now = current_timestamp()

    df_refined = (
        df_clean
        .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
        .withColumn("timestamp", current_timestamp())
    )

    # Extraire la date d’exécution
    annee = df_clean.select(year(now)).first()[0]
    mois = df_clean.select(month(now)).first()[0]
    jour = df_clean.select(dayofmonth(now)).first()[0]

    output_path = f"{refined_root_path}/{annee}/{mois}/{jour}/"

    df_refined.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    return output_path


def write_to_postgres(df_refined, jdbc_url=POSTGRES_URL):
    df_refined.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "equipe") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("overwrite") \
        .save()
import sys

if __name__ == "__main__":
    action = sys.argv[1]  
    app_name = f"ETL Equipe - {action}"
    spark = create_spark_session(app_name)

    if action == "read_raw_csv":
        df = read_raw_csv(spark, "s3a://01-raw/")

    elif action == "clean_and_transformed":
        df = read_raw_csv(spark, "s3a://01-raw/")
        clean_and_write(df, "s3a://02-transformed/equipe_spark")
    elif action == "add_timestamp_refined":
        df = read_raw_csv(spark, "s3a://01-raw/equipe.csv")
        df_clean = clean_and_write(df, "s3a://02-transformed/equipe_spark")
        add_timestamp_and_write(df_clean, "s3a://03-refined/equipe_spark")


    elif action == "write_postgres":
        today = datetime.today()
        df = spark.read.csv(f"s3a://03-refined/equipe_spark/{today.year}/{today.month}/{today.day}/", header=True, inferSchema=True)
        write_to_postgres(df)

    else:
        print("Unknown action:", action)
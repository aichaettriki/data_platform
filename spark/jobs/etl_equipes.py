from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.functions import year, month, dayofmonth, current_timestamp
from datetime import datetime


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ETL Equipe via Spark + MinIO")
        .getOrCreate()
    )

    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "minio")
    hadoopConf.set("fs.s3a.secret.key", "minio123")
    hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    return spark


def read_raw_csv(spark, raw_path):
    raw_path = "s3a://raw/"
    df = spark.read.csv(raw_path, header=True, sep=";", inferSchema=True)
    df.show(5)
    df.printSchema()
    return df


from pyspark.sql.functions import year, month, dayofmonth, current_timestamp

def clean_and_write(df, transformed_root_path="s3a://transformed", dataset_name="equipe_spark"):
    now = current_timestamp()

    df_clean = df.dropDuplicates()

    # date extraction
    annee = df_clean.select(year(now)).first()[0]
    mois = df_clean.select(month(now)).first()[0]
    jour = df_clean.select(dayofmonth(now)).first()[0]

    # ajouter le nom du dataset dans le chemin
    output_path = f"{transformed_root_path}/{dataset_name}/{annee}/{mois}/{jour}/"

    # Écriture
    df_clean.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    return df_clean

def add_timestamp_and_write(df_clean, refined_root_path="s3a://refined", dataset_name="equipe_spark"):
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


def write_to_postgres(df_refined, jdbc_url):
    
    df_refined.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "equipe") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("overwrite") \
        .save()
import sys

if __name__ == "__main__":
    action = sys.argv[1]  
    spark = create_spark_session()

    if action == "read_raw_csv":
        df = read_raw_csv(spark, "s3a://raw/")

    elif action == "clean_and_transformed":
        df = read_raw_csv(spark, "s3a://raw/")
        clean_and_write(df, "s3a://transformed/equipe_spark")

    elif action == "add_timestamp_refined":
        df = read_raw_csv(spark, "s3a://raw/equipe.csv")
        df_clean = clean_and_write(df, "s3a://transformed/equipe_spark")
        add_timestamp_and_write(df_clean, "s3a://refined/equipe_spark")

    elif action == "write_postgres":
        today = datetime.today()
        df = spark.read.csv(f"s3a://refined/equipe_spark/{today.year}/{today.month}/{today.day}/", header=True, inferSchema=True)
        write_to_postgres(df, "jdbc:postgresql://postgres-airflow:5432/airflow")
        # df = spark.read.csv("s3a://refined/equipe", header=True, inferSchema=True)
        # write_to_postgres(df, "jdbc:postgresql://postgres-airflow:5432/airflow")

    else:
        print("Unknown action:", action)

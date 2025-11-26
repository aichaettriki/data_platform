from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp

def create_spark_session():
    # spark = (
    #     SparkSession.builder
    #     .appName("ETL Equipe via Spark + MinIO")
    #     .getOrCreate()

    spark = SparkSession.builder \
    .appName("ETL with Atlas") \
    .config("spark.sql.queryExecutionListeners", "org.apache.atlas.spark.atlas.SparkAtlasEventListener") \
    .config("spark.hadoop.atlas.rest.address", "http://atlas:21000") \
    .getOrCreate()
    # )

    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "minio")
    hadoopConf.set("fs.s3a.secret.key", "minio123")
    hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    return spark


def read_raw_csv(spark, raw_path):
    raw_path = "s3a://raw/equipe1.csv"
    df = spark.read.csv(raw_path, header=True, sep=";")
    df.show(5)
    return df


def clean_and_write(df, transformed_path):
    transformed_path = "s3a://transformed/equipe_spark.csv"
    df_clean = df.dropDuplicates()
    df_clean.coalesce(1).write.csv(transformed_path, mode="overwrite", header=True)
    return df_clean


def add_timestamp_and_write(df_clean, refined_path):
    refined_path = refined_path
    df_refined = (
        df_clean
        .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
        .withColumn("timestamp", current_timestamp())
    )
    df_refined.coalesce(1).write.csv(refined_path, mode="overwrite", header=True)
    return df_refined


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
        df = read_raw_csv(spark, "s3a://raw/equipe1.csv")

    elif action == "clean_and_transformed":
        df = read_raw_csv(spark, "s3a://raw/equipe1.csv")
        clean_and_write(df, "s3a://transformed/equipe_spark.csv")

    elif action == "add_timestamp_refined":
        df = read_raw_csv(spark, "s3a://raw/equipe1.csv")
        df_clean = clean_and_write(df, "s3a://transformed/equipe_spark.csv")
        add_timestamp_and_write(df_clean, "s3a://refined/equipe_spark.csv")

    elif action == "write_postgres":
        df = spark.read.csv("s3a://refined/equipe_spark.csv", header=True , inferSchema=True)
        write_to_postgres(df, "jdbc:postgresql://postgres-airflow:5432/airflow")

    else:
        print("Unknown action:", action)

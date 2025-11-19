from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp

def main():

    spark = (
        SparkSession.builder
        .appName("ETL Equipe via Spark + MinIO")
        .getOrCreate()
    )

    # --- Config MinIO ---
    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "minio")
    hadoopConf.set("fs.s3a.secret.key", "minio123")
    hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoopConf.set("fs.s3a.path.style.access", "true")

    RAW_PATH = "/data/raw/equipe.csv"
    TRANSFORMED_PATH = "/data/transformed/equipe_transformed.csv"
    REFINED_PATH = "/data/refined/equipe_refined.csv"


    # --- READ RAW ---
    df = spark.read.csv(RAW_PATH, header=True, sep=";")

    # --- CLEAN / TRANSFORM ---
    df_clean = df.dropDuplicates()

    df_clean.write.csv(TRANSFORMED_PATH, mode="overwrite", header=True)

    # --- REFINED ---
    df_refined = (
        df_clean
        .withColumn("nom", col("nom"))
        .withColumn("prenom", col("prenom"))
        .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
        .withColumn("timestamp", current_timestamp())
    )

    df_refined.write.csv(REFINED_PATH, mode="overwrite", header=True)

    # --- LOAD PostgreSQL ---
    jdbc_url = "jdbc:postgresql://postgres-airflow:5432/airflow"

    df_refined.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "equipe") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("overwrite") \
        .save()

    print("ETL Spark Terminé !")

    spark.stop()


if __name__ == "__main__":
    main()

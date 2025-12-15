import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.functions import year, month, dayofmonth
from datetime import datetime

def create_spark_session(app_name="ETL Equipe via Spark + MinIO"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport() # Essential for Atlas
        .config("spark.extraListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker")
        .config("spark.atlas.rest.address", "http://atlas:21000")
        .config("spark.atlas.kafka.bootstrap.servers", "kafka:9092")
        .config("spark.atlas.cluster.name", "primary")
        .getOrCreate()
    )

    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "minio") 
    hadoopConf.set("fs.s3a.secret.key", "minio123")
    hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark

def read_raw_csv(spark, raw_path):
    # Registering the input as a Temp View helps Atlas track the read
    df = spark.read.csv(raw_path, header=True, sep=";", inferSchema=True)
    df.createOrReplaceTempView("source_raw_data")
    return df

def clean_and_write(df, transformed_root_path="s3a://transformed", dataset_name="equipe_spark"):
    now = current_timestamp()
    df_clean = df.dropDuplicates()

    row = df_clean.select(year(now), month(now), dayofmonth(now)).head()
    if not row: return df_clean
    annee, mois, jour = row

    output_path = f"{transformed_root_path}/{dataset_name}/{annee}/{mois}/{jour}/"
    
    # --- FIX IS HERE ---
    # Instead of just .csv(), we use .saveAsTable()
    # We provide the 'path' option so it still goes to your MinIO folder
    print(f"Writing Cleaned Data to Table: cleaned_{dataset_name}")
    
    df_clean.write \
        .mode("overwrite") \
        .option("header", True) \
        .option("path", output_path) \
        .format("csv") \
        .saveAsTable(f"cleaned_{dataset_name}") 
        # Atlas detects 'saveAsTable' and generates lineage!

    return df_clean

def add_timestamp_and_write(df_clean, refined_root_path="s3a://refined", dataset_name="equipe_spark"):
    now = current_timestamp()

    df_refined = (
        df_clean
        .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
        .withColumn("timestamp", current_timestamp())
    )

    row = df_clean.select(year(now), month(now), dayofmonth(now)).head()
    if not row: return None
    annee, mois, jour = row

    output_path = f"{refined_root_path}/{dataset_name}/{annee}/{mois}/{jour}/"

    # --- FIX IS HERE ---
    print(f"Writing Refined Data to Table: refined_{dataset_name}")
    
    df_refined.write \
        .mode("overwrite") \
        .option("header", True) \
        .option("path", output_path) \
        .format("csv") \
        .saveAsTable(f"refined_{dataset_name}")

    return output_path

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "read_raw_csv"
    spark = create_spark_session(f"ETL Equipe - {action}")

    if action == "read_raw_csv":
        read_raw_csv(spark, "s3a://raw/")
    elif action == "add_timestamp_refined":
        # We read, clean (write table 1), refine (write table 2)
        df = read_raw_csv(spark, "s3a://raw/") 
        df_clean = clean_and_write(df, "s3a://transformed", "equipe_spark")
        add_timestamp_and_write(df_clean, "s3a://refined", "equipe_spark")

    spark.stop()
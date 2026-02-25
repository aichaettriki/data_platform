import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType , LongType

TARGET_SCHEMA = StructType([
    StructField("annee", LongType(), True),
    StructField("variable", StringType(), True),
    StructField("version", StringType(), True),
    StructField("rang", LongType(), True),
    StructField("base", StringType(), True),
    StructField("valeur", DoubleType(), True),
    StructField("date_chargement", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("version_active", LongType(), True),
    StructField("pays", StringType(), True),
    StructField("code_secteur", StringType(), True),
    StructField("lib_secteur", StringType(), True),
    StructField("dim_id", StringType(), True),
    StructField("dim_key", StringType(), True),
])
# --------------------------------------------------
# CONFIG
# --------------------------------------------------
 
class MinIOConfig:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.bucket_transformed = "02-transformed"
        self.bucket_refined = "03-refined"
 
# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
 
def create_spark():
    return (
        SparkSession.builder
        .appName("Refined Builder")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )
 
# --------------------------------------------------
# MAIN PROCESSOR
# --------------------------------------------------
 
class RefinedProcessor:
 
    def __init__(self):
        self.config = MinIOConfig()
        self.spark = create_spark()
 
    def run(self):
        df = self.read_transformed()
        df = df.filter(F.col("version_active") == 1)
 
        dim_pays = self.build_dimension(df, "pays", "id_pays")
        dim_variable = self.build_dimension(df, "variable", "id_variable")
        dim_source = self.build_dimension(df, "Source", "id_source")
        dim_periode = self.build_dimension(df, "annee", "id_periode")
 
        fact = self.build_fact(df, dim_pays, dim_variable, dim_source, dim_periode)
 
        self.write_refined(dim_pays,"dimensions/dim_pays")
        self.write_refined(dim_variable, "dimensions/dim_variable")
        self.write_refined(dim_source, "dimensions/dim_source")
        self.write_refined(dim_periode, "dimensions/dim_periode")
        self.write_refined(fact, "fact_macroeco")
 
        self.spark.stop()
 
    # --------------------------------------------------
    # READ TRANSFORMED
    # --------------------------------------------------
 
    def read_transformed(self):

        path = f"s3a://{self.config.bucket_transformed}/"
        df = (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .parquet(path)
        )
        for field in TARGET_SCHEMA.fields:
            if field.name in df.columns:
                df = df.withColumn(
                    field.name,
                    F.col(field.name).cast(field.dataType)
                )

        return df
 
    # --------------------------------------------------
    # BUILD DIMENSION GENERIC
    # --------------------------------------------------
 
    def build_dimension(self, df, column_name, id_name):
        window_spec = Window.orderBy(column_name)
 
        dim = (
            df.select(column_name)
            .distinct()
            .withColumn(id_name, F.row_number().over(window_spec))
        )
 
        return dim
 
    # --------------------------------------------------
    # BUILD FACT
    # --------------------------------------------------
 
    def build_fact(self, df, dim_pays, dim_variable, dim_source, dim_periode):
 
        fact = (
            df.join(dim_pays, "pays", "left")
              .join(dim_variable, "variable", "left")
              .join(dim_source, "source", "left")
              .join(dim_periode, "annee", "left")
              .select(
                  "id_pays",
                  "id_variable",
                  "id_source",
                  "id_periode",
                  F.col("valeur").cast("double").alias("valeur"),
                  F.col("rang").cast("int").alias("rang")
              )
        )
 
        return fact
 
    # --------------------------------------------------
    # WRITE
    # --------------------------------------------------
 
    def write_refined(self, df, folder_name):
        path = f"s3a://{self.config.bucket_refined}/{folder_name}/"
        df.coalesce(1).write.mode("overwrite").parquet(path)
 
 
# --------------------------------------------------
# EXECUTION
# --------------------------------------------------
 
if __name__ == "__main__":
    RefinedProcessor().run()
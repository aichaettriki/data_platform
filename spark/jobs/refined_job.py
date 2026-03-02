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
        self.null_values = ["", "null", "NULL", "None", "none", "N/A", "n/a", "NA"]
    def run(self):
        df = self.read_transformed()
        df = df.filter(F.col("version_active") == 1)
 
        dim_pays = self.build_dimension(df, "pays", "id_pays")
        dim_variable = self.build_dim_variable(df)
        dim_source = self.build_dimension(df, "Source", "id_source")
        dim_periode = self.build_dimension(df, "annee", "id_periode")
 
        fact = self.build_fact(df, dim_pays, dim_variable, dim_source, dim_periode)
 
        self.write_refined(dim_pays,"dimensions/dim_pays")
        self.write_refined(dim_variable, "dimensions/dim_variable")
        self.write_refined(dim_source, "dimensions/dim_source")
        self.write_refined(dim_periode, "dimensions/dim_periode")
        self.write_refined(fact, "fact_macroeco")
        
        self.spark.stop()
 
    def normalize_column(self, df, column_name):
 
        return df.withColumn(
            column_name,
            F.when(
                F.col(column_name).isNull() |
                F.trim(F.col(column_name)).isin(self.null_values),
                F.lit("None")
            ).otherwise(F.col(column_name))
        )
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
 
        column_name = column_name.lower()
 
        df_clean = self.normalize_column(df, column_name)
 
        window_spec = Window.orderBy(column_name)
 
        dim = (
            df_clean
            .filter(F.col(column_name) != "None")
            .select(column_name)
            .distinct()
            .withColumn(id_name, F.row_number().over(window_spec))
        )
 
        # UNKNOWN row
        unknown_row = self.spark.createDataFrame(
            [("None", -1)],
            [column_name, id_name]
        )
 
        dim = dim.unionByName(unknown_row)
 
        return dim
    

    def build_dim_variable(self, df):

        # Normalisation
        df = self.normalize_column(df, "variable")
        df = self.normalize_column(df, "source")
        df = self.normalize_column(df, "dim_key")

        # On garde seulement les colonnes utiles
        df_var = df.select("variable", "source", "dim_key").distinct()

        # Séparer celles qui ont déjà un dim_key
        df_with_key = df_var.filter(F.col("dim_key") != "None")

        # Celles sans dim_key
        df_without_key = df_var.filter(F.col("dim_key") == "None")

        # Fenêtre par source pour auto increment
        window_spec = Window.partitionBy("source").orderBy("variable")

        df_generated = (
            df_without_key
            .withColumn("auto_id", F.row_number().over(window_spec))
            .withColumn(
                "dim_key",
                F.concat(
                    F.lower(F.substring(F.col("source"), 1, 3)),
                    F.col("auto_id")
                )
            )
            .drop("auto_id")
        )

        # Union des deux
        dim_variable = df_with_key.unionByName(df_generated)

        # Création surrogate key numérique
        final_window = Window.orderBy("dim_key")

        dim_variable = (
            dim_variable
            .withColumn("id_variable", F.row_number().over(final_window))
        )

        return dim_variable
    
 
    # --------------------------------------------------
    # BUILD FACT
    # --------------------------------------------------
    def build_fact(self, df, dim_pays, dim_variable, dim_source, dim_periode):
        df_alias = df.alias("f")
        dim_pays_alias = dim_pays.alias("dp")
        dim_variable_alias = dim_variable.alias("dv")
        dim_source_alias = dim_source.alias("ds")
        dim_periode_alias = dim_periode.alias("dt")

        fact = (
            df_alias
            .join(dim_pays_alias, F.col("f.pays") == F.col("dp.pays"), "left")
            .join(dim_variable_alias, F.col("f.variable") == F.col("dv.variable"), "left")
            .join(dim_source_alias, F.col("f.source") == F.col("ds.source"), "left")
            .join(dim_periode_alias, F.col("f.annee") == F.col("dt.annee"), "left")
            .select(
                F.col("dp.id_pays"),
                F.col("f.pays"),
                F.col("dv.id_variable"),
                F.col("f.variable"),
                F.col("ds.id_source"),
                F.col("f.source"),
                F.col("f.annee"),
                F.col("f.version"),
                F.col("f.valeur").cast("double").alias("valeur"),
                F.col("f.rang").cast("int").alias("rang"),
                F.col("f.base"),
                F.col("f.version_active")
            )
        )

        return fact
    # def build_fact(self, df, dim_pays, dim_variable, dim_source, dim_periode):

    #     fact = (
    #         df.join(dim_pays, "pays", "left")
    #         .join(dim_variable, "variable", "left")
    #         .join(dim_source, "source", "left")
    #         .join(dim_periode, "annee", "left")
    #         .select(
    #             "id_pays",
    #             "pays",                
    #             "id_variable",
    #             "variable",            
    #             "id_source",
    #             "source",              
    #             "annee",      
    #             "version",        
    #             F.col("valeur").cast("double").alias("valeur"),
    #             F.col("rang").cast("int").alias("rang"),
    #             "base",
    #             "version_active"
    #         )
    #     )

    #     return fact
 
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
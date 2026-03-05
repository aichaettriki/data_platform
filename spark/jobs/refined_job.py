import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

# ============================================================
# TARGET SCHEMA
# ============================================================

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

# ============================================================
# CONFIG
# ============================================================

class MinIOConfig:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

        self.bucket_transformed = os.getenv("BUCKET_TRANSFORMED", "02-transformed")
        self.bucket_refined = os.getenv("BUCKET_REFINED", "03-refined")

# ============================================================
# SPARK SESSION
# ============================================================

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

# ============================================================
# MAIN PROCESSOR
# ============================================================

class RefinedProcessor:

    def __init__(self):
        self.config = MinIOConfig()
        self.spark = create_spark()

        self.null_values = ["", "null", "NULL", "None", "none", "N/A", "n/a", "NA"]

    # --------------------------------------------------------
    # READ TRANSFORMED
    # --------------------------------------------------------

    def read_transformed(self):

        path = f"s3a://{self.config.bucket_transformed}/"

        df = (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .parquet(path)
        )

        # Cast schema
        for field in TARGET_SCHEMA.fields:
            if field.name in df.columns:
                df = df.withColumn(
                    field.name,
                    F.col(field.name).cast(field.dataType)
                )

        return df

    # --------------------------------------------------------
    # NULL NORMALIZATION
    # --------------------------------------------------------

    def normalize_column(self, df, column_name):

        return df.withColumn(
            column_name,
            F.when(
                F.col(column_name).isNull() |
                F.trim(F.col(column_name)).isin(self.null_values),
                F.lit("None")
            ).otherwise(F.col(column_name))
        )

    # --------------------------------------------------------
    # BUILD DIMENSION
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # BUILD FACT
    # --------------------------------------------------------

    def build_fact(self, df, dim_pays, dim_variable, dim_source, dim_periode, dim_lib_secteur):

        fact = (
            df
            .join(dim_pays, "pays", "left")
            .join(dim_variable, "variable", "left")
            .join(dim_source, "source", "left")
            .join(dim_periode, "annee", "left")
            .join(dim_lib_secteur, "lib_secteur", "left")   

            .select(
                F.coalesce(F.col("id_pays"), F.lit(-1)).alias("id_pays"),
                F.coalesce(F.col("id_variable"), F.lit(-1)).alias("id_variable"),
                F.coalesce(F.col("id_source"), F.lit(-1)).alias("id_source"),
                F.coalesce(F.col("id_periode"), F.lit(-1)).alias("id_periode"),
                F.coalesce(F.col("id_lib_secteur"), F.lit(-1)).alias("id_lib_secteur"),
                F.col("valeur").cast("double").alias("valeur"),
                F.col("rang").cast("int").alias("rang")
            )
        )

        return fact
    # --------------------------------------------------------
    # WRITE
    # --------------------------------------------------------

    def write_refined(self, df, folder_name):

        path = f"s3a://{self.config.bucket_refined}/{folder_name}/"

        df.coalesce(1).write.mode("overwrite").parquet(path)

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------

    def run(self):

        df = self.read_transformed()

        # ✅ Normaliser les colonnes métier AVANT tout
        df = self.normalize_column(df, "pays")
        df = self.normalize_column(df, "variable")
        df = self.normalize_column(df, "source")
        df = self.normalize_column(df, "annee")

        df = df.filter(F.col("version_active") == 1)

        dim_pays = self.build_dimension(df, "pays", "id_pays")
        dim_variable = self.build_dimension(df, "variable", "id_variable")
        dim_source = self.build_dimension(df, "source", "id_source")
        dim_periode = self.build_dimension(df, "annee", "id_periode")
        dim_lib_secteur = self.build_dimension(df, "lib_secteur", "id_lib_secteur")
        fact = self.build_fact(df, dim_pays, dim_variable, dim_source, dim_periode, dim_lib_secteur)

        self.write_refined(dim_pays, "dimensions/dim_pays")
        self.write_refined(dim_variable, "dimensions/dim_variable")
        self.write_refined(dim_source, "dimensions/dim_source")
        self.write_refined(dim_periode, "dimensions/dim_periode")
        self.write_refined(dim_lib_secteur, "dimensions/dim_lib_secteur")

        self.write_refined(fact, "fact_macroeco")

        self.spark.stop()


# ============================================================
# EXECUTION
# ============================================================

if __name__ == "__main__":
    RefinedProcessor().run()
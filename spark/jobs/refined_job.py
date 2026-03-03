import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DoubleType, TimestampType, IntegerType
)
 
# --------------------------------------------------
# SCHEMA DEFINITION
# --------------------------------------------------
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
        self.endpoint   = os.getenv("MINIO_ENDPOINT",      "http://minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER",     "minioadmin")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.bucket_transformed = "02-transformed"
        self.bucket_refined     = "03-refined"
 
# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
def create_spark():
    return (
        SparkSession.builder
        .appName("Refined Builder")
        .config("spark.hadoop.fs.s3a.access.key",             os.getenv("MINIO_ROOT_USER",     "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",             os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.endpoint",               os.getenv("MINIO_ENDPOINT",      "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.enableVectorizedReader",   "false")
        .getOrCreate()
    )
 
# --------------------------------------------------
# MAIN PROCESSOR
# --------------------------------------------------
class RefinedProcessor:
 
    def __init__(self):
        self.config    = MinIOConfig()
        self.spark     = create_spark()
        self.tech_cols = ["date_chargement", "version_active"]
       
        # ✅ Tech snapshot collected ONCE at init — not repeated per dimension
        self._tech_snapshot = None
 
    # --------------------------------------------------
    # COLLECT TECH SNAPSHOT ONCE
    # --------------------------------------------------
    def _get_tech_snapshot(self, df):
        """
        Collects the latest date_chargement and version_active ONCE.
        Cached in self._tech_snapshot to avoid multiple .collect() calls.
        """
        if self._tech_snapshot is None:
            row = (
                df.select(*self.tech_cols)
                  .orderBy(F.col("date_chargement").desc())
                  .limit(1)
                  .collect()
            )
            if not row:
                raise ValueError("Source dataframe is empty — cannot extract tech snapshot.")
            self._tech_snapshot = {col: row[0][col] for col in self.tech_cols}
            logging.info(f"[tech_snapshot] Cached: {self._tech_snapshot}")
        return self._tech_snapshot
 
    # --------------------------------------------------
    # ENFORCE TECH COLS (No collect — uses cached values)
    # --------------------------------------------------
    def _enforce_tech_cols(self, dim_df, source_df):
        """
        Injects missing tech cols using the cached snapshot (no extra Spark job).
        """
        missing = [c for c in self.tech_cols if c not in dim_df.columns]
        if not missing:
            return dim_df
 
        snapshot = self._get_tech_snapshot(source_df)
        for col in missing:
            dim_df = dim_df.withColumn(col, F.lit(snapshot[col]))
 
        return dim_df
 
    # --------------------------------------------------
    # RUN
    # --------------------------------------------------
    def run(self):
        df = self.read_transformed()
        df = df.filter(F.col("version_active") == 1)
        df = self.clean_nulls(df)
 
        # ✅ Cache df to avoid re-reading parquet for every dimension build
        df.cache()
 
        # ✅ Trigger tech snapshot collection ONCE here, before any dimension
        self._get_tech_snapshot(df)
 
        # ---------------- Dimensions ----------------
        dim_pays     = self.build_dimension_auto_id(df, "pays",    "id_pays")
        dim_source   = self.build_dimension_auto_id(df, "source",  "id_source")
        dim_version  = self.build_dimension_auto_id(df, "version", "id_version")
        dim_base     = self.build_dimension_auto_id(df, "base",    "id_base")
        dim_periode  = self.build_dimension_periode(df)
        dim_variable = self.build_dimension_variable(df)
        dim_secteur  = self.build_dimension_secteur(df)
 
        # ---------------- Fact ----------------
        fact = self.build_fact(
            df, dim_pays, dim_variable, dim_periode,
            dim_source, dim_base, dim_version, dim_secteur
        )
 
        # ---------------- Write ----------------
        self.write_refined(dim_pays,     "dimensions/dim_pays")
        self.write_refined(dim_source,   "dimensions/dim_source")
        self.write_refined(dim_version,  "dimensions/dim_version")
        self.write_refined(dim_base,     "dimensions/dim_base")
        self.write_refined(dim_periode,  "dimensions/dim_periode")
        self.write_refined(dim_variable, "dimensions/dim_variable")
        self.write_refined(dim_secteur,  "dimensions/dim_secteur")
        self.write_refined(fact,         "fact_macroeco")
 
        df.unpersist()
        self.spark.stop()
 
    # --------------------------------------------------
    # READ & CLEAN
    # --------------------------------------------------
    def read_transformed(self):
        path = f"s3a://{self.config.bucket_transformed}/"
        df   = self.spark.read.option("recursiveFileLookup", "true").parquet(path)
        for field in TARGET_SCHEMA.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        return df
 
    def clean_nulls(self, df):
        """
        Schema-driven null replacement:
          - StringType  → null or "" → "NA"
          - DoubleType  → null       → float("nan")
          - LongType    → keep NULL  (no NaN for integers in JVM)
        """
        for field in df.schema.fields:
            col_name = field.name
            dtype    = field.dataType
 
            if isinstance(dtype, StringType):
                df = df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
                        F.lit("NA")
                    ).otherwise(F.col(col_name))
                )
            elif isinstance(dtype, DoubleType):
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(),
                           F.lit(float("nan")).cast(DoubleType()))
                     .otherwise(F.col(col_name))
                )
        return df
 
    # --------------------------------------------------
    # GENERIC SIMPLE DIMENSIONS
    # --------------------------------------------------
    def build_dimension_auto_id(self, df, col_name, id_name):
        field_type = dict(df.dtypes).get(col_name, "string")
        is_missing = (
            F.isnan(F.col(col_name)) | F.col(col_name).isNull()
            if field_type == "double"
            else (F.col(col_name) == "NA") | F.col(col_name).isNull()
        )
 
        window_dedup = Window.partitionBy(col_name).orderBy(F.col("date_chargement").desc())
        window_id    = Window.orderBy(col_name)
 
        dim = (
            df.select(col_name, *self.tech_cols)
              .withColumn("rn", F.row_number().over(window_dedup))
              .filter(F.col("rn") == 1)
              .drop("rn")
              .withColumn(
                  id_name,
                  F.when(is_missing, F.lit(0))
                   .otherwise(F.row_number().over(window_id))
              )
              .select(id_name, col_name, *self.tech_cols)
        )
        # tech cols already present — _enforce_tech_cols is a no-op here
        return self._enforce_tech_cols(dim, df)
 
    # --------------------------------------------------
    # DIM PERIODE
    # --------------------------------------------------
    def build_dimension_periode(self, df):
        """
        Calendar spine 2010-2030.
        Tech cols injected from cached snapshot — NO extra collect().
        """
        window_id = Window.orderBy("full_date")
 
        date_df = self.spark.sql("""
            SELECT explode(
                sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)
            ) AS full_date
        """)
 
        dim = (
            date_df
            .withColumn("annee",           F.year("full_date"))
            .withColumn("mois",            F.month("full_date"))
            .withColumn("trimestre",       F.quarter("full_date"))
            .withColumn("annee_mois",      F.concat_ws("-", F.col("annee"),
                                               F.lpad(F.col("mois"), 2, "0")))
            .withColumn("annee_trimestre", F.concat_ws("-T", F.col("annee"),
                                               F.col("trimestre")))
            .withColumn("id_periode",      F.row_number().over(window_id))
            .drop("full_date")
        )
        # tech cols are missing from calendar → inject from cached snapshot
        return self._enforce_tech_cols(dim, df)
 
    # --------------------------------------------------
    # DIM VARIABLE
    # --------------------------------------------------
    def build_dimension_variable(self, df):
        is_missing   = (F.col("dim_key") == "NA") | F.col("dim_key").isNull()
        window_dedup = Window.partitionBy("dim_key").orderBy(F.col("date_chargement").desc())
        window_id    = Window.orderBy("dim_key")
 
        dim = (
            df.select("variable", "dim_id", "dim_key", *self.tech_cols)
              .withColumn("rn", F.row_number().over(window_dedup))
              .filter(F.col("rn") == 1)
              .drop("rn")
              .withColumn(
                  "id_variable",
                  F.when(is_missing, F.lit(0)).otherwise(F.row_number().over(window_id))
              )
              .select("id_variable", "variable", "dim_id", "dim_key", *self.tech_cols)
        )
        return self._enforce_tech_cols(dim, df)
 
    # --------------------------------------------------
    # DIM SECTEUR
    # --------------------------------------------------
    def build_dimension_secteur(self, df):
        is_missing   = (F.col("code_secteur") == "NA") | F.col("code_secteur").isNull()
        window_dedup = Window.partitionBy("code_secteur").orderBy(F.col("date_chargement").desc())
        window_id    = Window.orderBy("code_secteur")
 
        dim = (
            df.select("code_secteur", "lib_secteur", *self.tech_cols)
              .withColumn("rn", F.row_number().over(window_dedup))
              .filter(F.col("rn") == 1)
              .drop("rn")
              .withColumn(
                  "id_secteur",
                  F.when(is_missing, F.lit(0)).otherwise(F.row_number().over(window_id))
              )
              .select("id_secteur", "code_secteur", "lib_secteur", *self.tech_cols)
        )
        return self._enforce_tech_cols(dim, df)
 
    # --------------------------------------------------
    # FACT TABLE
    # --------------------------------------------------
    def build_fact(self, df, dim_pays, dim_variable, dim_periode,
                   dim_source, dim_base, dim_version, dim_secteur):
 
        dp   = dim_pays.select("pays",          "id_pays")
        dv   = dim_variable.select("variable",  "id_variable")
        dper = dim_periode.select("annee",       "id_periode")
        ds   = dim_source.select("source",       "id_source")
        db   = dim_base.select("base",           "id_base")
        dver = dim_version.select("version",     "id_version")
        dsec = dim_secteur.select("code_secteur","id_secteur")
 
        fact = (
            df.join(dp,   "pays",         "left")
              .join(dv,   "variable",     "left")
              .join(dper, df.annee == dper.annee, "left")
              .join(ds,   "source",       "left")
              .join(db,   "base",         "left")
              .join(dver, "version",      "left")
              .join(dsec, "code_secteur", "left")
              .select(
                  "id_pays", "id_variable", "id_periode",
                  "id_source", "id_base", "id_version", "id_secteur",
                  F.col("valeur").cast(DoubleType()).alias("valeur"),
                  *[F.col(c) for c in self.tech_cols]
              )
        )
        return self._enforce_tech_cols(fact, df)
 
    # --------------------------------------------------
    # WRITE (with pre-write guard)
    # --------------------------------------------------
    def write_refined(self, df, folder_name):
        missing = [c for c in self.tech_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"[write_refined] '{folder_name}' missing tech cols: {missing}. Aborting."
            )
        path = f"s3a://{self.config.bucket_refined}/{folder_name}/"
        df.coalesce(1).write.mode("overwrite").parquet(path)
        logging.info(f"[write_refined] ✓ {path} | columns: {df.columns}")
 
 
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    RefinedProcessor().run()
 
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
    StructField("rang", StringType(), True),
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
    StructField("country_iso3", StringType(), True),   # ✅ AJOUTER

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
        self._tech_snapshot = None

    # --------------------------------------------------
    # COLLECT TECH SNAPSHOT ONCE
    # --------------------------------------------------
    def _get_tech_snapshot(self, df):
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
    # RUN
    # --------------------------------------------------
    def run(self):
        df = self.read_transformed()
        df = df.filter(F.col("version_active") == 1)
        countries_df = self.read_countries()
        df.printSchema()
        df = self.clean_nulls(df)
        df.cache()

        self._get_tech_snapshot(df)

        # ---------------- Dimensions ----------------
        dim_pays     = self.build_dim_pays(countries_df)
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
        logging.info("-------------------------------- FACT -----------------------------")
        logging.info("Fact table preview:")
        fact.show(10, truncate=False)

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
        # 🔹 Nettoyage des valeurs "N/A"
        df = df.select([
            F.when(F.col(c) == "N/A", None).otherwise(F.col(c)).alias(c)
            for c in df.columns
        ])
        for field in TARGET_SCHEMA.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        return df

    def clean_nulls(self, df):
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
            # elif isinstance(dtype, StringType):
            #     df = df.withColumn(
            #         col_name,
            #         F.when(F.col(col_name).isNull(),
            #             F.lit("NA"))   # ← NULL au lieu de NaN
            #         .otherwise(F.col(col_name))
            #     )
        return df

    def read_countries(self):

        path = "s3a://02-transformed/WORLD_BANK/Countries/Axe_Referentiel/"

        df = self.spark.read.parquet(path)

        return df
    # --------------------------------------------------
    # GENERIC SIMPLE DIMENSIONS
    # ✅ Uses collect() on driver — dimensions are small,
    #    avoids global Window (no-partition) → no OOM
    # --------------------------------------------------
    def build_dimension_auto_id(self, df, col_name, id_name):
        snapshot = self._get_tech_snapshot(df)

        # Collect distinct values with latest tech cols — safe because dimensions are small
        rows = (
            df.select(col_name, *self.tech_cols)
              .groupBy(col_name)
              .agg(
                  F.max("date_chargement").alias("date_chargement"),
                  F.max("version_active").alias("version_active")
              )
              .orderBy(col_name)
              .collect()
        )

        data = []
        id_counter = 1
        for row in rows:
            val = row[col_name]
            is_missing = (val is None or val == "NA")
            assigned_id = 0 if is_missing else id_counter
            if not is_missing:
                id_counter += 1
            data.append((
                assigned_id,
                val,
                row["date_chargement"],
                row["version_active"]
            ))

        schema = StructType([
            StructField(id_name,           LongType(),      True),
            StructField(col_name,          StringType(),    True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active",  LongType(),      True),
        ])

        dim = self.spark.createDataFrame(data, schema)
        logging.info(f"[dim] {id_name} built with {len(data)} rows (driver-side, no Window)")
        return dim


    # --------------------------------------------------
    # DIM PAYS
    # --------------------------------------------------
    def build_dim_pays(self, df):

        rows = (
            df.select(
                F.col("id").alias("country_iso3"),
                "iso2_code",
                "pays",
                "region_id",
                "region_iso2",
                "region_name",
                "date_chargement"
            )
            .orderBy("country_iso3")
            .collect()
        )

        data = []
        id_counter = 1

        for row in rows:

            data.append((
                id_counter,
                row["country_iso3"],
                row["iso2_code"],
                row["pays"],
                row["region_id"],
                row["region_iso2"],
                row["region_name"],
                row["date_chargement"],
                1
            ))

            id_counter += 1

        schema = StructType([
            StructField("id_pays", LongType(), True),
            StructField("country_iso3", StringType(), True),
            StructField("iso2_code", StringType(), True),
            StructField("pays", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("region_iso2", StringType(), True),
            StructField("region_name", StringType(), True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active", LongType(), True),
        ])

        return self.spark.createDataFrame(data, schema)
    # --------------------------------------------------
    # DIM PERIODE
    # ✅ Calendar spine — no data join needed, no Window global
    # --------------------------------------------------
    def build_dimension_periode(self, df):
        snapshot = self._get_tech_snapshot(df)

        # Generate calendar 2010-2030 via SQL sequence
        date_df = self.spark.sql("""
            SELECT explode(
                sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)
            ) AS full_date
        """)

        # ✅ Use monotonically_increasing_id() — no global Window, distributed-safe
        dim = (
            date_df
            .withColumn("annee",           F.year("full_date"))
            .withColumn("mois",            F.month("full_date"))
            .withColumn("trimestre",       F.quarter("full_date"))
            .withColumn("annee_mois",      F.concat_ws("-", F.col("annee"),
                                               F.lpad(F.col("mois"), 2, "0")))
            .withColumn("annee_trimestre", F.concat_ws("-T", F.col("annee"),
                                               F.col("trimestre")))
            .withColumn("id_periode",      F.monotonically_increasing_id())
            .withColumn("date_chargement", F.lit(snapshot["date_chargement"]).cast(TimestampType()))
            .withColumn("version_active",  F.lit(snapshot["version_active"]).cast(LongType()))
            .drop("full_date")
        )
        return dim

    # --------------------------------------------------
    # DIM VARIABLE
    # ✅ collect() on driver — small dimension
    # --------------------------------------------------
    def build_dimension_variable(self, df):
        snapshot = self._get_tech_snapshot(df)

        rows = (
            df.select("variable", "dim_id", "dim_key", *self.tech_cols)
              .groupBy("variable", "dim_id", "dim_key")
              .agg(
                  F.max("date_chargement").alias("date_chargement"),
                  F.max("version_active").alias("version_active")
              )
              .orderBy("dim_key")
              .collect()
        )

        data = []
        id_counter = 1
        for row in rows:
            val = row["dim_key"]
            is_missing = (val is None or val == "NA")
            assigned_id = 0 if is_missing else id_counter
            if not is_missing:
                id_counter += 1
            data.append((
                assigned_id,
                row["variable"],
                row["dim_id"],
                row["dim_key"],
                row["date_chargement"],
                row["version_active"]
            ))

        schema = StructType([
            StructField("id_variable",     LongType(),      True),
            StructField("variable",        StringType(),    True),
            StructField("dim_id",          StringType(),    True),
            StructField("dim_key",         StringType(),    True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active",  LongType(),      True),
        ])

        dim = self.spark.createDataFrame(data, schema)
        logging.info(f"[dim] id_variable built with {len(data)} rows (driver-side, no Window)")
        return dim

    # --------------------------------------------------
    # DIM SECTEUR
    # ✅ collect() on driver — small dimension
    # --------------------------------------------------
    def build_dimension_secteur(self, df):
        snapshot = self._get_tech_snapshot(df)

        rows = (
            df.select("code_secteur", "lib_secteur", *self.tech_cols)
              .groupBy("code_secteur", "lib_secteur")
              .agg(
                  F.max("date_chargement").alias("date_chargement"),
                  F.max("version_active").alias("version_active")
              )
              .orderBy("code_secteur")
              .collect()
        )

        data = []
        id_counter = 1
        for row in rows:
            val = row["code_secteur"]
            is_missing = (val is None or val == "NA")
            assigned_id = 0 if is_missing else id_counter
            if not is_missing:
                id_counter += 1
            data.append((
                assigned_id,
                row["code_secteur"],
                row["lib_secteur"],
                row["date_chargement"],
                row["version_active"]
            ))

        schema = StructType([
            StructField("id_secteur",      LongType(),      True),
            StructField("code_secteur",    StringType(),    True),
            StructField("lib_secteur",     StringType(),    True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active",  LongType(),      True),
        ])

        dim = self.spark.createDataFrame(data, schema)
        logging.info(f"[dim] id_secteur built with {len(data)} rows (driver-side, no Window)")
        return dim

    # --------------------------------------------------
    # FACT TABLE
    # --------------------------------------------------
    def build_fact(self, df, dim_pays, dim_variable, dim_periode,
                   dim_source, dim_base, dim_version, dim_secteur):
 
        # dp   = dim_pays.select("pays",          "id_pays")
        dp = dim_pays.select(
            F.col("country_iso3").alias("pays_iso3"),
            "id_pays"
        )
        dv   = dim_variable.select("variable",  "id_variable")
        # dper = dim_periode.select("annee",       "id_periode")
        ds   = dim_source.select("source",       "id_source")
        db   = dim_base.select("base",           "id_base")
        dver = dim_version.select("version",     "id_version")
        dsec = dim_secteur.select("code_secteur","id_secteur")
        df.select("pays").distinct().show(20, False)
        dim_pays.select("country_iso3").distinct().show(20, False)
        fact = (
            # df.join(dp,   "pays",         "left")
            df.join(dp, df.pays == dp.pays_iso3, "left")
              .join(dv,   "variable",     "left")
            #   .join(dper, "annee",        "left")
              .join(ds,   "source",       "left")
              .join(db,   "base",         "left")
              .join(dver, "version",      "left")
              .join(dsec, "code_secteur", "left")
              .select(
                # Foreign Keys
                "id_pays",     F.col("pays"),
                "id_variable", F.col("variable"),
               
                "id_source",   F.col("source"),
                "id_base",     F.col("base"),
                "id_version",  F.col("version"),
                "id_secteur",  F.col("code_secteur"),
 
                # Measures
                F.col("valeur").cast(DoubleType()).alias("valeur"),
                F.when(
                    F.col("rang").isNull(),
 
                    F.lit("NA")
 
                ).otherwise(F.col("rang").cast(StringType())).alias("rang"),
               
                F.col("annee").alias("annee"),
                # Technical Cols
                *[F.col(c) for c in self.tech_cols]
            )
        )
        return fact

    # --------------------------------------------------
    # WRITE
    # --------------------------------------------------
    def write_refined(self, df, folder_name):
        missing = [c for c in self.tech_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"[write_refined] '{folder_name}' missing tech cols: {missing}. Aborting."
            )
        path = f"s3a://{self.config.bucket_refined}/{folder_name}/"
        # df.coalesce(1).write.mode("overwrite").csv(path, header=True)
        df.coalesce(1).write.mode("overwrite").parquet(path)
        logging.info(f"[write_refined] ✓ {path} | columns: {df.columns}")

        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    RefinedProcessor().run()
    
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DoubleType, TimestampType, IntegerType
)
from datetime import datetime

# --------------------------------------------------
# SCHEMA DEFINITION
# --------------------------------------------------
TARGET_SCHEMA = StructType([
    StructField("periode", StringType(), True),  # ← garder StringType
    StructField("variable", StringType(), True),
    StructField("version", StringType(), True),
    StructField("base", StringType(), True),
    StructField("valeur", DoubleType(), True),
    StructField("date_chargement", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("version_active", LongType(), True),
    StructField("pays", StringType(), True),
    StructField("pays_fr", StringType(), True),
    StructField("code_secteur/produit", StringType(), True),
    StructField("lib_secteur/produit", StringType(), True),
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
        .config("spark.sql.parquet.enableVectorizedReader",   "true")
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
        logging.info("============= INPUT DATA =============")
        logging.info(f"Total rows: {df.count()}")
        logging.info(f"Columns: {df.columns}")
        df.show(20, False)
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
        # 🟢 MODIFICATION ICI : Ajout de l'option "mergeSchema"
        df   = (self.spark.read
                .option("recursiveFileLookup", "true")
                .option("mergeSchema", "true") 
                .parquet(path))
        
        # 🔹 Nettoyage des valeurs "N/A"
        df = df.select([
            F.when(F.col(c) == "N/A", None).otherwise(F.col(c)).alias(c)
            for c in df.columns
        ])
        
        # ✅ renommer la colonne id en country_iso3
        if "id" in df.columns:
            df = df.withColumnRenamed("id", "country_iso3")

        for field in TARGET_SCHEMA.fields:
            if field.name in df.columns:
                if field.name != "valeur":
                    df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        
        # ✅ Cast explicite pour éviter Parquet Decimal issues
        if "valeur" in df.columns:
            df = df.withColumn("valeur", F.col("valeur").cast(DoubleType()))

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
        return df

    def read_countries(self):
        path = "s3a://02-transformed/WORLD_BANK/Countries/Axe_Referentiel/"
        df = self.spark.read.parquet(path)
        if "id" in df.columns:
            df = df.withColumnRenamed("id", "country_iso3")
        return df

    # --------------------------------------------------
    # GENERIC SIMPLE DIMENSIONS
    # --------------------------------------------------
    def build_dimension_auto_id(self, df, col_name, id_name):
        snapshot = self._get_tech_snapshot(df)
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
            data.append((assigned_id, val, row["date_chargement"], row["version_active"]))

        schema = StructType([
            StructField(id_name, LongType(), True),
            StructField(col_name, StringType(), True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active", LongType(), True),
        ])
        return self.spark.createDataFrame(data, schema)

    # --------------------------------------------------
    # DIM PAYS
    # --------------------------------------------------
    def build_dim_pays(self, df):
        rows = df.select("country_iso3","iso2_code","pays","pays_fr",
                         "region_id","region_iso2","region_name","date_chargement") \
                 .orderBy("country_iso3").collect()
        data = []
        # --------------------------------------------------
        # 1️⃣ Ajout de la ligne UNKNOWN obligatoire
        # --------------------------------------------------
        data.append((
            0,
            "NA",
            "NA",
            "Unknown",
            "Non disponible",
            "NA",
            "NA",
            "Unknown",
            datetime.now(),
            1
        ))
        id_counter = 1
        for row in rows:
            data.append((
                id_counter,
                row["country_iso3"],
                row["iso2_code"],
                row["pays"],
                row["pays_fr"], 
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
            StructField("pays_fr", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("region_iso2", StringType(), True),
            StructField("region_name", StringType(), True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active", LongType(), True),
        ])
        return self.spark.createDataFrame(data, schema)

    # --------------------------------------------------
    # DIM PERIODE
    # --------------------------------------------------
    def build_dimension_periode(self, df):
        snapshot = self._get_tech_snapshot(df)
 
        # Generate a monthly spine from 1960-01-01 to 2030-12-31
        # (one row per month — we'll explode into 3 rows each)
        month_df = self.spark.sql("""
            SELECT explode(
                sequence(to_date('1960-01-01'), to_date('2030-12-31'), interval 1 month)
            ) AS month_date
        """)
 
        # Derive calendar attributes
        month_df = (
            month_df
            .withColumn("annee",      F.year("month_date"))
            .withColumn("mois",       F.month("month_date"))
            .withColumn("trimestre",  F.quarter("month_date"))
            .withColumn("annee_mois", F.concat_ws("-", F.col("annee"),
                                        F.lpad(F.col("mois"), 2, "0")))
            .withColumn("annee_trimestre", F.concat_ws("-T", F.col("annee"),
                                            F.col("trimestre")))
        )
 
        # For each month, create 3 levels of periode:
        #   level 1 → "YYYY"
        #   level 2 → "YYYY" + zero-padded quarter (e.g. "196001")
        #   level 3 → level2 + zero-padded month   (e.g. "19600102")
 
        level1 = month_df.withColumn("niveau",   F.lit(1)) \
                        .withColumn("periode",  F.col("annee").cast(StringType()))
 
        level2 = month_df.withColumn("niveau",   F.lit(2)) \
                        .withColumn("periode",
                            F.concat(
                                F.col("annee").cast(StringType()),
                                F.lpad(F.col("trimestre").cast(StringType()), 2, "0")
                            ))
 
        level3 = month_df.withColumn("niveau",   F.lit(3)) \
                        .withColumn("periode",
                            F.concat(
                                F.col("annee").cast(StringType()),
                                F.lpad(F.col("trimestre").cast(StringType()), 2, "0"),
                                F.lpad(F.col("mois").cast(StringType()), 2, "0")
                            ))
 
        # Union all 3 levels, deduplicate (level1 & level2 repeat across months)
        all_levels = level1.union(level2).union(level3) \
                        .select("annee", "mois", "trimestre",
                                "annee_mois", "annee_trimestre",
                                 "periode") \
                        .dropDuplicates(["periode"])
 
        # Assign surrogate key — distributed-safe, no global Window
        dim = (
            all_levels
            .orderBy("periode")
            .withColumn("id_periode",      F.monotonically_increasing_id())
            .withColumn("date_chargement", F.lit(snapshot["date_chargement"]).cast(TimestampType()))
            .withColumn("version_active",  F.lit(snapshot["version_active"]).cast(LongType()))
        )
 
        return dim
       
 
    # --------------------------------------------------
    # DIM VARIABLE
    # --------------------------------------------------
    def build_dimension_variable(self, df):
        snapshot = self._get_tech_snapshot(df)
        rows = df.select("variable","dim_id","dim_key",*self.tech_cols)\
                 .groupBy("variable","dim_id","dim_key")\
                 .agg(F.max("date_chargement").alias("date_chargement"),
                      F.max("version_active").alias("version_active"))\
                 .orderBy("dim_key").collect()
        data = []
        id_counter = 1
        for row in rows:
            val = row["dim_key"]
            is_missing = (val is None or val == "NA")
            assigned_id = 0 if is_missing else id_counter
            if not is_missing:
                id_counter += 1
            data.append((assigned_id,row["variable"],row["dim_id"],row["dim_key"],
                         row["date_chargement"],row["version_active"]))
        schema = StructType([
            StructField("id_variable", LongType(), True),
            StructField("variable", StringType(), True),
            StructField("dim_id", StringType(), True),
            StructField("dim_key", StringType(), True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active", LongType(), True),
        ])
        return self.spark.createDataFrame(data, schema)

    # --------------------------------------------------
    # DIM SECTEUR
    # --------------------------------------------------
    def build_dimension_secteur(self, df):
        snapshot = self._get_tech_snapshot(df)
        rows = df.select("code_secteur/produit","lib_secteur/produit",*self.tech_cols)\
                 .groupBy("code_secteur/produit","lib_secteur/produit")\
                 .agg(F.max("date_chargement").alias("date_chargement"),
                      F.max("version_active").alias("version_active"))\
                 .orderBy("code_secteur/produit").collect()
        data = []
        id_counter = 1
        for row in rows:
            val = row["code_secteur/produit"]
            is_missing = (val is None or val == "NA")
            assigned_id = 0 if is_missing else id_counter
            if not is_missing:
                id_counter += 1
            data.append((assigned_id,row["code_secteur/produit"],row["lib_secteur/produit"],
                         row["date_chargement"],row["version_active"]))
        schema = StructType([
            StructField("id_secteur", LongType(), True),
            StructField("code_secteur/produit", StringType(), True),
            StructField("lib_secteur/produit", StringType(), True),
            StructField("date_chargement", TimestampType(), True),
            StructField("version_active", LongType(), True),
        ])
        return self.spark.createDataFrame(data, schema)

    # --------------------------------------------------
    # FACT TABLE
    # --------------------------------------------------
    def build_fact(self, df, dim_pays, dim_variable, dim_periode,
               dim_source, dim_base, dim_version, dim_secteur):

    # -------------------------------
    # Nettoyage des pays
    # -------------------------------
        df = df.withColumn(
            "pays_clean",
            F.upper(F.trim(F.col("pays")))
        ).alias("df")

        dp = (
            dim_pays
            .withColumn("pays_clean", F.upper(F.trim(F.col("pays"))))
            .withColumn("pays_fr_clean", F.upper(F.trim(F.col("pays_fr"))))
            .select(
                "id_pays",
                "country_iso3",
                F.col("pays").alias("nom_pays"),
                "pays_clean",
                "pays_fr_clean"
            )
            .alias("dp")
        )

        dv = dim_variable.select(
            "variable",
            "id_variable"
        ).alias("dv")

        ds = dim_source.select(
            "source",
            "id_source"
        ).alias("ds")

        db = dim_base.select(
            "base",
            "id_base"
        ).alias("db")

        dver = dim_version.select(
            "version",
            "id_version"
        ).alias("dver")

        # dimension secteur avec libellé
        dsec = dim_secteur.select(
            "code_secteur/produit",
            "id_secteur",
            "lib_secteur/produit"
        ).alias("dsec")

        # -------------------------------
        # Jointures
        # -------------------------------
        fact = (
            df.join(
                dp,
                (F.col("df.pays_clean") == F.col("dp.country_iso3")) |
                (F.col("df.pays_clean") == F.col("dp.pays_clean")) |
                (F.col("df.pays_clean") == F.col("dp.pays_fr_clean")),
                "left"
            )
            .join(dv, F.col("df.variable") == F.col("dv.variable"), "left")
            .join(ds, F.col("df.source") == F.col("ds.source"), "left")
            .join(db, F.col("df.base") == F.col("db.base"), "left")
            .join(dver, F.col("df.version") == F.col("dver.version"), "left")
            .join(dsec, F.col("df.code_secteur/produit") == F.col("dsec.code_secteur/produit"), "left")
            .select(

                # keys
                F.coalesce(F.col("dp.id_pays"), F.lit(0)).alias("id_pays"),
                F.coalesce(F.col("dv.id_variable"), F.lit(0)).alias("id_variable"),
                F.coalesce(F.col("ds.id_source"), F.lit(0)).alias("id_source"),
                F.coalesce(F.col("db.id_base"), F.lit(0)).alias("id_base"),
                F.coalesce(F.col("dver.id_version"), F.lit(0)).alias("id_version"),
                F.coalesce(F.col("dsec.id_secteur"), F.lit(0)).alias("id_secteur"),

                # colonnes métier (toujours NA si null)
                F.coalesce(F.col("dp.nom_pays"), F.lit("NA")).alias("pays"),
                F.coalesce(F.col("dp.country_iso3"), F.lit("NA")).alias("country_iso3"),
                F.coalesce(F.col("df.variable"), F.lit("NA")).alias("variable"),
                F.coalesce(F.col("df.source"), F.lit("NA")).alias("source"),
                F.coalesce(F.col("df.base"), F.lit("NA")).alias("base"),
                F.coalesce(F.col("df.version"), F.lit("NA")).alias("version"),
                F.coalesce(F.col("df.code_secteur/produit"), F.lit("NA")).alias("code_secteur/produit"),
                F.coalesce(F.col("dsec.lib_secteur/produit"), F.lit("NA")).alias("lib_secteur/produit"),

                # valeurs
                F.col("df.valeur").cast(DoubleType()).alias("valeur"),
                F.coalesce(F.col("df.periode"), F.lit("NA")).alias("periode"),

                *[F.col(f"df.{c}") for c in self.tech_cols]
            )
    )

        return fact
            # --------------------------------------------------
        # WRITE
        # --------------------------------------------------
    def write_refined(self, df, folder_name):
        missing = [c for c in self.tech_cols if c not in df.columns]
        if missing:
            raise ValueError(f"[write_refined] '{folder_name}' missing tech cols: {missing}. Aborting.")
        path = f"s3a://{self.config.bucket_refined}/{folder_name}/"
        df.coalesce(1).write.mode("overwrite").parquet(path)
        logging.info(f"[write_refined] ✓ {path} | columns: {df.columns}")
       
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    RefinedProcessor().run()


# import os
# import logging
# import uuid
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# from pyspark.sql.types import (
#     StructType, StructField, LongType, StringType,
#     DoubleType, TimestampType
# )
# from openlineage.client import OpenLineageClient
# from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

# # --------------------------------------------------
# # LOGGING
# # --------------------------------------------------
# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger("REFINED_BUILDER")

# # --------------------------------------------------
# # MARQUEZ CONFIG
# # --------------------------------------------------
# MARQUEZ_URL     = "http://marquez:5000"
# PIPELINE_RUN_ID = str(uuid.uuid4())

# # Namespace routing rules:
# #   s3a://03-refined/... → data_warehouse   ← outputs of this job
# #   everything else      → data_lake        ← inputs (transformed) + memory intermediates
# NS_DATA_LAKE      = "data_lake"
# NS_DATA_WAREHOUSE = "data_warehouse"

# # --------------------------------------------------
# # MARQUEZ HELPERS
# # --------------------------------------------------
# def resolve_namespace(path: str) -> str:
#     """
#     Resolves the OpenLineage namespace for a given dataset path.

#     Rules:
#         s3a://03-refined/...  → data_warehouse
#         everything else       → data_lake
#             (covers: s3a://02-transformed/, memory://spark_df/...)
#     """
#     if path.startswith("s3a://03-refined") or path.startswith("s3://03-refined"):
#         return NS_DATA_WAREHOUSE
#     return NS_DATA_LAKE


# def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs,
#                       input_schema_fields=None, column_lineage=None):
#     """
#     Sends rich lineage metadata to Marquez for a given pipeline step.

#     Namespace is resolved automatically per dataset path:
#         - s3a://03-refined/... → data_warehouse
#         - all others           → data_lake

#     Parameters:
#         spark_df            : Spark DataFrame — used to extract output schema
#         step_name           : Logical name of the step (e.g. '01_Read_Transformed')
#         description         : Human-readable description of what this step does
#         trans_type          : Transformation type label (e.g. 'EXTRACT', 'TRANSFORMATION', 'LOAD')
#         inputs              : List of input dataset paths/names (source)
#         outputs             : List of output dataset paths/names (destination)
#         input_schema_fields : Optional list of dicts describing the input schema
#         column_lineage      : Optional dict describing per-column lineage
#     """
#     try:
#         client = OpenLineageClient(url=MARQUEZ_URL)

#         # ── Output schema facet ───────────────────────────────────────────────
#         output_fields = [
#             {
#                 "name": field.name,
#                 "type": field.dataType.simpleString(),
#                 "description": f"nullable={field.nullable}",
#             }
#             for field in spark_df.schema.fields
#         ]

#         output_dataset_facets = {
#             "schema": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
#                 "fields": output_fields,
#             },
#             "documentation": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json#",
#                 "description": f"[{trans_type}] {description}",
#             },
#         }

#         if column_lineage:
#             output_dataset_facets["columnLineage"] = {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json#",
#                 "fields": column_lineage,
#             }

#         input_dataset_facets = {}
#         if input_schema_fields:
#             input_dataset_facets["schema"] = {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
#                 "fields": input_schema_fields,
#             }

#         run_facets = {
#             "processing_engine": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ProcessingEngineRunFacet.json#",
#                 "version": "3.5.1",
#                 "name": "Apache Spark",
#                 "openlineageAdapterVersion": "itceq-1.0",
#             }
#         }

#         job_facets = {
#             "sql": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json#",
#                 "query": description,
#             }
#         }

#         input_datasets = [
#             Dataset(namespace=resolve_namespace(i), name=i, facets=input_dataset_facets)
#             for i in inputs
#         ]
#         output_datasets = [
#             Dataset(namespace=resolve_namespace(o), name=o, facets=output_dataset_facets)
#             for o in outputs
#         ]

#         for i in inputs:
#             print(f"     ↳ INPUT  [{resolve_namespace(i)}] {i}")
#         for o in outputs:
#             print(f"     ↳ OUTPUT [{resolve_namespace(o)}] {o}")

#         event = RunEvent(
#             eventType=RunState.COMPLETE,
#             eventTime=datetime.now().isoformat() + "Z",
#             run=Run(runId=PIPELINE_RUN_ID, facets=run_facets),
#             job=Job(namespace=NS_DATA_LAKE, name=step_name, facets=job_facets),
#             inputs=input_datasets,
#             outputs=output_datasets,
#             producer="itceq-spark-producer",
#         )

#         client.emit(event)
#         print(f"📡 Marquez Updated: [{trans_type}] {step_name} | {len(output_fields)} output columns")

#     except Exception as e:
#         print(f"⚠️  Marquez metadata error at step '{step_name}': {e}")


# # --------------------------------------------------
# # SCHEMA DEFINITION  — rang supprimé
# # --------------------------------------------------
# TARGET_SCHEMA = StructType([
#     StructField("periode",           LongType(),      True),
#     StructField("variable",        StringType(),    True),
#     StructField("version",         StringType(),    True),
#     StructField("base",            StringType(),    True),
#     StructField("valeur",          DoubleType(),    True),
#     StructField("date_chargement", TimestampType(), True),
#     StructField("source",          StringType(),    True),
#     StructField("version_active",  LongType(),      True),
#     StructField("pays",            StringType(),    True),
#     StructField("code_secteur/produit",    StringType(),    True),
#     StructField("lib_secteur/produit",     StringType(),    True),
#     StructField("dim_id",          StringType(),    True),
#     StructField("dim_key",         StringType(),    True),
# ])

# # --------------------------------------------------
# # CONFIG
# # --------------------------------------------------
# class MinIOConfig:
#     def __init__(self):
#         self.endpoint           = os.getenv("MINIO_ENDPOINT",      "http://minio:9000")
#         self.access_key         = os.getenv("MINIO_ROOT_USER",     "minioadmin")
#         self.secret_key         = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
#         self.bucket_transformed = "02-transformed"
#         self.bucket_refined     = "03-refined"


# # --------------------------------------------------
# # SPARK SESSION
# # --------------------------------------------------
# def create_spark():
#     return (
#         SparkSession.builder
#         .appName("Refined Builder")
#         .config("spark.hadoop.fs.s3a.access.key",             os.getenv("MINIO_ROOT_USER",     "minioadmin"))
#         .config("spark.hadoop.fs.s3a.secret.key",             os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
#         .config("spark.hadoop.fs.s3a.endpoint",               os.getenv("MINIO_ENDPOINT",      "http://minio:9000"))
#         .config("spark.hadoop.fs.s3a.path.style.access",      "true")
#         .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#         .config("spark.sql.parquet.enableVectorizedReader",   "false")
#         .getOrCreate()
#     )


# # --------------------------------------------------
# # MAIN PROCESSOR
# # --------------------------------------------------
# class RefinedProcessor:

#     # Paths constants
#     _SOURCE_PATH        = "memory://spark_df/refined/source_clean"
#     _DIM_PAYS_PATH      = "memory://spark_df/refined/dim_pays"
#     _DIM_SOURCE_PATH    = "memory://spark_df/refined/dim_source"
#     _DIM_VERSION_PATH   = "memory://spark_df/refined/dim_version"
#     _DIM_BASE_PATH      = "memory://spark_df/refined/dim_base"
#     _DIM_PERIODE_PATH   = "memory://spark_df/refined/dim_periode"
#     _DIM_VARIABLE_PATH  = "memory://spark_df/refined/dim_variable"
#     _DIM_SECTEUR_PATH   = "memory://spark_df/refined/dim_secteur"
#     _FACT_PATH          = "memory://spark_df/refined/fact_macroeco"

#     def __init__(self):
#         self.config         = MinIOConfig()
#         self.spark          = create_spark()
#         self.tech_cols      = ["date_chargement", "version_active"]
#         self._tech_snapshot = None

#     # --------------------------------------------------
#     # COLLECT TECH SNAPSHOT ONCE
#     # --------------------------------------------------
#     def _get_tech_snapshot(self, df):
#         """
#         Collects the latest date_chargement and version_active ONCE.
#         Cached in self._tech_snapshot to avoid multiple .collect() calls.
#         """
#         if self._tech_snapshot is None:
#             row = (
#                 df.select(*self.tech_cols)
#                   .orderBy(F.col("date_chargement").desc())
#                   .limit(1)
#                   .collect()
#             )
#             if not row:
#                 raise ValueError("Source dataframe is empty — cannot extract tech snapshot.")
#             self._tech_snapshot = {col: row[0][col] for col in self.tech_cols}
#             log.info(f"[tech_snapshot] Cached: {self._tech_snapshot}")
#         return self._tech_snapshot

#     # --------------------------------------------------
#     # ENFORCE TECH COLS (uses cached snapshot — no extra Spark job)
#     # --------------------------------------------------
#     def _enforce_tech_cols(self, dim_df, source_df):
#         missing = [c for c in self.tech_cols if c not in dim_df.columns]
#         if not missing:
#             return dim_df
#         snapshot = self._get_tech_snapshot(source_df)
#         for col in missing:
#             dim_df = dim_df.withColumn(col, F.lit(snapshot[col]))
#         return dim_df

#     # --------------------------------------------------
#     # RUN
#     # --------------------------------------------------
#     def run(self):
#         print(f"\n{'='*80}")
#         print(f"🚀 REFINED BUILDER — Starting pipeline run {PIPELINE_RUN_ID}")
#         print(f"{'='*80}")

#         df = self.read_transformed()
#         df = df.filter(F.col("version_active") == 1)
#         df = self.clean_nulls(df)

#         # Cache df to avoid re-reading parquet for every dimension build
#         df.cache()
#         # Trigger tech snapshot collection ONCE before any dimension
#         self._get_tech_snapshot(df)

#         # ── STEP 1 emit — source_clean ────────────────────────────────
#         self._emit_source_clean(df)

#         # ---------------- Dimensions ----------------
#         dim_pays     = self.build_dimension_auto_id(df, "pays",    "id_pays",    self._DIM_PAYS_PATH)
#         dim_source   = self.build_dimension_auto_id(df, "source",  "id_source",  self._DIM_SOURCE_PATH)
#         dim_version  = self.build_dimension_auto_id(df, "version", "id_version", self._DIM_VERSION_PATH)
#         dim_base     = self.build_dimension_auto_id(df, "base",    "id_base",    self._DIM_BASE_PATH)
#         dim_periode  = self.build_dimension_periode(df)
#         dim_variable = self.build_dimension_variable(df)
#         dim_secteur  = self.build_dimension_secteur(df)

#         # ---------------- Fact ----------------
#         fact = self.build_fact(
#             df, dim_pays, dim_variable, dim_periode,
#             dim_source, dim_base, dim_version, dim_secteur
#         )

#         # ---------------- Write ----------------
#         self.write_refined(dim_pays,     "dimensions/dim_pays",    self._DIM_PAYS_PATH)
#         self.write_refined(dim_source,   "dimensions/dim_source",  self._DIM_SOURCE_PATH)
#         self.write_refined(dim_version,  "dimensions/dim_version", self._DIM_VERSION_PATH)
#         self.write_refined(dim_base,     "dimensions/dim_base",    self._DIM_BASE_PATH)
#         self.write_refined(dim_periode,  "dimensions/dim_periode", self._DIM_PERIODE_PATH)
#         self.write_refined(dim_variable, "dimensions/dim_variable",self._DIM_VARIABLE_PATH)
#         self.write_refined(dim_secteur,  "dimensions/dim_secteur", self._DIM_SECTEUR_PATH)
#         self.write_refined(fact,         "fact_macroeco",          self._FACT_PATH)

#         df.unpersist()
#         self.spark.stop()

#     # --------------------------------------------------
#     # STEP 1 — READ TRANSFORMED + CLEAN + FILTER
#     # --------------------------------------------------
#     def read_transformed(self):
#         path = f"s3a://{self.config.bucket_transformed}/"
#         df   = self.spark.read.option("recursiveFileLookup", "true").parquet(path)
#         for field in TARGET_SCHEMA.fields:
#             if field.name in df.columns:
#                 df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
#         return df

#     def clean_nulls(self, df):
#         """
#         Schema-driven null replacement:
#           - StringType  → null or "" → "NA"
#           - DoubleType  → null       → float("nan")
#           - LongType    → keep NULL  (no NaN for integers in JVM)
#         """
#         for field in df.schema.fields:
#             col_name = field.name
#             dtype    = field.dataType
#             if isinstance(dtype, StringType):
#                 df = df.withColumn(
#                     col_name,
#                     F.when(
#                         F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
#                         F.lit("NA")
#                     ).otherwise(F.col(col_name))
#                 )
#             elif isinstance(dtype, DoubleType):
#                 df = df.withColumn(
#                     col_name,
#                     F.when(F.col(col_name).isNull(),
#                            F.lit(float("nan")).cast(DoubleType()))
#                      .otherwise(F.col(col_name))
#                 )
#         return df

#     def _emit_source_clean(self, df):
#         """Emits STEP 1 Marquez event after read + clean + filter."""
#         print(f"\n[STEP 1] Read Transformed — s3a://02-transformed/ → source_clean")

#         _src = f"s3a://{self.config.bucket_transformed}/"
#         _ns  = resolve_namespace(_src)

#         column_lineage = {}
#         for field in df.schema.fields:
#             col_name = field.name
#             # version_active is used as filter criterion
#             if col_name == "version_active":
#                 column_lineage[col_name] = {
#                     "inputFields": [{"namespace": _ns, "name": _src, "field": col_name}],
#                     "transformationDescription": (
#                         "Filtered: only rows where version_active=1 are kept. "
#                         f"Cast to {field.dataType.simpleString()} per TARGET_SCHEMA."
#                     ),
#                     "transformationType": "DIRECT",
#                 }
#             else:
#                 dtype_desc = field.dataType.simpleString()
#                 null_desc  = (
#                     f"Null/empty → 'NA' (StringType clean_nulls)"  if isinstance(field.dataType, StringType) else
#                     f"Null → NaN (DoubleType clean_nulls)"          if isinstance(field.dataType, DoubleType) else
#                     "NULL preserved (LongType — no NaN in JVM)"
#                 )
#                 column_lineage[col_name] = {
#                     "inputFields": [{"namespace": _ns, "name": _src, "field": col_name}],
#                     "transformationDescription": (
#                         f"Cast to {dtype_desc} per TARGET_SCHEMA. {null_desc}. "
#                         f"Read via recursiveFileLookup=true across all sub-folders of 02-transformed."
#                     ),
#                     "transformationType": "DIRECT",
#                 }

#         emit_marquez_step(
#             spark_df=df,
#             step_name="01_Read_Transformed",
#             description=(
#                 f"Read all Parquet files from s3a://02-transformed/ (recursiveFileLookup=true). "
#                 f"Cast columns to TARGET_SCHEMA. "
#                 f"clean_nulls: StringType→'NA', DoubleType→NaN. "
#                 f"Filtered: version_active=1 only. "
#                 f"Dropped: rang (removed from TARGET_SCHEMA). "
#                 f"Result: {len(df.columns)} columns."
#             ),
#             trans_type="EXTRACT",
#             inputs=[_src],
#             outputs=[self._SOURCE_PATH],
#             column_lineage=column_lineage,
#         )
#         print(f"  ✅ STEP 1 complete — {len(df.columns)} columns")

#     # --------------------------------------------------
#     # STEP 2 — GENERIC SIMPLE DIMENSIONS
#     # --------------------------------------------------
#     def build_dimension_auto_id(self, df, col_name, id_name, output_mem_path):
#         print(f"\n[STEP 2] Build Dimension — {col_name} → {id_name}")

#         field_type = dict(df.dtypes).get(col_name, "string")
#         is_missing = (
#             F.isnan(F.col(col_name)) | F.col(col_name).isNull()
#             if field_type == "double"
#             else (F.col(col_name) == "NA") | F.col(col_name).isNull()
#         )

#         window_dedup = Window.partitionBy(col_name).orderBy(F.col("date_chargement").desc())
#         window_id    = Window.orderBy(col_name)

#         dim = (
#             df.select(col_name, *self.tech_cols)
#               .withColumn("rn", F.row_number().over(window_dedup))
#               .filter(F.col("rn") == 1)
#               .drop("rn")
#               .withColumn(
#                   id_name,
#                   F.when(is_missing, F.lit(0))
#                    .otherwise(F.row_number().over(window_id))
#               )
#               .select(id_name, col_name, *self.tech_cols)
#         )
#         dim = self._enforce_tech_cols(dim, df)

#         _ns = resolve_namespace(self._SOURCE_PATH)
#         column_lineage = {
#             id_name: {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": col_name}],
#                 "transformationDescription": (
#                     f"Surrogate key generated by ROW_NUMBER() OVER (ORDER BY {col_name}). "
#                     f"Missing/NA values → id=0."
#                 ),
#                 "transformationType": "AGGREGATE",
#             },
#             col_name: {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": col_name}],
#                 "transformationDescription": (
#                     f"Deduplicated by ROW_NUMBER() OVER (PARTITION BY {col_name} "
#                     f"ORDER BY date_chargement DESC) — keep latest row per distinct value."
#                 ),
#                 "transformationType": "DIRECT",
#             },
#         }
#         for tc in self.tech_cols:
#             column_lineage[tc] = {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": tc}],
#                 "transformationDescription": "Tech column passed through from source_clean (latest row per value).",
#                 "transformationType": "DIRECT",
#             }

#         emit_marquez_step(
#             spark_df=dim,
#             step_name=f"02_Build_Dim_{col_name}",
#             description=(
#                 f"Built dimension '{col_name}' from source_clean. "
#                 f"Dedup: ROW_NUMBER() OVER (PARTITION BY {col_name} ORDER BY date_chargement DESC). "
#                 f"Surrogate key '{id_name}': ROW_NUMBER() OVER (ORDER BY {col_name}), missing→0. "
#                 f"Result: {dim.count()} distinct values."
#             ),
#             trans_type="TRANSFORMATION",
#             inputs=[self._SOURCE_PATH],
#             outputs=[output_mem_path],
#             column_lineage=column_lineage,
#         )
#         print(f"  ✅ dim_{col_name} built")
#         return dim

#     # --------------------------------------------------
#     # STEP 3 — DIM PERIODE
#     # --------------------------------------------------
#     def build_dimension_periode(self, df):
#         print(f"\n[STEP 3] Build Dimension — periode (calendar spine 2010–2030)")

#         window_id = Window.orderBy("full_date")

#         date_df = self.spark.sql("""
#             SELECT explode(
#                 sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)
#             ) AS full_date
#         """)

#         dim = (
#             date_df
#             .withColumn("periode",           F.year("full_date"))
#             .withColumn("mois",            F.month("full_date"))
#             .withColumn("trimestre",       F.quarter("full_date"))
#             .withColumn("periode_mois",      F.concat_ws("-", F.col("periode"),
#                                                F.lpad(F.col("mois"), 2, "0")))
#             .withColumn("periode_trimestre", F.concat_ws("-T", F.col("periode"),
#                                                F.col("trimestre")))
#             .withColumn("id_periode",      F.row_number().over(window_id))
#             .drop("full_date")
#         )
#         dim = self._enforce_tech_cols(dim, df)

#         _ns = resolve_namespace(self._SOURCE_PATH)
#         column_lineage = {
#             "id_periode": {
#                 "inputFields": [],
#                 "transformationDescription": "Surrogate key: ROW_NUMBER() OVER (ORDER BY full_date) on calendar spine.",
#                 "transformationType": "IDENTITY",
#             },
#             "periode": {
#                 "inputFields": [],
#                 "transformationDescription": "YEAR(full_date) from calendar spine sequence(2010-01-01, 2030-12-31, 1 day).",
#                 "transformationType": "IDENTITY",
#             },
#             "mois": {
#                 "inputFields": [],
#                 "transformationDescription": "MONTH(full_date) from calendar spine.",
#                 "transformationType": "IDENTITY",
#             },
#             "trimestre": {
#                 "inputFields": [],
#                 "transformationDescription": "QUARTER(full_date) from calendar spine.",
#                 "transformationType": "IDENTITY",
#             },
#             "periode_mois": {
#                 "inputFields": [],
#                 "transformationDescription": "CONCAT_WS('-', periode, LPAD(mois, 2, '0')) from calendar spine.",
#                 "transformationType": "IDENTITY",
#             },
#             "periode_trimestre": {
#                 "inputFields": [],
#                 "transformationDescription": "CONCAT_WS('-T', periode, trimestre) from calendar spine.",
#                 "transformationType": "IDENTITY",
#             },
#         }
#         for tc in self.tech_cols:
#             column_lineage[tc] = {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": tc}],
#                 "transformationDescription": (
#                     "Tech column injected from cached tech_snapshot "
#                     "(latest date_chargement/version_active from source_clean). "
#                     "Not present in calendar spine — added via _enforce_tech_cols()."
#                 ),
#                 "transformationType": "IDENTITY",
#             }

#         emit_marquez_step(
#             spark_df=dim,
#             step_name="03_Build_Dim_periode",
#             description=(
#                 "Built dimension 'periode' as calendar spine 2010-01-01 → 2030-12-31 "
#                 "(1 row per day via SQL sequence + explode). "
#                 "Columns: id_periode, periode, mois, trimestre, periode_mois, periode_trimestre. "
#                 "Tech cols (date_chargement, version_active) injected from cached tech_snapshot. "
#                 f"Result: {dim.count()} rows."
#             ),
#             trans_type="TRANSFORMATION",
#             inputs=[self._SOURCE_PATH],
#             outputs=[self._DIM_PERIODE_PATH],
#             column_lineage=column_lineage,
#         )
#         print(f"  ✅ dim_periode built")
#         return dim

#     # --------------------------------------------------
#     # STEP 4 — DIM VARIABLE
#     # --------------------------------------------------
#     def build_dimension_variable(self, df):
#         print(f"\n[STEP 4] Build Dimension — variable")

#         is_missing   = (F.col("dim_key") == "NA") | F.col("dim_key").isNull()
#         window_dedup = Window.partitionBy("dim_key").orderBy(F.col("date_chargement").desc())
#         window_id    = Window.orderBy("dim_key")

#         dim = (
#             df.select("variable", "dim_id", "dim_key", *self.tech_cols)
#               .withColumn("rn", F.row_number().over(window_dedup))
#               .filter(F.col("rn") == 1)
#               .drop("rn")
#               .withColumn(
#                   "id_variable",
#                   F.when(is_missing, F.lit(0)).otherwise(F.row_number().over(window_id))
#               )
#               .select("id_variable", "variable", "dim_id", "dim_key", *self.tech_cols)
#         )
#         dim = self._enforce_tech_cols(dim, df)

#         _ns = resolve_namespace(self._SOURCE_PATH)
#         column_lineage = {
#             "id_variable": {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "dim_key"}],
#                 "transformationDescription": (
#                     "Surrogate key: ROW_NUMBER() OVER (ORDER BY dim_key). "
#                     "dim_key='NA' or NULL → id_variable=0."
#                 ),
#                 "transformationType": "AGGREGATE",
#             },
#             "variable": {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "variable"}],
#                 "transformationDescription": "Variable name. Dedup by ROW_NUMBER() OVER (PARTITION BY dim_key ORDER BY date_chargement DESC).",
#                 "transformationType": "DIRECT",
#             },
#             "dim_id": {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "dim_id"}],
#                 "transformationDescription": "Dimension ID from source. Deduplicated with variable.",
#                 "transformationType": "DIRECT",
#             },
#             "dim_key": {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "dim_key"}],
#                 "transformationDescription": "Dimension key — dedup partition key.",
#                 "transformationType": "DIRECT",
#             },
#         }
#         for tc in self.tech_cols:
#             column_lineage[tc] = {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": tc}],
#                 "transformationDescription": "Tech column passed through (latest row per dim_key).",
#                 "transformationType": "DIRECT",
#             }

#         emit_marquez_step(
#             spark_df=dim,
#             step_name="04_Build_Dim_variable",
#             description=(
#                 "Built dimension 'variable' from source_clean. "
#                 "Dedup: ROW_NUMBER() OVER (PARTITION BY dim_key ORDER BY date_chargement DESC). "
#                 "Surrogate key 'id_variable': ROW_NUMBER() OVER (ORDER BY dim_key), NA→0. "
#                 f"Result: {dim.count()} distinct variables."
#             ),
#             trans_type="TRANSFORMATION",
#             inputs=[self._SOURCE_PATH],
#             outputs=[self._DIM_VARIABLE_PATH],
#             column_lineage=column_lineage,
#         )
#         print(f"  ✅ dim_variable built")
#         return dim

#     # --------------------------------------------------
#     # STEP 5 — DIM SECTEUR
#     # --------------------------------------------------
#     def build_dimension_secteur(self, df):
#         print(f"\n[STEP 5] Build Dimension — secteur")

#         is_missing   = (F.col("code_secteur/produit") == "NA") | F.col("code_secteur/produit").isNull()
#         window_dedup = Window.partitionBy("code_secteur/produit").orderBy(F.col("date_chargement").desc())
#         window_id    = Window.orderBy("code_secteur/produit")

#         dim = (
#             df.select("code_secteur/produit", "lib_secteur/produit", *self.tech_cols)
#               .withColumn("rn", F.row_number().over(window_dedup))
#               .filter(F.col("rn") == 1)
#               .drop("rn")
#               .withColumn(
#                   "id_secteur",
#                   F.when(is_missing, F.lit(0)).otherwise(F.row_number().over(window_id))
#               )
#               .select("id_secteur", "code_secteur/produit", "lib_secteur/produit", *self.tech_cols)
#         )
#         dim = self._enforce_tech_cols(dim, df)

#         _ns = resolve_namespace(self._SOURCE_PATH)
#         column_lineage = {
#             "id_secteur": {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "code_secteur/produit"}],
#                 "transformationDescription": (
#                     "Surrogate key: ROW_NUMBER() OVER (ORDER BY code_secteur/produit). "
#                     "code_secteur/produit='NA' or NULL → id_secteur=0."
#                 ),
#                 "transformationType": "AGGREGATE",
#             },
#             "code_secteur/produit": {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "code_secteur/produit"}],
#                 "transformationDescription": "Sector code. Dedup by ROW_NUMBER() OVER (PARTITION BY code_secteur/produit ORDER BY date_chargement DESC).",
#                 "transformationType": "DIRECT",
#             },
#             "lib_secteur/produit": {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "lib_secteur/produit"}],
#                 "transformationDescription": "Sector label. Deduplicated with code_secteur/produit.",
#                 "transformationType": "DIRECT",
#             },
#         }
#         for tc in self.tech_cols:
#             column_lineage[tc] = {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": tc}],
#                 "transformationDescription": "Tech column passed through (latest row per code_secteur/produit).",
#                 "transformationType": "DIRECT",
#             }

#         emit_marquez_step(
#             spark_df=dim,
#             step_name="05_Build_Dim_secteur",
#             description=(
#                 "Built dimension 'secteur' from source_clean. "
#                 "Dedup: ROW_NUMBER() OVER (PARTITION BY code_secteur/produit ORDER BY date_chargement DESC). "
#                 "Surrogate key 'id_secteur': ROW_NUMBER() OVER (ORDER BY code_secteur/produit), NA→0. "
#                 f"Result: {dim.count()} distinct sectors."
#             ),
#             trans_type="TRANSFORMATION",
#             inputs=[self._SOURCE_PATH],
#             outputs=[self._DIM_SECTEUR_PATH],
#             column_lineage=column_lineage,
#         )
#         print(f"  ✅ dim_secteur built")
#         return dim

#     # --------------------------------------------------
#     # STEP 6 — FACT TABLE
#     # --------------------------------------------------
#     def build_fact(self, df, dim_pays, dim_variable, dim_periode,
#                    dim_source, dim_base, dim_version, dim_secteur):
#         print(f"\n[STEP 6] Build Fact Table — 7 LEFT JOINs → fact_macroeco")

#         dp   = dim_pays.select("pays",          "id_pays")
#         dv   = dim_variable.select("variable",  "id_variable")
#         dper = dim_periode.select("periode",       "id_periode")
#         ds   = dim_source.select("source",       "id_source")
#         db   = dim_base.select("base",           "id_base")
#         dver = dim_version.select("version",     "id_version")
#         dsec = dim_secteur.select("code_secteur/produit","id_secteur")

#         fact = (
#             df.join(dp,   "pays",         "left")
#               .join(dv,   "variable",     "left")
#               .join(dper, df.periode == dper.periode, "left")
#               .join(ds,   "source",       "left")
#               .join(db,   "base",         "left")
#               .join(dver, "version",      "left")
#               .join(dsec, "code_secteur/produit", "left")
#               .select(
#                   "id_pays", "id_variable", "id_periode",
#                   "id_source", "id_base", "id_version", "id_secteur",
#                   F.col("valeur").cast(DoubleType()).alias("valeur"),
#                   *[F.col(c) for c in self.tech_cols]
#               )
#         )
#         fact = self._enforce_tech_cols(fact, df)

#         _ns = resolve_namespace(self._SOURCE_PATH)

#         # Dimension surrogate key sources
#         dim_join_lineage = {
#             "id_pays":     (self._DIM_PAYS_PATH,     "pays",         "LEFT JOIN source_clean ON pays → id_pays from dim_pays"),
#             "id_variable": (self._DIM_VARIABLE_PATH, "variable",     "LEFT JOIN source_clean ON variable → id_variable from dim_variable"),
#             "id_periode":  (self._DIM_PERIODE_PATH,  "periode",        "LEFT JOIN source_clean ON periode → id_periode from dim_periode"),
#             "id_source":   (self._DIM_SOURCE_PATH,   "source",       "LEFT JOIN source_clean ON source → id_source from dim_source"),
#             "id_base":     (self._DIM_BASE_PATH,     "base",         "LEFT JOIN source_clean ON base → id_base from dim_base"),
#             "id_version":  (self._DIM_VERSION_PATH,  "version",      "LEFT JOIN source_clean ON version → id_version from dim_version"),
#             "id_secteur":  (self._DIM_SECTEUR_PATH,  "code_secteur/produit", "LEFT JOIN source_clean ON code_secteur/produit → id_secteur from dim_secteur"),
#         }

#         column_lineage_fact = {}
#         for fk, (dim_path, join_col, desc) in dim_join_lineage.items():
#             column_lineage_fact[fk] = {
#                 "inputFields": [
#                     {"namespace": resolve_namespace(dim_path), "name": dim_path, "field": fk},
#                     {"namespace": _ns, "name": self._SOURCE_PATH, "field": join_col},
#                 ],
#                 "transformationDescription": desc,
#                 "transformationType": "DIRECT",
#             }

#         column_lineage_fact["valeur"] = {
#             "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": "valeur"}],
#             "transformationDescription": "Measure column. Cast to DoubleType().",
#             "transformationType": "DIRECT",
#         }
#         for tc in self.tech_cols:
#             column_lineage_fact[tc] = {
#                 "inputFields": [{"namespace": _ns, "name": self._SOURCE_PATH, "field": tc}],
#                 "transformationDescription": "Tech column passed through from source_clean.",
#                 "transformationType": "DIRECT",
#             }

#         emit_marquez_step(
#             spark_df=fact,
#             step_name="06_Build_Fact_Macroeco",
#             description=(
#                 "Built fact table 'fact_macroeco' by joining source_clean with all 7 dimensions. "
#                 "LEFT JOINs on: pays→id_pays, variable→id_variable, periode→id_periode, "
#                 "source→id_source, base→id_base, version→id_version, code_secteur/produit→id_secteur. "
#                 "valeur cast to DoubleType(). "
#                 f"Result: {fact.count()} rows, {len(fact.columns)} columns."
#             ),
#             trans_type="TRANSFORMATION",
#             inputs=[
#                 self._SOURCE_PATH,
#                 self._DIM_PAYS_PATH,
#                 self._DIM_VARIABLE_PATH,
#                 self._DIM_PERIODE_PATH,
#                 self._DIM_SOURCE_PATH,
#                 self._DIM_BASE_PATH,
#                 self._DIM_VERSION_PATH,
#                 self._DIM_SECTEUR_PATH,
#             ],
#             outputs=[self._FACT_PATH],
#             column_lineage=column_lineage_fact,
#         )
#         print(f"  ✅ fact_macroeco built")
#         return fact

#     # --------------------------------------------------
#     # STEP 7 — WRITE REFINED
#     # --------------------------------------------------
#     def write_refined(self, df, folder_name, mem_input_path):
#         missing = [c for c in self.tech_cols if c not in df.columns]
#         if missing:
#             raise ValueError(
#                 f"[write_refined] '{folder_name}' missing tech cols: {missing}. Aborting."
#             )

#         output_path = f"s3a://{self.config.bucket_refined}/{folder_name}/"
#         step_name   = f"07_Write_Refined_{folder_name.replace('/', '_').replace('dimensions_', 'dim_')}"

#         print(f"\n[STEP 7] Write Refined — {folder_name}")
#         print(f"  📂 Output: {output_path}")

#         # ── Column lineage ────────────────────────────────────────────
#         _ns_in = resolve_namespace(mem_input_path)
#         column_lineage = {
#             field.name: {
#                 "inputFields": [
#                     {"namespace": _ns_in, "name": mem_input_path, "field": field.name}
#                 ],
#                 "transformationDescription": (
#                     f"Written to Parquet in refined zone. "
#                     f"CAST to {field.dataType.simpleString()} for schema enforcement. "
#                     f"coalesce(1) — single output file."
#                 ),
#                 "transformationType": "DIRECT",
#             }
#             for field in df.schema.fields
#         }

#         emit_marquez_step(
#             spark_df=df,
#             step_name=step_name,
#             description=(
#                 f"Write '{folder_name}' to refined zone. "
#                 f"coalesce(1).write.mode('overwrite').parquet('{output_path}'). "
#                 f"{df.count()} rows, {len(df.columns)} columns."
#             ),
#             trans_type="LOAD",
#             inputs=[mem_input_path],
#             outputs=[output_path],
#             column_lineage=column_lineage,
#         )

#         df.coalesce(1).write.mode("overwrite").parquet(output_path)
#         log.info(f"[write_refined] ✓ {output_path} | columns: {df.columns}")
#         print(f"  ✅ {folder_name} written to refined")


# # --------------------------------------------------
# # ENTRYPOINT
# # --------------------------------------------------
# if __name__ == "__main__":
#     RefinedProcessor().run()
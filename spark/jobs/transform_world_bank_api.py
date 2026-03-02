from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number, desc, round as spark_round, coalesce
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import os
import logging

from common.spark_session import create_spark_session, stop_spark_session

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("WORLD_BANK_TRANSFORM")

# =====================================================
# SPARK SESSION
# =====================================================
spark = create_spark_session("WORLD-BANK-RAW-to-TRANSFORMED")
spark.sparkContext.setLogLevel("WARN")

RAW_ROOT = "s3a://01-raw"
TARGET_BASE = "s3a://02-transformed/WORLD_BANK"

# =====================================================
# UTIL FUNCTIONS
# =====================================================
def get_latest_world_bank_path(spark, raw_root):
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI
    fs = FileSystem.get(URI(raw_root), sc._jsc.hadoopConfiguration())

    years = [f.getPath().getName() for f in fs.listStatus(Path(raw_root)) 
             if f.isDirectory() and f.getPath().getName().isdigit()]
    if not years:
        raise RuntimeError("❌ No year folders found in RAW")
    latest_year = sorted(years)[-1]

    months_path = f"{raw_root}/{latest_year}"
    months = [f.getPath().getName() for f in fs.listStatus(Path(months_path)) 
              if f.isDirectory() and f.getPath().getName().isdigit()]
    if not months:
        raise RuntimeError(f"❌ No month folders found under {latest_year}")
    latest_month = sorted(months)[-1]

    world_bank_path = f"{raw_root}/{latest_year}/{latest_month}/WORLD_BANK"
    if not fs.exists(Path(world_bank_path)):
        raise RuntimeError(f"❌ WORLD_BANK folder not found at {world_bank_path}")

    log.info(f"📅 Latest WORLD_BANK path: {world_bank_path}")
    return world_bank_path

def list_csv_files(spark, base_path):
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI
    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    stack = [Path(base_path)]
    files = []

    while stack:
        current = stack.pop()
        for status in fs.listStatus(current):
            if status.isDirectory():
                stack.append(status.getPath())
            elif status.isFile() and status.getPath().toString().endswith(".csv"):
                files.append(status.getPath().toString())
    return files

# =====================================================
# MAIN PROCESS
# =====================================================
world_bank_path = get_latest_world_bank_path(spark, RAW_ROOT)
csv_files = list_csv_files(spark, world_bank_path)
log.info(f"📄 Found {len(csv_files)} WORLD_BANK CSV files")

for file_path in csv_files:
    log.info("="*80)
    log.info(f"🚀 Processing {file_path}")
    indicator_folder = file_path.split("/")[-2]
    log.info(f"📊 Indicator detected: {indicator_folder}")

    # -------------------------
    # READ CSV
    # -------------------------
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("sep", ";")   # 👈 AJOUTE ÇA
        .csv(file_path)
    )

    # -------------------------
    # TRANSFORMATION CLEAN
    # -------------------------
    transformed_df = (
        df
        .withColumnRenamed("indicator_name", "Variable")
        .withColumnRenamed("year", "annee")
        .withColumnRenamed("value", "valeur")
        .withColumnRenamed("country_iso3", "pays")
        .withColumn("pays", coalesce(col("pays"), lit("UNKNOWN")))
        .withColumn("date_chargement", current_timestamp())
        .withColumn("source", lit("WORLD_BANK_API"))
        .withColumn("valeur", spark_round(col("valeur"), 6))
        # Filtrer les lignes avec valeur non NULL avant SCD2
        .filter(col("valeur").isNotNull())
        .select("annee", "Variable", "valeur", "pays", "date_chargement", "source")
    )

    # -------------------------
    # VERSION ACTIVE LOGIC (SCD2)
    # -------------------------
    window_spec = Window.partitionBy("annee", "Variable", "pays").orderBy(desc("date_chargement"))
    transformed_df = (
        transformed_df
        .withColumn("_rank", row_number().over(window_spec))
        .withColumn("version_active", (col("_rank") == 1).cast("int"))
        .drop("_rank")
    )

    # -------------------------
    # WRITE PARTITIONED BY YEAR
    # -------------------------
    years = [row["annee"] for row in transformed_df.select("annee").distinct().collect()]

    for y in years:
        df_year = transformed_df.filter(col("annee") == y)
        output_path = os.path.join(TARGET_BASE, indicator_folder, str(y))
        log.info(f"💾 Processing year {y}")

        try:
            existing_df = spark.read.parquet(output_path).cache()
            existing_df.count()  # force read
            log.info(f"📂 Existing dataset found for year {y}")

            # Round valeurs et gérer NULL pour pays
            from pyspark.sql.types import DecimalType
            # Normaliser le type 'valeur' pour comparer correctement
            existing_df = existing_df.withColumn("valeur", col("valeur").cast(DecimalType(38, 6)))
            df_year = df_year.withColumn("valeur", col("valeur").cast(DecimalType(38, 6)))

            # Toujours coalescer les pays et arrondir si besoin
            existing_df = existing_df.withColumn("pays", coalesce(col("pays"), lit("UNKNOWN")))
            df_year = df_year.withColumn("pays", coalesce(col("pays"), lit("UNKNOWN")))

            # Colonnes à comparer pour détection SCD2
            compare_cols = ["annee", "Variable", "pays", "valeur"]

            # Lignes nouvelles ou modifiées
            new_rows = df_year.join(
                existing_df.select(compare_cols),
                on=compare_cols,
                how="left_anti"
            )

            new_count = new_rows.count()
            log.info(f"🧐 Rows considered new by Spark for year {y}: {new_count}")
            new_rows.show(20, truncate=False)

            if new_count == 0:
                log.info(f"✅ No new or changed rows detected for {y}")
                existing_df.unpersist()
                continue

            log.info(f"🆕 {new_count} new/changed rows detected")
            # Version temporaire 0 pour nouvelles lignes
            new_rows = new_rows.withColumn("version_active", lit(0).cast("int"))
            final_df = existing_df.unionByName(new_rows)
            existing_df.unpersist()

        except AnalysisException:
            log.info(f"🆕 First load detected for year {y}")
            final_df = df_year

        # Recalculer version_active après union
        window_spec = Window.partitionBy("annee", "Variable", "pays").orderBy(desc("date_chargement"))
        final_df = (
            final_df
            .filter(col("valeur").isNotNull())  # filtrer les NULL dans le dataset final aussi
            .withColumn("_rank", row_number().over(window_spec))
            .withColumn("version_active", (col("_rank") == 1).cast("int"))
            .drop("_rank")
        )

        # Write final dataset
        (
            final_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

        log.info(f"✅ Final dataset written for year {y}")

log.info("🎉 WORLD_BANK transformation completed")
stop_spark_session(spark)

# from pyspark.sql.functions import (
#     col, current_timestamp, lit
# )
# from pyspark.sql.types import StringType
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number, desc
# from pyspark.sql.utils import AnalysisException
# import os
# import logging

# from common.spark_session import create_spark_session, stop_spark_session

# # =====================================================
# # LOGGING
# # =====================================================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )
# log = logging.getLogger("WORLD_BANK_TRANSFORM")

# # =====================================================
# # SPARK SESSION
# # =====================================================
# spark = create_spark_session("WORLD-BANK-RAW-to-TRANSFORMED")
# spark.sparkContext.setLogLevel("WARN")

# RAW_ROOT = "s3a://01-raw"
# TARGET_BASE = "s3a://02-transformed/WORLD_BANK"

# # =====================================================
# # FIND LATEST YEAR/MONTH FOLDER
# # =====================================================
# def get_latest_world_bank_path(spark, raw_root):

#     sc = spark.sparkContext
#     Path = sc._jvm.org.apache.hadoop.fs.Path
#     FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#     URI = sc._jvm.java.net.URI

#     fs = FileSystem.get(URI(raw_root), sc._jsc.hadoopConfiguration())

#     # --------- LIST YEARS (ONLY NUMERIC) ---------
#     years = [
#         f.getPath().getName()
#         for f in fs.listStatus(Path(raw_root))
#         if f.isDirectory() and f.getPath().getName().isdigit()
#     ]

#     if not years:
#         raise RuntimeError("❌ No year folders found in RAW")

#     latest_year = sorted(years)[-1]
#     log.info(f"📅 Latest year detected: {latest_year}")

#     # --------- LIST MONTHS (ONLY NUMERIC) ---------
#     months_path = f"{raw_root}/{latest_year}"

#     months = [
#         f.getPath().getName()
#         for f in fs.listStatus(Path(months_path))
#         if f.isDirectory() and f.getPath().getName().isdigit()
#     ]

#     if not months:
#         raise RuntimeError(f"❌ No month folders found under {latest_year}")

#     latest_month = sorted(months)[-1]
#     log.info(f"📅 Latest month detected: {latest_month}")

#     world_bank_path = f"{raw_root}/{latest_year}/{latest_month}/WORLD_BANK"

#     # --------- VERIFY PATH EXISTS ---------
#     if not fs.exists(Path(world_bank_path)):
#         raise RuntimeError(f"❌ WORLD_BANK folder not found at {world_bank_path}")

#     log.info(f"📂 Latest WORLD_BANK path: {world_bank_path}")
#     return world_bank_path

# # =====================================================
# # LIST ALL CSV FILES
# # =====================================================
# def list_csv_files(spark, base_path):

#     sc = spark.sparkContext
#     Path = sc._jvm.org.apache.hadoop.fs.Path
#     FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#     URI = sc._jvm.java.net.URI

#     fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

#     stack = [Path(base_path)]
#     files = []

#     while stack:
#         current = stack.pop()

#         for status in fs.listStatus(current):
#             if status.isDirectory():
#                 stack.append(status.getPath())
#             elif status.isFile() and status.getPath().toString().endswith(".csv"):
#                 files.append(status.getPath().toString())

#     return files


# # =====================================================
# # MAIN PROCESS
# # =====================================================
# world_bank_path = get_latest_world_bank_path(spark, RAW_ROOT)
# csv_files = list_csv_files(spark, world_bank_path)

# log.info(f"📄 Found {len(csv_files)} WORLD_BANK CSV files")

# for file_path in csv_files:

#     log.info("=" * 80)
#     log.info(f"🚀 Processing {file_path}")
#     # Extract indicator folder name
#     indicator_folder = file_path.split("/")[-2]
#     log.info(f"📊 Indicator detected: {indicator_folder}")
#     df = (
#         spark.read
#         .option("header", True)
#         .option("inferSchema", True)
#         .option("sep", ";")   # 👈 AJOUTE ÇA
#         .csv(file_path)
#     )

#     df = df.filter(col("value").isNotNull())

#     # --------------------------------------------------
#     # TRANSFORMATION
#     # --------------------------------------------------
#     transformed_df = (
#         df
#         .withColumn("dim_id", col("indicator_id").cast(StringType()))
#         .withColumn("dim_key", col("indicator_id").cast(StringType()))
#         .withColumnRenamed("indicator_name", "Variable")
#         .withColumnRenamed("year", "annee")
#         .withColumnRenamed("value", "valeur")
#         .withColumnRenamed("country_iso3", "pays")
#         .withColumn("version", lit("N/A"))
#         .withColumn("base", lit("N/A"))
#         .withColumn("source", lit("WORLD_BANK_API"))
#         .withColumn("code_secteur", lit("N/A"))
#         .withColumn("lib_secteur", lit("N/A"))
#         .withColumn("rang", lit(None).cast("int"))
#         .withColumn("date_chargement", current_timestamp())
#         .select(
#             "annee",
#             "dim_id",
#             "dim_key",
#             "Variable",
#             "valeur",
#             "version",
#             "base",
#             "source",
#             "code_secteur",
#             "lib_secteur",
#             "pays",
#             "rang",
#             "date_chargement"
#         )
#     )

#     # --------------------------------------------------
#     # VERSION ACTIVE LOGIC
#     # --------------------------------------------------
#     window_spec = Window.partitionBy(
#         "annee", "dim_id", "dim_key", "Variable"
#     ).orderBy(desc("date_chargement"))

#     transformed_df = (
#         transformed_df
#         .withColumn("_rank", row_number().over(window_spec))
#         .withColumn("version_active", (col("_rank") == 1).cast("int"))
#         .drop("_rank")
#     )

#     # --------------------------------------------------
#     # WRITE PARTITIONED BY YEAR
#     # --------------------------------------------------
#     # --------------------------------------------------
#     years = [row["annee"] for row in transformed_df.select("annee").distinct().collect()]

#     for y in years:

#         df_year = transformed_df.filter(col("annee") == y)

#         output_path = os.path.join(
#             TARGET_BASE,
#             indicator_folder,
#             str(y)
#         )

#         log.info(f"💾 Processing year {y}")

#         try:
#             # --------------------------------------------------
#             # CHECK IF DATA ALREADY EXISTS
#             # --------------------------------------------------
#             existing_df = spark.read.parquet(output_path).cache()
#             existing_df.count()  # force read

#             log.info(f"📂 Existing dataset found for year {y}")

#             # Colonnes métier pour comparaison (SANS date_chargement)
#             compare_cols = [
#                 "annee",
#                 "dim_id",
#                 "dim_key",
#                 "Variable",
#                 "valeur"
#             ]

#             # --------------------------------------------------
#             # DETECT NEW OR CHANGED ROWS
#             # --------------------------------------------------
#             new_rows = df_year.join(
#                 existing_df.select(compare_cols),
#                 on=compare_cols,
#                 how="left_anti"
#             )

#             debug_join = df_year.alias("new").join(
#                 existing_df.alias("old"),
#                 on=[
#                     "annee",
#                     "dim_id",
#                     "dim_key",
#                     "Variable",
#                     "valeur"
#                 ],
#                 how="inner"
#             )
#             if debug_join.count() > 0:
#                 log.error("🚨 REAL VALUE DIFFERENCES FOUND:")
#                 debug_join.select(
#                     col("new.annee"),
#                     col("new.dim_key"),
#                     col("old.valeur").alias("old_valeur"),
#                     col("new.valeur").alias("new_valeur")
#                 ).show(5, truncate=False)

#             new_count = new_rows.count()
#             # --------------------------------------------------
#             # DEBUG: SHOW NEW / CHANGED ROWS
#             # --------------------------------------------------
#             if new_count > 0:
#                 log.warning(f"⚠️ Showing new/changed rows for year {y}")
                
#                 new_rows.select(
#                     "annee",
#                     "dim_id",
#                     "dim_key",
#                     "Variable",
#                     "valeur",
#                     "date_chargement"
#                 ).show(20, truncate=False)
            
#             if new_count == 0:
#                 log.info(f"✅ No new or changed rows detected for {y}")
#                 existing_df.unpersist()
#                 continue

#             log.info(f"🆕 {new_count} new/changed rows detected")

#             # Ajouter version_active=0 temporairement
#             new_rows = new_rows.withColumn("version_active", lit(0).cast("int"))

#             # Union ancien + nouveau
#             final_df = existing_df.unionByName(new_rows)

#             existing_df.unpersist()

#         except AnalysisException:
#             # --------------------------------------------------
#             # FIRST LOAD
#             # --------------------------------------------------
#             log.info(f"🆕 First load detected for year {y}")
#             final_df = df_year

#         # --------------------------------------------------
#         # RECALCULATE version_active (SCD2)
#         # --------------------------------------------------
#         window_spec = Window.partitionBy(
#             "annee", "dim_id", "dim_key", "Variable"
#         ).orderBy(desc("date_chargement"))

#         final_df = (
#             final_df
#             .withColumn("_rank", row_number().over(window_spec))
#             .withColumn("version_active", (col("_rank") == 1).cast("int"))
#             .drop("_rank")
#         )

#         # --------------------------------------------------
#         # WRITE
#         # --------------------------------------------------
#         (
#             final_df
#             .coalesce(1)
#             .write
#             .mode("overwrite")
#             .parquet(output_path)
#         )

#         log.info(f"✅ Final dataset written for year {y}")
#         log.info("🎉 WORLD_BANK transformation completed")
# stop_spark_session(spark)


#############################################################################################################################

# from pyspark.sql.functions import (
#     col, current_timestamp, lit
# )
# from pyspark.sql.types import StringType
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number, desc
# from pyspark.sql.utils import AnalysisException
# import os
# import logging

# from common.spark_session import create_spark_session, stop_spark_session

# # =====================================================
# # LOGGING
# # =====================================================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )
# log = logging.getLogger("WORLD_BANK_TRANSFORM")

# # =====================================================
# # SPARK SESSION
# # =====================================================
# spark = create_spark_session("WORLD-BANK-RAW-to-TRANSFORMED")
# spark.sparkContext.setLogLevel("WARN")

# RAW_ROOT = "s3a://01-raw"
# TARGET_BASE = "s3a://02-transformed/WORLD_BANK"

# # =====================================================
# # FIND LATEST YEAR/MONTH FOLDER
# # =====================================================
# def get_latest_world_bank_path(spark, raw_root):

#     sc = spark.sparkContext
#     Path = sc._jvm.org.apache.hadoop.fs.Path
#     FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#     URI = sc._jvm.java.net.URI

#     fs = FileSystem.get(URI(raw_root), sc._jsc.hadoopConfiguration())

#     # --------- LIST YEARS (ONLY NUMERIC) ---------
#     years = [
#         f.getPath().getName()
#         for f in fs.listStatus(Path(raw_root))
#         if f.isDirectory() and f.getPath().getName().isdigit()
#     ]

#     if not years:
#         raise RuntimeError("❌ No year folders found in RAW")

#     latest_year = sorted(years)[-1]
#     log.info(f"📅 Latest year detected: {latest_year}")

#     # --------- LIST MONTHS (ONLY NUMERIC) ---------
#     months_path = f"{raw_root}/{latest_year}"

#     months = [
#         f.getPath().getName()
#         for f in fs.listStatus(Path(months_path))
#         if f.isDirectory() and f.getPath().getName().isdigit()
#     ]

#     if not months:
#         raise RuntimeError(f"❌ No month folders found under {latest_year}")

#     latest_month = sorted(months)[-1]
#     log.info(f"📅 Latest month detected: {latest_month}")

#     world_bank_path = f"{raw_root}/{latest_year}/{latest_month}/WORLD_BANK"

#     # --------- VERIFY PATH EXISTS ---------
#     if not fs.exists(Path(world_bank_path)):
#         raise RuntimeError(f"❌ WORLD_BANK folder not found at {world_bank_path}")

#     log.info(f"📂 Latest WORLD_BANK path: {world_bank_path}")
#     return world_bank_path

# # =====================================================
# # LIST ALL CSV FILES
# # =====================================================
# def list_csv_files(spark, base_path):

#     sc = spark.sparkContext
#     Path = sc._jvm.org.apache.hadoop.fs.Path
#     FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#     URI = sc._jvm.java.net.URI

#     fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

#     stack = [Path(base_path)]
#     files = []

#     while stack:
#         current = stack.pop()

#         for status in fs.listStatus(current):
#             if status.isDirectory():
#                 stack.append(status.getPath())
#             elif status.isFile() and status.getPath().toString().endswith(".csv"):
#                 files.append(status.getPath().toString())

#     return files


# # =====================================================
# # MAIN PROCESS
# # =====================================================
# world_bank_path = get_latest_world_bank_path(spark, RAW_ROOT)
# csv_files = list_csv_files(spark, world_bank_path)

# log.info(f"📄 Found {len(csv_files)} WORLD_BANK CSV files")

# for file_path in csv_files:

#     log.info("=" * 80)
#     log.info(f"🚀 Processing {file_path}")
#     # Extract indicator folder name
#     indicator_folder = file_path.split("/")[-2]
#     log.info(f"📊 Indicator detected: {indicator_folder}")
#     df = (
#         spark.read
#         .option("header", True)
#         .option("inferSchema", True)
#         .csv(file_path)
#     )

#     df = df.filter(col("value").isNotNull())

#     # --------------------------------------------------
#     # TRANSFORMATION
#     # --------------------------------------------------
#     transformed_df = (
#         df
#         .withColumn("dim_id", col("indicator_id").cast(StringType()))
#         .withColumn("dim_key", col("country_iso3").cast(StringType()))
#         .withColumnRenamed("indicator_name", "Variable")
#         .withColumnRenamed("year", "annee")
#         .withColumnRenamed("value", "valeur")
#         .withColumnRenamed("country_name", "pays")
#         .withColumn("version", lit("N/A"))
#         .withColumn("base", lit("WORLD_BANK"))
#         .withColumn("source", lit("WORLD_BANK_API"))
#         .withColumn("code_secteur", lit("N/A"))
#         .withColumn("lib_secteur", lit("N/A"))
#         .withColumn("rang", lit(None).cast("int"))
#         .withColumn("date_chargement", current_timestamp())
#         .select(
#             "annee",
#             "dim_id",
#             "dim_key",
#             "Variable",
#             "valeur",
#             "version",
#             "base",
#             "source",
#             "code_secteur",
#             "lib_secteur",
#             "pays",
#             "rang",
#             "date_chargement"
#         )
#     )

#     # --------------------------------------------------
#     # VERSION ACTIVE LOGIC
#     # --------------------------------------------------
#     window_spec = Window.partitionBy(
#         "annee", "dim_id", "dim_key", "Variable"
#     ).orderBy(desc("date_chargement"))

#     transformed_df = (
#         transformed_df
#         .withColumn("_rank", row_number().over(window_spec))
#         .withColumn("version_active", (col("_rank") == 1).cast("int"))
#         .drop("_rank")
#     )

#     # --------------------------------------------------
#     # WRITE PARTITIONED BY YEAR
#     # --------------------------------------------------
#     years = [row["annee"] for row in transformed_df.select("annee").distinct().collect()]

#     for y in years:

#         output_path = os.path.join(
#             TARGET_BASE,
#             indicator_folder,
#             str(y)
#         )

#         (
#             transformed_df
#             .filter(col("annee") == y)
#             .coalesce(1)
#             .write
#             .mode("overwrite")
#             .parquet(output_path)
#         )

#         log.info(f"💾 Written year {y} to {output_path}")

# log.info("🎉 WORLD_BANK transformation completed")
# stop_spark_session(spark)
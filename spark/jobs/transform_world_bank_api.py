from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number, desc, round as spark_round, coalesce
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import DecimalType
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

# Sentinelle pour remplacer NULL dans les comparaisons de join
# (NULL != NULL dans Spark, donc on utilise une valeur impossible)
NULL_SENTINEL = -99999.999999

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
    log.info("=" * 80)
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
        # .option("sep", ";")
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
        # ✅ On NE filtre PAS les NULL ici — les années avec toutes valeurs NULL
        #    doivent quand même être écrites (ex: HCI 2015)
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
    log.info(f"📆 Years found in CSV: {sorted(years)}")

    for y in years:
        df_year = transformed_df.filter(col("annee") == y)
        output_path = os.path.join(TARGET_BASE, indicator_folder, str(y))
        log.info(f"💾 Processing year {y}")

        try:
            existing_df = spark.read.parquet(output_path).cache()
            existing_df.count()  # force read
            log.info(f"📂 Existing dataset found for year {y}")

            # ✅ Normaliser les types AVANT le join pour éviter les faux positifs
            existing_df = existing_df.withColumn("valeur", col("valeur").cast(DecimalType(38, 6)))
            df_year = df_year.withColumn("valeur", col("valeur").cast(DecimalType(38, 6)))
            existing_df = existing_df.withColumn("pays", coalesce(col("pays"), lit("UNKNOWN")))
            df_year = df_year.withColumn("pays", coalesce(col("pays"), lit("UNKNOWN")))

            # ✅ Remplacer NULL par sentinelle pour la comparaison
            #    car NULL != NULL dans un Spark join → faux positifs sans ça
            df_year_cmp = df_year.withColumn(
                "valeur_cmp", coalesce(col("valeur"), lit(NULL_SENTINEL))
            )
            existing_cmp = existing_df.withColumn(
                "valeur_cmp", coalesce(col("valeur"), lit(NULL_SENTINEL))
            )

            compare_cols = ["annee", "Variable", "pays", "valeur_cmp"]

            # Lignes nouvelles ou modifiées
            new_rows = df_year_cmp.join(
                existing_cmp.select(compare_cols),
                on=compare_cols,
                how="left_anti"
            ).drop("valeur_cmp")  # ✅ retirer la colonne sentinelle après le join

            new_count = new_rows.count()
            log.info(f"🧐 Rows considered new by Spark for year {y}: {new_count}")
            new_rows.show(20, truncate=False)

            if new_count == 0:
                log.info(f"✅ No new or changed rows detected for {y} — MinIO file untouched")
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
            # ✅ On NE filtre PAS les NULL — on garde toutes les lignes y compris valeur NULL
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
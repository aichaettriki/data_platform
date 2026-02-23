from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_extract
import os
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("INS_ENRICH")

# =====================================================
# SPARK SESSION
# =====================================================
spark = (
    SparkSession.builder
    .appName("INS-RAW-to-SILVER-Enrichment")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# HADOOP HELPERS
# =====================================================
def find_ins_paths(spark, base_path="s3a://01-raw"):
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    source_path = None
    dimension_path = None
    stack = [Path(base_path)]

    while stack and (not source_path or not dimension_path):
        current = stack.pop()
        try:
            for status in fs.listStatus(current):
                if status.isDirectory():
                    p = status.getPath().toString()
                    if p.endswith("/INS/Agregat/Source"):
                        source_path = p
                        log.info(f"✅ Found FACT base: {source_path}")
                    elif p.endswith("/INS/Agregat/Dimension"):
                        dimension_path = p
                        log.info(f"✅ Found DIMENSION base: {dimension_path}")
                    else:
                        stack.append(status.getPath())
        except Exception:
            pass

    return source_path, dimension_path


def extract_category_path(full_s3_path, raw_bucket_root):
    clean_path = full_s3_path.replace("s3a://", "").strip("/")
    clean_root = raw_bucket_root.replace("s3a://", "").strip("/")
    if clean_path.startswith(clean_root):
        relative_path = clean_path[len(clean_root):].strip("/")
        parts = relative_path.split("/")
        if len(parts) > 3:
            category_parts = parts[2:-1]
            return "/".join(category_parts)
    return "UNKNOWN_CATEGORY"

def list_csv_files(spark, base_path):
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())
    files = []

    for status in fs.listStatus(Path(base_path)):
        if status.isFile() and status.getPath().toString().endswith(".csv"):
            files.append(status.getPath().toString())

    return sorted(files)

# =====================================================
# PATH RESOLUTION
# =====================================================
RAW_ROOT = "s3a://01-raw"
SILVER_BASE = "s3a://02-transformed/INS/Agregat"

log.info("🔍 Searching for INS Source / Dimension folders...")
FACT_BASE, DIM_BASE = find_ins_paths(spark, RAW_ROOT)

if not FACT_BASE or not DIM_BASE:
    raise RuntimeError("❌ INS Source or Dimension folder not found")

# =====================================================
# READ DIMENSIONS (ONCE)
# =====================================================
log.info("📥 Reading DIMENSION files")
dim_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{DIM_BASE}/*/*.csv")
)

dim_lookup = (
    dim_df
    .select(
        col("dimension_id"),
        col("KEY").alias("dim_indicator_key"),
        col("FULLNAME").alias("indicator_name")
    )
    .dropDuplicates()
)

log.info(f"📘 DIMENSION lookup rows = {dim_lookup.count()}")
log.info(f"DIM columns = {dim_df.columns}")
log.info("📘 Dimension sample:")
dim_lookup.show(5, truncate=False)

# =====================================================
# PROCESS EACH FACT FILE
# =====================================================
fact_files = list_csv_files(spark, FACT_BASE)
log.info(f"📄 Found {len(fact_files)} FACT files")

for fact_file in fact_files:

    file_name = fact_file.split("/")[-1].replace(".csv", "")
    source_category = extract_category_path(fact_file, RAW_ROOT)  # ✅ AJOUT
    log.info(f"📂 Source category: {source_category}")
    log.info("=" * 80)
    log.info(f"🚀 Processing FACT file: {file_name}")

    # READ ONE FACT FILE
    fact_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(fact_file)
    )

    fact_count = fact_df.count()
    log.info(f"📥 {file_name} rows = {fact_count}")
    log.info(f"📄 Sample of {file_name}:")
    fact_df.show(5, truncate=False)

    # JOIN WITH DIMENSION
    enriched_df = (
        fact_df.alias("f")
        .join(
            dim_lookup.alias("d"),
            on=[
                col("f.dimension_id") == col("d.dimension_id"),
                col("f.dimension_key") == col("d.dim_indicator_key"),
            ],
            how="left"
        )
    )

    # Drop colonnes dupliquées venant de la dimension
    enriched_df = enriched_df.drop(
        col("d.dimension_id")
    ).drop(
        col("d.dim_indicator_key")
    )

    enriched_df = enriched_df.withColumn(
    "date_chargement",
    current_timestamp()
    )
    # EXTRACT YEAR FROM period (ex: YEARS:2015 -> 2015)
    enriched_df = enriched_df.withColumn(
        "year",
        regexp_extract(col("period"), r"YEARS:(\d{4})", 1).cast("int")
    )

    enriched_count = enriched_df.count()
    log.info(f"🔗 {file_name} enriched rows = {enriched_count}")
    log.info(f"🔗 Sample enriched {file_name}:")
    enriched_df.show(5, truncate=False)

    # =====================================================
    # FINAL STRUCTURE (DROP + RENAME + REORDER)
    # =====================================================

    enriched_df = (
        enriched_df
        .drop("period")  
        .withColumnRenamed("dimension_id", "dim_id")
        .withColumnRenamed("dimension_key", "dim_key")
        .withColumnRenamed("year", "annee")
        .withColumnRenamed("value", "valeur")
        .withColumnRenamed("indicator_name", "Variable")
        .withColumn("version", lit("N/A"))
        .withColumn("base", lit("2015"))          
        .withColumn("source", lit(source_category)) 
        .select(
            "annee",
            "dim_id",
            "dim_key",
            "Variable",
            "valeur",
            "version",
            "base",    
            "source",   
            "date_chargement"
        )
    )

    log.info("************************ FINAL ENRICHED ******************")
    log.info("🔗 Sample enriched:")
    enriched_df.show(5, truncate=False)

    # DATA QUALITY CHECK
    missing = enriched_df.filter(col("Variable").isNull()).count()
    if missing > 0:
        raise RuntimeError(
            f"MISSING_DIMENSIONS::{file_name}::{missing}"
        )

    log.warning(f"⚠️ {file_name} missing indicator_name = {missing}")

    # WRITE PARQUET FILES PER YEAR (ONE FOLDER PER YEAR)
    years = [row["annee"] for row in enriched_df.select("annee").distinct().collect()]
    # DATA QUALITY CHECK ✅
    missing = enriched_df.filter(col("Variable").isNull()).count()

    for y in years:
        df_year = enriched_df.filter(col("annee") == y)
        output_path = os.path.join(SILVER_BASE, str(y))
        log.info(f"💾 Processing year {y}")

        try:
            existing_df = spark.read.parquet(output_path).cache()
            existing_df.count()  # force lecture immédiate
            log.info(f"📂 Existing file found for year {y}, checking new rows...")

            compare_cols = ["annee", "dim_id", "dim_key", "Variable", "valeur", "base", "source"]

            new_rows = df_year.join(
                existing_df.select(compare_cols),
                on=compare_cols,
                how="left_anti"
            )

            new_count = new_rows.count()
            if new_count == 0:
                log.info(f"✅ No new or changed rows for year {y}")
                existing_df.unpersist()
                continue

            log.info(f"🆕 {new_count} new/changed rows detected")

            # ✅ Ajouter is_latest=0 temporaire aux nouvelles lignes
            # pour que le schéma soit compatible avec existing_df
            if "is_latest" in existing_df.columns:
                new_rows = new_rows.withColumn("is_latest", lit(0).cast("int"))

            final_df = existing_df.unionByName(new_rows)
            existing_df.unpersist()

        except AnalysisException as e:
            log.info(f"🆕 No existing file for {y} (AnalysisException: {str(e)[:100]}), writing full dataset")
            final_df = df_year

        # Recalcul is_latest sur le final_df complet
        window_spec = Window.partitionBy("annee", "dim_id", "dim_key", "Variable").orderBy(desc("date_chargement"))
        final_df = final_df.withColumn(
            "_rank", row_number().over(window_spec)
        ).withColumn(
            "is_latest", (col("_rank") == 1).cast("int")
        ).drop("_rank")

        (
            final_df
            .repartition(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )
        log.info(f"💾 Final file written for year {y}")

    log.info(f"✅ Finished processing {file_name}")

# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 INS enrichment job completed successfully")
spark.stop()
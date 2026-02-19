from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_extract

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
                    if p.endswith("/INS/Source/Agregat"):
                        source_path = p
                        log.info(f"✅ Found FACT base: {source_path}")
                    elif p.endswith("/INS/Dimension/Agregat"):
                        dimension_path = p
                        log.info(f"✅ Found DIMENSION base: {dimension_path}")
                    else:
                        stack.append(status.getPath())
        except Exception:
            pass

    return source_path, dimension_path


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
        .drop("period")  # supprimer period
        .withColumnRenamed("dimension_id", "dim_id")
        .withColumnRenamed("dimension_key", "dim_key")
        .select(
            "year",
            "dim_id",
            "dim_key",
            "indicator_name",
            "value",
            "date_chargement"
        )
    )
    log.info(f"************************ FINAL ENRICHED ******************")
    log.info(f"🔗 Sample enriched {file_name}:")
    enriched_df.show(5, truncate=False)

    # DATA QUALITY CHECK
    missing = enriched_df.filter(col("indicator_name").isNull()).count()
    if missing > 0:
        raise RuntimeError(
            f"MISSING_DIMENSIONS::{file_name}::{missing}"
        )

    log.warning(f"⚠️ {file_name} missing indicator_name = {missing}")

    # WRITE OUTPUT (ONE FOLDER PER FILE)
    output_path = f"{SILVER_BASE}/{file_name}_transformed"
    log.info(f"💾 Writing output to {output_path}")

    (
        enriched_df
        .repartition("year")          # contrôle du nombre de fichiers par année
        .write
        .mode("overwrite")
        .partitionBy("year")          # création des dossiers year=2015, year=2016, etc.
        .option("header", True)
        .csv(output_path)
    )


    log.info(f"✅ Finished processing {file_name}")

# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 INS enrichment job completed successfully")
spark.stop()

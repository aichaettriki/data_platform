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
def find_ins_paths(spark, base_path, source_id, dimension_id):

    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    source_file = None
    dimension_file = None
    stack = [Path(base_path)]

    while stack:
        current = stack.pop()
        try:
            for status in fs.listStatus(current):
                p = status.getPath().toString()

                if status.isDirectory():
                    stack.append(status.getPath())

                elif status.isFile() and p.endswith(".csv"):

                    file_name = p.split("/")[-1]

                    # FACT
                    if file_name.startswith(source_id):
                        source_file = p
                        log.info(f"✅ Found FACT file: {source_file}")

                    # DIMENSION
                    if file_name.startswith(dimension_id):
                        dimension_file = p
                        log.info(f"✅ Found DIMENSION file: {dimension_file}")

        except Exception:
            pass

        if source_file and dimension_file:
            break

    return source_file, dimension_file

def extract_category_path(full_s3_path, raw_bucket_root):
    """
    Extrait le chemin de catégorie depuis le chemin S3 complet.
    Exemple : s3a://01-raw/2026/02/INS/API-Sources/file.csv → INS/API-Sources
    """
    clean_path = full_s3_path.replace("s3a://", "").strip("/")
    clean_root = raw_bucket_root.replace("s3a://", "").strip("/")

    if clean_path.startswith(clean_root):
        relative_path = clean_path[len(clean_root):].strip("/")
        parts = relative_path.split("/")
        # Structure : YEAR/MONTH/source/SUBsource/.../filename.csv
        # On garde  : source/SUBsource/... (sans année, mois et nom de fichier)
        if len(parts) > 3:
            category_parts = parts[2:-1]
            return "/".join(category_parts)

    return "UNKNOWN_CATEGORY"


# =====================================================
# IDs DES SOURCES ET DIMENSIONS
# =====================================================
SOURCE_ID    = "OBJ11288479"  
DIMENSION_ID = "OBJ11288499"    

# =====================================================
# PATH RESOLUTION
# =====================================================
RAW_ROOT    = "s3a://01-raw"
SILVER_BASE = "s3a://02-transformed/INS/Agregat"

log.info("🔍 Searching for INS Source / Dimension folders...")
FACT_FILE, DIM_FILE  = find_ins_paths(spark, RAW_ROOT, SOURCE_ID, DIMENSION_ID)

if not FACT_FILE or not DIM_FILE :
    raise RuntimeError(
        f"❌ INS Source file (ID={SOURCE_ID}) or Dimension folder not found under {RAW_ROOT}"
    )

# =====================================================
# READ DIMENSIONS (ONCE)
# =====================================================
log.info("📥 Reading DIMENSION files")
dim_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DIM_FILE)
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
# PROCESS FACT FILE
# =====================================================
fact_files = [FACT_FILE]  # on traite le fichier trouvé par ID
log.info(f"📄 Found 1 FACT file: {FACT_FILE}")

for fact_file in fact_files:

    file_name       = fact_file.split("/")[-1].replace(".csv", "")
    source_category = extract_category_path(fact_file, RAW_ROOT)

    log.info("=" * 80)
    log.info(f"🚀 Processing FACT file : {file_name}")
    log.info(f"📂 Source category     : {source_category}")

    # --------------------------------------------------
    # READ FACT FILE
    # --------------------------------------------------
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

    # --------------------------------------------------
    # JOIN WITH DIMENSION
    # --------------------------------------------------
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
    enriched_df = (
        enriched_df
        .drop(col("d.dimension_id"))
        .drop(col("d.dim_indicator_key"))
    )

    # Ajout timestamps et année
    enriched_df = enriched_df.withColumn("date_chargement", current_timestamp())
    enriched_df = enriched_df.withColumn(
        "year",
        regexp_extract(col("period"), r"YEARS:(\d{4})", 1).cast("int")
    )

    log.info(f"🔗 {file_name} enriched rows = {enriched_df.count()}")
    log.info(f"🔗 Sample enriched {file_name}:")
    enriched_df.show(5, truncate=False)

    # --------------------------------------------------
    # FINAL STRUCTURE
    # --------------------------------------------------
    enriched_df = (
        enriched_df
        .drop("period")
        .withColumnRenamed("dimension_id",  "dim_id")
        .withColumnRenamed("dimension_key", "dim_key")
        .withColumnRenamed("year",          "annee")
        .withColumnRenamed("value",         "valeur")
        .withColumnRenamed("indicator_name","Variable")
        .withColumn("version", lit("N/A"))
        .withColumn("base",    lit("2015"))
        .withColumn("source",  lit(source_category))
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

    # --------------------------------------------------
    # DATA QUALITY CHECK
    # --------------------------------------------------
    missing = enriched_df.filter(col("Variable").isNull()).count()
    if missing > 0:
        raise RuntimeError(f"MISSING_DIMENSIONS::{file_name}::{missing}")
    log.warning(f"⚠️ {file_name} missing Variable = {missing}")

    # --------------------------------------------------
    # WRITE PARQUET PER YEAR
    # --------------------------------------------------
    years = [row["annee"] for row in enriched_df.select("annee").distinct().collect()]

    for y in years:
        df_year     = enriched_df.filter(col("annee") == y)
        output_path = os.path.join(SILVER_BASE, str(y))
        log.info(f"💾 Processing year {y}")

        try:
            existing_df = spark.read.parquet(output_path).cache()
            existing_df.count()  # force la lecture immédiate
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

            # Aligner le schéma avant unionByName
            if "is_latest" in existing_df.columns:
                new_rows = new_rows.withColumn("is_latest", lit(0).cast("int"))

            final_df = existing_df.unionByName(new_rows)
            existing_df.unpersist()

        except AnalysisException as e:
            log.info(f"🆕 No existing file for {y} (AnalysisException: {str(e)[:100]}), writing full dataset")
            final_df = df_year

        # Recalcul is_latest sur le final_df complet
        window_spec = Window.partitionBy("annee", "dim_id", "dim_key", "Variable").orderBy(desc("date_chargement"))
        final_df = (
            final_df
            .withColumn("_rank", row_number().over(window_spec))
            .withColumn("is_latest", (col("_rank") == 1).cast("int"))
            .drop("_rank")
        )

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
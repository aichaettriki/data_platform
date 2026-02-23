from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_extract
from common import create_spark_session, stop_spark_session

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
spark = create_spark_session(app_name="INS_Agregat_ETL")
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


def rename_year_partitions(spark, base_path):
    """
    Renomme les dossiers 'year=2015' en '2015' directement sous base_path.
    Fonctionne avec S3A via l'API Hadoop FileSystem (rename = move).
    """
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    for status in fs.listStatus(Path(base_path)):
        if status.isDirectory():
            p = status.getPath().toString()
            folder_name = p.split("/")[-1]
            if folder_name.startswith("year="):
                year_value = folder_name.replace("year=", "")
                new_path = p.replace(folder_name, year_value)
                fs.rename(status.getPath(), Path(new_path))
                log.info(f"🔄 Renamed: {folder_name} → {year_value}")


# =====================================================
# PATH RESOLUTION
# =====================================================
RAW_ROOT = "s3a://01-raw"
SILVER_BASE = "s3a://02-transformed/INS/Agregat"
TEMP_BASE = "s3a://02-transformed/INS/Agregat_tmp"   # dossier temporaire

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
# PROCESS EACH FACT FILE → accumulate enriched DataFrames
# =====================================================
fact_files = list_csv_files(spark, FACT_BASE)
log.info(f"📄 Found {len(fact_files)} FACT files")

all_enriched = []   # ← on accumule tous les DataFrames ici

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

    enriched_df = enriched_df.drop(col("d.dimension_id")).drop(col("d.dim_indicator_key"))

    enriched_df = enriched_df.withColumn("date_chargement", current_timestamp())

    enriched_df = enriched_df.withColumn(
        "year",
        regexp_extract(col("period"), r"YEARS:(\d{4})", 1).cast("int")
    )

    # FINAL STRUCTURE
    enriched_df = (
        enriched_df
        .drop("period")
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

    # DATA QUALITY CHECK
    missing = enriched_df.filter(col("indicator_name").isNull()).count()
    if missing > 0:
        raise RuntimeError(f"MISSING_DIMENSIONS::{file_name}::{missing}")

    log.info(f"✅ {file_name} enriched OK ({enriched_df.count()} rows)")

    all_enriched.append(enriched_df)   # ← on accumule, on n'écrit pas encore

# =====================================================
# UNION DE TOUS LES FICHIERS → WRITE UNIQUE EN SILVER_BASE
# =====================================================
from functools import reduce
from pyspark.sql import DataFrame

log.info("🔀 Union de tous les DataFrames enrichis...")
merged_df = reduce(DataFrame.unionByName, all_enriched)

log.info(f"📊 Total rows après union = {merged_df.count()}")

# Écriture dans un dossier temporaire (Spark génère year=2015 par défaut)
log.info(f"💾 Écriture temporaire dans {TEMP_BASE}")
(
    merged_df
    .repartition("year")
    .write
    .mode("overwrite")
    .partitionBy("year")
    .option("header", True)
    .parquet(TEMP_BASE)
)

# Renommage des dossiers year=XXXX → XXXX puis déplacement vers SILVER_BASE
log.info("🔄 Renommage des partitions year=XXXX → XXXX...")

sc = spark.sparkContext
Path = sc._jvm.org.apache.hadoop.fs.Path
FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
URI = sc._jvm.java.net.URI
fs = FileSystem.get(URI(TEMP_BASE), sc._jsc.hadoopConfiguration())

# Supprimer la destination finale si elle existe déjà (overwrite global)
silver_path = Path(SILVER_BASE)
if fs.exists(silver_path):
    fs.delete(silver_path, True)
    log.info(f"🗑️ Ancien {SILVER_BASE} supprimé")

fs.mkdirs(silver_path)

# Déplacer chaque partition year=XXXX → SILVER_BASE/XXXX
for status in fs.listStatus(Path(TEMP_BASE)):
    if status.isDirectory():
        folder_name = status.getPath().toString().split("/")[-1]
        if folder_name.startswith("year="):
            year_value = folder_name.replace("year=", "")
            dest = Path(f"{SILVER_BASE}/{year_value}")
            fs.rename(status.getPath(), dest)
            log.info(f"📁 Moved & renamed: {folder_name} → {SILVER_BASE}/{year_value}")

# Nettoyage du dossier temporaire
fs.delete(Path(TEMP_BASE), True)
log.info(f"🗑️ Dossier temporaire {TEMP_BASE} supprimé")

# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 INS enrichment job completed successfully")
stop_spark_session(spark)

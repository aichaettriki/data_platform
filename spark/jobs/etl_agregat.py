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
from pyspark.sql.types import StringType
from common.spark_session import create_spark_session, stop_spark_session
 
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
spark = create_spark_session("INS-RAW-to-SILVER-Enrichment")
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
 
# cherce les fichier de plus recent dossier
def find_latest_ins_paths(spark, base_path, source_id, dimension_id):

    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    source_candidates = []
    dimension_candidates = []

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

                    # Extraction année / mois depuis path
                    # Format attendu : s3a://01-raw/YYYY/MM/...
                    parts = p.replace("s3a://", "").split("/")
                    if len(parts) >= 4:
                        year = parts[1]
                        month = parts[2]

                        try:
                            year = int(year)
                            month = int(month)
                        except:
                            continue

                        if file_name.startswith(source_id):
                            source_candidates.append((year, month, p))

                        if file_name.startswith(dimension_id):
                            dimension_candidates.append((year, month, p))

        except Exception:
            pass

    # Sélection du plus récent
    latest_source = max(source_candidates, default=None)
    latest_dimension = max(dimension_candidates, default=None)

    source_file = latest_source[2] if latest_source else None
    dimension_file = latest_dimension[2] if latest_dimension else None

    if source_file:
        log.info(f"✅ Latest FACT file selected: {source_file}")

    if dimension_file:
        log.info(f"✅ Latest DIMENSION file selected: {dimension_file}")

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
# FACT_FILE, DIM_FILE  = find_ins_paths(spark, RAW_ROOT, SOURCE_ID, DIMENSION_ID)
FACT_FILE, DIM_FILE  = find_latest_ins_paths(spark, RAW_ROOT, SOURCE_ID, DIMENSION_ID)

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
    fact_dim_id_col  = f"{DIMENSION_ID}_id"
    fact_dim_key_col = f"{DIMENSION_ID}_key"
    enriched_df = (
        fact_df.alias("f")
        .join(
            dim_lookup.alias("d"),
            on=[
                col(f"f.{fact_dim_id_col}") == col("d.dimension_id"),
                col(f"f.{fact_dim_key_col}") == col("d.dim_indicator_key"),
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
    # enriched_df = enriched_df.withColumn(
    #     "year",
    #     regexp_extract(col("period"), r"YEARS:(\d{4})", 1).cast("int")
    # )
 
    log.info(f"🔗 {file_name} enriched rows = {enriched_df.count()}")
    log.info(f"🔗 Sample enriched {file_name}:")
    enriched_df.show(5, truncate=False)
 
    # --------------------------------------------------
    # FINAL STRUCTURE
    # --------------------------------------------------
    enriched_df = (
        enriched_df
        # .drop("period")
        .withColumnRenamed(fact_dim_id_col,  "dim_id").withColumn("dim_id", col("dim_id").cast(StringType()))
        .withColumnRenamed(fact_dim_key_col, "dim_key").withColumn("dim_key", col("dim_key").cast(StringType()))
        .withColumnRenamed("year",          "annee")
        .withColumnRenamed("value",         "valeur")
        .withColumnRenamed("indicator_name","Variable")
        .withColumn("version", lit("N/A"))
        .withColumn("pays", lit("TUN"))
        .withColumn("lib_secteur", lit("N/A"))
        .withColumn("code_secteur", lit("N/A"))
        .withColumn("rang", lit(None).cast(StringType()))

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
            "code_secteur",
            "lib_secteur",
            "pays",
            "rang",
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
            if "version_active" in existing_df.columns:
                new_rows = new_rows.withColumn("version_active", lit(0).cast("int"))
 
            final_df = existing_df.unionByName(new_rows)
            existing_df.unpersist()
 
        except AnalysisException as e:
            log.info(f"🆕 No existing file for {y} (AnalysisException: {str(e)[:100]}), writing full dataset")
            final_df = df_year
 
        # Recalcul version_active sur le final_df complet
        window_spec = Window.partitionBy("annee", "dim_id", "dim_key", "Variable").orderBy(desc("date_chargement"))
        final_df = (
            final_df
            .withColumn("_rank", row_number().over(window_spec))
            .withColumn("version_active", (col("_rank") == 1).cast("int"))
            .drop("_rank")
        )
 
        (
            final_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )
        log.info("📊 FINAL DATAFRAME SCHEMA (Detailed)")
 
        for field in final_df.schema.fields:
            log.info(
                f"Column: {field.name} | "
                f"Type: {field.dataType.simpleString()} | "
                f"Nullable: {field.nullable}"
            )
 
        log.info(f"📊 Total columns: {len(final_df.columns)}")
        log.info(f"💾 Final file written for year {y}")
 
    log.info(f"✅ Finished processing {file_name}")
 
# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 INS enrichment job completed successfully")
stop_spark_session(spark)
 
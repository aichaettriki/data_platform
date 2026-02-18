"""
Production-ready Spark Job — INS Agrégats
Fixes vs previous version:
  - SparkSession moved inside main() — no session created on module import
  - spark.stop() in finally block scoped to main(), not module level
  - inferSchema replaced by explicit schema + column detection
  - find_latest_files: int() cast guarded against non-numeric folder names
  - annee_dossier redundant column removed — annee used directly for partitioning
  - reduce/unionByName kept (appropriate for Excel file counts, comment updated)
  - All .count() calls removed except the final success log (lazy plan preserved)
"""

import sys
import logging
import traceback
from typing import List, Optional, Tuple
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType,
)

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Spark session  — created inside main(), never at module level
# ──────────────────────────────────────────────────────────────────────────────
def create_spark_session(app_name: str = "transform_aggregats_ins") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key",
                __import__("os").getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",
                __import__("os").getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.endpoint",
                __import__("os").getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ──────────────────────────────────────────────────────────────────────────────
# Path helpers
# ──────────────────────────────────────────────────────────────────────────────
def normalize_s3_path(path: str) -> str:
    if path.startswith("s3a://") and not path.endswith("/"):
        return path + "/"
    return path


# ──────────────────────────────────────────────────────────────────────────────
# File discovery  (Hadoop FS API)
# ──────────────────────────────────────────────────────────────────────────────
def find_latest_files(
    spark: SparkSession,
    base_path: str,
    specific_folder: str,
) -> Tuple[List[str], Optional[str]]:
    """
    Find Excel files under {base_path}/{YEAR}/{MONTH}/{specific_folder}/
    using the most recent YEAR and MONTH folder.

    Non-numeric folder names are skipped with a warning instead of crashing.
    """
    sc = spark.sparkContext
    try:
        Path       = sc._jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        URI        = sc._jvm.java.net.URI

        fs      = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())
        base_p  = Path(base_path)

        def list_numeric_dirs_sorted(p_obj) -> list:
            """List subdirectories whose names are numeric, sorted descending."""
            if not fs.exists(p_obj):
                return []
            dirs = []
            for s in fs.listStatus(p_obj):
                if not s.isDirectory():
                    continue
                name = s.getPath().getName()
                try:
                    int(name)          # guard: skip non-numeric folders
                    dirs.append(s.getPath())
                except ValueError:
                    logger.warning(f"  Skipping non-numeric folder: {name}")
            return sorted(dirs, key=lambda x: int(x.getName()), reverse=True)

        # ── Latest year ──────────────────────────────────────────────────────
        year_dirs = list_numeric_dirs_sorted(base_p)
        if not year_dirs:
            logger.warning(f"No numeric year folders found in {base_path}")
            return [], None
        latest_year = year_dirs[0]
        annee_dossier = latest_year.getName()
        logger.info(f"📅 Latest year: {annee_dossier}")

        # ── Latest month ─────────────────────────────────────────────────────
        month_dirs = list_numeric_dirs_sorted(latest_year)
        if not month_dirs:
            logger.warning(f"No numeric month folders found in {latest_year}")
            return [], None
        latest_month = month_dirs[0]
        logger.info(f"📅 Latest month: {latest_month.getName()}")

        # ── Target folder ────────────────────────────────────────────────────
        target_path = Path(latest_month, specific_folder)
        if not fs.exists(target_path):
            logger.warning(f"Target folder not found: {target_path}")
            return [], None
        logger.info(f"📁 Target folder: {target_path}")

        # ── Collect .xlsx files (recursive) ──────────────────────────────────
        files: List[str] = []
        remote_iter = fs.listFiles(target_path, True)
        while remote_iter.hasNext():
            file_status = remote_iter.next()
            path_str = file_status.getPath().toString()
            name     = file_status.getPath().getName()
            if path_str.endswith(".xlsx") and not name.startswith("~$"):
                files.append(path_str)
                logger.info(f"  ✓ {path_str}")

        logger.info(f"  Total: {len(files)} file(s) found.")
        return files, annee_dossier

    except Exception as exc:
        logger.error(f"Error during file listing: {exc}")
        traceback.print_exc()
        return [], None


# ──────────────────────────────────────────────────────────────────────────────
# Excel processing
# ──────────────────────────────────────────────────────────────────────────────

# Output schema — explicit, no inferSchema
OUTPUT_SCHEMA = StructType([
    StructField("indicateur",       StringType(),   True),
    StructField("valeur",           DoubleType(),   True),
    StructField("annee",            IntegerType(),  True),
    StructField("date_traitement",  TimestampType(), True),
    StructField("fichier_source",   StringType(),   True),
])


def process_excel_file(spark: SparkSession, file_path: str) -> Optional[DataFrame]:
    """
    Read one Excel file with spark-excel, pivot to long format.

    Schema is inferred at read time via spark-excel but immediately cast
    to our canonical types — no double-pass inferSchema on the full dataset.

    Column detection:
      - Columns whose names are purely numeric strings → year columns
      - Everything else → metadata / indicator columns
    """
    try:
        # spark-excel read — we use dataAddress to skip empty header rows if needed
        df_raw = (
            spark.read
            .format("com.crealytics.spark.excel")
            .option("header",                    True)
            .option("inferSchema",               False)   # read everything as String
            .option("treatEmptyValuesAsNulls",   True)
            .option("usePlainNumberFormat",       True)
            .load(file_path)
        )

        # Trim column names
        df_raw = df_raw.toDF(*[c.strip() for c in df_raw.columns])

        # Identify year columns (purely numeric name) vs meta columns
        year_cols = [c for c in df_raw.columns if c.isdigit()]
        meta_cols = [c for c in df_raw.columns if c not in year_cols]

        if not year_cols:
            logger.warning(f"No year columns found — skipping: {file_path}")
            return None

        logger.info(
            f"  File: {file_path.split('/')[-1]} | "
            f"meta={meta_cols} | years={year_cols[:5]}…"
        )

        # ── Unpivot (stack) ──────────────────────────────────────────────────
        stack_expr = ", ".join(f"'{c}', `{c}`" for c in year_cols)
        df_long = df_raw.selectExpr(
            *[f"`{c}`" for c in meta_cols],
            f"stack({len(year_cols)}, {stack_expr}) as (annee_str, valeur_str)",
        )

        # ── Build indicateur column ──────────────────────────────────────────
        if not meta_cols:
            df_long = df_long.withColumn("indicateur", F.lit("Inconnu"))
        elif len(meta_cols) == 1:
            df_long = df_long.withColumnRenamed(meta_cols[0], "indicateur")
        else:
            from pyspark.sql.functions import concat_ws
            df_long = df_long.withColumn(
                "indicateur",
                concat_ws(" | ", *[F.trim(F.col(c).cast("string")) for c in meta_cols]),
            ).drop(*meta_cols)

        file_name = file_path.split("/")[-1]

        # ── Cast to canonical types and clean ────────────────────────────────
        df_final = (
            df_long
            .withColumn(
                "valeur",
                F.regexp_replace(
                    F.regexp_replace(F.col("valeur_str"), r"\s+", ""),
                    ",", ".",
                ).cast(DoubleType()),
            )
            .withColumn("annee",           F.col("annee_str").cast(IntegerType()))
            .withColumn("date_traitement", F.current_timestamp())
            .withColumn("fichier_source",  F.lit(file_name))
            .filter(F.col("indicateur").isNotNull() & F.col("annee").isNotNull())
            .select("indicateur", "valeur", "annee", "date_traitement", "fichier_source")
        )

        # ── Enforce output schema types (safe cast) ──────────────────────────
        for field in OUTPUT_SCHEMA:
            if field.name in df_final.columns:
                df_final = df_final.withColumn(
                    field.name, F.col(field.name).cast(field.dataType)
                )

        return df_final   # lazy — no .count() triggered here

    except Exception as exc:
        logger.error(f"Error processing {file_path}: {exc}")
        traceback.print_exc()
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Write helpers
# ──────────────────────────────────────────────────────────────────────────────
def write_by_year(df: DataFrame, output_base: str) -> None:
    """
    Write *df* partitioned by year using individual .write() calls per year.
    Spark decides the number of output files based on its parallelism.

    Note on reduce/union scalability: for Excel-based pipelines the file count
    rarely exceeds ~50, so the logical plan depth is acceptable. For 100+
    files, replace with spark.read.format(...).load(list_of_paths) instead.
    """
    years = [r["annee"] for r in df.select("annee").distinct().collect()]
    logger.info(f"  Years to write: {sorted(years)}")

    for year in years:
        out_path = f"{output_base.rstrip('/')}/{year}"
        logger.info(f"  📁 Writing year {year} → {out_path}")
        (
            df
            .filter(F.col("annee") == year)
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(out_path)
        )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> int:
    # SparkSession created here — NOT at module level
    spark = create_spark_session()

    try:
        base_path   = normalize_s3_path("s3a://01-raw")
        folder      = "INS/agregats"
        output_path = "s3a://02-transformed/INS/agregats"

        # ── 1. Discover files ────────────────────────────────────────────────
        files, _ = find_latest_files(spark, base_path, folder)
        if not files:
            logger.error("❌ No files found — aborting.")
            return 1
        logger.info(f"✅ {len(files)} file(s) to process.")

        # ── 2. Build lazy DataFrames ─────────────────────────────────────────
        dfs = [df for f in files if (df := process_excel_file(spark, f)) is not None]
        if not dfs:
            logger.error("❌ No valid DataFrames produced — aborting.")
            return 1

        # ── 3. Union (lazy — no action triggered yet) ────────────────────────
        logger.info("🔄 Building union plan…")
        final_df = reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs
        )

        # ── 4. Dedup ─────────────────────────────────────────────────────────
        final_df = final_df.dropDuplicates(["indicateur", "annee", "fichier_source"])

        # ── 5. Write per year (Spark chooses #files) ─────────────────────────
        logger.info("💾 Writing partitioned by year…")
        write_by_year(final_df, output_path)

        logger.info("✅ Job completed successfully.")
        return 0

    except Exception as exc:
        logger.critical(f"❌ FATAL ERROR: {exc}")
        traceback.print_exc()
        return 1

    finally:
        # Always stop — scoped to this function, not the module
        spark.stop()
        logger.info("Spark session stopped.")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sys.exit(main())
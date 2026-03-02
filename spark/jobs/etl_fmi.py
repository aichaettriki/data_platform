from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import logging
import os
from pyspark.sql.utils import AnalysisException
from common.spark_session import create_spark_session, stop_spark_session

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("FMI_TRANSFORM")

# =====================================================
# SPARK SESSION
# =====================================================
spark = create_spark_session("FMI-RAW-to-SILVER")
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# PATH CONFIG
# =====================================================
RAW_ROOT = "s3a://01-raw"
SILVER_BASE = "s3a://02-transformed/FMI"

# =====================================================
# FIND LATEST RAW FOLDER
# =====================================================
def find_latest_fmi_folder(spark, base_path):

    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    years = []

    for status in fs.listStatus(Path(base_path)):
        if status.isDirectory():
            years.append(status.getPath().getName())

    latest_year = sorted(years)[-1]

    months = []
    year_path = f"{base_path}/{latest_year}"

    for status in fs.listStatus(Path(year_path)):
        if status.isDirectory():
            months.append(status.getPath().getName())

    latest_month = sorted(months)[-1]

    fmi_path = f"{year_path}/{latest_month}/fmi"

    log.info(f"📅 Latest RAW folder detected : {fmi_path}")

    return fmi_path


# =====================================================
# FIND CSV FILES
# =====================================================
def find_csv_files(spark, folder_path):

    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(folder_path), sc._jsc.hadoopConfiguration())

    csv_files = []
    stack = [Path(folder_path)]

    while stack:

        current = stack.pop()

        try:
            for status in fs.listStatus(current):

                if status.isDirectory():
                    stack.append(status.getPath())

                elif status.isFile():

                    p = status.getPath().toString()

                    if p.endswith(".csv"):
                        csv_files.append(p)

        except Exception:
            pass

    return csv_files


# =====================================================
# GET DATASET TYPE
# =====================================================
def detect_dataset(file_name):

    name = file_name.lower()

    if "reer" in name:
        return "taux_change_effectif_reel"

    elif "neer" in name:
        return "taux_change_effectif_nominal"

    else:
        return "autre"


# =====================================================
# FIND LATEST RAW DATA
# =====================================================
log.info("🔍 Searching latest FMI folder")

FMI_FOLDER = find_latest_fmi_folder(spark, RAW_ROOT)

csv_files = find_csv_files(spark, FMI_FOLDER)

if len(csv_files) == 0:
    raise RuntimeError("❌ No FMI CSV files found")

log.info(f"📄 {len(csv_files)} CSV files found")


# =====================================================
# PROCESS FILES
# =====================================================
for file_path in csv_files:

    file_name = file_path.split("/")[-1]

    dataset = detect_dataset(file_name)

    SILVER_DATASET_PATH = f"{SILVER_BASE}/{dataset}"

    log.info("=" * 80)
    log.info(f"🚀 Processing file : {file_name}")
    log.info(f"📊 Dataset type : {dataset}")

    # --------------------------------------------------
    # READ CSV
    # --------------------------------------------------
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    log.info(f"📥 Rows read = {df.count()}")

    # --------------------------------------------------
    # TRANSFORMATION
    # --------------------------------------------------
    transformed_df = (
        df
        .withColumnRenamed("country_iso3", "pays")
        .withColumnRenamed("year", "annee")
        .withColumnRenamed("value", "valeur")
        .withColumnRenamed("indicator", "variable")
        .withColumn("date_chargement", current_timestamp())
    )

    log.info("🔧 After transformation")
    transformed_df.show(5, truncate=False)

    # --------------------------------------------------
    # WRITE BY YEAR
    # --------------------------------------------------
    years = [row["annee"] for row in transformed_df.select("annee").distinct().collect()]

    for y in years:

        df_year = transformed_df.filter(col("annee") == y)

        output_path = os.path.join(SILVER_DATASET_PATH, str(y))

        log.info(f"💾 Writing year {y} -> {output_path}")

        try:

            existing_df = spark.read.parquet(output_path)

            compare_cols = ["annee", "pays", "variable", "valeur"]

            new_rows = df_year.join(
                existing_df.select(compare_cols),
                on=compare_cols,
                how="left_anti"
            )

            new_count = new_rows.count()

            if new_count == 0:

                log.info(f"✅ No new rows for year {y}")
                continue

            final_df = existing_df.unionByName(new_rows)

        except AnalysisException:

            log.info(f"🆕 First dataset for year {y}")
            final_df = df_year

        (
            final_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

        log.info(f"💾 Data written for year {y}")


# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 FMI transformation completed")

stop_spark_session(spark)
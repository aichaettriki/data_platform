from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, expr
from pyspark.sql.functions import col, current_timestamp, lit, trim, when
import logging
import os

from common.spark_session import create_spark_session, stop_spark_session

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("IMF_TRANSFORM")

# =====================================================
# SPARK SESSION
# =====================================================
spark = create_spark_session("IMF-RAW-to-TRANSFORMED")
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# PATHS
# =====================================================
RAW_ROOT = "s3a://01-raw"
TRANSFORMED_BASE = "s3a://02-transformed/FMI"

SOURCE_FILE_PREFIX = "FMI"

# =====================================================
# HADOOP FS HELPERS
# =====================================================
def find_latest_file(spark, base_path):

    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    csv_files = []
    stack = [Path(base_path)]

    while stack:

        current = stack.pop()

        for status in fs.listStatus(current):

            if status.isDirectory():
                stack.append(status.getPath())

            elif status.isFile():

                p = status.getPath().toString()

                if p.endswith(".csv") and "/FMI/" in p:
                    csv_files.append(p)

    if len(csv_files) == 0:
        raise RuntimeError("No FMI CSV file found")

    latest = sorted(csv_files)[-1]

    log.info(f"Latest file found : {latest}")

    return latest

# =====================================================
# FIND SOURCE FILE
# =====================================================
log.info("Searching raw file")

RAW_FILE = find_latest_file(
    spark,
    RAW_ROOT
)

# =====================================================
# READ RAW FILE
# =====================================================
log.info("Reading raw dataset")

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", ";")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("inferSchema", "true")
    .load(RAW_FILE)
)

log.info(f"Rows = {df.count()}")
log.info(f"Columns = {len(df.columns)}")

df.show(5, False)

# =====================================================
# IDENTIFY TIME COLUMNS
# =====================================================

dimension_cols = [
    "COUNTRY.ID",
    "INDICATOR.ID",
    "FREQUENCY"
]

time_cols = [c for c in df.columns if c not in df.columns[:15]]

log.info(f"time columns = {len(time_cols)}")

# =====================================================
# UNPIVOT (WIDE -> LONG)
# =====================================================

stack_expr = "stack({},{}) as (period,value)".format(
    len(time_cols),
    ",".join([f"'{c}', `{c}`" for c in time_cols])
)

long_df = df.select(
    col("`COUNTRY.ID`").alias("country_id"),
    col("`INDICATOR.ID`").alias("indicator"),
    col("FREQUENCY").alias("freq"),
    expr(stack_expr)
)

long_df = long_df.filter(col("value").isNotNull())

# =====================================================
# PERIOD EXTRACTION
# =====================================================

long_df = (
    long_df
    .withColumn(
        "year",
        regexp_extract("period","(\\d{4})",1).cast("int")
    )
    .withColumn(
        "yearM",
        when(col("period").contains("-M"), col("period"))
    )
    .withColumn(
        "yearQ",
        when(col("period").contains("-Q"), col("period"))
    )
)

# =====================================================
# FINAL STRUCTURE
# =====================================================

final_df = (
    long_df
    .join(
        df.select(
            col("`COUNTRY.ID`").alias("country_id"),
            col("INDICATOR").alias("indicateur")
        ).distinct(),
        "country_id"
    )
    .select(
        col("country_id").alias("pays"),
        col("indicateur"),
        col("freq"),
        col("year").alias("annee"),
        col("yearM").alias("anneeM"),
        col("yearQ").alias("anneeQ"),
        col("value").alias("valeur"),
        current_timestamp().alias("date_chargement"),
        lit(1).alias("version_active")
    )
)

log.info("Final schema")

final_df.printSchema()

final_df.show(10,False)

# =====================================================
# WRITE BY YEAR
# =====================================================

years = [r["annee"] for r in final_df.select("annee").distinct().collect()]

for y in years:

    df_year = final_df.filter(col("annee") == y)

    path = os.path.join(TRANSFORMED_BASE, str(y))

    log.info(f"--> Writing year {y}")

    (
        df_year
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(path)
    )

log.info("Transformation finished")

stop_spark_session(spark)
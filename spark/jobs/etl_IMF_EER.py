from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, trim, when, regexp_extract, expr, row_number, desc, lpad
from pyspark.sql.types import DecimalType

from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StringType
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

# =====================================================
# FIND LATEST RAW FILE
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

    log.info(f"📂 Latest file found : {latest}")

    return latest


# =====================================================
# READ RAW FILE
# =====================================================

log.info("🔎 Searching raw file")
RAW_FILE = find_latest_file(spark, RAW_ROOT)

log.info("📥 Reading raw dataset")

df = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("inferSchema", "false")
    .csv(RAW_FILE)
)

log.info(f"📊 Rows : {df.count()}")
log.info(f"📊 Columns : {len(df.columns)}")
df.show(5, False)
# =====================================================
# IDENTIFY TIME COLUMNS
# =====================================================

dimension_cols = [
    "DATASET","SERIES_CODE","OBS_MEASURE",
    "COUNTRY.ID","COUNTRY",
    "INDICATOR.ID","INDICATOR",
    "FREQUENCY.ID","FREQUENCY",
    "SCALE.ID","SCALE",
    "EXRATE.ID","EXRATE",
    "REF_YEAR.ID","REF_YEAR"
]

time_cols = [c for c in df.columns if c not in dimension_cols]

# =====================================================
# UNPIVOT
# =====================================================

stack_expr = "stack({},{}) as (period,value)".format(
    len(time_cols),
    ",".join([f"'{c}', `{c}`" for c in time_cols])
)

long_df = df.select(
    col("`COUNTRY.ID`").alias("pays"),
    col("INDICATOR").alias("variable"),
    col("`INDICATOR.ID`").alias("variable_id"),
    col("FREQUENCY").alias("freq"),
    expr(stack_expr)
# ).filter(col("value").isNotNull())
).filter(col("value").cast("double").isNotNull())
# =====================================================
# PERIOD EXTRACTION
# =====================================================
from pyspark.sql.functions import col, concat, lpad, floor

long_df = long_df.withColumn(
    "periode",
    when(
        col("period").rlike(r"\d{4}-M\d{1,2}"),
        concat(
            regexp_extract("period", r"(\d{4})-M(\d{1,2})", 1),  # année
            lpad(
                (floor((regexp_extract("period", r"(\d{4})-M(\d{1,2})", 2).cast("int") - 1)/3) + 1)
                .cast("int").cast("string"),
                2,
                "0"
            ),  # trimestre
            lpad(
                regexp_extract("period", r"(\d{4})-M(\d{1,2})", 2),  # mois réel
                2,
                "0"
            )
        )
    )
    .when(
        col("period").rlike(r"\d{4}-Q\d"),
        concat(
            regexp_extract("period", r"(\d{4})-Q(\d)", 1),  # année
            lpad(regexp_extract("period", r"(\d{4})-Q(\d)", 2), 2, "0")  # trimestre
        )
    )
    .otherwise(
        regexp_extract("period", r"(\d{4})", 1)  # année
    )
)


log.info("🔎 Sample period transformations")

samples = (
    long_df
    .select("period","periode")
    .distinct()
    .limit(10)
    .collect()
)

for r in samples:
    log.info(f"   {r['period']}  →  {r['periode']}")
# =====================================================
# FINAL STRUCTURE
# =====================================================

final_df = long_df.select(
    "pays","variable","variable_id","freq",
    "periode",
    # col("value").cast("double").alias("valeur"),

    col("value").cast(DecimalType(20,15)).alias("valeur"),
    lit(None).cast(StringType()).alias("base"),
    lit(None).cast(StringType()).alias("version"),
    lit(None).cast(StringType()).alias("code_secteur"),
    lit(None).cast(StringType()).alias("lib_secteur"),
    lit(None).cast(StringType()).alias("dim_id"),
    lit(None).cast(StringType()).alias("dim_key"),
    lit("FMI/EER").alias("source"),
    current_timestamp().alias("date_chargement"),
    lit(1).cast("int").alias("version_active")
)
final_df = final_df.withColumn(
    "year_partition",
    col("periode").substr(1,4)
)

# =====================================================
# DELTA KEYS
# =====================================================

BUSINESS_KEYS = ["pays","variable_id","freq","periode"]

years = sorted([r["year_partition"] for r in final_df.select("year_partition").distinct().collect()])
# =====================================================
# PROCESS YEARS
# =====================================================

for y in years:

    df_year = final_df.filter(col("year_partition") == y).cache()

    rows_new_file = df_year.count()

    output_path = os.path.join(TRANSFORMED_BASE, str(y))
    

    log.info("")
    log.info("===================================================")
    log.info(f"📅 --> Processing year {y}")
    log.info(f"📥 Rows in NEW dataset : {rows_new_file}")

    is_first_load = False

    try:

        existing_df = spark.read.parquet(output_path).cache()

        existing_count = existing_df.count()

        log.info(f"📂 Existing dataset rows : {existing_count}")

        active_count = existing_df.filter(col("version_active")==1).count()

        log.info(f"📂 Existing ACTIVE rows : {active_count}")

    except AnalysisException:

        is_first_load = True

    # =====================================================
    # FIRST LOAD
    # =====================================================

    if is_first_load:

        log.info(f"🆕 No existing file for year {y}, writing full dataset")

        final_merged = df_year

    # =====================================================
    # INCREMENTAL
    # =====================================================

    else:

        existing_active = existing_df.filter(col("version_active")==1)

        joined = df_year.alias("new").join(
            existing_active.alias("old"),
            on=BUSINESS_KEYS,
            how="left"
        )

        new_rows = joined.filter(col("old.pays").isNull()).select("new.*")

        modified_rows = joined.filter(
            col("old.valeur").isNotNull() &
            (col("new.valeur") != col("old.valeur"))
        ).select("new.*")

        new_count = new_rows.count()
        mod_count = modified_rows.count()

        log.info(f"🆕 NEW rows detected : {new_count}")
        log.info(f"✏️  MODIFIED rows detected : {mod_count}")

        # samples new
        if new_count > 0:

            log.info("   Sample NEW rows:")

            for r in new_rows.limit(5).collect():

                log.info(
                    f"   🆕 {r['pays']} | {r['variable_id']} | {r['freq']} | {r['periode']} | {r['valeur']}"
                )

        # samples modified
        if mod_count > 0:

            log.info("   Sample MODIFIED rows:")

            samples = joined.filter(
                col("old.valeur").isNotNull() &
                (col("new.valeur") != col("old.valeur"))
            ).select(
                col("new.pays").alias("pays"),
                col("new.variable_id").alias("variable_id"),
                col("new.freq").alias("freq"),
                col("new.periode").alias("periode"),
                col("old.valeur").alias("old_val"),
                col("new.valeur").alias("new_val")
            ).limit(5).collect()

            for r in samples:

                log.info(
                    f"   ✏️ {r['pays']} | {r['variable_id']} | {r['freq']} | {r['periode']} | {r['old_val']} → {r['new_val']}"
                )

        changed_rows = new_rows.union(modified_rows).cache()

        changed_count = changed_rows.count()

        log.info(f"🔄 TOTAL changed rows : {changed_count}")

        if changed_count == 0:

            log.info(f"✅ No changes detected for year {y}")
            continue

        changed_keys = changed_rows.select(BUSINESS_KEYS).distinct()

        existing_untouched = existing_df.join(
            changed_keys,
            on=BUSINESS_KEYS,
            how="left_anti"
        )

        existing_to_deactivate = existing_df.join(
            changed_keys,
            on=BUSINESS_KEYS,
            how="inner"
        ).withColumn("version_active", lit(0))

        deactivate_count = existing_to_deactivate.count()

        log.info(f"🛑 Rows to deactivate : {deactivate_count}")

        final_merged = (
            existing_untouched
            .unionByName(existing_to_deactivate)
            .unionByName(changed_rows)
        )

    # =====================================================
    # RECALCULATE ACTIVE VERSION
    # =====================================================

    window_spec = Window.partitionBy(*BUSINESS_KEYS).orderBy(desc("date_chargement"))

    final_merged = (
        final_merged
        .withColumn("_rank", row_number().over(window_spec))
        .withColumn("version_active",(col("_rank")==1).cast("int"))
        .drop("_rank")
    )

    total_rows = final_merged.count()

    (
        final_merged
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    log.info(f"💾 Year {y} written — total rows: {total_rows}")

log.info("")
log.info("✅ Transformation finished")

stop_spark_session(spark)

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import regexp_extract, expr
# from pyspark.sql.functions import col, current_timestamp, lit, trim, when, regexp_extract, expr
# import logging
# import os

# from common.spark_session import create_spark_session, stop_spark_session

# # =====================================================
# # LOGGING
# # =====================================================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )
# log = logging.getLogger("IMF_TRANSFORM")

# # =====================================================
# # SPARK SESSION
# # =====================================================
# spark = create_spark_session("IMF-RAW-to-TRANSFORMED")
# spark.sparkContext.setLogLevel("WARN")

# # =====================================================
# # PATHS
# # =====================================================
# RAW_ROOT = "s3a://01-raw"
# TRANSFORMED_BASE = "s3a://02-transformed/FMI"

# SOURCE_FILE_PREFIX = "FMI"

# # =====================================================
# # HADOOP FS HELPERS
# # =====================================================
# def find_latest_file(spark, base_path):

#     sc = spark.sparkContext
#     Path = sc._jvm.org.apache.hadoop.fs.Path
#     FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#     URI = sc._jvm.java.net.URI

#     fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

#     csv_files = []
#     stack = [Path(base_path)]

#     while stack:

#         current = stack.pop()

#         for status in fs.listStatus(current):

#             if status.isDirectory():
#                 stack.append(status.getPath())

#             elif status.isFile():

#                 p = status.getPath().toString()

#                 if p.endswith(".csv") and "/FMI/" in p:
#                     csv_files.append(p)

#     if len(csv_files) == 0:
#         raise RuntimeError("No FMI CSV file found")

#     latest = sorted(csv_files)[-1]

#     log.info(f"Latest file found : {latest}")

#     return latest

# # =====================================================
# # FIND SOURCE FILE
# # =====================================================
# log.info("Searching raw file")

# RAW_FILE = find_latest_file(
#     spark,
#     RAW_ROOT
# )

# # =====================================================
# # READ RAW FILE
# # =====================================================
# log.info("Reading raw dataset")

# df = (
#     spark.read
#     .format("csv")
#     .option("header", "true")
#     .option("sep", ";")
#     .option("quote", '"')
#     .option("escape", '"')
#     .option("multiLine", "true")
#     .option("inferSchema", "true")
#     .load(RAW_FILE)
# )

# log.info(f"Rows = {df.count()}")
# log.info(f"Columns = {len(df.columns)}")

# df.show(5, False)

# # =====================================================
# # IDENTIFY TIME COLUMNS
# # =====================================================

# # Colonnes fixes du CSV
# dimension_cols = [
#     "DATASET", "SERIES_CODE", "OBS_MEASURE",
#     "COUNTRY.ID", "COUNTRY",
#     "INDICATOR.ID", "INDICATOR",
#     "FREQUENCY.ID", "FREQUENCY",
#     "SCALE.ID", "SCALE",
#     "EXRATE.ID", "EXRATE",
#     "REF_YEAR.ID", "REF_YEAR"
# ]

# # Colonnes temporelles = tout le reste
# time_cols = [c for c in df.columns if c not in dimension_cols]

# log.info(f"time columns = {len(time_cols)}")

# # =====================================================
# # UNPIVOT CORRIGÉ
# # =====================================================

# stack_expr = "stack({},{}) as (period, value)".format(
#     len(time_cols),
#     ",".join([f"'{c}', `{c}`" for c in time_cols])
# )

# long_df = df.select(
#     col("`COUNTRY.ID`").alias("pays"),
#     col("INDICATOR").alias("indicateur"),   # ✅ libellé complet
#     col("`INDICATOR.ID`").alias("indicator_id"),  # ✅ code court
#     col("FREQUENCY").alias("freq"),
#     expr(stack_expr)
# ).filter(col("value").isNotNull())

# # =====================================================
# # PERIOD EXTRACTION
# # =====================================================

# long_df = (
#     long_df
#     .withColumn(
#         "annee",
#         regexp_extract("period", "(\\d{4})", 1).cast("int")
#     )
#     .withColumn(
#         "anneeM",
#         when(col("period").contains("-M"), col("period")).otherwise(None)
#     )
#     .withColumn(
#         "anneeQ",
#         when(col("period").contains("-Q"), col("period")).otherwise(None)
#     )
# )

# # =====================================================
# # STRUCTURE FINALE — PLUS DE JOIN !
# # =====================================================

# final_df = long_df.select(
#     col("pays"),
#     col("indicateur"),
#     col("indicator_id"),
#     col("freq"),
#     col("annee"),
#     col("anneeM"),
#     col("anneeQ"),
#     col("value").cast("double").alias("valeur"),
#     current_timestamp().alias("date_chargement"),
#     lit(1).alias("version_active")
# )



# log.info("Final schema")

# final_df.printSchema()

# final_df.show(10,False)

# # =====================================================
# # WRITE BY YEAR
# # =====================================================

# years = [r["annee"] for r in final_df.select("annee").distinct().collect()]

# for y in years:

#     df_year = final_df.filter(col("annee") == y)

#     path = os.path.join(TRANSFORMED_BASE, str(y))

#     log.info(f"--> Writing year {y}")

#     (
#         df_year
#         .coalesce(1)
#         .write
#         .mode("overwrite")
#         .parquet(path)
#     )

# log.info("Transformation finished")

# stop_spark_session(spark)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, trim, when
from pyspark.sql.utils import AnalysisException
from common.spark_session import create_spark_session, stop_spark_session
import logging
from pyspark.sql.types import StringType

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("WORLD_BANK_COUNTRIES_ETL")

# =====================================================
# SPARK SESSION
# =====================================================
spark = create_spark_session("WORLD_BANK-RAW-to-TRANSFORMED-Countries")
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# PATHS
# =====================================================
RAW_BASE       = "s3a://01-raw"
TRANSFORMED_PATH = "s3a://02-transformed/WORLD_BANK/Countries/Axe_Referentiel"

# =====================================================
# FIND LATEST CSV  (dossier YYYY/MM le plus récent)
# =====================================================
def find_latest_countries_file(spark, base_path, filename="worldbank_countries.csv"):
    """
    Parcourt s3a://01-raw/YYYY/MM/WORLD_BANK/Countries/
    et retourne le chemin du CSV le plus récent.
    """
    sc         = spark.sparkContext
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI        = sc._jvm.java.net.URI

    fs         = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())
    candidates = []
    stack      = [Path(base_path)]

    while stack:
        current = stack.pop()
        try:
            for status in fs.listStatus(current):
                p = status.getPath().toString()
                if status.isDirectory():
                    stack.append(status.getPath())
                elif status.isFile() and p.endswith(filename):
                    # Extraire YYYY et MM depuis le chemin
                    # Format : s3a://01-raw/YYYY/MM/WORLD_BANK/Countries/worldbank_countries.csv
                    parts = p.replace("s3a://", "").split("/")
                    if len(parts) >= 3:
                        try:
                            year  = int(parts[1])
                            month = int(parts[2])
                            candidates.append((year, month, p))
                            log.info(f"🔎 fichier trouvé : {p}")
                        except ValueError:
                            continue
        except Exception:
            pass

    if not candidates:
        raise RuntimeError(
            f"❌ Aucun fichier '{filename}' trouvé sous {base_path}/YYYY/MM/WORLD_BANK/Countries/"
        )

    latest = max(candidates)  # (year, month, path) → max = plus récent
    log.info(f"✅ Fichier le plus récent sélectionné : {latest[2]}")
    return latest[2]


# =====================================================
# FIND FILE
# =====================================================
log.info("🔍 Recherche du fichier countries le plus récent dans le RAW...")
csv_path = find_latest_countries_file(spark, RAW_BASE)

# =====================================================
# READ CSV
# =====================================================
log.info(f"📥 Lecture du CSV : {csv_path}")
raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("nullValue", "NULL")
    .csv(csv_path)
)

raw_count = raw_df.count()
log.info(f"📊 Lignes brutes lues : {raw_count}")
log.info("📄 Schéma brut :")
raw_df.printSchema()
raw_df.show(5, truncate=False)

# =====================================================
# TRANSFORM
# =====================================================
log.info("🔄 Application des transformations...")

transformed_df = (
    raw_df

    # Nettoyage
    .withColumn("id", trim(col("id")))
    .withColumn("iso2_code", trim(col("iso2_code")))
    .withColumn("name", trim(col("name")))
    .withColumn("region_id", trim(col("region_id")))
    .withColumn("region_iso2", trim(col("region_iso2")))
    .withColumn("region_name", trim(col("region_name")))

    # Colonnes techniques
    .withColumn("date_chargement", current_timestamp())
    .withColumn("source", lit("WORLD_BANK_Countries"))
    .withColumn("base", lit(None).cast(StringType()))
    .withColumn("version", lit(None).cast(StringType()))
    .withColumn("rang", lit(None).cast(StringType()))
    .withColumn("code_secteur", lit(None).cast(StringType()))
    .withColumn("lib_secteur", lit(None).cast(StringType()))
    .withColumn("dim_id", lit(None).cast(StringType()))
    .withColumn("dim_key", lit(None).cast(StringType()))
    .withColumn("variable", lit(None).cast(StringType()))
    .withColumn("annee", lit(None).cast(StringType()))
    # Sélection finale
    .select(
        col("id"),
        col("iso2_code"),
        col("name").alias("pays"),
        col("region_id"),
        col("region_iso2"),
        col("region_name"),
        col("base"),
        col("version"),
        col("rang"),
        col("code_secteur"),
        col("lib_secteur"),
        col("dim_id"),
        col("dim_key"),
        col("source"),
        col("date_chargement"),
        col("variable"),
        col("annee")
    )
)


total_transformed = transformed_df.count()

log.info("📄 Aperçu du résultat transformé :")
transformed_df.show(10, truncate=False)

# =====================================================
# DATA QUALITY CHECK
# =====================================================
log.info("🔍 Contrôle qualité...")

missing_iso3 = transformed_df.filter(col("id").isNull()).count()
missing_name = transformed_df.filter(col("pays").isNull()).count()

if missing_iso3 > 0:
    raise RuntimeError(f"❌ DATA QUALITY : {missing_iso3} lignes avec id NULL")
if missing_name > 0:
    raise RuntimeError(f"❌ DATA QUALITY : {missing_name} lignes avec country_name / pays NULL")

log.info("✅ Contrôle qualité OK — aucune valeur critique manquante")

# =====================================================
# WRITE PARQUET  (fichier unique, pas de partition year)
# =====================================================
log.info(f"💾 Écriture Parquet vers : {TRANSFORMED_PATH}")

try:
    existing_df = spark.read.parquet(TRANSFORMED_PATH).cache()
    existing_count = existing_df.count()
    log.info(f"📂 Fichier existant trouvé ({existing_count} lignes) — détection des nouveautés...")

    compare_cols = [
        "id",
        "iso2_code",
        "pays",
        "region_id",
        "region_iso2",
        "region_name"
    ]

    new_rows = transformed_df.join(
        existing_df.select(compare_cols),
        on=compare_cols,
        how="left_anti"
    )

    new_count = new_rows.count()

    if new_count == 0:
        log.info("✅ Aucune nouvelle ligne détectée — fichier Parquet inchangé")
        existing_df.unpersist()
    else:
        log.info(f"🆕 {new_count} nouvelles lignes détectées — mise à jour du Parquet")
        final_df = existing_df.unionByName(new_rows)
        existing_df.unpersist()

        (
            final_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(TRANSFORMED_PATH)
        )
        log.info(f"✅ Parquet mis à jour ({final_df.count()} lignes au total)")

except AnalysisException:
    log.info("🆕 Aucun fichier existant — première écriture complète")

    (
        transformed_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(TRANSFORMED_PATH)
    )
    log.info(f"✅ Parquet écrit ({total_transformed} lignes)")

# =====================================================
# SCHEMA FINAL
# =====================================================
log.info("📊 Schéma final du Parquet :")
for field in transformed_df.schema.fields:
    log.info(
        f"  Column: {field.name:<25} | "
        f"Type: {field.dataType.simpleString():<12} | "
        f"Nullable: {field.nullable}"
    )

# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 Job ETL World Bank Countries terminé avec succès")
stop_spark_session(spark)


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, current_timestamp, lit, trim, when
# from pyspark.sql.utils import AnalysisException
# from common.spark_session import create_spark_session, stop_spark_session
# import logging
# from pyspark.sql.types import StringType

# # =====================================================
# # LOGGING
# # =====================================================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )
# log = logging.getLogger("WORLD_BANK_COUNTRIES_ETL")

# # =====================================================
# # SPARK SESSION
# # =====================================================
# spark = create_spark_session("WORLD_BANK-RAW-to-TRANSFORMED-Countries")
# spark.sparkContext.setLogLevel("WARN")

# # =====================================================
# # PATHS
# # =====================================================
# RAW_BASE       = "s3a://01-raw"
# TRANSFORMED_PATH = "s3a://02-transformed/WORLD_BANK/Countries/Axe_Referentiel"

# # =====================================================
# # FIND LATEST CSV  (dossier YYYY/MM le plus récent)
# # =====================================================
# def find_latest_countries_file(spark, base_path, filename="worldbank_countries.csv"):
#     """
#     Parcourt s3a://01-raw/YYYY/MM/WORLD_BANK/Countries/
#     et retourne le chemin du CSV le plus récent.
#     """
#     sc         = spark.sparkContext
#     Path       = sc._jvm.org.apache.hadoop.fs.Path
#     FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#     URI        = sc._jvm.java.net.URI

#     fs         = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())
#     candidates = []
#     stack      = [Path(base_path)]

#     while stack:
#         current = stack.pop()
#         try:
#             for status in fs.listStatus(current):
#                 p = status.getPath().toString()
#                 if status.isDirectory():
#                     stack.append(status.getPath())
#                 elif status.isFile() and p.endswith(filename):
#                     # Extraire YYYY et MM depuis le chemin
#                     # Format : s3a://01-raw/YYYY/MM/WORLD_BANK/Countries/worldbank_countries.csv
#                     parts = p.replace("s3a://", "").split("/")
#                     if len(parts) >= 3:
#                         try:
#                             year  = int(parts[1])
#                             month = int(parts[2])
#                             candidates.append((year, month, p))
#                             log.info(f"🔎 Candidat trouvé : {p}")
#                         except ValueError:
#                             continue
#         except Exception:
#             pass

#     if not candidates:
#         raise RuntimeError(
#             f"❌ Aucun fichier '{filename}' trouvé sous {base_path}/YYYY/MM/WORLD_BANK/Countries/"
#         )

#     latest = max(candidates)  # (year, month, path) → max = plus récent
#     log.info(f"✅ Fichier le plus récent sélectionné : {latest[2]}")
#     return latest[2]


# # =====================================================
# # FIND FILE
# # =====================================================
# log.info("🔍 Recherche du fichier countries le plus récent dans le RAW...")
# csv_path = find_latest_countries_file(spark, RAW_BASE)

# # =====================================================
# # READ CSV
# # =====================================================
# log.info(f"📥 Lecture du CSV : {csv_path}")
# raw_df = (
#     spark.read
#     .option("header", True)
#     .option("inferSchema", True)
#     .option("nullValue", "NULL")
#     .csv(csv_path)
# )

# raw_count = raw_df.count()
# log.info(f"📊 Lignes brutes lues : {raw_count}")
# log.info("📄 Schéma brut :")
# raw_df.printSchema()
# raw_df.show(5, truncate=False)

# # =====================================================
# # TRANSFORM
# # =====================================================
# log.info("🔄 Application des transformations...")

# transformed_df = (
#     raw_df

#     # Nettoyage
#     .withColumn("id", trim(col("id")))
#     .withColumn("iso2_code", trim(col("iso2_code")))
#     .withColumn("name", trim(col("name")))

#     # Colonnes techniques
#     .withColumn("date_chargement", current_timestamp())
#     .withColumn("source", lit("WORLD_BANK_Countries"))
#     .withColumn("base", lit(None).cast(StringType()))
#     .withColumn("version", lit(None).cast(StringType()))
#     .withColumn("rang", lit(None).cast(StringType()))
#     .withColumn("code_secteur", lit(None).cast(StringType()))
#     .withColumn("lib_secteur", lit(None).cast(StringType()))
#     .withColumn("dim_id", lit(None).cast(StringType()))
#     .withColumn("dim_key", lit(None).cast(StringType()))
#     .withColumn("variable", lit(None).cast(StringType()))
#     .withColumn("annee", lit(None).cast(StringType()))
#     # Sélection finale
#     .select(
#         col("id").alias("country_iso3"),
#         col("name").alias("pays"),
#         col("base"),
#         col("version"),
#         col("rang"),
#         col("code_secteur"),
#         col("lib_secteur"),
#         col("dim_id"),
#         col("dim_key"),
#         col("source"),
#         col("date_chargement"),
#         col("variable"),
#         col("annee")
#     )
# )


# total_transformed = transformed_df.count()

# log.info("📄 Aperçu du résultat transformé :")
# transformed_df.show(10, truncate=False)

# # =====================================================
# # DATA QUALITY CHECK
# # =====================================================
# log.info("🔍 Contrôle qualité...")

# missing_iso3 = transformed_df.filter(col("country_iso3").isNull()).count()
# missing_name = transformed_df.filter(col("pays").isNull()).count()

# if missing_iso3 > 0:
#     raise RuntimeError(f"❌ DATA QUALITY : {missing_iso3} lignes avec country_iso3 NULL")
# if missing_name > 0:
#     raise RuntimeError(f"❌ DATA QUALITY : {missing_name} lignes avec country_name / pays NULL")

# log.info("✅ Contrôle qualité OK — aucune valeur critique manquante")

# # =====================================================
# # WRITE PARQUET  (fichier unique, pas de partition year)
# # =====================================================
# log.info(f"💾 Écriture Parquet vers : {TRANSFORMED_PATH}")

# try:
#     existing_df = spark.read.parquet(TRANSFORMED_PATH).cache()
#     existing_count = existing_df.count()
#     log.info(f"📂 Fichier existant trouvé ({existing_count} lignes) — détection des nouveautés...")

#     compare_cols = [
#         "country_iso3",
#         "pays"
#     ]

#     new_rows = transformed_df.join(
#         existing_df.select(compare_cols),
#         on=compare_cols,
#         how="left_anti"
#     )

#     new_count = new_rows.count()

#     if new_count == 0:
#         log.info("✅ Aucune nouvelle ligne détectée — fichier Parquet inchangé")
#         existing_df.unpersist()
#     else:
#         log.info(f"🆕 {new_count} nouvelles lignes détectées — mise à jour du Parquet")
#         final_df = existing_df.unionByName(new_rows)
#         existing_df.unpersist()

#         (
#             final_df
#             .coalesce(1)
#             .write
#             .mode("overwrite")
#             .parquet(TRANSFORMED_PATH)
#         )
#         log.info(f"✅ Parquet mis à jour ({final_df.count()} lignes au total)")

# except AnalysisException:
#     log.info("🆕 Aucun fichier existant — première écriture complète")

#     (
#         transformed_df
#         .coalesce(1)
#         .write
#         .mode("overwrite")
#         .parquet(TRANSFORMED_PATH)
#     )
#     log.info(f"✅ Parquet écrit ({total_transformed} lignes)")

# # =====================================================
# # SCHEMA FINAL
# # =====================================================
# log.info("📊 Schéma final du Parquet :")
# for field in transformed_df.schema.fields:
#     log.info(
#         f"  Column: {field.name:<25} | "
#         f"Type: {field.dataType.simpleString():<12} | "
#         f"Nullable: {field.nullable}"
#     )

# # =====================================================
# # STOP SPARK
# # =====================================================
# log.info("🎉 Job ETL World Bank Countries terminé avec succès")
# stop_spark_session(spark)
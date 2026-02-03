from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, current_timestamp
from functools import reduce
import sys
import traceback
import time

# =========================================================
# Spark Session
# =========================================================
spark = (
    SparkSession.builder
    .appName("transform_aggregats_ins")
    .getOrCreate()
)

sc = spark.sparkContext

print("=" * 70)
print("🚀 JOB SPARK - TRANSFORMATION AGRÉGATS INS")
print(f"✅ Spark version : {spark.version}")
print("=" * 70)

# =========================================================
# Utils Hadoop FS (S3A / MinIO)
# =========================================================
Path = sc._jvm.org.apache.hadoop.fs.Path
FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
URI = sc._jvm.java.net.URI

fs = FileSystem.get(
    URI("s3a://02-transformed"),
    sc._jsc.hadoopConfiguration()
)

# =========================================================
# Recherche des fichiers Excel récents
# =========================================================
def find_latest_files(spark, base_path, specific_folder):
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    if not base_path.startswith("s3a://"):
        raise ValueError("base_path must start with s3a://")

    parts = base_path.replace("s3a://", "").split("/", 1)
    bucket = parts[0]
    prefix = "/" + parts[1] if len(parts) > 1 else "/"

    fs_local = FileSystem.get(
        URI(f"s3a://{bucket}"),
        sc._jsc.hadoopConfiguration()
    )

    def list_dirs(path_str):
        p = Path(path_str)
        if not fs_local.exists(p):
            return []
        return sorted(
            [x.getPath() for x in fs_local.listStatus(p) if x.isDirectory()],
            key=lambda x: x.getName(),
            reverse=True
        )

    # Année la plus récente
    years = list_dirs(prefix)
    if not years:
        return []

    # Mois le plus récent
    months = list_dirs(years[0].toString())
    if not months:
        return []

    target = Path(months[0].toString() + "/" + specific_folder)
    if not fs_local.exists(target):
        return []

    # Recherche récursive des fichiers Excel
    files = []
    stack = [target]

    while stack:
        p = stack.pop()
        for s in fs_local.listStatus(p):
            if s.isDirectory():
                stack.append(s.getPath())
            elif s.isFile() and s.getPath().getName().endswith(".xlsx"):
                files.append(s.getPath().toString())

    return files

# =========================================================
# MAIN
# =========================================================
try:
    base_path = "s3a://01-raw"
    specific_folder = "INS/agregats"

    files = find_latest_files(spark, base_path, specific_folder)

    if not files:
        print("❌ Aucun fichier Excel trouvé")
        sys.exit(1)

    print(f"✅ {len(files)} fichier(s) trouvé(s)")

    dfs = []

    for file_path in files:
        try:
            print(f"📖 Lecture : {file_path}")

            df = (
                spark.read
                .format("com.crealytics.spark.excel")
                .option("header", True)
                .option("inferSchema", True)
                .load(file_path)
            )

            colonnes_annees = [c for c in df.columns if c.isdigit()]
            colonnes_meta = [c for c in df.columns if c not in colonnes_annees]

            if not colonnes_annees:
                print(f"⚠️ Ignoré (pas de colonnes années) : {file_path}")
                continue

            stack_expr = ", ".join([f"'{c}', `{c}`" for c in colonnes_annees])

            df_long = df.selectExpr(
                *[f"`{c}`" for c in colonnes_meta],
                f"stack({len(colonnes_annees)}, {stack_expr}) as (annee, valeur)"
            )

            # Construction indicateur
            if len(colonnes_meta) == 1:
                df_long = df_long.withColumnRenamed(colonnes_meta[0], "indicateur")
            else:
                df_long = df_long.withColumn("indicateur", col(colonnes_meta[0]))
                for c in colonnes_meta[1:]:
                    df_long = df_long.withColumn(
                        "indicateur",
                        col("indicateur") + lit(" | ") + col(c)
                    )
                df_long = df_long.drop(*colonnes_meta)

            df_long = (
                df_long
                .withColumn(
                    "valeur",
                    regexp_replace(
                        regexp_replace(col("valeur").cast("string"), " ", ""),
                        ",",
                        "."
                    ).cast("double")
                )
                .withColumn("annee", col("annee").cast("int"))
                .withColumn("date_traitement", current_timestamp())
                .select("indicateur", "valeur", "annee", "date_traitement")
            )

            dfs.append(df_long)

        except Exception as e:
            print(f"⚠️ Erreur fichier : {file_path}")
            print(e)

    if not dfs:
        print("❌ Aucun dataframe valide")
        sys.exit(1)

    # =====================================================
    # Union finale
    # =====================================================
    final_df = reduce(lambda d1, d2: d1.unionByName(d2), dfs)

    # =====================================================
    # Écriture PARQUET FINAL (1 FICHIER PAR ANNÉE DANS SON DOSSIER)
    # =====================================================
    output_base = "s3a://02-transformed/INS/agregats"
    tmp_global = f"{output_base}/_tmp"

    annees = [r.annee for r in final_df.select("annee").distinct().collect()]

    for annee in annees:
        print(f"🧱 Traitement année {annee}")

        df_annee = final_df.filter(col("annee") == annee).coalesce(1)

        # Dossier spécifique par année
        year_folder = f"{output_base}/{annee}"
        final_file = f"{year_folder}/PAE_TRANSFORMED_{annee}.parquet"

        # 1️⃣ écriture temporaire dans le dossier global
        tmp_path = f"{tmp_global}/{annee}"
        df_annee.write.mode("overwrite").parquet(tmp_path)

        # 2️⃣ renommage du part-xxxxx vers le dossier de l'année
        tmp_dir_path = Path(tmp_path)
        for f in fs.listStatus(tmp_dir_path):
            name = f.getPath().getName()
            if name.startswith("part-") and name.endswith(".parquet"):
                # Créer le dossier année si nécessaire
                year_path = Path(year_folder)
                if not fs.exists(year_path):
                    fs.mkdirs(year_path)
                fs.rename(
                    f.getPath(),
                    Path(final_file)
                )

    # Petite pause pour s'assurer que tout est libéré
    time.sleep(2)

    # =====================================================
    # Nettoyage final du dossier temporaire global
    # =====================================================
    tmp_path_global = Path(tmp_global)
    if fs.exists(tmp_path_global):
        print(f"🧹 Suppression du dossier temporaire global : {tmp_global}")
        fs.delete(tmp_path_global, True)  # supprime le dossier et tous les sous-dossiers

    spark.stop()
    sys.exit(0)

# =========================================================
# Gestion erreur fatale
# =========================================================
except Exception as e:
    print("❌ ERREUR FATALE")
    print(e)
    traceback.print_exc()
    spark.stop()
    sys.exit(1)

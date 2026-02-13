import sys
import logging
import traceback
from typing import List, Optional, Tuple
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, regexp_replace, current_timestamp, 
    trim, input_file_name
)

# =========================================================
# Configuration du logging
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================================================
# Configuration Spark
# =========================================================
# NOTE: En prod, évitez de hardcoder les configs mémoire/shuffle ici.
# Passez-les via spark-submit (--conf spark.sql.shuffle.partitions=200)
spark = (
    SparkSession.builder
    .appName("transform_aggregats_ins")
    # On garde l'adaptive execution qui est cruciale
    .config("spark.sql.adaptive.enabled", "true") 
    .getOrCreate()
)

# =========================================================
# Fonctions utilitaires
# =========================================================

def normalize_s3_path(path: str) -> str:
    if path.startswith("s3a://") and not path.endswith("/"):
        return path + "/"
    return path


def find_latest_files(spark: SparkSession, base_path: str, specific_folder: str) -> Tuple[List[str], Optional[str]]:
    """
    Recherche les fichiers Excel dans le dossier le plus récent via Hadoop FS API.
    """
    sc = spark.sparkContext
    # Utilisation de l'URI Java pour gérer correctement les schémas s3a/hdfs/file
    try:
        Path = sc._jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        URI = sc._jvm.java.net.URI
        
        # Gestion propre de l'URI
        uri_path = URI(base_path)
        fs = FileSystem.get(uri_path, sc._jsc.hadoopConfiguration())
        
        # Construction du chemin de base sans le préfixe s3a://bucket pour la navigation relative si besoin
        # Mais ici on travaille avec des Path objects complets, c'est plus sûr.
        base_p = Path(base_path)

        def list_dirs_sorted(p_obj):
            if not fs.exists(p_obj): return []
            status_list = fs.listStatus(p_obj)
            dirs = [s.getPath() for s in status_list if s.isDirectory()]
            # Tri décroissant (lexicographique)
            return sorted(dirs, key=lambda x: int(x.getName()), reverse=True)


        # 1. Lister les années (ex: 2024, 2023...)
        years_dirs = list_dirs_sorted(base_p)
        if not years_dirs:
            logger.warning(f"Aucune année trouvée dans {base_path}")
            return [], None
        
        latest_year_path = years_dirs[0]
        annee_dossier = latest_year_path.getName()
        logger.info(f"📅 Année la plus récente détectée: {annee_dossier}")

        # 2. Lister les mois dans l'année la plus récente
        months_dirs = list_dirs_sorted(latest_year_path)
        if not months_dirs:
            logger.warning(f"Aucun mois trouvé dans {latest_year_path}")
            return [], None
        
        latest_month_path = months_dirs[0]
        
        # 3. Construire le chemin cible complet
        target_path = Path(latest_month_path, specific_folder)
        
        if not fs.exists(target_path):
            logger.warning(f"Dossier cible non trouvé: {target_path}")
            return [], None

        logger.info(f"📁 Dossier cible: {target_path}")

        # 4. Récupérer les fichiers Excel (.xlsx)
        files = []
        remote_iter = fs.listFiles(target_path, True) # True = récursif
        while remote_iter.hasNext():
            file_status = remote_iter.next()
            path_str = file_status.getPath().toString()
            if path_str.endswith(".xlsx") and not file_status.getPath().getName().startswith("~$"):
                files.append(path_str)

        return files, annee_dossier

    except Exception as e:
        logger.error(f"Erreur lors du listing des fichiers: {str(e)}")
        traceback.print_exc()
        return [], None


def process_excel_file(spark, file_path):
    """
    Traite un fichier Excel : Pivot -> Format Long.
    Note: On ne passe plus annee_dossier ici car elle est écrasée par la logique interne.
    """
    try:
        # Optimisation : Si possible, définir un schema manuel pour éviter inferSchema=True
        # Ici on garde inferSchema pour la flexibilité, mais c'est un goulot d'étranglement.
        
        df = (
            spark.read
            .format("com.crealytics.spark.excel")
            .option("header", True)
            .option("inferSchema", True) 
            .option("treatEmptyValuesAsNulls", True)
            .option("usePlainNumberFormat", True)
            .load(file_path)
        )

        # Nettoyage des noms de colonnes (trim whitespace)
        cleaned_columns = [c.strip() for c in df.columns]
        df = df.toDF(*cleaned_columns)

        # Identification dynamique des colonnes
        # On suppose que les colonnes 'années' sont composées uniquement de chiffres (ex: "2010", "2011")
        colonnes_annees = [c for c in df.columns if c.isdigit()]
        colonnes_meta = [c for c in df.columns if c not in colonnes_annees]

        if not colonnes_annees:
            logger.warning(f"⚠️ Fichier ignoré (pas de colonnes années trouvées): {file_path}")
            return None

        # Transformation Stack (Unpivot)
        stack_expr = ", ".join([f"'{c}', `{c}`" for c in colonnes_annees])
        
        df_long = df.selectExpr(
            *[f"`{c}`" for c in colonnes_meta],
            f"stack({len(colonnes_annees)}, {stack_expr}) as (annee_col, valeur)"
        )

        # Gestion de la colonne indicateur
        if not colonnes_meta:
            # Cas rare où il n'y a que des années ?
            df_long = df_long.withColumn("indicateur", lit("Inconnu"))
        elif len(colonnes_meta) == 1:
            df_long = df_long.withColumnRenamed(colonnes_meta[0], "indicateur")
        else:
            # Concaténation propre
            concat_cols = [trim(col(c).cast("string")) for c in colonnes_meta]
            # Utilisation de concat_ws pour gérer proprement les séparateurs
            from pyspark.sql.functions import concat_ws
            df_long = df_long.select(
                concat_ws(" | ", *concat_cols).alias("indicateur"),
                col("annee_col"), 
                col("valeur")
            )

        # Nom du fichier pour le lignage (lineage)
        file_name = file_path.split("/")[-1]

        df_final = (
            df_long
            # Nettoyage valeur : suppression espaces et conversion locale (virgule -> point)
            .withColumn(
                "valeur",
                regexp_replace(
                    regexp_replace(col("valeur").cast("string"), "\\s+", ""), # Remove spaces
                    ",", "."
                ).cast("double")
            )
            .withColumn("annee", col("annee_col").cast("int"))
            # On renomme pour être cohérent avec la partition de sortie
            .withColumn("annee_dossier", col("annee")) 
            .withColumn("date_traitement", current_timestamp())
            .withColumn("fichier_source", lit(file_name))
            
            .filter(col("indicateur").isNotNull() & col("annee").isNotNull())
            .select("indicateur", "valeur", "annee_dossier", "date_traitement", "fichier_source")
        )
        
        # RETRAIT DU .count() ICI -> Performance gain
        return df_final

    except Exception as e_file:
        logger.error(f"⚠️ Erreur lors du traitement de {file_path}: {str(e_file)}")
        return None


# =========================================================
# MAIN
# =========================================================
def main():
    base_path = "s3a://01-raw"
    base_path = normalize_s3_path(base_path)

    specific_folder = "INS/agregats"
    output_path = "s3a://02-transformed/INS/agregats/"

    # 1. Trouver les fichiers
    files, _ = find_latest_files(spark, base_path, specific_folder)

    if not files:
        logger.error("❌ Aucun fichier trouvé.")
        return 1

    logger.info(f"✅ {len(files)} fichiers identifiés pour traitement.")

    # 2. Création de la liste des DataFrames (Lazy)
    dfs = []
    for file_path in files:
        # Note : process_excel_file ne lance plus d'action immédiate
        df = process_excel_file(spark, file_path)
        if df is not None:
            dfs.append(df)

    if not dfs:
        logger.error("❌ Aucun dataframe valide généré après lecture.")
        return 1

    # 3. Union
    # Pour beaucoup de fichiers, reduce/union peut créer un plan logique profond (StackOverflow).
    # Mais pour des fichiers Excel, on dépasse rarement quelques dizaines/centaines, donc c'est OK.
    logger.info("🔄 Préparation du plan d'exécution (Union)...")
    final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

    # 4. Transformations finales
    final_df = final_df.dropDuplicates(["indicateur", "annee_dossier", "fichier_source"])

    # 5. Écriture Optimisée (PartitionBy)
    logger.info("💾 Écriture par année (structure dossier métier)...")

    years = [row["annee_dossier"] for row in final_df.select("annee_dossier").distinct().collect()]

    for year in years:
        logger.info(f"📁 Écriture année {year}")

        (
            final_df
            .filter(col("annee_dossier") == year)
            .drop("annee_dossier")
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{output_path}/{year}")
        )


    logger.info("✅ Job terminé avec succès.")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        logger.critical(f"❌ ERREUR FATALE GLOBALE: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
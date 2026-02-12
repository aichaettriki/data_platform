from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, regexp_replace, current_timestamp, 
    input_file_name, trim
)
from functools import reduce
from typing import List, Optional, Tuple
import sys
import traceback
import logging

# =========================================================
# Configuration du logging
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================================================
# Configuration Spark optimisée
# =========================================================
spark = (
    SparkSession.builder
    .appName("transform_aggregats_ins")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

logger.info("=" * 60)
logger.info("🚀 JOB SPARK - TRANSFORMATION AGRÉGATS INS")
logger.info(f"✅ Spark version: {spark.version}")
logger.info("=" * 60)


# =========================================================
# Fonctions utilitaires
# =========================================================
def find_latest_files(
    spark: SparkSession, 
    base_path: str, 
    specific_folder: str
) -> Tuple[List[str], Optional[str]]:
    """
    Recherche les fichiers Excel dans le dossier le plus récent.
    
    Args:
        spark: Session Spark active
        base_path: Chemin S3 de base (ex: s3a://01-raw)
        specific_folder: Sous-dossier spécifique (ex: INS/agregats)
        
    Returns:
        Tuple (liste des chemins des fichiers Excel, année du dossier)
    """
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    if not base_path.startswith("s3a://"):
        raise ValueError("base_path must start with s3a://")

    # Extraction bucket et préfixe
    parts = base_path.replace("s3a://", "").split("/", 1)
    bucket = parts[0]
    prefix = "/" + parts[1] if len(parts) > 1 else "/"

    fs = FileSystem.get(URI(f"s3a://{bucket}"), sc._jsc.hadoopConfiguration())

    def list_dirs(path_str):
        """Liste les répertoires triés par ordre décroissant."""
        p = Path(path_str)
        if not fs.exists(p):
            return []
        return sorted(
            [x.getPath() for x in fs.listStatus(p) if x.isDirectory()],
            key=lambda x: x.getName(),
            reverse=True
        )

    # Navigation Année/Mois
    years = list_dirs(prefix)
    if not years:
        logger.warning(f"Aucune année trouvée dans {prefix}")
        return [], None

    # Extraction de l'année du dossier
    annee_dossier = years[0].getName()
    logger.info(f"📅 Année du dossier: {annee_dossier}")

    months = list_dirs(years[0].toString())
    if not months:
        logger.warning(f"Aucun mois trouvé dans {years[0]}")
        return [], None

    target = Path(months[0].toString() + "/" + specific_folder)
    if not fs.exists(target):
        logger.warning(f"Dossier cible non trouvé: {target}")
        return [], None

    logger.info(f"📁 Dossier cible: {target}")

    # Recherche récursive des fichiers Excel
    files = []
    stack = [target]
    while stack:
        p = stack.pop()
        for s in fs.listStatus(p):
            if s.isDirectory():
                stack.append(s.getPath())
            elif s.isFile() and s.getPath().getName().endswith(".xlsx"):
                files.append(s.getPath().toString())

    return files, annee_dossier


def process_excel_file(spark, file_path, annee_dossier):
    """
    Traite un fichier Excel et le transforme en format long.
    
    Args:
        spark: Session Spark active
        file_path: Chemin du fichier Excel
        annee_dossier: Année extraite du chemin du dossier
        
    Returns:
        DataFrame transformé ou None en cas d'erreur
    """
    try:
        logger.info(f"📖 Lecture: {file_path}")
        
        # Lecture Excel avec gestion des erreurs
        df = (
            spark.read
            .format("com.crealytics.spark.excel")
            .option("header", True)
            .option("inferSchema", True)
            .option("treatEmptyValuesAsNulls", True)
            .option("usePlainNumberFormat", True)
            .load(file_path)
        )

        # Identification des colonnes
        colonnes_annees = [c for c in df.columns if c and c.strip().isdigit()]
        colonnes_meta = [c for c in df.columns if c not in colonnes_annees]

        if not colonnes_annees:
            logger.warning(f"⚠️ Fichier ignoré (pas de colonnes années): {file_path}")
            return None

        logger.info(f"   → {len(colonnes_annees)} années trouvées: {min(colonnes_annees)} - {max(colonnes_annees)}")

        # Transformation pivot → long format
        stack_expr = ", ".join([f"'{c}', `{c}`" for c in colonnes_annees])
        
        df_long = df.selectExpr(
            *[f"`{c}`" for c in colonnes_meta],
            f"stack({len(colonnes_annees)}, {stack_expr}) as (annee, valeur)"
        )

        # Création de la colonne indicateur
        if len(colonnes_meta) == 1:
            df_long = df_long.withColumnRenamed(colonnes_meta[0], "indicateur")
        else:
            # Concaténation avec trim pour éviter les espaces multiples
            df_long = df_long.withColumn(
                "indicateur",
                trim(col(colonnes_meta[0]).cast("string"))
            )
            for c in colonnes_meta[1:]:
                df_long = df_long.withColumn(
                    "indicateur",
                    col("indicateur") + lit(" | ") + trim(col(c).cast("string"))
                )
            df_long = df_long.drop(*colonnes_meta)

        # Nettoyage et typage des données
        df_long = (
            df_long
            # Nettoyage valeur: suppression espaces et remplacement virgule par point
            .withColumn(
                "valeur",
                regexp_replace(
                    regexp_replace(col("valeur").cast("string"), "\\s+", ""),
                    ",", 
                    "."
                ).cast("double")
            )
            # Typage annee
            .withColumn("annee", col("annee").cast("int"))
            # Duplication de la colonne annee en annee_dossier
            .withColumn("annee_dossier", col("annee"))
            # Ajout métadonnées
            .withColumn("date_traitement", current_timestamp())
            .withColumn("fichier_source", lit(file_path.split("/")[-1]))
            # Filtrage des lignes nulles
            .filter(
                col("indicateur").isNotNull() & 
                col("annee").isNotNull()
            )
            # Sélection finale
            .select("indicateur", "valeur", "annee_dossier", "date_traitement", "fichier_source")
        )

        nb_rows = df_long.count()
        logger.info(f"   ✅ {nb_rows:,} lignes extraites")
        
        return df_long

    except Exception as e_file:
        logger.error(f"⚠️ Erreur lors du traitement de {file_path}: {str(e_file)}")
        return None


# =========================================================
# MAIN
# =========================================================
def main():
    """Fonction principale du job."""
    try:
        # Configuration des chemins
        base_path = "s3a://01-raw"
        specific_folder = "INS/agregats"
        output_path = "s3a://02-transformed/INS/agregats"

        # Recherche des fichiers et extraction de l'année du dossier
        files, annee_dossier = find_latest_files(spark, base_path, specific_folder)

        if not files or annee_dossier is None:
            logger.error("❌ Aucun fichier trouvé ou année du dossier non identifiable")
            return 1

        logger.info(f"✅ {len(files)} fichier(s) trouvé(s) pour l'année {annee_dossier}")

        # Traitement de chaque fichier
        dfs = []
        for file_path in files:
            df = process_excel_file(spark, file_path, annee_dossier)
            if df is not None:
                dfs.append(df)

        if not dfs:
            logger.error("❌ Aucun dataframe valide généré")
            return 1

        # Union de tous les dataframes
        logger.info("🔄 Union des dataframes...")
        final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
        
        # Statistiques finales
        total_rows = final_df.count()
        logger.info(f"📊 Total: {total_rows:,} lignes")
        
        # Dédoublonnage (optionnel mais recommandé)
        final_df = final_df.dropDuplicates(["indicateur", "annee_dossier", "fichier_source"])
        
        final_rows = final_df.count()
        if final_rows < total_rows:
            logger.info(f"🧹 {total_rows - final_rows:,} doublons supprimés")

        # Obtenir les années uniques pour créer les dossiers manuellement
        annees_list = [row.annee_dossier for row in final_df.select("annee_dossier").distinct().collect()]
        logger.info(f"📂 Années à traiter: {sorted(annees_list)}")

        # Écriture Parquet par année SANS partitionBy
        for annee in annees_list:
            annee_df = final_df.filter(col("annee_dossier") == annee)
            annee_output_path = f"{output_path}/{annee}"
            
            logger.info(f"💾 Écriture année {annee} vers {annee_output_path}...")
            (
                annee_df
                .coalesce(1)  # Optionnel: un seul fichier par année
                .write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(annee_output_path)
            )
            logger.info(f"✅ Année {annee} écrite")
        
        logger.info(f"✅ Écriture terminée: {output_path}")
        
        # Statistiques finales par année
        logger.info("📈 Distribution par année:")
        final_df.groupBy("annee_dossier").count().orderBy("annee_dossier").show()

        return 0

    except Exception as e:
        logger.error("❌ ERREUR FATALE")
        logger.error(str(e))
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        spark.stop()
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Erreur inattendue: {str(e)}")
        spark.stop()
        sys.exit(1)
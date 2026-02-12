from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER")
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD")
POSTGRES_URL = get_env_var("POSTGRES_URL")
POSTGRES_USER = get_env_var("POSTGRES_USER")
POSTGRES_PASSWORD = get_env_var("POSTGRES_PASSWORD")

def create_spark_session(app_name="ETL ITCEQ Data Lake"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoopConf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark


def read_raw_csv(spark, raw_path):
    """
    Lecture des données brutes depuis la zone RAW
    """
    df = spark.read.csv(raw_path, header=True, sep=";", inferSchema=True)
    print(f"📊 Données lues: {df.count()} lignes")
    print("-------->>>>>>>>>>>>>>> Fichiers sources :")
    for f in df.inputFiles():
        print(" -", f)
    df.printSchema()
    df.show(5)
    return df


def clean_and_write_transformed(df, domain, dataset_name, transformed_root="s3a://02-transformed"):
    """
    Nettoyage et écriture dans la zone TRANSFORMED
    
    Structure: s3a://02-transformed/{domain}/{dataset_name}/
    
    Domaines ITCEQ suggérés:
    - competitivite (indices, classements, benchmarks)
    - economie (PIB, inflation, commerce extérieur)
    - secteurs (industrie, services, agriculture)
    - emploi (chômage, salaires, formation)
    - innovation (R&D, brevets, startups)
    - regions (données régionales, disparités)
    - entreprises (démographie, performance)
    - conjoncture (indicateurs mensuels/trimestriels)
    """
    
    df_clean = (
        df.dropDuplicates()
          .dropna(how='all')  # Supprimer les lignes complètement vides
    )
    
    # Standardisation des noms de colonnes (minuscules, sans espaces)
    for col_name in df_clean.columns:
        new_col_name = col_name.strip().lower().replace(" ", "_").replace("-", "_")
        df_clean = df_clean.withColumnRenamed(col_name, new_col_name)
    
    output_path = f"{transformed_root}/{domain}/{dataset_name}/"
    
    df_clean.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)
    
    print(f"✅ TRANSFORMED: {df_clean.count()} lignes écrites dans {output_path}")
    return df_clean


def enrich_and_write_refined(df_clean, domain, dataset_name, refined_root="s3a://03-refined"):
    """
    Enrichissement et écriture dans la zone REFINED (prêt pour analyse)
    
    Structure: s3a://03-refined/{domain}/{dataset_name}/
    
    Cette zone contient les données prêtes pour:
    - Analyses statistiques
    - Tableaux de bord
    - Rapports ITCEQ
    - Data Warehouse
    """
    
    # Ajout de métadonnées pour traçabilité
    df_refined = (
        df_clean
        .withColumn("load_timestamp", current_timestamp())
        .withColumn("source_system", col("source_system") if "source_system" in df_clean.columns else lit("ITCEQ_ETL"))
    )
    
    output_path = f"{refined_root}/{domain}/{dataset_name}/"
    
    # Écriture en Parquet pour meilleures performances analytiques
    df_refined.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .parquet(output_path)
    
    print(f"✅ REFINED: {df_refined.count()} lignes écrites en Parquet dans {output_path}")
    return df_refined


def write_to_datawarehouse(df_refined, domain, dataset_name, jdbc_url=POSTGRES_URL):
    """
    Chargement dans le Data Warehouse (PostgreSQL)
    
    Nom de table: {domain}_{dataset_name}
    Ex: competitivite_indices, economie_pib, emploi_chomage
    """
    
    table_name = f"{domain}_{dataset_name}"
    
    df_refined.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("overwrite") \
        .save()
    
    print(f"✅ DATA WAREHOUSE: Données chargées dans la table '{table_name}'")


import sys
from pyspark.sql.functions import lit

if __name__ == "__main__":
    
    # Arguments: action, domain, dataset_name
    # Exemple: python script.py full_pipeline competitivite indices_2024
    
    if len(sys.argv) < 4:
        print("Usage: python script.py <action> <domain> <dataset_name>")
        print("\nExemples:")
        print("  python script.py full_pipeline competitivite indices")
        print("  python script.py full_pipeline economie pib")
        print("  python script.py full_pipeline emploi chomage")
        print("\nDomaines suggérés: competitivite, economie, secteurs, emploi, innovation, regions, entreprises, conjoncture")
        sys.exit(1)
    
    action = sys.argv[1]
    domain = sys.argv[2]
    dataset_name = sys.argv[3]
    
    app_name = f"ITCEQ ETL - {domain}/{dataset_name} - {action}"
    spark = create_spark_session(app_name)

    if action == "read_raw":
        df = read_raw_csv(spark, "s3a://01-raw/")

    elif action == "transformed":
        df = read_raw_csv(spark, "s3a://01-raw/")
        clean_and_write_transformed(df, domain, dataset_name)

    elif action == "refined":
        df = read_raw_csv(spark, "s3a://01-raw/")
        df_clean = clean_and_write_transformed(df, domain, dataset_name)
        enrich_and_write_refined(df_clean, domain, dataset_name)

    elif action == "datawarehouse":
        # Lecture depuis refined (Parquet)
        refined_path = f"s3a://03-refined/{domain}/{dataset_name}/"
        df = spark.read.parquet(refined_path)
        write_to_datawarehouse(df, domain, dataset_name)

    elif action == "full_pipeline":
        # Pipeline complet: RAW → TRANSFORMED → REFINED → DWH
        print(f"\n{'='*80}")
        print(f"🚀 PIPELINE COMPLET ITCEQ: {domain}/{dataset_name}")
        print(f"{'='*80}\n")
        
        # 1. RAW → TRANSFORMED
        print("📥 Étape 1/3: Lecture et nettoyage (RAW → TRANSFORMED)")
        df = read_raw_csv(spark, "s3a://01-raw/")
        df_clean = clean_and_write_transformed(df, domain, dataset_name)
        
        # 2. TRANSFORMED → REFINED
        print("\n🔧 Étape 2/3: Enrichissement (TRANSFORMED → REFINED)")
        df_refined = enrich_and_write_refined(df_clean, domain, dataset_name)
        
        # 3. REFINED → Data Warehouse
        print("\n💾 Étape 3/3: Chargement dans Data Warehouse")
        write_to_datawarehouse(df_refined, domain, dataset_name)
        
        print(f"\n{'='*80}")
        print(f"✅ PIPELINE TERMINÉ avec succès !")
        print(f"{'='*80}\n")

    else:
        print(f"❌ Action inconnue: {action}")
        print("Actions disponibles: read_raw, transformed, refined, datawarehouse, full_pipeline")
    
    spark.stop()
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ──────────────────────────────────────────────────────────────────────────────
# ENV VARS
# Chargés depuis les Variables Airflow ou l'env Docker — pas de dotenv en prod
# ──────────────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Variable d'environnement requise manquante : {name}"
        )
    return value


MINIO_USER     = _require_env("MINIO_ROOT_USER")
MINIO_PASSWORD = _require_env("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

RAW_BUCKET         = os.getenv("RAW_BUCKET",         "s3a://01-raw")
TRANSFORMED_BUCKET = os.getenv("TRANSFORMED_BUCKET", "s3a://02-transformed")

SPARK_SCRIPT_PATH = "/opt/spark/jobs/tre_processing.py"

# ──────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ──────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner":             "data_team",
    "depends_on_past":   False,
    "start_date":        datetime(2024, 1, 1),
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           1,                          # 1 retry en prod
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),         # évite les jobs zombie
}

# ──────────────────────────────────────────────────────────────────────────────
# SPARK CONF  (mutualisée pour être lisible et maintenable)
# ──────────────────────────────────────────────────────────────────────────────
SPARK_CONF = {
    # S3A / MinIO
    "spark.hadoop.fs.s3a.impl":               "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access":  "true",
    "spark.hadoop.fs.s3a.endpoint":           MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":         MINIO_USER,
    "spark.hadoop.fs.s3a.secret.key":         MINIO_PASSWORD,
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

    # Écriture en mode overwrite dynamique par partition
    "spark.sql.sources.partitionOverwriteMode": "dynamic",

    # Fiabilité réseau S3A
    "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
    "spark.hadoop.fs.s3a.connection.timeout":            "10000",
    "spark.hadoop.fs.s3a.attempts.maximum":              "3",
}

# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="Transform_TRE",
    default_args=default_args,
    description="ETL Spark pour les fichiers économiques INS/TRE",
    schedule_interval=None,   # déclenché manuellement ou par un sensor
    catchup=False,
    max_active_runs=1,        # évite les exécutions parallèles sur le même bucket
    tags=["INS", "TRE", "Spark", "ETL"],
) as dag:

    # ── Tâche principale : traitement Excel → Parquet ─────────────────────────
    transform_tre = SparkSubmitOperator(
        task_id="transform_excel_to_parquet",
        application=SPARK_SCRIPT_PATH,
        conn_id="spark_standalone",          # connexion définie dans Airflow UI
        name="ins_tre_etl",

        # Dépendances JAR nécessaires au job
        # spark-excel n'est PAS utilisé ici (parsing openpyxl côté driver)
        # hadoop-aws + aws-sdk sont nécessaires pour s3a://
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),

        # Variables d'environnement transmises au process Spark
        # Le script Python les lit via os.getenv()
        env_vars={
            "MINIO_ROOT_USER":     MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
            "MINIO_ENDPOINT":      MINIO_ENDPOINT,
        },

        application_args=[
            "--raw-bucket",         RAW_BUCKET,
            "--target-bucket",      TRANSFORMED_BUCKET,
            "--target-folder",      "INS/TRE",
        ],

        conf=SPARK_CONF,
        verbose=True,
    )

    # ── Déclenchement du DAG de nettoyage MinIO ───────────────────────────────
    trigger_cleanup = TriggerDagRunOperator(
        task_id="trigger_cleanup_minio",
        trigger_dag_id="Cleanup_Minio",
        wait_for_completion=True,
        poke_interval=30,          # vérifie toutes les 30 s si wait=True
        reset_dag_run=False,       # ne remet pas à zéro un run déjà en cours
        conf={
            "triggered_by": "Transform_TRE",
            "source_bucket": RAW_BUCKET,
        },
    )

    # ── Dépendances ───────────────────────────────────────────────────────────
    transform_tre >> trigger_cleanup
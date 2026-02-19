# common/spark_session.py
from pyspark.sql import SparkSession

def create_spark_session(
    app_name: str,
    driver_memory: str = "2g",
    executor_memory: str = "2g",
) -> SparkSession:
    """
    Crée une SparkSession de production.
    Toute la config infra est chargée depuis spark-defaults.conf.
    Seuls les paramètres job-specific sont définis ici :
    - app_name   : nom du job visible dans la Spark UI
    - driver_memory / executor_memory : adapter selon le volume du job
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Arrête proprement la SparkSession en fin de job.
    Toujours appeler en fin de pipeline pour libérer les ressources.
    """
    if spark and not spark.sparkContext._jsc.sc().isStopped():
        spark.stop()
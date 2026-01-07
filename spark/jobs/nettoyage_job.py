
from pyspark.sql import SparkSession
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
BUCKET_RAW = "raw"
BUCKET_TRANSFORMED = "transformed"
RAW_OBJECT = "clients.csv"

spark = SparkSession.builder.appName("SparkCleaningJob").getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

raw_path = f"s3a://{BUCKET_RAW}/{RAW_OBJECT}"
df = spark.read.csv(raw_path, header=True, sep=";")
df.show(5)  # aperçu
df_clean = df.dropDuplicates()
df_clean.show(5)  # aperçu après nettoyage

transformed_path = f"s3a://{BUCKET_TRANSFORMED}/spark_cleaned/"
df_clean.write.csv(transformed_path, mode="overwrite", header=True)

spark.stop()

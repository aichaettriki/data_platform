from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

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

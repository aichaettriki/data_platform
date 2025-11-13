from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JobExample").getOrCreate()

# Configurer MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio123")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Lire un fichier CSV depuis MinIO
df = spark.read.csv("s3a://raw/test.csv", header=True, inferSchema=True)
df.show()

# Transformation simple
df_transformed = df.select(df.columns[:2])  # exemple : garder les 2 premières colonnes
df_transformed.write.csv("s3a://transformed/test_transformed.csv", mode="overwrite")

spark.stop()

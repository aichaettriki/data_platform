from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("MinIO CSV Processing").getOrCreate()

# Set MinIO configurations
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio123")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

input_path = "s3a://raw/equipe.csv"
output_path = "s3a://test/equipe_with_test.csv"

df = spark.read.option("header", "true").csv(input_path)
df = df.withColumn("test", lit(1))
df.write.mode("overwrite").option("header", "true").csv(output_path)

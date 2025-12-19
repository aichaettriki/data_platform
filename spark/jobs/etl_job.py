import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session(app_name="ETL_Equipe_Production"):
    """
    Creates a Spark Session configured for MinIO and Apache Atlas.
    The 'itceq_prod' cluster name ensures stable lineage in Atlas.
    """
    spark = (SparkSession.builder
        .appName(app_name)
        .enableHiveSupport() # Required for Atlas to track tables
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.atlas.cluster.name", "itceq_prod")
        .config("spark.atlas.rest.address", "http://atlas-server:21000")
        .config("spark.atlas.kafka.bootstrap.servers", "kafka:9092")
        .config("spark.extraListeners", "com.hortonworks.spark.atlas.SparkAtlasEventTracker")
        .getOrCreate())

    # Hadoop/MinIO Configuration
    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "minio") 
    hadoopConf.set("fs.s3a.secret.key", "minio123")
    hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark

def read_raw_csv(spark, raw_path):
    """
    Reads the raw CSV with a fixed schema to ensure Atlas metadata is precise.
    """
    schema = StructType([
        StructField("nom", StringType(), True),
        StructField("prenom", StringType(), True),
        StructField("number", IntegerType(), True)
    ])
    
    df = spark.read.csv(raw_path, header=True, sep=";", schema=schema)
    df.createOrReplaceTempView("source_raw_data")
    return df

def clean_and_write(spark, df, transformed_root_path="s3a://transformed", dataset_name="equipe_spark"):
    """
    Removes duplicates and writes to the transformed zone.
    Pushes modification details to Atlas using TBLPROPERTIES.
    """
    df_clean = df.dropDuplicates()
    
    now = datetime.now()
    output_path = f"{transformed_root_path}/{dataset_name}/{now.year}/{now.month}/{now.day}/"
    table_name = f"cleaned_{dataset_name}"

    print(f"Writing Cleaned Data to Table: {table_name}")
    
    # Save the table
    (df_clean.write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("path", output_path)
        .saveAsTable(table_name))

    # PUSH DETAILS TO ATLAS: This tells Atlas WHAT happened
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'comment' = 'Data Quality: Removed duplicates from raw CSV',
            'transformation_type' = 'Deduplication',
            'processed_by' = 'Spark_ETL_Job'
        )
    """)

    return df_clean

def add_timestamp_and_write(spark, df_clean, refined_root_path="s3a://refined", dataset_name="equipe_spark"):
    """
    Performs logic changes (Concatenation) and writes to the refined zone.
    Pushes transformation details to Atlas.
    """
    # Modification logic: Create fullname and add audit timestamp
    df_refined = (df_clean
        .withColumn("fullname", concat_ws(" ", col("nom"), col("prenom")))
        .withColumn("processed_at", current_timestamp()))

    now = datetime.now()
    output_path = f"{refined_root_path}/{dataset_name}/{now.year}/{now.month}/{now.day}/"
    table_name = f"refined_{dataset_name}"

    print(f"Writing Refined Data to Table: {table_name}")

    # Save the table
    (df_refined.write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("path", output_path)
        .saveAsTable(table_name))

    # PUSH DETAILS TO ATLAS: Explains the logic change
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'comment' = 'Business Logic: Generated Fullname from Nom/Prenom',
            'logic' = 'concat_ws(" ", nom, prenom)',
            'audit' = 'Added current_timestamp as processed_at'
        )
    """)

    return output_path

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "read_raw_csv"
    spark_session = create_spark_session(f"ETL_Job_{action}")

    try:
        if action == "read_raw_csv":
            read_raw_csv(spark_session, "s3a://raw/")
            
        elif action == "add_timestamp_refined":
            # 1. READ
            raw_df = read_raw_csv(spark_session, "s3a://raw/")
            
            # 2. CLEAN (Writes table 1 + Pushes "Deduplication" metadata)
            cleaned_df = clean_and_write(spark_session, raw_df, "s3a://transformed", "equipe_spark")
            
            # 3. REFINE (Writes table 2 + Pushes "Concatenation" metadata)
            add_timestamp_and_write(spark_session, cleaned_df, "s3a://refined", "equipe_spark")
            
            print("✅ ETL Job Completed Successfully!")

    finally:
        spark_session.stop()
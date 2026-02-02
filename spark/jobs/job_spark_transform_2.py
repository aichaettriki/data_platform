"""
Spark Job: Transform INS TRE Excel files to long format CSV
Author: Data Engineering Team
Description: Reads Excel files from MinIO, transforms them to long format, and saves as CSV
"""

import os
import sys
import tempfile
import logging
from typing import List, Optional, Tuple
import pandas as pd
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, current_timestamp, lit
from pyspark.sql.types import StringType, StructType, StructField

# ======================
# LOGGING CONFIGURATION
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ======================
# ENVIRONMENT VARIABLES
# ======================
load_dotenv()

def get_env_var(name: str, default: Optional[str] = None, required: bool = False) -> str:
    """Retrieve environment variable with validation"""
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

# MinIO Configuration
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = get_env_var("MINIO_ROOT_USER", required=True)
MINIO_SECRET_KEY = get_env_var("MINIO_ROOT_PASSWORD", required=True)

# Bucket Configuration
RAW_BUCKET = get_env_var("RAW_BUCKET", "01-raw")
TRANS_BUCKET = get_env_var("TRANS_BUCKET", "02-transformed")
RAW_OBJECT = get_env_var("RAW_OBJECT", "2026/01/22/INS/TRE 2015-2023.xlsx")

# ======================
# SPARK SESSION
# ======================
def create_spark_session(app_name: str = "TRE_Transform") -> SparkSession:
    """Create and configure Spark session with MinIO S3A support"""
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configure Hadoop for MinIO (S3A)
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    logger.info(f"✔ Spark session created successfully - Version: {spark.version}")
    return spark

# ======================
# MINIO CLIENT
# ======================
def create_minio_client() -> Minio:
    """Create MinIO client with error handling"""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logger.info("✔ MinIO client created successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to create MinIO client: {e}")
        raise

# ======================
# UTILITY FUNCTIONS
# ======================
def make_columns_unique(cols: List[str]) -> List[str]:
    """Make duplicate column names unique by appending suffix"""
    seen = {}
    result = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            result.append(c)
        else:
            seen[c] += 1
            result.append(f"{c}_{seen[c]}")
    return result

def find_table_split(df: pd.DataFrame) -> Optional[int]:
    """Find the index where tables are split by empty row"""
    for i in range(1, len(df)):
        if df.iloc[i].isna().all():
            return i
    return None

def merge_split_tables(xls: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    """Read and merge split tables from Excel sheet"""
    df_full = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    
    if df_full.empty:
        logger.warning(f"Sheet {sheet_name} is empty")
        return pd.DataFrame()
    
    split_index = find_table_split(df_full)
    
    if split_index is None:
        logger.info(f"No split found in {sheet_name}, reading as single table")
        return pd.read_excel(xls, sheet_name=sheet_name)
    
    logger.info(f"Split found at row {split_index} in {sheet_name}")
    df1 = pd.read_excel(xls, sheet_name=sheet_name, header=0, nrows=split_index-1)
    df2 = pd.read_excel(xls, sheet_name=sheet_name, header=split_index+1)
    df_merged = pd.concat([df1, df2], axis=1)
    
    logger.info(f"Tables merged: {df1.shape} + {df2.shape} = {df_merged.shape}")
    return df_merged

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe by removing header rows and making columns unique"""
    # Remove first 2 rows if they exist (typically header info)
    df_clean = df.iloc[2:].reset_index(drop=True) if len(df) > 2 else df
    
    # Make column names unique
    df_clean.columns = make_columns_unique(df_clean.columns.tolist())
    
    logger.info(f"Cleaned dataframe: {len(df_clean)} rows, {len(df_clean.columns)} columns")
    return df_clean

def transform_to_long_format(df_pandas: pd.DataFrame, spark: SparkSession) -> DataFrame:
    """Transform pandas DataFrame to long format using PySpark"""
    
    if df_pandas.empty or len(df_pandas.columns) <= 2:
        logger.warning("DataFrame too small or empty, returning empty schema")
        schema = StructType([
            StructField("produit", StringType(), True),
            StructField("indicateur", StringType(), True),
            StructField("valeur", StringType(), True)
        ])
        return spark.createDataFrame([], schema)
    
    # Convert all columns to string to avoid type issues
    df_pandas_str = df_pandas.astype(str)
    
    # Convert to Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas_str)
    
    # Identify columns
    code_col = df_spark.columns[0]
    produit_col = df_spark.columns[1]
    value_cols = df_spark.columns[2:]
    
    if not value_cols:
        logger.warning("No value columns to transform")
        return df_spark.select(produit_col).withColumnRenamed(produit_col, "produit")
    
    logger.info(f"Transforming {len(value_cols)} value columns to long format")
    
    # Create stack expression for pivot
    stack_parts = [f"'{c}', `{c.replace('`','``')}`" for c in value_cols]
    stack_expr = f"stack({len(value_cols)}, {', '.join(stack_parts)}) as (indicateur, valeur)"
    
    # Transform to long format
    df_long = df_spark.select(produit_col, expr(stack_expr)) \
        .filter(
            (col("valeur").isNotNull()) & 
            (col("valeur") != "") & 
            (col("valeur") != "nan")
        ) \
        .withColumnRenamed(produit_col, "produit")
    
    # Add metadata columns
    df_long = df_long.select(
        "produit", 
        "indicateur", 
        "valeur"
    ).withColumn("processed_at", current_timestamp())
    
    row_count = df_long.count()
    logger.info(f"Long format created with {row_count} rows")
    
    return df_long

def download_file_from_minio(client: Minio, bucket: str, object_path: str, local_path: str) -> bool:
    """Download file from MinIO with error handling"""
    try:
        logger.info(f"Downloading {object_path} from bucket {bucket}...")
        client.fget_object(bucket, object_path, local_path)
        logger.info(f"✔ File downloaded to {local_path}")
        return True
    except S3Error as e:
        logger.error(f"MinIO S3 error: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to download file: {e}")
        return False

def save_to_minio(df: DataFrame, output_path: str, sheet_name: str) -> bool:
    """Save DataFrame to MinIO as CSV with error handling"""
    try:
        logger.info(f"Saving {sheet_name} to {output_path}...")
        
        # Write with single partition for single output file
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .option("encoding", "UTF-8") \
            .csv(output_path)
        
        logger.info(f"✔ Sheet {sheet_name} saved successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to save {sheet_name}: {e}")
        return False

# ======================
# MAIN PROCESSING LOGIC
# ======================
def process_excel_sheet(xls: pd.ExcelFile, sheet_name: str, spark: SparkSession) -> Optional[DataFrame]:
    """Process a single Excel sheet"""
    logger.info(f"\n{'='*60}")
    logger.info(f"📄 Processing sheet: {sheet_name}")
    logger.info(f"{'='*60}")
    
    try:
        # Read and merge tables
        df = merge_split_tables(xls, sheet_name)
        
        if df.empty:
            logger.warning(f"⚠ Sheet {sheet_name} is empty, skipping")
            return None
        
        # Clean dataframe
        df_clean = clean_dataframe(df)
        logger.info(f"Column names: {list(df_clean.columns)}")
        
        # Transform to long format
        df_long = transform_to_long_format(df_clean, spark)
        
        return df_long
        
    except Exception as e:
        logger.error(f"Error processing sheet {sheet_name}: {e}")
        return None

def main():
    """Main execution function"""
    success_count = 0
    failure_count = 0
    
    try:
        # Initialize clients
        logger.info("="*60)
        logger.info("Starting TRE Transform Job")
        logger.info("="*60)
        
        spark = create_spark_session()
        minio_client = create_minio_client()
        
        # Create temporary directory for file processing
        with tempfile.TemporaryDirectory() as tmpdir:
            local_raw_file = os.path.join(tmpdir, "source.xlsx")
            
            # Download source file
            if not download_file_from_minio(minio_client, RAW_BUCKET, RAW_OBJECT, local_raw_file):
                logger.error("Failed to download source file, aborting")
                sys.exit(1)
            
            # Load Excel file
            logger.info(f"Loading Excel file: {local_raw_file}")
            xls = pd.ExcelFile(local_raw_file)
            logger.info(f"Found {len(xls.sheet_names)} sheets: {xls.sheet_names}")
            
            # Process each sheet
            for sheet_name in xls.sheet_names:
                df_long = process_excel_sheet(xls, sheet_name, spark)
                
                if df_long is None:
                    failure_count += 1
                    continue
                
                # Extract year from sheet name (e.g., "2015 T1" -> "2015")
                year = sheet_name.split()[0]
                output_path = f"s3a://{TRANS_BUCKET}/INS/TRE/{year}/{sheet_name}"
                
                # Save to MinIO
                if save_to_minio(df_long, output_path, sheet_name):
                    success_count += 1
                else:
                    failure_count += 1
        
        # Final summary
        logger.info("\n" + "="*60)
        logger.info("Job Execution Summary")
        logger.info("="*60)
        logger.info(f"✅ Successfully processed: {success_count} sheets")
        logger.info(f"❌ Failed: {failure_count} sheets")
        logger.info(f"📊 Total sheets: {success_count + failure_count}")
        logger.info("="*60)
        
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")
        
        # Exit with appropriate code
        if failure_count > 0:
            logger.warning("Job completed with some failures")
            sys.exit(1)
        else:
            logger.info("✅ Job completed successfully")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
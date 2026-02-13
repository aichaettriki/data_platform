"""
Production-ready Spark Job for Competitive Scores Processing
- Fully Spark-based (no Pandas)
- Robust detection of titles and years
- Ranking calculation
- Save to MinIO partitioned by year
- No coalesce (multiple files per year)
"""

import os
import sys
import logging
import re
from datetime import datetime
from io import BytesIO

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

from minio import Minio
import openpyxl

# ===============================
# Logging
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CompetitifScoresProcessor")

# ===============================
# Helper Functions
# ===============================
def create_spark_session(app_name="Competitif Scores Processor"):
    """Create Spark session with MinIO configuration"""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    return spark

def clean_roman_prefix(text: str) -> str:
    """Remove Roman numeral prefixes from titles"""
    roman_pattern = r'^\s*(?P<roman>M{0,4}(CM|CD|D?C{0,3})' \
                    r'(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))[\s\-\.\:\/]+'
    return re.sub(roman_pattern, '', text, flags=re.IGNORECASE).strip()

def find_latest_excel(minio_client, bucket, prefix):
    """Find the latest Excel file in a MinIO bucket/prefix"""
    objects = minio_client.list_objects(bucket, prefix=prefix, recursive=True)
    excel_files = [
        obj.object_name for obj in objects
        if obj.object_name.endswith(('.xlsx', '.xls')) and not obj.object_name.split('/')[-1].startswith('~')
    ]
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {bucket}/{prefix}")
    excel_files.sort(reverse=True)
    return excel_files[0]

# ===============================
# Main Processor
# ===============================
class CompetitifScoresProcessor:
    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day

        self.bucket_raw = "01-raw"
        self.bucket_transformed = "02-transformed"
        self.input_path = f"{year}/{month}/ITCEQ/competitivite/positionnement/"
        self.output_path = "ITCEQ/competitivite/Positionnement/"

        self.spark = create_spark_session()
        logger.info("Spark session initialized")

        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
        self.minio_client = Minio(
            endpoint,
            access_key=os.getenv("MINIO_ROOT_USER", "minio"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
            secure=False
        )

    # -------------------------------
    # Extract Excel as Spark DataFrame
    # -------------------------------
    def extract_excel_to_spark(self):
        file_key = find_latest_excel(self.minio_client, self.bucket_raw, self.input_path)
        logger.info(f"Using Excel file: {file_key}")

        response = self.minio_client.get_object(self.bucket_raw, file_key)
        excel_bytes = BytesIO(response.read())
        response.close()
        response.release_conn()

        wb = openpyxl.load_workbook(excel_bytes, read_only=True)
        sheet_name = next((s for s in wb.sheetnames if 'positionnement' in s.lower() and 'compétitif' in s.lower()), wb.sheetnames[0])
        ws = wb[sheet_name]

        data = []
        for row in ws.iter_rows(values_only=True):
            data.append([str(cell).strip() if cell is not None else None for cell in row])

        max_cols = max(len(r) for r in data)
        data_fixed = [r + [None]*(max_cols - len(r)) for r in data]

        schema = StructType([StructField(f"c{i}", StringType(), True) for i in range(max_cols)])
        self.raw_df = self.spark.createDataFrame(data_fixed, schema=schema)
        logger.info(f"Excel loaded into Spark DataFrame: {self.raw_df.count()} rows x {len(self.raw_df.columns)} cols")

    # -------------------------------
    # Transform Data
    # -------------------------------
    def transform_data(self):
        df = self.raw_df
        rows = df.collect()
        loading_date = datetime.now().date()

        # Detect titles: first column non-empty, second column empty
        title_rows = [
            (i, clean_roman_prefix(r.c0))
            for i, r in enumerate(rows)
            if r.c0 and (r.c1 is None)
        ]

        all_records = []

        for idx, (start_idx, title) in enumerate(title_rows):
            end_idx = title_rows[idx + 1][0] if idx + 1 < len(title_rows) else len(rows)
            section_rows = rows[start_idx:end_idx]

            # Detect years by scanning all cells in the first 10 columns of each row
            years = []
            for r in section_rows:
                for val in r[1:]:
                    if val:
                        try:
                            y = int(float(val))
                            if 2000 <= y <= 2030:
                                years.append(y)
                        except:
                            continue
                if years:
                    break
            if not years:
                continue

            # Extract country data
            for r in section_rows[1:]:
                country = r.c0
                if not country or len(str(country)) < 2:
                    continue
                for col_idx, year in enumerate(years, start=1):
                    if col_idx >= len(r):
                        continue
                    score = r[col_idx]
                    if score is None:
                        continue
                    try:
                        score_val = float(score)
                        if 0 <= score_val <= 100:
                            all_records.append((str(country).strip(), year, title.strip(), score_val, loading_date))
                    except:
                        continue

        schema = StructType([
            StructField("pays", StringType(), True),
            StructField("annee", IntegerType(), True),
            StructField("indicateur", StringType(), True),
            StructField("valeur", DoubleType(), True),
            StructField("date_chargement", DateType(), True)
        ])

        self.transformed_df = self.spark.createDataFrame(all_records, schema=schema).dropDuplicates(["pays","annee","indicateur"])
        logger.info(f"Transformed data: {self.transformed_df.count()} records")

    # -------------------------------
    # Calculate Rankings
    # -------------------------------
    def calculate_rankings(self):
        window_spec = Window.partitionBy("indicateur", "annee").orderBy(F.col("valeur").desc())
        self.ranked_df = self.transformed_df.withColumn("rang", F.rank().over(window_spec))
        logger.info(f"Ranking calculated: {self.ranked_df.count()} records")

    # -------------------------------
    # Save to MinIO partitioned by year (no coalesce)
    # -------------------------------
    def save_to_minio(self):
        """
        Save to MinIO by business year folders (no Spark partitionBy)
        Output structure:
        Positionnement/
            2010/
            2011/
            2012/
        """

        logger.info("💾 Writing data by year folders (no partitionBy)")

        # récupérer la liste des années présentes
        years = [row["annee"] for row in self.ranked_df.select("annee").distinct().collect()]

        logger.info(f"Years detected: {years}")

        for year in years:
            logger.info(f"📁 Writing year {year}")

            (
                self.ranked_df
                .filter(F.col("annee") == year)
                .write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(f"s3a://{self.bucket_transformed}/{self.output_path}{year}")
            )

        logger.info("Data successfully saved by year folders")

    # -------------------------------
    # Run Pipeline
    # -------------------------------
    def run(self):
        try:
            self.extract_excel_to_spark()
            self.transform_data()
            self.calculate_rankings()
            self.save_to_minio()
            logger.info("Pipeline completed successfully")
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")


# ===============================
# Main
# ===============================
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: spark-submit script.py <year> <month> <day>")
        sys.exit(1)

    year, month, day = sys.argv[1], sys.argv[2], sys.argv[3]
    processor = CompetitifScoresProcessor(year, month, day)
    processor.run()


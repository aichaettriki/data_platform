import pandas as pd
import logging
from openpyxl import load_workbook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse
import sys
import re
import os
from io import BytesIO
from common.spark_session import create_spark_session, stop_spark_session
 
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
 
 
def find_latest_files_in_minio(minio_client, bucket, base_folder):
    objects = minio_client.list_objects(bucket, prefix=base_folder, recursive=True)
    excel_files = [obj.object_name for obj in objects if obj.object_name.endswith(('.xlsx', '.xls')) and not obj.object_name.split('/')[-1].startswith('~')]
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {bucket}/{base_folder}")
    excel_files.sort(reverse=True)
    return excel_files[0]
 
def clean_roman_prefix(text: str) -> str:
    roman_pattern = r'^\s*(?P<roman>M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))[\s\-\.\:\/]+'
    return re.sub(roman_pattern, '', text, flags=re.IGNORECASE).strip()
 
class MinIOConfig:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.bucket_raw = "01-raw"
        self.bucket_transformed = "02-transformed"
 
class CompetitifScoresProcessor:
    def __init__(self, year, month, day):
        self.year, self.month, self.day = year, month, day
        self.minio_config = MinIOConfig()
        self.input_path = f"{year}/{month}/ITCEQ/competitivite/positionnement/"
        self.output_path = "ITCEQ/competitivite/Positionnement/"
        self.detection_config = {'excluded_texts': ['Score', 'Rang', 'Pays', 'Rank', 'Country', 'Year'], 'min_text_length': 3}
        self.spark = create_spark_session("Competitivité Positionnement Processing")
        self._init_minio_client()
 
    def _init_minio_client(self):
        from minio import Minio
        endpoint = self.minio_config.endpoint.replace("http://", "").replace("https://", "")
        self.minio_client = Minio(endpoint, access_key=self.minio_config.access_key, secret_key=self.minio_config.secret_key, secure=False)
 
    def run(self):
        try:
            self.find_excel_file()
            self.extract_data_from_minio()
            self.identify_titles()
            self.transform_data()
            self.calculate_rankings()
            save_results = self.save_to_minio()
            self.validate_minio_output()
            return {'status': 'success', 'output_path': save_results['path'], 'years': save_results['years']}
        finally:
            stop_spark_session(self.spark)
 
    def find_excel_file(self):
        self.input_file_key = find_latest_files_in_minio(self.minio_client, self.minio_config.bucket_raw, self.input_path)
        return self.input_file_key
 
    def extract_data_from_minio(self):
        response = self.minio_client.get_object(self.minio_config.bucket_raw, self.input_file_key)
        self.excel_data_bytes = BytesIO(response.read())
        wb = load_workbook(self.excel_data_bytes, read_only=True)
        self.sheet_name = next((n for n in wb.sheetnames if 'positionnement' in n.lower()), wb.sheetnames[0])
        self.excel_data_bytes.seek(0)
        self.raw_data = pd.read_excel(self.excel_data_bytes, sheet_name=self.sheet_name, header=None)
        return self.raw_data
 
 
    def identify_titles(self):
        self.excel_data_bytes.seek(0)
        ws = load_workbook(self.excel_data_bytes)[self.sheet_name]
        self.title_sections = []
 
        for r in range(1, ws.max_row + 1):
            val_a = ws.cell(row=r, column=1).value
            val_b = ws.cell(row=r, column=2).value
 
            # Clean value for logging and detection
            val_a_clean = clean_roman_prefix(str(val_a).strip()) if isinstance(val_a, str) else ''
 
            # Logging
            logger.info(f"  ✅ ✅ ℹ️ Row {r}: Column A={repr(val_a)}, Column B={repr(val_b)}, Cleaned Title={repr(val_a_clean)}")
 
            if val_a_clean and (val_b is None or val_b == ''):
                if len(val_a_clean) >= self.detection_config['min_text_length'] and val_a_clean not in self.detection_config['excluded_texts']:
                    self.title_sections.append({'row_index': r - 1, 'title': val_a_clean})
 
        logger.info(f"Detected {len(self.title_sections)} titles: {[t['title'] for t in self.title_sections]}")
        return self.title_sections
 
    def transform_data(self):
        df, all_records = self.raw_data, []
        for i, section in enumerate(self.title_sections):
            start = section['row_index']
            end = self.title_sections[i+1]['row_index'] if i < len(self.title_sections)-1 else len(df)
            sec_df = df.iloc[start:end, :].copy().reset_index(drop=True)
           
            years_row_idx = None
            for r_idx in range(min(10, len(sec_df))):
                row = sec_df.iloc[r_idx]
                years = [int(float(v)) for v in row[1:6] if pd.notna(v) and str(v).replace('.0','').isdigit() and 2000 <= float(v) <= 2030]
                if len(years) >= 1:
                    years_row_idx = r_idx
                    break
           
            if years_row_idx is None: continue
            years = [int(float(y)) for y in sec_df.iloc[years_row_idx, 1:15] if pd.notna(y)]
            countries_data = sec_df.iloc[years_row_idx+1:, :]
 
            for _, row in countries_data.iterrows():
                country = row[0]
                if pd.isna(country) or str(country).strip() in self.detection_config['excluded_texts']: continue
                for year, score in zip(years, row[1:15]):
                    if pd.notna(score):
                        all_records.append({'pays': str(country).strip(), 'annee': int(year), 'variable': section['title'].strip(), 'valeur': float(score)})
       
        self.transformed_data = pd.DataFrame(all_records).drop_duplicates(subset=['pays', 'annee', 'variable'])
        return self.transformed_data
 
    def calculate_rankings(self):
        """Calculates rankings and adds metadata: Base, version, Source, dim_id, dim_key, code_secteur, lib_secteur"""
        logger.info("STEP 4: CALCULATING RANKINGS (SPARK NATIVE)")
       
        # Convert initial Pandas extraction to Spark
        spark_df = self.spark.createDataFrame(self.transformed_data)
       
        # Define window for ranking
        window_spec = Window.partitionBy("variable", "annee").orderBy(F.col("valeur").desc())
       
        # Apply ranking and add ALL requested columns
        # Using .cast("string") on Nulls prevents the "CANNOT_DETERMINE_TYPE" error
        self.ranked_data = spark_df.withColumn("rang", F.rank().over(window_spec)) \
                           .withColumn("base", F.lit(None).cast("string")) \
                           .withColumn("version", F.lit(None).cast("string")) \
                           .withColumn("source", F.lit("competitivité positionnelle")) \
                           .withColumn("dim_id", F.lit(None).cast("string")) \
                           .withColumn("dim_key", F.lit(None).cast("string")) \
                           .withColumn("code_secteur", F.lit(None).cast("string")) \
                           .withColumn("lib_secteur", F.lit(None).cast("string"))
 
        return self.ranked_data
   
 
    def save_to_minio(self):
        logger.info("="*80)
        logger.info("STEP 5: SAVING TO MINIO (STRICT AUDIT TRAIL)")
        logger.info("="*80)
 
        # self.ranked_data is already a Spark DataFrame from calculate_rankings
        spark_df_new = self.ranked_data \
                                   .withColumn("valeur", F.col("valeur").cast("double")) \
                                   .withColumn("rang", F.col("rang").cast("int"))
 
        # Get unique years using Spark
        years_rows = spark_df_new.select("annee").distinct().collect()
        years = sorted([int(row['annee']) for row in years_rows])
       
        sc = self.spark.sparkContext
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        Path = sc._jvm.org.apache.hadoop.fs.Path
        conf = sc._jsc.hadoopConfiguration()
 
        for year in years:
            logger.info(f"\n📅 Check Year: {year}")
            df_year_new = spark_df_new.filter(F.col("annee") == year)
            df_year_new.createOrReplaceTempView("v_new_data")
 
            output_path = f"s3a://{self.minio_config.bucket_transformed}/{self.output_path}{year}"
            temp_output_path = output_path + "_temp_write"
 
            history_exists = False
            try:
                df_history = self.spark.read.parquet(output_path)
                df_history.createOrReplaceTempView("v_history")
                history_exists = True
            except:
                logger.info(f"   ℹ️ No history for {year}.")
 
            if not history_exists:
                # First load: Add Audit columns
                df_to_write = df_year_new \
                    .withColumn("date_chargement", F.current_timestamp()) \
                    .withColumn("version_active", F.lit(1).cast("int"))
            else:
                # Delta detection logic
                delta_query = """
                    SELECT
                        n.*,
                        CURRENT_TIMESTAMP as date_chargement
                    FROM v_new_data n
                    LEFT JOIN (
                        SELECT pays, variable, valeur, rang
                        FROM (
                            SELECT *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY LOWER(TRIM(pays)), LOWER(TRIM(variable))
                                    ORDER BY date_chargement DESC
                                ) as rn
                            FROM v_history
                        )
                        WHERE rn = 1
                    ) h
                    ON LOWER(TRIM(n.pays)) = LOWER(TRIM(h.pays))
                    AND LOWER(TRIM(n.variable)) = LOWER(TRIM(h.variable))
                    WHERE h.pays IS NULL
                    OR (ROUND(n.valeur, 5) <> ROUND(h.valeur, 5))
                    OR (n.valeur IS NULL AND h.valeur IS NOT NULL)
                    OR (n.valeur IS NOT NULL AND h.valeur IS NULL)
                    OR (n.rang <> h.rang)
                """
 
                df_changes = self.spark.sql(delta_query)
                change_count = df_changes.cache().count()
 
                if change_count > 0:
                    logger.info(f"   🔄 {change_count} changes detected. Updating history...")
 
                    df_changes_latest = df_changes.withColumn("version_active", F.lit(1).cast("int"))
                    df_changes_latest.createOrReplaceTempView("v_changes")
                   
                    df_outdated_keys = self.spark.sql("""
                        SELECT LOWER(TRIM(pays)) as pays_key, LOWER(TRIM(variable)) as variable_key
                        FROM v_changes
                    """)
                    df_outdated_keys.createOrReplaceTempView("v_outdated_keys")
 
                    df_history_updated = df_history.alias("h").join(
                        df_outdated_keys.alias("k"),
                        (F.lower(F.trim(F.col("h.pays"))) == F.col("k.pays_key")) &
                        (F.lower(F.trim(F.col("h.variable"))) == F.col("k.variable_key")),
                        how="left"
                    ).withColumn(
                        "version_active",
                        F.when(F.col("k.pays_key").isNotNull(), F.lit(0).cast("int"))
                        .otherwise(F.col("h.version_active").cast("int"))
                    ).drop("pays_key", "variable_key")
 
                    # 🔥 UnionByName with allowMissingColumns=True handles the new schema
                    df_to_write = df_history_updated.unionByName(df_changes_latest, allowMissingColumns=True)
                else:
                    logger.info("   ✅ Data is identical to history. No file modification.")
                    df_to_write = None
                df_changes.unpersist()
 
            if df_to_write is not None:
                df_to_write.coalesce(1).write.mode("overwrite").parquet(temp_output_path)
                try:
                    target_uri = sc._jvm.java.net.URI(output_path)
                    fs = FileSystem.get(target_uri, conf)
                    if fs.exists(Path(output_path)): fs.delete(Path(output_path), True)
                    fs.rename(Path(temp_output_path), Path(output_path))
                    logger.info(f"   ✅ Year {year} successfully updated with all 7 new columns.")
                except Exception as e:
                    logger.error(f"   ❌ FS Error: {e}")
 
            # Clean up Views
            self.spark.catalog.dropTempView("v_new_data")
            if history_exists: self.spark.catalog.dropTempView("v_history")
            try:
                self.spark.catalog.dropTempView("v_changes")
                self.spark.catalog.dropTempView("v_outdated_keys")
            except:
                pass
       
        return {'path': self.output_path, 'years': years}
 
 
    def validate_minio_output(self):
        logger.info("STEP 6: VALIDATION")
        base_path = f"s3a://{self.minio_config.bucket_transformed}/{self.output_path}"
        try:
            df_all = self.spark.read.parquet(f"{base_path}*/")
            total = df_all.count()
            actives = df_all.filter(F.col("version_active") == 1).count()
            historiques = df_all.filter(F.col("version_active") == 0).count()
            logger.info(f"✅ Full dataset validated: {total:,} records total.")
            logger.info(f"   └── 🟢 version_active = 1 : {actives:,} lignes actives")
            logger.info(f"   └── 🔴 version_active = 0 : {historiques:,} lignes historiques")
        except:
            logger.warning("⚠️ Validation skipped (dataset might be empty or years missing).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--day", required=True)

    args = parser.parse_args()

    processor = CompetitifScoresProcessor(
        year=args.year,
        month=args.month,
        day=args.day
    )

    processor.run()
 
if __name__ == "__main__":
    main()
 
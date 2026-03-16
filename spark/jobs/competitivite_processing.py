import pandas as pd
import logging
from openpyxl import load_workbook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from datetime import datetime
import argparse
import uuid
import sys
import re
import os
from io import BytesIO
from common.spark_session import create_spark_session, stop_spark_session
from pyspark.sql.types import StringType

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================================
# MARQUEZ CONFIG
# =====================================================
MARQUEZ_URL     = "http://marquez:5000"
PIPELINE_RUN_ID = str(uuid.uuid4())

# Namespace routing rules:
#   s3a://03-refined/... → data_warehouse
#   everything else      → data_lake  (raw, transformed, memory intermediates)
NS_DATA_LAKE      = "data_lake"
NS_DATA_WAREHOUSE = "data_warehouse"

# =====================================================
# MARQUEZ HELPERS
# =====================================================
def resolve_namespace(path: str) -> str:
    """
    Resolves the OpenLineage namespace for a given dataset path.

    Rules:
        s3a://03-refined/...  → data_warehouse
        everything else       → data_lake
            (covers: s3a://01-raw/, s3a://02-transformed/, memory://spark_df/...)
    """
    if path.startswith("s3a://03-refined") or path.startswith("s3://03-refined"):
        return NS_DATA_WAREHOUSE
    return NS_DATA_LAKE


def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs,
                      input_schema_fields=None, column_lineage=None):
    """
    Sends rich lineage metadata to Marquez for a given pipeline step.

    Namespace is resolved automatically per dataset path:
        - s3a://03-refined/... → data_warehouse
        - all others           → data_lake

    Parameters:
        spark_df            : Spark DataFrame — used to extract output schema
        step_name           : Logical name of the step (e.g. '01_Find_Excel_File')
        description         : Human-readable description of what this step does
        trans_type          : Transformation type label (e.g. 'EXTRACT', 'TRANSFORMATION', 'LOAD')
        inputs              : List of input dataset paths/names (source)
        outputs             : List of output dataset paths/names (destination)
        input_schema_fields : Optional list of dicts describing the input schema
                              (used for raw files like Excel where we have no Spark DF yet)
        column_lineage      : Optional dict describing per-column lineage
                              (which input field each output field comes from)
    """
    try:
        client = OpenLineageClient(url=MARQUEZ_URL)

        # ── Output schema facet ───────────────────────────────────────────────
        output_fields = [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "description": f"nullable={field.nullable}",
            }
            for field in spark_df.schema.fields
        ]

        output_dataset_facets = {
            "schema": {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
                "fields": output_fields,
            },
            "documentation": {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json#",
                "description": f"[{trans_type}] {description}",
            },
        }

        # ── Column lineage facet (if provided) ───────────────────────────────
        if column_lineage:
            output_dataset_facets["columnLineage"] = {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json#",
                "fields": column_lineage,
            }

        # ── Input schema facet (if provided — e.g. raw Excel file) ───────────
        input_dataset_facets = {}
        if input_schema_fields:
            input_dataset_facets["schema"] = {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
                "fields": input_schema_fields,
            }

        # ── Run facets (processing engine info) ──────────────────────────────
        run_facets = {
            "processing_engine": {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ProcessingEngineRunFacet.json#",
                "version": "3.5.1",
                "name": "Apache Spark",
                "openlineageAdapterVersion": "itceq-1.0",
            }
        }

        # ── Job facets (step description as SQL-like query) ───────────────────
        job_facets = {
            "sql": {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json#",
                "query": description,
            }
        }

        # ── Build input / output Dataset objects with resolved namespaces ─────
        input_datasets = [
            Dataset(namespace=resolve_namespace(i), name=i, facets=input_dataset_facets)
            for i in inputs
        ]
        output_datasets = [
            Dataset(namespace=resolve_namespace(o), name=o, facets=output_dataset_facets)
            for o in outputs
        ]

        # Log resolved namespaces for traceability
        for i in inputs:
            print(f"     ↳ INPUT  [{resolve_namespace(i)}] {i}")
        for o in outputs:
            print(f"     ↳ OUTPUT [{resolve_namespace(o)}] {o}")

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat() + "Z",
            run=Run(runId=PIPELINE_RUN_ID, facets=run_facets),
            job=Job(namespace=NS_DATA_LAKE, name=step_name, facets=job_facets),
            inputs=input_datasets,
            outputs=output_datasets,
            producer="itceq-spark-producer",
        )

        client.emit(event)
        print(f"📡 Marquez Updated: [{trans_type}] {step_name} | {len(output_fields)} output columns")

    except Exception as e:
        print(f"⚠️  Marquez metadata error at step '{step_name}': {e}")


# =====================================================
# HELPERS
# =====================================================
def find_latest_files_in_minio(minio_client, bucket, base_folder):
    objects = minio_client.list_objects(bucket, prefix=base_folder, recursive=True)
    excel_files = [
        obj.object_name for obj in objects
        if obj.object_name.endswith(('.xlsx', '.xls'))
        and not obj.object_name.split('/')[-1].startswith('~')
    ]
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {bucket}/{base_folder}")
    excel_files.sort(reverse=True)
    return excel_files[0]


def clean_roman_prefix(text: str) -> str:
    roman_pattern = r'^\s*(?P<roman>M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))[\s\-\.\:\/]+'
    return re.sub(roman_pattern, '', text, flags=re.IGNORECASE).strip()


# =====================================================
# CONFIG
# =====================================================
class MinIOConfig:
    def __init__(self):
        self.endpoint        = os.getenv("MINIO_ENDPOINT",       "http://minio:9000")
        self.access_key      = os.getenv("MINIO_ROOT_USER",      "minioadmin")
        self.secret_key      = os.getenv("MINIO_ROOT_PASSWORD",  "minioadmin")
        self.bucket_raw         = "01-raw"
        self.bucket_transformed = "02-transformed"


# =====================================================
# PROCESSOR
# =====================================================
class CompetitifScoresProcessor:

    def __init__(self, year, month, day):
        self.year  = year
        self.month = month
        self.day   = day
        self.minio_config    = MinIOConfig()
        self.input_path      = f"{year}/{month}/ITCEQ/competitivite/positionnement/"
        self.output_path     = "ITCEQ/competitivite/Positionnement/"
        self.detection_config = {
            'excluded_texts': ['Score', 'Rang', 'Pays', 'Rank', 'Country', 'Year'],
            'min_text_length': 3,
        }
        self.spark = create_spark_session("Compétitivité Positionnement Processing")
        self._init_minio_client()

    def _init_minio_client(self):
        from minio import Minio
        endpoint = self.minio_config.endpoint.replace("http://", "").replace("https://", "")
        self.minio_client = Minio(
            endpoint,
            access_key=self.minio_config.access_key,
            secret_key=self.minio_config.secret_key,
            secure=False,
        )

    # ------------------------------------------------------------------
    def run(self):
        try:
            self.find_excel_file()
            self.extract_data_from_minio()
            self.identify_titles()
            self.transform_data()
            self.calculate_rankings()
            save_results = self.save_to_minio()
            self.validate_minio_output()
            return {
                'status':      'success',
                'output_path': save_results['path'],
                'years':       save_results['years'],
            }
        finally:
            stop_spark_session(self.spark)

    # ══════════════════════════════════════════════════════════════════════
    # STEP 1 — FIND EXCEL FILE
    # ══════════════════════════════════════════════════════════════════════
    def find_excel_file(self):
        print(f"\n{'='*80}")
        print(f"[STEP 1] Find Excel File — Searching under 01-raw/{self.year}/.../competitivite/positionnement/")
        print(f"{'='*80}")

        objects = self.minio_client.list_objects(
            self.minio_config.bucket_raw,
            prefix=f"{self.year}/",
            recursive=True,
        )
        excel_files = [
            obj.object_name for obj in objects
            if obj.object_name.endswith(('.xlsx', '.xls'))
            and 'competitivite/positionnement' in obj.object_name
            and not obj.object_name.split('/')[-1].startswith('~')
        ]

        if not excel_files:
            raise FileNotFoundError(
                f"No Excel files found under 01-raw/{self.year}/.../competitivite/positionnement/"
            )

        excel_files.sort(reverse=True)
        self.input_file_key = excel_files[0]
        self.full_input_s3_path = f"s3a://{self.minio_config.bucket_raw}/{self.input_file_key}"

        logger.info(f"✅ Found Excel file: {self.input_file_key}")
        print(f"  ✅ STEP 1 complete — File found: {self.input_file_key}")
        return self.input_file_key

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2 — EXCEL INGESTION  (+ STEP 3 title detection folded in)
    # ══════════════════════════════════════════════════════════════════════
    def extract_data_from_minio(self):
        print(f"\n[STEP 2] Excel Ingestion — Reading sheet from: {self.input_file_key.split('/')[-1]}")

        response = self.minio_client.get_object(
            self.minio_config.bucket_raw, self.input_file_key
        )
        self.excel_data_bytes = BytesIO(response.read())

        wb = load_workbook(self.excel_data_bytes, read_only=True)
        self.sheet_name = next(
            (n for n in wb.sheetnames if 'positionnement' in n.lower()),
            wb.sheetnames[0],
        )

        self.excel_data_bytes.seek(0)
        self.raw_data = pd.read_excel(
            self.excel_data_bytes, sheet_name=self.sheet_name, header=None
        )

        row_count = len(self.raw_data)
        col_count = len(self.raw_data.columns)
        logger.info(f"📥 Raw Excel sheet '{self.sheet_name}': {row_count} rows × {col_count} columns")

        # ── Raw Excel schema for Marquez ─────────────────────────────────
        self._excel_raw_schema_fields = [
            {
                "name": f"col_{i}",
                "type": "string",
                "description": f"Raw Excel column index {i} from sheet '{self.sheet_name}'",
            }
            for i in range(col_count)
        ]

        # ── Convert to Spark for the emit (Pandas → Spark) ───────────────
        raw_spark = self.spark.createDataFrame(
            self.raw_data.astype(str).rename(columns=lambda x: f"col_{x}")
        )

        # ── Column lineage ────────────────────────────────────────────────
        column_lineage_t2 = {
            f"col_{i}": {
                "inputFields": [
                    {
                        "namespace": resolve_namespace(self.full_input_s3_path),
                        "name": self.full_input_s3_path,
                        "field": f"col_{i}",
                    }
                ],
                "transformationDescription": f"Raw Excel column index {i} — read as-is from sheet '{self.sheet_name}'",
                "transformationType": "DIRECT",
            }
            for i in range(col_count)
        }

        # 📡 MARQUEZ — STEP 2
        emit_marquez_step(
            spark_df=raw_spark,
            step_name="02_Excel_Ingestion",
            description=(
                f"Read Excel file '{self.input_file_key.split('/')[-1]}' "
                f"from sheet '{self.sheet_name}' ({row_count} rows × {col_count} columns). "
                f"Loaded as raw Pandas DataFrame (header=None). "
                f"Sheet selected by matching 'positionnement' in sheet name."
            ),
            trans_type="EXTRACT",
            inputs=[self.full_input_s3_path],
            outputs=["memory://spark_df/competitivite/raw_excel"],
            input_schema_fields=self._excel_raw_schema_fields,
            column_lineage=column_lineage_t2,
        )
        print(f"  ✅ STEP 2 complete — {row_count} rows × {col_count} columns ingested")
        return self.raw_data

    # ══════════════════════════════════════════════════════════════════════
    # STEP 3 — TITLE DETECTION
    # ══════════════════════════════════════════════════════════════════════
    def identify_titles(self):
        print(f"\n[STEP 3] Title Detection — Identifying section headers in sheet '{self.sheet_name}'")

        self.excel_data_bytes.seek(0)
        ws = load_workbook(self.excel_data_bytes)[self.sheet_name]
        self.title_sections = []

        for r in range(1, ws.max_row + 1):
            val_a = ws.cell(row=r, column=1).value
            val_b = ws.cell(row=r, column=2).value

            val_a_clean = clean_roman_prefix(str(val_a).strip()) if isinstance(val_a, str) else ''

            logger.info(f"  ✅ ✅ ℹ️ Row {r}: Column A={repr(val_a)}, Column B={repr(val_b)}, Cleaned Title={repr(val_a_clean)}")

            if val_a_clean and (val_b is None or val_b == ''):
                if (
                    len(val_a_clean) >= self.detection_config['min_text_length']
                    and val_a_clean not in self.detection_config['excluded_texts']
                ):
                    self.title_sections.append({'row_index': r - 1, 'title': val_a_clean})

        logger.info(f"Detected {len(self.title_sections)} titles: {[t['title'] for t in self.title_sections]}")

        # ── Build a minimal Spark DF to represent the title metadata ─────
        titles_spark = self.spark.createDataFrame(
            [{"row_index": t["row_index"], "title": t["title"]} for t in self.title_sections]
        )

        # ── Column lineage ────────────────────────────────────────────────
        column_lineage_t3 = {
            "row_index": {
                "inputFields": [
                    {
                        "namespace": resolve_namespace("memory://spark_df/competitivite/raw_excel"),
                        "name": "memory://spark_df/competitivite/raw_excel",
                        "field": "col_0",
                    }
                ],
                "transformationDescription": (
                    "Row index (0-based) of the detected section title in the raw Excel sheet. "
                    "Detected when col_0 is non-empty after roman numeral prefix stripping "
                    "and col_1 is null/empty."
                ),
                "transformationType": "DIRECT",
            },
            "title": {
                "inputFields": [
                    {
                        "namespace": resolve_namespace("memory://spark_df/competitivite/raw_excel"),
                        "name": "memory://spark_df/competitivite/raw_excel",
                        "field": "col_0",
                    }
                ],
                "transformationDescription": (
                    "Section title extracted from col_0 after clean_roman_prefix() stripping. "
                    f"Excluded values: {self.detection_config['excluded_texts']}. "
                    f"Minimum length: {self.detection_config['min_text_length']} chars."
                ),
                "transformationType": "DIRECT",
            },
        }

        # 📡 MARQUEZ — STEP 3
        emit_marquez_step(
            spark_df=titles_spark,
            step_name="03_Title_Detection",
            description=(
                f"Detected {len(self.title_sections)} section headers in sheet '{self.sheet_name}'. "
                f"Rule: col_0 non-empty after roman prefix strip AND col_1 null/empty "
                f"AND length >= {self.detection_config['min_text_length']} "
                f"AND not in {self.detection_config['excluded_texts']}. "
                f"Titles found: {[t['title'] for t in self.title_sections]}."
            ),
            trans_type="TRANSFORMATION",
            inputs=["memory://spark_df/competitivite/raw_excel"],
            outputs=["memory://spark_df/competitivite/title_sections"],
            column_lineage=column_lineage_t3,
        )
        print(f"  ✅ STEP 3 complete — {len(self.title_sections)} section titles detected")
        return self.title_sections

    # ══════════════════════════════════════════════════════════════════════
    # STEP 4 — TRANSFORM DATA (wide → long, Score rows)
    # ══════════════════════════════════════════════════════════════════════
    def transform_data(self):
        print(f"\n[STEP 4] Transform Data — Unpivoting wide Excel layout to (pays, periode, variable, valeur)")

        df, all_records = self.raw_data, []

        for i, section in enumerate(self.title_sections):
            start = section['row_index']
            end   = self.title_sections[i+1]['row_index'] if i < len(self.title_sections)-1 else len(df)
            sec_df = df.iloc[start:end, :].copy().reset_index(drop=True)

            years_row_idx = None
            for r_idx in range(min(10, len(sec_df))):
                row = sec_df.iloc[r_idx]
                years = [
                    int(float(v)) for v in row[1:6]
                    if pd.notna(v) and str(v).replace('.0', '').isdigit()
                    and 2000 <= float(v) <= 2030
                ]
                if len(years) >= 1:
                    years_row_idx = r_idx
                    break

            if years_row_idx is None:
                continue

            years          = [int(float(y)) for y in sec_df.iloc[years_row_idx, 1:15] if pd.notna(y)]
            countries_data = sec_df.iloc[years_row_idx+1:, :]

            for _, row in countries_data.iterrows():
                country = row[0]
                if pd.isna(country) or str(country).strip() in self.detection_config['excluded_texts']:
                    continue
                for year, score in zip(years, row[1:15]):
                    if pd.notna(score):
                        all_records.append({
                            'pays':     str(country).strip(),
                            'periode':    int(year),
                            'variable': section['title'].strip() + ' - Score',
                            'valeur':   float(score),
                        })

        self.transformed_data = pd.DataFrame(all_records).drop_duplicates(
            subset=['pays', 'periode', 'variable']
        )

        record_count = len(self.transformed_data)
        logger.info(f"📊 Transformed: {record_count} records across {self.transformed_data['periode'].nunique()} years")

        # ── Convert Pandas → Spark for the emit ──────────────────────────
        transformed_spark = self.spark.createDataFrame(self.transformed_data)

        # ── Column lineage ────────────────────────────────────────────────
        column_lineage_t4 = {
            "pays": {
                "inputFields": [
                    {
                        "namespace": resolve_namespace("memory://spark_df/competitivite/raw_excel"),
                        "name": "memory://spark_df/competitivite/raw_excel",
                        "field": "col_0",
                    }
                ],
                "transformationDescription": (
                    "Country name extracted from col_0 of each data row within a section. "
                    "Rows where col_0 is null or in excluded_texts are skipped."
                ),
                "transformationType": "DIRECT",
            },
            "periode": {
                "inputFields": [
                    {
                        "namespace": resolve_namespace("memory://spark_df/competitivite/raw_excel"),
                        "name": "memory://spark_df/competitivite/raw_excel",
                        "field": "col_1",
                    }
                ],
                "transformationDescription": (
                    "Year extracted from the years header row of each section (col_1 to col_14). "
                    "Detected by finding a row where values are integers in [2000, 2030]."
                ),
                "transformationType": "DIRECT",
            },
            "variable": {
                "inputFields": [
                    {
                        "namespace": resolve_namespace("memory://spark_df/competitivite/title_sections"),
                        "name": "memory://spark_df/competitivite/title_sections",
                        "field": "title",
                    }
                ],
                "transformationDescription": (
                    "Section title concatenated with ' - Score' suffix. "
                    "Example: 'Compétitivité globale' → 'Compétitivité globale - Score'."
                ),
                "transformationType": "DIRECT",
            },
            "valeur": {
                "inputFields": [
                    {
                        "namespace": resolve_namespace("memory://spark_df/competitivite/raw_excel"),
                        "name": "memory://spark_df/competitivite/raw_excel",
                        "field": "col_1",
                    }
                ],
                "transformationDescription": (
                    "Score value from cells col_1 to col_14 on each country row. "
                    "Null values are skipped. Cast to float."
                ),
                "transformationType": "DIRECT",
            },
        }

        # 📡 MARQUEZ — STEP 4
        emit_marquez_step(
            spark_df=transformed_spark,
            step_name="04_Transform_Data",
            description=(
                f"Unpivoted wide Excel layout → long format (pays, periode, variable, valeur). "
                f"Iterated over {len(self.title_sections)} detected sections. "
                f"For each section: located years header row (integers in [2000, 2030]), "
                f"iterated country rows, zipped with years to produce score records. "
                f"Variable = section title + ' - Score'. "
                f"drop_duplicates on (pays, periode, variable). "
                f"Result: {record_count} records."
            ),
            trans_type="TRANSFORMATION",
            inputs=[
                "memory://spark_df/competitivite/raw_excel",
                "memory://spark_df/competitivite/title_sections",
            ],
            outputs=["memory://spark_df/competitivite/transformed"],
            column_lineage=column_lineage_t4,
        )
        print(f"  ✅ STEP 4 complete — {record_count} Score records produced")
        return self.transformed_data

    # ══════════════════════════════════════════════════════════════════════
    # STEP 5 — CALCULATE RANKINGS
    # ══════════════════════════════════════════════════════════════════════
    def calculate_rankings(self):
        print(f"\n[STEP 5] Calculate Rankings — Generating '- Rang' rows via Spark window function")

        # Convert Pandas DataFrame to Spark
        spark_df = self.spark.createDataFrame(self.transformed_data)

        # Extract base indicator name by stripping the " - Score" suffix
        spark_df = spark_df.withColumn(
            "base_variable",
            F.regexp_replace(F.col("variable"), r" - Score$", "")
        )

        # Define ranking window per base indicator and year (descending valeur)
        window_spec = Window.partitionBy("base_variable", "periode").orderBy(F.col("valeur").desc())
        spark_df    = spark_df.withColumn("rang_value", F.rank().over(window_spec))

        # ── Score rows: keep variable as-is, drop helper columns ─────────
        df_scores = spark_df.drop("base_variable", "rang_value")

        # ── Rang rows: new rows with " - Rang" variable and rank as valeur
        df_rangs = spark_df.select(
            F.col("pays"),
            F.col("periode"),
            F.concat(F.col("base_variable"), F.lit(" - Rang")).alias("variable"),
            F.col("rang_value").cast("double").alias("valeur"),
        )

        # ── Metadata columns injected on both Score and Rang rows ─────────
        meta_cols = {
            "base":         F.lit(None).cast("string"),
            "version":      F.lit(None).cast("string"),
            "source":       F.lit("competitivité positionnement"),
            "dim_id":       F.lit(None).cast("string"),
            "dim_key":      F.lit(None).cast("string"),
            "code_secteur": F.lit(None).cast("string"),
            "lib_secteur":  F.lit(None).cast("string"),
        }
        for col_name, col_expr in meta_cols.items():
            df_scores = df_scores.withColumn(col_name, col_expr)
            df_rangs  = df_rangs.withColumn(col_name, col_expr)

        # Cast periode to int on both sides
        df_scores = df_scores.withColumn("periode", F.col("periode").cast(StringType()))
        df_rangs  = df_rangs.withColumn("periode",  F.col("periode").cast(StringType()))

        # Union Score rows and Rang rows
        self.ranked_data = df_scores.unionByName(df_rangs)

        ranked_count = self.ranked_data.count()
        logger.info(f"Rankings calculated and merged. Total rows: {ranked_count}")

        # ── Column lineage ────────────────────────────────────────────────
        _transformed_path = "memory://spark_df/competitivite/transformed"
        _ns = resolve_namespace(_transformed_path)

        added_meta = {
            "base":         "Pipeline-injected: hardcoded null (no base year for this source)",
            "version":      "Pipeline-injected: hardcoded null (no version for this source)",
            "source":       "Pipeline-injected: hardcoded constant = 'competitivité positionnement'",
            "dim_id":       "Pipeline-injected: hardcoded null (no dimension mapping at this stage)",
            "dim_key":      "Pipeline-injected: hardcoded null (no dimension key at this stage)",
            "code_secteur": "Pipeline-injected: hardcoded null (no sector code for this source)",
            "lib_secteur":  "Pipeline-injected: hardcoded null (no sector label for this source)",
        }

        column_lineage_t5 = {
            "pays": {
                "inputFields": [{"namespace": _ns, "name": _transformed_path, "field": "pays"}],
                "transformationDescription": "Directly passed through from transformed DataFrame",
                "transformationType": "DIRECT",
            },
            "periode": {
                "inputFields": [{"namespace": _ns, "name": _transformed_path, "field": "periode"}],
                "transformationDescription": "Directly passed through. Cast to int.",
                "transformationType": "DIRECT",
            },
            "variable": {
                "inputFields": [{"namespace": _ns, "name": _transformed_path, "field": "variable"}],
                "transformationDescription": (
                    "Score rows: kept as-is (e.g. 'Indicateur - Score'). "
                    "Rang rows: base_variable (regexp_replace removing ' - Score') + ' - Rang' "
                    "via CONCAT. Both sets unioned via unionByName."
                ),
                "transformationType": "AGGREGATE",
            },
            "valeur": {
                "inputFields": [{"namespace": _ns, "name": _transformed_path, "field": "valeur"}],
                "transformationDescription": (
                    "Score rows: original score value passed through. "
                    "Rang rows: RANK() OVER (PARTITION BY base_variable, periode ORDER BY valeur DESC) "
                    "cast to double. Both sets unioned via unionByName."
                ),
                "transformationType": "AGGREGATE",
            },
        }

        # Pipeline-injected metadata columns
        for col_name, desc_text in added_meta.items():
            column_lineage_t5[col_name] = {
                "inputFields": [],
                "transformationDescription": desc_text,
                "transformationType": "IDENTITY",
            }

        # 📡 MARQUEZ — STEP 5
        emit_marquez_step(
            spark_df=self.ranked_data,
            step_name="05_Calculate_Rankings",
            description=(
                f"Computed rankings from Score rows using Spark window function. "
                f"RANK() OVER (PARTITION BY base_variable, periode ORDER BY valeur DESC). "
                f"Generated new '- Rang' rows with rank as valeur. "
                f"Unioned Score + Rang rows via unionByName. "
                f"Added metadata constants: base=null, version=null, source='competitivité positionnement', "
                f"dim_id=null, dim_key=null, code_secteur=null, lib_secteur=null. "
                f"Total rows after union: {ranked_count}."
            ),
            trans_type="TRANSFORMATION",
            inputs=["memory://spark_df/competitivite/transformed"],
            outputs=["memory://spark_df/competitivite/ranked"],
            column_lineage=column_lineage_t5,
        )
        print(f"  ✅ STEP 5 complete — {ranked_count} rows total (Score + Rang)")
        return self.ranked_data

    # ══════════════════════════════════════════════════════════════════════
    # STEP 6 — DELTA DETECTION & SAVE TO MINIO (per year)
    # ══════════════════════════════════════════════════════════════════════
    def save_to_minio(self):
        print(f"\n{'='*80}")
        print(f"[STEP 6] Delta Detection & Save — Processing by year")
        print(f"{'='*80}")

        spark_df_new = self.ranked_data \
            .withColumn("valeur", F.col("valeur").cast("double"))

        years_rows = spark_df_new.select("periode").distinct().collect()
        years      = sorted([int(row['periode']) for row in years_rows])

        sc         = self.spark.sparkContext
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        Path       = sc._jvm.org.apache.hadoop.fs.Path
        conf       = sc._jsc.hadoopConfiguration()

        for year in years:
            print(f"\n  📅 Processing Year  : {year}")
            df_year_new = spark_df_new.filter(F.col("periode") == year)
            df_year_new.createOrReplaceTempView("v_new_data")

            output_path      = f"s3a://{self.minio_config.bucket_transformed}/{self.output_path}{year}"
            temp_output_path = output_path + "_temp_write"
            print(f"  📂 Output path      : {output_path}")

            history_exists = False
            change_count   = 0

            try:
                df_history = self.spark.read.parquet(output_path)
                df_history.createOrReplaceTempView("v_history")
                history_exists = True
                print(f"  ✅ History found for year {year}")
            except Exception:
                print(f"  ℹ️  No history for {year}. Treating as new dataset.")

            if not history_exists:
                df_to_write = df_year_new \
                    .withColumn("date_chargement", F.current_timestamp()) \
                    .withColumn("version_active",  F.lit(1).cast("int"))

            else:
                delta_query = """
                    SELECT
                        n.*,
                        CURRENT_TIMESTAMP as date_chargement
                    FROM v_new_data n
                    LEFT JOIN (
                        SELECT pays, variable, valeur
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
                    ON  LOWER(TRIM(n.pays))     = LOWER(TRIM(h.pays))
                    AND LOWER(TRIM(n.variable)) = LOWER(TRIM(h.variable))
                    WHERE h.pays IS NULL
                    OR (ROUND(n.valeur, 5) <> ROUND(h.valeur, 5))
                    OR (n.valeur IS NULL AND h.valeur IS NOT NULL)
                    OR (n.valeur IS NOT NULL AND h.valeur IS NULL)
                """

                df_changes   = self.spark.sql(delta_query)
                change_count = df_changes.cache().count()

                if change_count > 0:
                    print(f"  🔄 {change_count} changed row(s) detected")

                    df_changes_latest = df_changes.withColumn("version_active", F.lit(1).cast("int"))
                    df_changes_latest.createOrReplaceTempView("v_changes")

                    df_outdated_keys = self.spark.sql("""
                        SELECT LOWER(TRIM(pays))     as pays_key,
                               LOWER(TRIM(variable)) as variable_key
                        FROM v_changes
                    """)
                    df_outdated_keys.createOrReplaceTempView("v_outdated_keys")

                    df_history_updated = df_history.alias("h").join(
                        df_outdated_keys.alias("k"),
                        (F.lower(F.trim(F.col("h.pays")))     == F.col("k.pays_key")) &
                        (F.lower(F.trim(F.col("h.variable"))) == F.col("k.variable_key")),
                        how="left",
                    ).withColumn(
                        "version_active",
                        F.when(F.col("k.pays_key").isNotNull(), F.lit(0).cast("int"))
                         .otherwise(F.col("h.version_active").cast("int"))
                    ).drop("pays_key", "variable_key")

                    df_to_write = df_history_updated.unionByName(
                        df_changes_latest, allowMissingColumns=True
                    )
                else:
                    print(f"  ✅ No changes detected for year {year}. File left untouched.")
                    df_to_write = None

                df_changes.unpersist()

            if df_to_write is not None:
                # ── Column lineage for the write ──────────────────────────
                _ranked_path = "memory://spark_df/competitivite/ranked"
                _ns_ranked   = resolve_namespace(_ranked_path)

                column_lineage_t6 = {}
                for field in df_to_write.schema.fields:
                    if field.name == "date_chargement":
                        column_lineage_t6[field.name] = {
                            "inputFields": [],
                            "transformationDescription": "Pipeline-injected: CURRENT_TIMESTAMP() at write time",
                            "transformationType": "IDENTITY",
                        }
                    elif field.name == "version_active":
                        column_lineage_t6[field.name] = {
                            "inputFields": [
                                {"namespace": _ns_ranked, "name": _ranked_path, "field": "valeur"}
                            ],
                            "transformationDescription": (
                                "SCD Type 2 flag. "
                                + (
                                    f"New rows: version_active=1. "
                                    f"Old rows matching changed (pays, variable) keys: version_active=0 "
                                    f"(LEFT JOIN on v_outdated_keys + WHEN pays_key IS NOT NULL THEN 0). "
                                    f"{change_count} changed rows detected."
                                    if history_exists
                                    else "New dataset: all rows set to version_active=1."
                                )
                            ),
                            "transformationType": "AGGREGATE",
                        }
                    else:
                        column_lineage_t6[field.name] = {
                            "inputFields": [
                                {"namespace": _ns_ranked, "name": _ranked_path, "field": field.name}
                            ],
                            "transformationDescription": (
                                f"Passed through to Parquet output. "
                                f"CAST to {field.dataType.simpleString()} for schema enforcement."
                            ),
                            "transformationType": "DIRECT",
                        }

                # 📡 MARQUEZ — STEP 6 (per year)
                emit_marquez_step(
                    spark_df=df_to_write,
                    step_name=f"06_Delta_Write_Positionnement_{year}",
                    description=(
                        f"Delta detection + Parquet write for 'competitivite/Positionnement', year {year}. "
                        f"History existed: {history_exists}. "
                        + (
                            f"Changed rows detected: {change_count}. "
                            f"Old matching rows set version_active=0 via LEFT JOIN on (pays, variable). "
                            f"New changed rows set version_active=1. "
                            f"Final write = history_updated UNION changed_rows."
                            if history_exists
                            else "No history — full write. All rows version_active=1."
                        )
                    ),
                    trans_type="LOAD",
                    inputs=["memory://spark_df/competitivite/ranked"],
                    outputs=[output_path],
                    column_lineage=column_lineage_t6,
                )

                df_to_write.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

                print(f"  📊 FINAL DATAFRAME SCHEMA (Detailed)")
                for field in df_to_write.schema.fields:
                    print(
                        f"     Column: {field.name} | "
                        f"Type: {field.dataType.simpleString()} | "
                        f"Nullable: {field.nullable}"
                    )
                print(f"     📊 Total columns: {len(df_to_write.columns)}")

                try:
                    target_uri = sc._jvm.java.net.URI(output_path)
                    fs = FileSystem.get(target_uri, conf)
                    if fs.exists(Path(output_path)):
                        fs.delete(Path(output_path), True)
                    fs.rename(Path(temp_output_path), Path(output_path))
                    print(f"  ✅ Year {year} successfully saved.")
                except Exception as e:
                    logger.error(f"  ❌ FS Error: {e}")

            # ── Clean up temp views ───────────────────────────────────────
            self.spark.catalog.dropTempView("v_new_data")
            if history_exists:
                self.spark.catalog.dropTempView("v_history")
            for view in ["v_changes", "v_outdated_keys"]:
                try:
                    self.spark.catalog.dropTempView(view)
                except Exception:
                    pass

        return {'path': self.output_path, 'years': years}

    # ══════════════════════════════════════════════════════════════════════
    # STEP 7 — VALIDATION (read-only, no Marquez emit)
    # ══════════════════════════════════════════════════════════════════════
    def validate_minio_output(self):
        print(f"\n[STEP 7] Validation — Reading back output for quality check")

        base_path = f"s3a://{self.minio_config.bucket_transformed}/{self.output_path}"
        try:
            df_all      = self.spark.read.parquet(f"{base_path}*/")
            total       = df_all.count()
            actives     = df_all.filter(F.col("version_active") == 1).count()
            historiques = df_all.filter(F.col("version_active") == 0).count()

            scores_count = df_all.filter(F.col("variable").endswith("- Score")).count()
            rangs_count  = df_all.filter(F.col("variable").endswith("- Rang")).count()

            logger.info(f"✅ Full dataset validated: {total:,} records total.")
            logger.info(f"   └── 🟢 version_active = 1 : {actives:,} active rows")
            logger.info(f"   └── 🔴 version_active = 0 : {historiques:,} historical rows")
            logger.info(f"   └── 📊 Variables ending in '- Score' : {scores_count:,}")
            logger.info(f"   └── 🏅 Variables ending in '- Rang'  : {rangs_count:,}")
        except Exception:
            logger.warning("⚠️ Validation skipped (dataset might be empty or years missing).")


# =====================================================
# ENTRYPOINT
# =====================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",  required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--day",   required=True)
    args = parser.parse_args()

    processor = CompetitifScoresProcessor(
        year=args.year,
        month=args.month,
        day=args.day,
    )
    processor.run()


if __name__ == "__main__":
    main()
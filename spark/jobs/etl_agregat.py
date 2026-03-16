from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, lit, desc, row_number
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from datetime import datetime
import uuid
import logging
import os
from common.spark_session import create_spark_session, stop_spark_session

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("INS_ENRICH")

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
# SPARK SESSION
# =====================================================
spark = create_spark_session("INS-RAW-to-SILVER-Enrichment")
spark.sparkContext.setLogLevel("WARN")

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
        step_name           : Logical name of the step (e.g. '01_Dimension_Ingestion')
        description         : Human-readable description of what this step does
        trans_type          : Transformation type label (e.g. 'EXTRACT', 'TRANSFORMATION', 'LOAD')
        inputs              : List of input dataset paths/names (source)
        outputs             : List of output dataset paths/names (destination)
        input_schema_fields : Optional list of dicts describing the input schema
                              (used for raw files like CSV where we have no Spark DF yet)
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

        # ── Input schema facet (if provided — e.g. raw CSV file) ─────────────
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
# HADOOP HELPERS
# =====================================================
def find_ins_paths(spark, base_path, source_id, dimension_id):

    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    source_file = None
    dimension_file = None
    stack = [Path(base_path)]

    while stack:
        current = stack.pop()
        try:
            for status in fs.listStatus(current):
                p = status.getPath().toString()
                if status.isDirectory():
                    stack.append(status.getPath())
                elif status.isFile() and p.endswith(".csv"):
                    file_name = p.split("/")[-1]
                    if file_name.startswith(source_id):
                        source_file = p
                        log.info(f"✅ Found FACT file: {source_file}")
                    if file_name.startswith(dimension_id):
                        dimension_file = p
                        log.info(f"✅ Found DIMENSION file: {dimension_file}")
        except Exception:
            pass
        if source_file and dimension_file:
            break

    return source_file, dimension_file


def find_latest_ins_paths(spark, base_path, source_id, dimension_id):
    """Cherche les fichiers dans le dossier le plus récent (par année/mois)."""
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    source_candidates    = []
    dimension_candidates = []
    stack = [Path(base_path)]

    while stack:
        current = stack.pop()
        try:
            for status in fs.listStatus(current):
                p = status.getPath().toString()
                if status.isDirectory():
                    stack.append(status.getPath())
                elif status.isFile() and p.endswith(".csv"):
                    file_name = p.split("/")[-1]
                    # Format attendu : s3a://01-raw/YYYY/MM/...
                    parts = p.replace("s3a://", "").split("/")
                    if len(parts) >= 4:
                        try:
                            year  = int(parts[1])
                            month = int(parts[2])
                        except ValueError:
                            continue
                        if file_name.startswith(source_id):
                            source_candidates.append((year, month, p))
                        if file_name.startswith(dimension_id):
                            dimension_candidates.append((year, month, p))
        except Exception:
            pass

    latest_source    = max(source_candidates,    default=None)
    latest_dimension = max(dimension_candidates, default=None)

    source_file    = latest_source[2]    if latest_source    else None
    dimension_file = latest_dimension[2] if latest_dimension else None

    if source_file:
        log.info(f"✅ Latest FACT file selected: {source_file}")
    if dimension_file:
        log.info(f"✅ Latest DIMENSION file selected: {dimension_file}")

    return source_file, dimension_file


def extract_category_path(full_s3_path, raw_bucket_root):
    """
    Extrait le chemin de catégorie depuis le chemin S3 complet.
    Exemple : s3a://01-raw/2026/02/INS/API-Sources/file.csv → INS/API-Sources
    """
    clean_path = full_s3_path.replace("s3a://", "").strip("/")
    clean_root = raw_bucket_root.replace("s3a://", "").strip("/")

    if clean_path.startswith(clean_root):
        relative_path = clean_path[len(clean_root):].strip("/")
        parts = relative_path.split("/")
        # Structure : YEAR/MONTH/source/SUBsource/.../filename.csv
        # On garde  : source/SUBsource/... (sans année, mois et nom de fichier)
        if len(parts) > 3:
            return "/".join(parts[2:-1])

    return "UNKNOWN_CATEGORY"


# =====================================================
# IDs DES SOURCES ET DIMENSIONS
# =====================================================
SOURCE_ID    = "OBJ11288479"
DIMENSION_ID = "OBJ11288499"

# =====================================================
# PATH RESOLUTION
# =====================================================
RAW_ROOT    = "s3a://01-raw"
SILVER_BASE = "s3a://02-transformed/INS/Agregat"

log.info("🔍 Searching for INS Source / Dimension files...")
FACT_FILE, DIM_FILE = find_latest_ins_paths(spark, RAW_ROOT, SOURCE_ID, DIMENSION_ID)

if not FACT_FILE or not DIM_FILE:
    raise RuntimeError(
        f"❌ INS Source file (ID={SOURCE_ID}) or Dimension file not found under {RAW_ROOT}"
    )

# ══════════════════════════════════════════════════════════════════════
# TASK 1 — DIMENSION INGESTION
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"[TASK 1] Dimension Ingestion — Reading: {DIM_FILE.split('/')[-1]}")
print(f"{'='*80}")

dim_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DIM_FILE)
)

dim_lookup = (
    dim_df
    .select(
        col("dimension_id"),
        col("KEY").alias("dim_indicator_key"),
        col("FULLNAME").alias("indicator_name"),
    )
    .dropDuplicates()
)

dim_count = dim_lookup.count()
log.info(f"📘 DIMENSION lookup rows = {dim_count}")
log.info(f"DIM columns = {dim_df.columns}")
dim_lookup.show(5, truncate=False)

# ── Raw CSV schema for Marquez input ─────────────────────────────────
dim_raw_schema_fields = [
    {
        "name": c,
        "type": "string",
        "description": f"Column '{c}' read directly from dimension CSV file",
    }
    for c in dim_df.columns
]

# ── Column lineage ────────────────────────────────────────────────────
dim_column_lineage = {
    "dimension_id": {
        "inputFields": [{"namespace": resolve_namespace(DIM_FILE), "name": DIM_FILE, "field": "dimension_id"}],
        "transformationDescription": "Directly mapped from dimension CSV",
        "transformationType": "DIRECT",
    },
    "dim_indicator_key": {
        "inputFields": [{"namespace": resolve_namespace(DIM_FILE), "name": DIM_FILE, "field": "KEY"}],
        "transformationDescription": "Renamed from 'KEY' in dimension CSV",
        "transformationType": "DIRECT",
    },
    "indicator_name": {
        "inputFields": [{"namespace": resolve_namespace(DIM_FILE), "name": DIM_FILE, "field": "FULLNAME"}],
        "transformationDescription": "Renamed from 'FULLNAME' in dimension CSV",
        "transformationType": "DIRECT",
    },
}

# 📡 MARQUEZ — TASK 1
emit_marquez_step(
    spark_df=dim_lookup,
    step_name="01_Dimension_Ingestion",
    description=(
        f"Read dimension CSV '{DIM_FILE.split('/')[-1]}' ({dim_count} rows). "
        f"Selected: dimension_id, KEY→dim_indicator_key, FULLNAME→indicator_name. "
        f"Applied dropDuplicates()."
    ),
    trans_type="EXTRACT",
    inputs=[DIM_FILE],
    outputs=["memory://spark_df/dim_lookup"],
    input_schema_fields=dim_raw_schema_fields,
    column_lineage=dim_column_lineage,
)
print(f"  ✅ TASK 1 complete — Dimension lookup ready ({dim_count} rows)")


# =====================================================
# PROCESS FACT FILE
# =====================================================
fact_files = [FACT_FILE]
log.info(f"📄 Found 1 FACT file: {FACT_FILE}")

for fact_file in fact_files:

    file_name       = fact_file.split("/")[-1].replace(".csv", "")
    source_category = extract_category_path(fact_file, RAW_ROOT)

    print(f"\n{'='*80}")
    print(f"🚀 Processing FACT file : {file_name}")
    print(f"📂 Source category     : {source_category}")
    print(f"{'='*80}")

    # ══════════════════════════════════════════════════════════════════════
    # TASK 2 — FACT FILE INGESTION
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[TASK 2] Fact Ingestion — Reading: {file_name}.csv")

    fact_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(fact_file)
    )

    fact_count = fact_df.count()
    log.info(f"📥 {file_name} rows = {fact_count}")
    fact_df.show(5, truncate=False)

    # ── Raw CSV schema for Marquez input ─────────────────────────────
    fact_raw_schema_fields = [
        {
            "name": c,
            "type": "string",
            "description": f"Column '{c}' read directly from fact CSV file",
        }
        for c in fact_df.columns
    ]

    # ── Column lineage ────────────────────────────────────────────────
    fact_column_lineage_t2 = {
        c: {
            "inputFields": [{"namespace": resolve_namespace(fact_file), "name": fact_file, "field": c}],
            "transformationDescription": "Directly mapped from fact CSV column",
            "transformationType": "DIRECT",
        }
        for c in fact_df.columns
    }

    # 📡 MARQUEZ — TASK 2
    emit_marquez_step(
        spark_df=fact_df,
        step_name="02_Fact_Ingestion",
        description=(
            f"Read fact CSV '{file_name}.csv' ({fact_count} rows). "
            f"Columns: {', '.join(fact_df.columns)}."
        ),
        trans_type="EXTRACT",
        inputs=[fact_file],
        outputs=[f"memory://spark_df/{file_name}/raw"],
        input_schema_fields=fact_raw_schema_fields,
        column_lineage=fact_column_lineage_t2,
    )
    print(f"  ✅ TASK 2 complete — {fact_count} rows ingested")

    # ══════════════════════════════════════════════════════════════════════
    # TASK 3 — JOIN FACT × DIMENSION
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[TASK 3] Enrichment — Joining fact × dimension")

    fact_dim_id_col  = f"{DIMENSION_ID}_id"
    fact_dim_key_col = f"{DIMENSION_ID}_key"

    enriched_df = (
        fact_df.alias("f")
        .join(
            dim_lookup.alias("d"),
            on=[
                col(f"f.{fact_dim_id_col}") == col("d.dimension_id"),
                col(f"f.{fact_dim_key_col}") == col("d.dim_indicator_key"),
            ],
            how="left",
        )
        .drop(col("d.dimension_id"))
        .drop(col("d.dim_indicator_key"))
        .withColumn("date_chargement", current_timestamp())
    )

    enriched_count = enriched_df.count()
    log.info(f"🔗 {file_name} enriched rows = {enriched_count}")
    enriched_df.show(5, truncate=False)

    # ── Column lineage ────────────────────────────────────────────────
    column_lineage_t3 = {}

    # All fact columns — passed through from raw
    for c in fact_df.columns:
        column_lineage_t3[c] = {
            "inputFields": [
                {"namespace": resolve_namespace(f"memory://spark_df/{file_name}/raw"), "field": c}
            ],
            "transformationDescription": "Directly mapped from fact raw DataFrame",
            "transformationType": "DIRECT",
        }

    # Dimension column brought in by the JOIN
    column_lineage_t3["indicator_name"] = {
        "inputFields": [
            {"namespace": resolve_namespace("memory://spark_df/dim_lookup"), "name": "memory://spark_df/dim_lookup", "field": "indicator_name"}
        ],
        "transformationDescription": (
            f"Brought in via LEFT JOIN on "
            f"(f.{fact_dim_id_col}=d.dimension_id AND f.{fact_dim_key_col}=d.dim_indicator_key). "
            f"Originates from FULLNAME column of dimension CSV."
        ),
        "transformationType": "DIRECT",
    }

    # Pipeline-injected timestamp
    column_lineage_t3["date_chargement"] = {
        "inputFields": [],
        "transformationDescription": "Pipeline-injected: current timestamp at load time (current_timestamp())",
        "transformationType": "IDENTITY",
    }

    # 📡 MARQUEZ — TASK 3
    emit_marquez_step(
        spark_df=enriched_df,
        step_name="03_Fact_Dimension_Join",
        description=(
            f"LEFT JOIN fact '{file_name}.csv' × dimension '{DIM_FILE.split('/')[-1]}' "
            f"on ({fact_dim_id_col}=dimension_id AND {fact_dim_key_col}=dim_indicator_key). "
            f"Dropped duplicate join keys: dimension_id, dim_indicator_key. "
            f"Added date_chargement=current_timestamp(). "
            f"Result: {enriched_count} rows, {len(enriched_df.columns)} columns."
        ),
        trans_type="TRANSFORMATION",
        inputs=[
            f"memory://spark_df/{file_name}/raw",
            "memory://spark_df/dim_lookup",
        ],
        outputs=[f"memory://spark_df/{file_name}/enriched"],
        column_lineage=column_lineage_t3,
    )
    print(f"  ✅ TASK 3 complete — {enriched_count} rows after join")

    # ══════════════════════════════════════════════════════════════════════
    # TASK 4 — FINAL STRUCTURE + BUSINESS COLUMNS
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[TASK 4] Final Structure — Renaming, casting and adding business columns")

    structured_df = (
        enriched_df
        .withColumnRenamed(fact_dim_id_col,  "dim_id")
        .withColumn("dim_id",  col("dim_id").cast(StringType()))
        .withColumnRenamed(fact_dim_key_col, "dim_key")
        .withColumn("dim_key", col("dim_key").cast(StringType()))
        .withColumnRenamed("year",           "periode")
        .withColumn("periode",col("periode").cast(StringType()))
        .withColumnRenamed("value",          "valeur")
        .withColumnRenamed("indicator_name", "Variable")
        .withColumn("version",      lit("NA"))
        .withColumn("pays",         lit("Tunisie"))
        .withColumn("lib_secteur",  lit("NA"))
        .withColumn("code_secteur", lit("NA"))
        .withColumn("base",         lit("2015"))
        .withColumn("source",       lit("INS"))
        .select(
            "periode",
            "dim_id",
            "dim_key",
            "Variable",
            "valeur",
            "version",
            "base",
            "source",
            "code_secteur",
            "lib_secteur",
            "pays",
            "date_chargement",
        )
    )

    log.info("************************ FINAL STRUCTURED ******************")
    structured_df.show(5, truncate=False)

    # ── Column lineage ────────────────────────────────────────────────
    added_cols_t4 = {
        "version":      "Pipeline-injected: hardcoded constant = 'N/A'",
        "pays":         "Pipeline-injected: hardcoded constant = 'Tunisie'",
        "lib_secteur":  "Pipeline-injected: hardcoded constant = 'N/A'",
        "code_secteur": "Pipeline-injected: hardcoded constant = 'N/A'",
        "base":         "Pipeline-injected: hardcoded constant = '2015'",
        "source":       "Pipeline-injected: derived from extract_category_path() on fact file S3 path",
    }

    renamed_cols_t4 = {
        "dim_id":   (fact_dim_id_col,  f"Renamed from '{fact_dim_id_col}', cast to StringType()"),
        "dim_key":  (fact_dim_key_col, f"Renamed from '{fact_dim_key_col}', cast to StringType()"),
        "periode":    ("year",           "Renamed from 'year' in fact CSV"),
        "valeur":   ("value",          "Renamed from 'value' in fact CSV"),
        "Variable": ("indicator_name", "Renamed from 'indicator_name' (dimension FULLNAME) brought in by JOIN"),
    }

    column_lineage_t4 = {}
    for field in structured_df.schema.fields:
        col_name = field.name
        if col_name in added_cols_t4:
            column_lineage_t4[col_name] = {
                "inputFields": [],
                "transformationDescription": added_cols_t4[col_name],
                "transformationType": "IDENTITY",
            }
        elif col_name in renamed_cols_t4:
            source_field, desc_text = renamed_cols_t4[col_name]
            _enriched_path = f"memory://spark_df/{file_name}/enriched"
            column_lineage_t4[col_name] = {
                "inputFields": [
                    {
                        "namespace": resolve_namespace(_enriched_path),
                        "name": _enriched_path,
                        "field": source_field,
                    }
                ],
                "transformationDescription": desc_text,
                "transformationType": "DIRECT",
            }
        else:
            # date_chargement and any other passthrough
            _enriched_path = f"memory://spark_df/{file_name}/enriched"
            column_lineage_t4[col_name] = {
                "inputFields": [
                    {
                        "namespace": resolve_namespace(_enriched_path),
                        "name": _enriched_path,
                        "field": col_name,
                    }
                ],
                "transformationDescription": "Directly passed through from enriched DataFrame",
                "transformationType": "DIRECT",
            }

    # 📡 MARQUEZ — TASK 4
    emit_marquez_step(
        spark_df=structured_df,
        step_name="04_Final_Structure",
        description=(
            f"Final column renaming and business column injection. "
            f"Renamed: {fact_dim_id_col}→dim_id (StringType), {fact_dim_key_col}→dim_key (StringType), "
            f"year→periode, value→valeur, indicator_name→Variable. "
            f"Added constants: version='N/A', pays='Tunisie', lib_secteur='N/A', "
            f"code_secteur='N/A', base='2015', source='{source_category}'. "
            f"Dropped: rang. "
            f"Final columns: {', '.join(structured_df.columns)}."
        ),
        trans_type="TRANSFORMATION",
        inputs=[f"memory://spark_df/{file_name}/enriched"],
        outputs=[f"memory://spark_df/{file_name}/structured"],
        column_lineage=column_lineage_t4,
    )
    print(f"  ✅ TASK 4 complete — Structured DataFrame ready ({len(structured_df.columns)} columns)")

    # ══════════════════════════════════════════════════════════════════════
    # DATA QUALITY CHECK
    # ══════════════════════════════════════════════════════════════════════
    missing = structured_df.filter(col("Variable").isNull()).count()
    if missing > 0:
        raise RuntimeError(f"MISSING_DIMENSIONS::{file_name}::{missing}")
    log.info(f"✅ {file_name} missing Variable = {missing}")

    # ══════════════════════════════════════════════════════════════════════
    # TASK 5 — DELTA DETECTION & PARQUET WRITE (per year)
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[TASK 5] Delta Detection & Write — Processing by year")

    years = [row["periode"] for row in structured_df.select("periode").distinct().collect()]

    sc         = spark.sparkContext
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    conf       = sc._jsc.hadoopConfiguration()

    for y in years:
        df_year     = structured_df.filter(col("periode") == y)
        output_path = os.path.join(SILVER_BASE, str(y))

        print(f"\n  📅 Processing Year  : {y}")
        print(f"  📂 Output path      : {output_path}")

        history_exists = False
        new_count      = 0

        try:
            existing_df = spark.read.parquet(output_path).cache()
            existing_df.count()  # force la lecture immédiate
            history_exists = True
            print(f"  ✅ History found for year {y}, checking new rows...")

            compare_cols = ["periode", "dim_id", "dim_key", "Variable", "valeur", "base", "source"]

            new_rows  = df_year.join(
                existing_df.select(compare_cols),
                on=compare_cols,
                how="left_anti",
            )
            new_count = new_rows.count()

            if new_count == 0:
                print(f"  ✅ No new or changed rows for year {y}")
                existing_df.unpersist()
                continue

            print(f"  🆕 {new_count} new/changed rows detected")

            if "version_active" in existing_df.columns:
                new_rows = new_rows.withColumn("version_active", lit(0).cast("int"))

            final_df = existing_df.unionByName(new_rows)
            existing_df.unpersist()

        except AnalysisException as e:
            print(f"  🆕 No existing file for {y} ({str(e)[:80]}), writing full dataset")
            final_df = df_year

        # ── Recompute version_active on the full final_df ─────────────
        window_spec = Window.partitionBy("periode", "dim_id", "dim_key", "Variable").orderBy(desc("date_chargement"))
        final_df = (
            final_df
            .withColumn("_rank", row_number().over(window_spec))
            .withColumn("version_active", (col("_rank") == 1).cast("int"))
            .drop("_rank")
        )

        # ── Column lineage for the final write ────────────────────────
        column_lineage_t5 = {}
        _structured_path = f"memory://spark_df/{file_name}/structured"
        for field in final_df.schema.fields:
            if field.name == "version_active":
                column_lineage_t5[field.name] = {
                    "inputFields": [
                        {
                            "namespace": resolve_namespace(_structured_path),
                            "name": _structured_path,
                            "field": "date_chargement",
                        }
                    ],
                    "transformationDescription": (
                        "SCD Type 2 flag: ROW_NUMBER() OVER "
                        "(PARTITION BY periode, dim_id, dim_key, Variable ORDER BY date_chargement DESC) "
                        "— rank=1 → version_active=1, others → 0. "
                        + (
                            f"{new_count} new rows appended to existing history via unionByName."
                            if history_exists
                            else "New dataset: version_active computed from scratch, all latest rows = 1."
                        )
                    ),
                    "transformationType": "AGGREGATE",
                }
            else:
                column_lineage_t5[field.name] = {
                    "inputFields": [
                        {
                            "namespace": resolve_namespace(_structured_path),
                            "name": _structured_path,
                            "field": field.name,
                        }
                    ],
                    "transformationDescription": (
                        f"Passed through to Parquet output. "
                        f"CAST to {field.dataType.simpleString()} for schema enforcement."
                    ),
                    "transformationType": "DIRECT",
                }

        # 📡 MARQUEZ — TASK 5
        emit_marquez_step(
            spark_df=final_df,
            step_name=f"05_Delta_Write_{source_category.replace('/','_')}_{y}",
            description=(
                f"Delta detection + Parquet write for '{file_name}', year {y}. "
                f"History existed: {history_exists}. "
                + (
                    f"New/changed rows: {new_count} (left_anti join on compare_cols). "
                    f"Appended to history via unionByName. "
                    f"Recomputed version_active: ROW_NUMBER() OVER "
                    f"(PARTITION BY periode, dim_id, dim_key, Variable ORDER BY date_chargement DESC)."
                    if history_exists
                    else "No history — full write. version_active computed from scratch."
                )
            ),
            trans_type="LOAD",
            inputs=[f"memory://spark_df/{file_name}/structured"],
            outputs=[output_path],
            column_lineage=column_lineage_t5,
        )

        (
            final_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

        print(f"  📊 FINAL DATAFRAME SCHEMA (Detailed)")
        for field in final_df.schema.fields:
            print(
                f"     Column: {field.name} | "
                f"Type: {field.dataType.simpleString()} | "
                f"Nullable: {field.nullable}"
            )
        print(f"     📊 Total columns: {len(final_df.columns)}")
        print(f"  ✅ File written for year {y}")

    print(f"\n{'='*80}")
    print(f"✅ Finished processing: {file_name}")
    print(f"{'='*80}\n")

# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 INS enrichment job completed successfully")
stop_spark_session(spark)
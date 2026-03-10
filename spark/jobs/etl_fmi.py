from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from datetime import datetime
import logging
import uuid
import os
from pyspark.sql.utils import AnalysisException
from common.spark_session import create_spark_session, stop_spark_session

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("FMI_TRANSFORM")

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
spark = create_spark_session("FMI-RAW-to-SILVER")
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
        step_name           : Logical name of the step (e.g. '01_Discover_Files')
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
# PATH CONFIG
# =====================================================
RAW_ROOT    = "s3a://01-raw"
SILVER_BASE = "s3a://02-transformed/FMI"


# =====================================================
# FIND LATEST RAW FOLDER
# =====================================================
def find_latest_fmi_folder(spark, base_path):

    sc = spark.sparkContext
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI        = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    years = []
    for status in fs.listStatus(Path(base_path)):
        if status.isDirectory():
            years.append(status.getPath().getName())

    latest_year = sorted(years)[-1]
    year_path   = f"{base_path}/{latest_year}"

    months = []
    for status in fs.listStatus(Path(year_path)):
        if status.isDirectory():
            months.append(status.getPath().getName())

    latest_month = sorted(months)[-1]
    fmi_path     = f"{year_path}/{latest_month}/fmi"

    log.info(f"📅 Latest RAW folder detected : {fmi_path}")
    return fmi_path


# =====================================================
# FIND CSV FILES
# =====================================================
def find_csv_files(spark, folder_path):

    sc = spark.sparkContext
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI        = sc._jvm.java.net.URI

    fs = FileSystem.get(URI(folder_path), sc._jsc.hadoopConfiguration())

    csv_files = []
    stack     = [Path(folder_path)]

    while stack:
        current = stack.pop()
        try:
            for status in fs.listStatus(current):
                if status.isDirectory():
                    stack.append(status.getPath())
                elif status.isFile():
                    p = status.getPath().toString()
                    if p.endswith(".csv"):
                        csv_files.append(p)
        except Exception:
            pass

    return csv_files


# =====================================================
# GET DATASET TYPE
# =====================================================
def detect_dataset(file_name):
    name = file_name.lower()
    if "reer" in name:
        return "taux_change_effectif_reel"
    elif "neer" in name:
        return "taux_change_effectif_nominal"
    else:
        return "autre"


# =====================================================
# STEP 1 — DISCOVER FILES
# =====================================================
print(f"\n{'='*80}")
print(f"[STEP 1] Discover Files — Scanning latest FMI folder under {RAW_ROOT}")
print(f"{'='*80}")

log.info("🔍 Searching latest FMI folder")

FMI_FOLDER = find_latest_fmi_folder(spark, RAW_ROOT)
csv_files  = find_csv_files(spark, FMI_FOLDER)

if len(csv_files) == 0:
    raise RuntimeError("❌ No FMI CSV files found")

log.info(f"📄 {len(csv_files)} CSV files found")
print(f"  📂 FMI folder : {FMI_FOLDER}")
print(f"  📄 Files found: {len(csv_files)}")
for f in csv_files:
    print(f"     • {f.split('/')[-1]}")

# ── Build a minimal Spark DF to represent discovered files ────────────
discovered_spark = spark.createDataFrame(
    [{"file_path": f, "dataset_type": detect_dataset(f.split("/")[-1])} for f in csv_files]
)

# ── Column lineage ────────────────────────────────────────────────────
column_lineage_t1 = {
    "file_path": {
        "inputFields": [
            {"namespace": resolve_namespace(RAW_ROOT), "name": RAW_ROOT, "field": "file_path"}
        ],
        "transformationDescription": (
            "Full S3 path of each CSV file discovered under the latest FMI folder "
            f"({FMI_FOLDER}). Selected by find_latest_fmi_folder() picking max year/month."
        ),
        "transformationType": "DIRECT",
    },
    "dataset_type": {
        "inputFields": [
            {"namespace": resolve_namespace(RAW_ROOT), "name": RAW_ROOT, "field": "file_path"}
        ],
        "transformationDescription": (
            "Dataset type derived from file name via detect_dataset(): "
            "'reer' → taux_change_effectif_reel, "
            "'neer' → taux_change_effectif_nominal, "
            "else → autre."
        ),
        "transformationType": "DIRECT",
    },
}

# 📡 MARQUEZ — STEP 1
emit_marquez_step(
    spark_df=discovered_spark,
    step_name="01_Discover_FMI_Files",
    description=(
        f"Scanned latest FMI folder under {RAW_ROOT}. "
        f"Selected latest year/month via find_latest_fmi_folder(). "
        f"Found {len(csv_files)} CSV file(s) under {FMI_FOLDER}: "
        f"{[f.split('/')[-1] for f in csv_files]}."
    ),
    trans_type="EXTRACT",
    inputs=[RAW_ROOT],
    outputs=["memory://spark_df/fmi/discovered_files"],
    column_lineage=column_lineage_t1,
)
print(f"  ✅ STEP 1 complete — {len(csv_files)} file(s) discovered")


# =====================================================
# PROCESS FILES
# =====================================================
for file_path in csv_files:

    file_name = file_path.split("/")[-1]
    dataset   = detect_dataset(file_name)

    SILVER_DATASET_PATH = f"{SILVER_BASE}/{dataset}"

    print(f"\n{'='*80}")
    print(f"🚀 Processing file : {file_name}")
    print(f"📊 Dataset type    : {dataset}")
    print(f"{'='*80}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2 — READ CSV
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 2] CSV Ingestion — Reading: {file_name}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    row_count = df.count()
    log.info(f"📥 Rows read = {row_count}")

    # ── Raw CSV schema for Marquez input ─────────────────────────────
    raw_schema_fields = [
        {
            "name": c,
            "type": "string",
            "description": f"Column '{c}' read directly from FMI CSV file '{file_name}'",
        }
        for c in df.columns
    ]

    # ── Column lineage ────────────────────────────────────────────────
    column_lineage_t2 = {
        c: {
            "inputFields": [
                {"namespace": resolve_namespace(file_path), "name": file_path, "field": c}
            ],
            "transformationDescription": f"Directly mapped from CSV column '{c}'",
            "transformationType": "DIRECT",
        }
        for c in df.columns
    }

    # 📡 MARQUEZ — STEP 2
    emit_marquez_step(
        spark_df=df,
        step_name=f"02_CSV_Ingestion_{file_name.replace('.csv', '')}",
        description=(
            f"Read FMI CSV file '{file_name}' ({row_count} rows). "
            f"Dataset type: {dataset}. "
            f"Columns: {', '.join(df.columns)}."
        ),
        trans_type="EXTRACT",
        inputs=[file_path],
        outputs=[f"memory://spark_df/fmi/{file_name}/raw"],
        input_schema_fields=raw_schema_fields,
        column_lineage=column_lineage_t2,
    )
    print(f"  ✅ STEP 2 complete — {row_count} rows ingested")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 3 — TRANSFORMATION
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 3] Transformation — Renaming columns and adding metadata")

    transformed_df = (
        df
        .withColumnRenamed("country_iso3", "pays")
        .withColumnRenamed("year",         "annee")
        .withColumnRenamed("value",        "valeur")
        .withColumnRenamed("indicator",    "variable")
        .withColumn("date_chargement", current_timestamp())
    )

    log.info("🔧 After transformation")
    transformed_df.show(5, truncate=False)

    # ── Column lineage ────────────────────────────────────────────────
    _raw_path = f"memory://spark_df/fmi/{file_name}/raw"
    _ns_raw   = resolve_namespace(_raw_path)

    renamed_cols_t3 = {
        "pays":     ("country_iso3", "Renamed from 'country_iso3' in FMI CSV"),
        "annee":    ("year",         "Renamed from 'year' in FMI CSV"),
        "valeur":   ("value",        "Renamed from 'value' in FMI CSV"),
        "variable": ("indicator",    "Renamed from 'indicator' in FMI CSV"),
    }

    column_lineage_t3 = {}
    for field in transformed_df.schema.fields:
        col_name = field.name
        if col_name == "date_chargement":
            column_lineage_t3[col_name] = {
                "inputFields": [],
                "transformationDescription": "Pipeline-injected: current timestamp at load time (current_timestamp())",
                "transformationType": "IDENTITY",
            }
        elif col_name in renamed_cols_t3:
            source_field, desc_text = renamed_cols_t3[col_name]
            column_lineage_t3[col_name] = {
                "inputFields": [
                    {"namespace": _ns_raw, "name": _raw_path, "field": source_field}
                ],
                "transformationDescription": desc_text,
                "transformationType": "DIRECT",
            }
        else:
            # Any other columns passed through as-is
            column_lineage_t3[col_name] = {
                "inputFields": [
                    {"namespace": _ns_raw, "name": _raw_path, "field": col_name}
                ],
                "transformationDescription": "Directly passed through from raw CSV",
                "transformationType": "DIRECT",
            }

    # 📡 MARQUEZ — STEP 3
    emit_marquez_step(
        spark_df=transformed_df,
        step_name=f"03_Transformation_{file_name.replace('.csv', '')}",
        description=(
            f"Column renaming and metadata injection for '{file_name}'. "
            f"Renamed: country_iso3→pays, year→annee, value→valeur, indicator→variable. "
            f"Added: date_chargement=current_timestamp(). "
            f"Result: {len(transformed_df.columns)} columns."
        ),
        trans_type="TRANSFORMATION",
        inputs=[_raw_path],
        outputs=[f"memory://spark_df/fmi/{file_name}/transformed"],
        column_lineage=column_lineage_t3,
    )
    print(f"  ✅ STEP 3 complete — {len(transformed_df.columns)} columns after renaming")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 4 — DELTA DETECTION & WRITE (per year)
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 4] Delta Detection & Write — Processing by year")

    years = [row["annee"] for row in transformed_df.select("annee").distinct().collect()]

    for y in years:

        df_year     = transformed_df.filter(col("annee") == y)
        output_path = os.path.join(SILVER_DATASET_PATH, str(y))

        print(f"\n  📅 Processing Year  : {y}")
        print(f"  📂 Output path      : {output_path}")

        history_exists = False
        new_count      = 0

        try:
            existing_df    = spark.read.parquet(output_path)
            history_exists = True
            print(f"  ✅ History found for year {y}, checking new rows...")

            compare_cols = ["annee", "pays", "variable", "valeur"]

            new_rows  = df_year.join(
                existing_df.select(compare_cols),
                on=compare_cols,
                how="left_anti",
            )
            new_count = new_rows.count()

            if new_count == 0:
                print(f"  ✅ No new rows for year {y}")
                continue

            print(f"  🆕 {new_count} new row(s) detected")
            final_df = existing_df.unionByName(new_rows)

        except AnalysisException:
            print(f"  🆕 First dataset for year {y}")
            final_df = df_year

        # ── Column lineage for the write ──────────────────────────────
        _transformed_path = f"memory://spark_df/fmi/{file_name}/transformed"
        _ns_transformed   = resolve_namespace(_transformed_path)

        column_lineage_t4 = {}
        for field in final_df.schema.fields:
            column_lineage_t4[field.name] = {
                "inputFields": [
                    {
                        "namespace": _ns_transformed,
                        "name": _transformed_path,
                        "field": field.name,
                    }
                ],
                "transformationDescription": (
                    f"Passed through to Parquet output. "
                    + (
                        f"Delta detected via left_anti join on (annee, pays, variable, valeur). "
                        f"{new_count} new row(s) appended to existing history via unionByName."
                        if history_exists
                        else "New dataset — full write, no history existed."
                    )
                ),
                "transformationType": "DIRECT",
            }

        # 📡 MARQUEZ — STEP 4 (per year)
        emit_marquez_step(
            spark_df=final_df,
            step_name=f"04_Delta_Write_{dataset}_{y}",
            description=(
                f"Delta detection + Parquet write for '{file_name}', dataset '{dataset}', year {y}. "
                f"History existed: {history_exists}. "
                + (
                    f"New rows detected: {new_count} (left_anti join on annee, pays, variable, valeur). "
                    f"Appended to history via unionByName."
                    if history_exists
                    else "No history — full write."
                )
            ),
            trans_type="LOAD",
            inputs=[_transformed_path],
            outputs=[output_path],
            column_lineage=column_lineage_t4,
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
        print(f"  ✅ Data written for year {y}")

    print(f"\n{'='*80}")
    print(f"✅ Finished processing: {file_name}")
    print(f"{'='*80}\n")


# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 FMI transformation completed")
stop_spark_session(spark)
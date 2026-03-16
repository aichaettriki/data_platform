from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number, desc, round as spark_round, coalesce
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import DecimalType
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from datetime import datetime
import logging
import uuid
import os
from pyspark.sql.types import StringType
from common.spark_session import create_spark_session, stop_spark_session

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("WORLD_BANK_TRANSFORM")

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
spark = create_spark_session("WORLD-BANK-RAW-to-TRANSFORMED")
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
TARGET_BASE = "s3a://02-transformed/WORLD_BANK"

# Sentinelle pour remplacer NULL dans les comparaisons de join
# (NULL != NULL dans Spark, donc on utilise une valeur impossible)
NULL_SENTINEL = -99999.999999


# =====================================================
# UTIL FUNCTIONS
# =====================================================
def get_latest_world_bank_path(spark, raw_root):
    sc = spark.sparkContext
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI        = sc._jvm.java.net.URI
    fs = FileSystem.get(URI(raw_root), sc._jsc.hadoopConfiguration())

    years = [
        f.getPath().getName() for f in fs.listStatus(Path(raw_root))
        if f.isDirectory() and f.getPath().getName().isdigit()
    ]
    if not years:
        raise RuntimeError("❌ No year folders found in RAW")
    latest_year = sorted(years)[-1]

    months_path = f"{raw_root}/{latest_year}"
    months = [
        f.getPath().getName() for f in fs.listStatus(Path(months_path))
        if f.isDirectory() and f.getPath().getName().isdigit()
    ]
    if not months:
        raise RuntimeError(f"❌ No month folders found under {latest_year}")
    latest_month = sorted(months)[-1]

    world_bank_path = f"{raw_root}/{latest_year}/{latest_month}/WORLD_BANK"
    if not fs.exists(Path(world_bank_path)):
        raise RuntimeError(f"❌ WORLD_BANK folder not found at {world_bank_path}")

    log.info(f"📅 Latest WORLD_BANK path: {world_bank_path}")
    return world_bank_path


def list_csv_files(spark, base_path):
    sc = spark.sparkContext
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI        = sc._jvm.java.net.URI
    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())

    stack = [Path(base_path)]
    files = []

    while stack:
        current = stack.pop()
        for status in fs.listStatus(current):
            if status.isDirectory():
                path_str = status.getPath().toString().lower()
                if "/countries" in path_str:
                    continue
                stack.append(status.getPath())
            elif status.isFile() and status.getPath().toString().endswith(".csv"):
                files.append(status.getPath().toString())
    return files


# ══════════════════════════════════════════════════════════════════════
# STEP 1 — DISCOVER FILES
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"[STEP 1] Discover Files — Scanning latest WORLD_BANK folder under {RAW_ROOT}")
print(f"{'='*80}")

world_bank_path = get_latest_world_bank_path(spark, RAW_ROOT)
csv_files       = list_csv_files(spark, world_bank_path)

log.info(f"📄 Found {len(csv_files)} WORLD_BANK CSV files")
print(f"  📂 WORLD_BANK folder : {world_bank_path}")
print(f"  📄 Files found       : {len(csv_files)}")
for f in csv_files:
    print(f"     • {f.split('/')[-2]}/{f.split('/')[-1]}")

# ── Build minimal Spark DF to represent discovered files ──────────────
discovered_spark = spark.createDataFrame([
    {
        "file_path":        f,
        "indicator_folder": f.split("/")[-2],
    }
    for f in csv_files
])

# ── Column lineage ────────────────────────────────────────────────────
column_lineage_t1 = {
    "file_path": {
        "inputFields": [
            {"namespace": resolve_namespace(RAW_ROOT), "name": RAW_ROOT, "field": "file_path"}
        ],
        "transformationDescription": (
            "Full S3 path of each CSV file discovered under the latest WORLD_BANK folder. "
            "Selected by get_latest_world_bank_path() picking max(year)/max(month), "
            "then list_csv_files() recursive scan."
        ),
        "transformationType": "DIRECT",
    },
    "indicator_folder": {
        "inputFields": [
            {"namespace": resolve_namespace(RAW_ROOT), "name": RAW_ROOT, "field": "file_path"}
        ],
        "transformationDescription": (
            "Indicator name extracted from the parent folder of each CSV file "
            "(second-to-last path component, e.g. 'GDP', 'HCI')."
        ),
        "transformationType": "DIRECT",
    },
}

# 📡 MARQUEZ — STEP 1
emit_marquez_step(
    spark_df=discovered_spark,
    step_name="01_Discover_WorldBank_Files",
    description=(
        f"Scanned {RAW_ROOT} for latest WORLD_BANK folder. "
        f"Selected: {world_bank_path}. "
        f"Found {len(csv_files)} CSV file(s): "
        f"{[f.split('/')[-2] + '/' + f.split('/')[-1] for f in csv_files]}."
    ),
    trans_type="EXTRACT",
    inputs=[RAW_ROOT],
    outputs=["memory://spark_df/world_bank/transform/discovered_files"],
    column_lineage=column_lineage_t1,
)
print(f"  ✅ STEP 1 complete — {len(csv_files)} file(s) discovered")


# =====================================================
# PROCESS FILES
# =====================================================
for file_path in csv_files:

    file_name        = file_path.split("/")[-1]
    indicator_folder = file_path.split("/")[-2]

    print(f"\n{'='*80}")
    print(f"🚀 Processing : {indicator_folder}/{file_name}")
    print(f"{'='*80}")

    log.info("=" * 80)
    log.info(f"🚀 Processing {file_path}")
    log.info(f"📊 Indicator detected: {indicator_folder}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2 — READ CSV
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 2] CSV Ingestion — Reading: {indicator_folder}/{file_name}")

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
            "description": f"Column '{c}' read directly from WORLD_BANK CSV '{file_name}' (indicator: {indicator_folder})",
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
        step_name=f"02_CSV_Ingestion_{indicator_folder}",
        description=(
            f"Read WORLD_BANK CSV '{indicator_folder}/{file_name}' ({row_count} rows). "
            f"Columns: {', '.join(df.columns)}."
        ),
        trans_type="EXTRACT",
        inputs=[file_path],
        outputs=[f"memory://spark_df/world_bank/transform/{indicator_folder}/raw"],
        input_schema_fields=raw_schema_fields,
        column_lineage=column_lineage_t2,
    )
    print(f"  ✅ STEP 2 complete — {row_count} rows ingested")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 3 — TRANSFORMATION + SCD2 version_active
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 3] Transformation — Renaming, cleaning and computing version_active")

    transformed_df = (
        df
        .withColumnRenamed("indicator_name", "Variable")
        .withColumnRenamed("year",           "periode")
        .withColumnRenamed("value",          "valeur")
        .withColumnRenamed("country_iso3",   "pays")
        .withColumn("periode", col("periode").cast(StringType()) )
        .withColumn("pays",            coalesce(col("pays"), lit("UNKNOWN")))
        .withColumn("date_chargement", current_timestamp())
        .withColumn("source",          lit("WORLD_BANK"))
        .withColumn("valeur",          spark_round(col("valeur").cast("double"), 6))
        # colonnes business communes (standard data lake)
        .withColumn("code_secteur", lit("NA"))
        .withColumn("lib_secteur", lit("NA"))
        .withColumn("dim_id", lit("NA"))
        .withColumn("dim_key", lit("NA"))
        .withColumn("version", lit("NA"))
        .withColumn("base", lit("NA"))

        # NULL values on valeur are preserved intentionally (e.g. HCI 2015)
        .select("periode", "Variable", "valeur", "pays", "date_chargement", "code_secteur",
        "lib_secteur",
        "dim_id",
        "dim_key",
        "version",
        "base",
        "source")
    )

    # SCD2 — version_active via ROW_NUMBER()
    window_spec = Window.partitionBy("periode", "Variable", "pays").orderBy(desc("date_chargement"))
    transformed_df = (
        transformed_df
        .withColumn("_rank", row_number().over(window_spec))
        .withColumn("version_active", (col("_rank") == 1).cast("int"))
        .drop("_rank")
    )

    transformed_count = transformed_df.count()

    # ── Column lineage ────────────────────────────────────────────────
    _raw_path = f"memory://spark_df/world_bank/transform/{indicator_folder}/raw"
    _ns_raw   = resolve_namespace(_raw_path)

    renamed_cols_t3 = {
        "Variable": ("indicator_name", "Renamed from 'indicator_name' in WORLD_BANK CSV"),
        "periode":    ("year",           "Renamed from 'year' in WORLD_BANK CSV"),
        "valeur":   ("value",          "Renamed from 'value'. spark_round(valeur, 6). NULL values preserved intentionally."),
        "pays":     ("country_iso3",   "Renamed from 'country_iso3'. coalesce(pays, 'UNKNOWN') applied."),
    }

    injected_cols_t3 = {
        "date_chargement": "Pipeline-injected: current timestamp at load time (current_timestamp())",
        "source":          "Pipeline-injected: hardcoded constant = 'WORLD_BANK_API'",
    }

    column_lineage_t3 = {}
    for field in transformed_df.schema.fields:
        col_name = field.name
        if col_name == "version_active":
            column_lineage_t3[col_name] = {
                "inputFields": [
                    {"namespace": _ns_raw, "name": _raw_path, "field": "year"},
                    {"namespace": _ns_raw, "name": _raw_path, "field": "indicator_name"},
                    {"namespace": _ns_raw, "name": _raw_path, "field": "country_iso3"},
                ],
                "transformationDescription": (
                    "SCD Type 2 flag: ROW_NUMBER() OVER "
                    "(PARTITION BY periode, Variable, pays ORDER BY date_chargement DESC) "
                    "— rank=1 → version_active=1, others → 0. "
                    "Computed before delta detection to initialize new records."
                ),
                "transformationType": "AGGREGATE",
            }
        elif col_name in injected_cols_t3:
            column_lineage_t3[col_name] = {
                "inputFields": [],
                "transformationDescription": injected_cols_t3[col_name],
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
            column_lineage_t3[col_name] = {
                "inputFields": [
                    {"namespace": _ns_raw, "name": _raw_path, "field": col_name}
                ],
                "transformationDescription": "Directly passed through from raw CSV.",
                "transformationType": "DIRECT",
            }

    # 📡 MARQUEZ — STEP 3
    emit_marquez_step(
        spark_df=transformed_df,
        step_name=f"03_Transformation_{indicator_folder}",
        description=(
            f"Transformed WORLD_BANK CSV '{indicator_folder}/{file_name}' ({transformed_count} rows). "
            f"Renamed: indicator_name→Variable, year→periode, value→valeur, country_iso3→pays. "
            f"coalesce(pays, 'UNKNOWN'). spark_round(valeur, 6). "
            f"NULL valeur preserved (e.g. HCI 2015 case). "
            f"Added: date_chargement=now(), source='WORLD_BANK_API'. "
            f"SCD2: version_active=ROW_NUMBER() OVER (PARTITION BY periode, Variable, pays ORDER BY date_chargement DESC)."
        ),
        trans_type="TRANSFORMATION",
        inputs=[_raw_path],
        outputs=[f"memory://spark_df/world_bank/transform/{indicator_folder}/transformed"],
        column_lineage=column_lineage_t3,
    )
    print(f"  ✅ STEP 3 complete — {transformed_count} rows, {len(transformed_df.columns)} columns")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 4 — DELTA DETECTION & WRITE (per year)
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 4] Delta Detection & Write — Processing by year")

    years = [row["periode"] for row in transformed_df.select("periode").distinct().collect()]
    log.info(f"📆 Years found in CSV: {sorted(years)}")

    _transformed_path = f"memory://spark_df/world_bank/transform/{indicator_folder}/transformed"
    _ns_transformed   = resolve_namespace(_transformed_path)

    for y in years:

        df_year     = transformed_df.filter(col("periode") == y)
        output_path = os.path.join(TARGET_BASE, indicator_folder, str(y))

        print(f"\n  📅 Processing Year  : {y}")
        print(f"  📂 Output path      : {output_path}")

        history_exists = False
        new_count      = 0

        try:
            existing_df = spark.read.parquet(output_path).cache()
            existing_df.count()  # force read
            history_exists = True
            log.info(f"📂 Existing dataset found for year {y}")

            # ── Normalise types before join ───────────────────────────
            # existing_df = existing_df.withColumn("valeur", col("valeur").cast(DecimalType(38, 6)))
            # df_year     = df_year.withColumn("valeur",     col("valeur").cast(DecimalType(38, 6)))
            existing_df = existing_df.withColumn("valeur", col("valeur").cast("double"))
            df_year     = df_year.withColumn("valeur", col("valeur").cast("double"))
            existing_df = existing_df.withColumn("pays", coalesce(col("pays"), lit("UNKNOWN")))
            df_year     = df_year.withColumn("pays",     coalesce(col("pays"), lit("UNKNOWN")))

            # ── NULL sentinel trick — NULL != NULL in Spark joins ─────
            df_year_cmp  = df_year.withColumn("valeur_cmp",    coalesce(col("valeur"), lit(NULL_SENTINEL)))
            existing_cmp = existing_df.withColumn("valeur_cmp", coalesce(col("valeur"), lit(NULL_SENTINEL)))

            compare_cols = ["periode", "Variable", "pays", "valeur_cmp"]

            new_rows  = df_year_cmp.join(
                existing_cmp.select(compare_cols),
                on=compare_cols,
                how="left_anti",
            ).drop("valeur_cmp")

            new_count = new_rows.count()
            log.info(f"🧐 Rows considered new by Spark for year {y}: {new_count}")
            new_rows.show(20, truncate=False)

            if new_count == 0:
                log.info(f"✅ No new or changed rows detected for {y} — file untouched")
                existing_df.unpersist()
                continue

            log.info(f"🆕 {new_count} new/changed rows detected")
            new_rows  = new_rows.withColumn("version_active", lit(0).cast("int"))
            final_df  = existing_df.unionByName(new_rows)
            existing_df.unpersist()

        except AnalysisException:
            log.info(f"🆕 First load detected for year {y}")
            final_df = df_year

        # ── Recompute version_active on full final_df ─────────────────
        window_spec = Window.partitionBy("periode", "Variable", "pays").orderBy(desc("date_chargement"))
        final_df = (
            final_df
            .withColumn("_rank", row_number().over(window_spec))
            .withColumn("version_active", (col("_rank") == 1).cast("int"))
            .drop("_rank")
        )

        # ── Column lineage for the write ──────────────────────────────
        column_lineage_t4 = {}
        for field in final_df.schema.fields:
            if field.name == "version_active":
                column_lineage_t4[field.name] = {
                    "inputFields": [
                        {"namespace": _ns_transformed, "name": _transformed_path, "field": "date_chargement"},
                        {"namespace": _ns_transformed, "name": _transformed_path, "field": "periode"},
                        {"namespace": _ns_transformed, "name": _transformed_path, "field": "Variable"},
                        {"namespace": _ns_transformed, "name": _transformed_path, "field": "pays"},
                    ],
                    "transformationDescription": (
                        "SCD Type 2 flag recomputed on final_df after union. "
                        "ROW_NUMBER() OVER (PARTITION BY periode, Variable, pays ORDER BY date_chargement DESC) "
                        "— rank=1 → version_active=1, others → 0. "
                        + (
                            f"{new_count} new/changed rows detected via NULL_SENTINEL left_anti join. "
                            f"New rows temporarily set to version_active=0 before window recomputation."
                            if history_exists
                            else "New dataset — version_active computed from scratch."
                        )
                    ),
                    "transformationType": "AGGREGATE",
                }
            else:
                column_lineage_t4[field.name] = {
                    "inputFields": [
                        {"namespace": _ns_transformed, "name": _transformed_path, "field": field.name}
                    ],
                    "transformationDescription": (
                        f"Passed through to Parquet output. "
                        + (
                            f"NULL sentinel ({NULL_SENTINEL}) used for valeur comparison in left_anti join "
                            f"to avoid NULL != NULL false positives. "
                            f"{new_count} new row(s) appended to history via unionByName."
                            if history_exists and field.name == "valeur"
                            else (
                                f"Delta via left_anti join on (periode, Variable, pays, valeur_cmp). "
                                f"{new_count} new row(s) appended via unionByName."
                                if history_exists
                                else "New dataset — full write."
                            )
                        )
                    ),
                    "transformationType": "DIRECT",
                }

        # 📡 MARQUEZ — STEP 4 (per year)
        emit_marquez_step(
            spark_df=final_df,
            step_name=f"04_Delta_Write_{indicator_folder}_{y}",
            description=(
                f"Delta detection + Parquet write for indicator '{indicator_folder}', year {y}. "
                f"History existed: {history_exists}. "
                + (
                    f"NULL sentinel trick applied (valeur_cmp=coalesce(valeur, {NULL_SENTINEL})) "
                    f"to handle NULL comparisons in left_anti join. "
                    f"New/changed rows: {new_count}. "
                    f"Appended to history via unionByName. "
                    f"Recomputed version_active: ROW_NUMBER() OVER "
                    f"(PARTITION BY periode, Variable, pays ORDER BY date_chargement DESC)."
                    if history_exists
                    else "No history — full write. version_active already set in STEP 3."
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

        log.info(f"✅ Final dataset written for year {y}")
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
    print(f"✅ Finished processing: {indicator_folder}/{file_name}")
    print(f"{'='*80}\n")


log.info("🎉 WORLD_BANK transformation completed")
stop_spark_session(spark)
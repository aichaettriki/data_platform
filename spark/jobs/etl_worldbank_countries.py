from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, trim, when
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StringType
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from datetime import datetime
import logging
import uuid
from common.spark_session import create_spark_session, stop_spark_session

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("WORLD_BANK_COUNTRIES_ETL")

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
spark = create_spark_session("WORLD_BANK-RAW-to-TRANSFORMED-Countries")
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
        step_name           : Logical name of the step (e.g. '01_Discover_File')
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
# PATHS
# =====================================================
RAW_BASE         = "s3a://01-raw"
TRANSFORMED_PATH = "s3a://02-transformed/WORLD_BANK/Countries/Axe_Referentiel"


# =====================================================
# FIND LATEST CSV  (dossier YYYY/MM le plus récent)
# =====================================================
def find_latest_countries_file(spark, base_path, filename="worldbank_countries.csv"):
    """
    Parcourt s3a://01-raw/YYYY/MM/WORLD_BANK/Countries/
    et retourne le chemin du CSV le plus récent.
    """
    sc         = spark.sparkContext
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI        = sc._jvm.java.net.URI

    fs         = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())
    candidates = []
    stack      = [Path(base_path)]

    while stack:
        current = stack.pop()
        try:
            for status in fs.listStatus(current):
                p = status.getPath().toString()
                if status.isDirectory():
                    stack.append(status.getPath())
                elif status.isFile() and p.endswith(filename):
                    # Format : s3a://01-raw/YYYY/MM/WORLD_BANK/Countries/worldbank_countries.csv
                    parts = p.replace("s3a://", "").split("/")
                    if len(parts) >= 3:
                        try:
                            year  = int(parts[1])
                            month = int(parts[2])
                            candidates.append((year, month, p))
                            log.info(f"🔎 fichier trouvé : {p}")
                        except ValueError:
                            continue
        except Exception:
            pass

    if not candidates:
        raise RuntimeError(
            f"❌ Aucun fichier '{filename}' trouvé sous {base_path}/YYYY/MM/WORLD_BANK/Countries/"
        )

    latest = max(candidates)
    log.info(f"✅ Fichier le plus récent sélectionné : {latest[2]}")
    return latest[2]


# ══════════════════════════════════════════════════════════════════════
# STEP 1 — DISCOVER FILE
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"[STEP 1] Discover File — Scanning latest worldbank_countries.csv under {RAW_BASE}")
print(f"{'='*80}")

log.info("🔍 Recherche du fichier countries le plus récent dans le RAW...")
csv_path = find_latest_countries_file(spark, RAW_BASE)

# ── Build a minimal Spark DF to represent the discovered file ─────────
discovered_spark = spark.createDataFrame([{"file_path": csv_path}])

# ── Column lineage ────────────────────────────────────────────────────
column_lineage_t1 = {
    "file_path": {
        "inputFields": [
            {"namespace": resolve_namespace(RAW_BASE), "name": RAW_BASE, "field": "file_path"}
        ],
        "transformationDescription": (
            "Full S3 path of the most recent worldbank_countries.csv file. "
            "Selected by find_latest_countries_file() picking max(year, month) "
            "from all candidates under s3a://01-raw/YYYY/MM/WORLD_BANK/Countries/."
        ),
        "transformationType": "DIRECT",
    }
}

# 📡 MARQUEZ — STEP 1
emit_marquez_step(
    spark_df=discovered_spark,
    step_name="01_Discover_Countries_File",
    description=(
        f"Scanned {RAW_BASE} recursively for 'worldbank_countries.csv'. "
        f"Selected most recent file by max(year, month): {csv_path}."
    ),
    trans_type="EXTRACT",
    inputs=[RAW_BASE],
    outputs=["memory://spark_df/world_bank/countries/discovered_file"],
    column_lineage=column_lineage_t1,
)
print(f"  ✅ STEP 1 complete — File found: {csv_path}")


# ══════════════════════════════════════════════════════════════════════
# STEP 2 — READ CSV
# ══════════════════════════════════════════════════════════════════════
print(f"\n[STEP 2] CSV Ingestion — Reading: {csv_path.split('/')[-1]}")

log.info(f"📥 Lecture du CSV : {csv_path}")
raw_df = (
    spark.read
    .option("header",    True)
    .option("inferSchema", True)
    .option("nullValue", "NULL")
    .csv(csv_path)
)

raw_count = raw_df.count()
log.info(f"📊 Lignes brutes lues : {raw_count}")
log.info("📄 Schéma brut :")
raw_df.printSchema()
raw_df.show(5, truncate=False)

# ── Raw CSV schema for Marquez input ─────────────────────────────────
raw_schema_fields = [
    {
        "name": c,
        "type": "string",
        "description": f"Column '{c}' read directly from worldbank_countries.csv",
    }
    for c in raw_df.columns
]

# ── Column lineage ────────────────────────────────────────────────────
column_lineage_t2 = {
    c: {
        "inputFields": [
            {"namespace": resolve_namespace(csv_path), "name": csv_path, "field": c}
        ],
        "transformationDescription": f"Directly mapped from CSV column '{c}'",
        "transformationType": "DIRECT",
    }
    for c in raw_df.columns
}

# 📡 MARQUEZ — STEP 2
emit_marquez_step(
    spark_df=raw_df,
    step_name="02_CSV_Ingestion_Countries",
    description=(
        f"Read CSV '{csv_path.split('/')[-1]}' ({raw_count} rows). "
        f"Options: header=True, inferSchema=True, nullValue='NULL'. "
        f"Columns: {', '.join(raw_df.columns)}."
    ),
    trans_type="EXTRACT",
    inputs=[csv_path],
    outputs=["memory://spark_df/world_bank/countries/raw"],
    input_schema_fields=raw_schema_fields,
    column_lineage=column_lineage_t2,
)
print(f"  ✅ STEP 2 complete — {raw_count} rows ingested")


# ══════════════════════════════════════════════════════════════════════
# STEP 3 — TRANSFORM
# ══════════════════════════════════════════════════════════════════════
print(f"\n[STEP 3] Transformation — Cleaning, renaming and adding business columns")

log.info("🔄 Application des transformations...")

transformed_df = (
    raw_df

    # Nettoyage TRIM
    .withColumn("id",          trim(col("id")))
    .withColumn("iso2_code",   trim(col("iso2_code")))
    .withColumn("name",        trim(col("name")))
    .withColumn("region_id",   trim(col("region_id")))
    .withColumn("region_iso2", trim(col("region_iso2")))
    .withColumn("region_name", trim(col("region_name")))

    # Colonnes techniques injectées
    .withColumn("date_chargement", current_timestamp())
    .withColumn("source",          lit("WORLD_BANK_Countries"))
    .withColumn("base",            lit(None).cast(StringType()))
    .withColumn("version",         lit(None).cast(StringType()))
    .withColumn("code_secteur",    lit(None).cast(StringType()))
    .withColumn("lib_secteur",     lit(None).cast(StringType()))
    .withColumn("dim_id",          lit(None).cast(StringType()))
    .withColumn("dim_key",         lit(None).cast(StringType()))
    .withColumn("variable",        lit(None).cast(StringType()))
    .withColumn("annee",           lit(None).cast(StringType()))

    # Sélection finale — rang supprimé
    .select(
        col("id"),
        col("iso2_code"),
        col("name").alias("pays"),
        col("region_id"),
        col("region_iso2"),
        col("region_name"),
        col("base"),
        col("version"),
        col("code_secteur"),
        col("lib_secteur"),
        col("dim_id"),
        col("dim_key"),
        col("source"),
        col("date_chargement"),
        col("variable"),
        col("annee"),
    )
)

total_transformed = transformed_df.count()
log.info("📄 Aperçu du résultat transformé :")
transformed_df.show(10, truncate=False)

# ── Column lineage ────────────────────────────────────────────────────
_raw_path = "memory://spark_df/world_bank/countries/raw"
_ns_raw   = resolve_namespace(_raw_path)

# Columns trimmed directly from raw
trimmed_cols = {"id", "iso2_code", "region_id", "region_iso2", "region_name"}

# Injected pipeline constants (no input source)
injected_cols = {
    "date_chargement": "Pipeline-injected: current timestamp at load time (current_timestamp())",
    "source":          "Pipeline-injected: hardcoded constant = 'WORLD_BANK_Countries'",
    "base":            "Pipeline-injected: hardcoded null",
    "version":         "Pipeline-injected: hardcoded null",
    "code_secteur":    "Pipeline-injected: hardcoded null",
    "lib_secteur":     "Pipeline-injected: hardcoded null",
    "dim_id":          "Pipeline-injected: hardcoded null",
    "dim_key":         "Pipeline-injected: hardcoded null",
    "variable":        "Pipeline-injected: hardcoded null (no indicator variable for this referential)",
    "annee":           "Pipeline-injected: hardcoded null (no year partition for this referential)",
}

column_lineage_t3 = {}
for field in transformed_df.schema.fields:
    col_name = field.name
    if col_name in injected_cols:
        column_lineage_t3[col_name] = {
            "inputFields": [],
            "transformationDescription": injected_cols[col_name],
            "transformationType": "IDENTITY",
        }
    elif col_name == "pays":
        column_lineage_t3[col_name] = {
            "inputFields": [{"namespace": _ns_raw, "name": _raw_path, "field": "name"}],
            "transformationDescription": "Renamed from 'name' via .alias('pays'). Applied TRIM().",
            "transformationType": "DIRECT",
        }
    elif col_name in trimmed_cols:
        column_lineage_t3[col_name] = {
            "inputFields": [{"namespace": _ns_raw, "name": _raw_path, "field": col_name}],
            "transformationDescription": f"Directly mapped from raw CSV. Applied TRIM().",
            "transformationType": "DIRECT",
        }
    else:
        column_lineage_t3[col_name] = {
            "inputFields": [{"namespace": _ns_raw, "name": _raw_path, "field": col_name}],
            "transformationDescription": "Directly passed through from raw CSV.",
            "transformationType": "DIRECT",
        }

# 📡 MARQUEZ — STEP 3
emit_marquez_step(
    spark_df=transformed_df,
    step_name="03_Transform_Countries",
    description=(
        f"Cleaned and enriched worldbank_countries data ({total_transformed} rows). "
        f"Applied TRIM() on: id, iso2_code, name, region_id, region_iso2, region_name. "
        f"Renamed: name→pays. "
        f"Added constants: source='WORLD_BANK_Countries', date_chargement=now(), "
        f"base=null, version=null, code_secteur=null, lib_secteur=null, "
        f"dim_id=null, dim_key=null, variable=null, annee=null. "
        f"Dropped: rang. "
        f"Final columns: {', '.join(transformed_df.columns)}."
    ),
    trans_type="TRANSFORMATION",
    inputs=[_raw_path],
    outputs=["memory://spark_df/world_bank/countries/transformed"],
    column_lineage=column_lineage_t3,
)
print(f"  ✅ STEP 3 complete — {len(transformed_df.columns)} columns, {total_transformed} rows")


# =====================================================
# DATA QUALITY CHECK
# =====================================================
log.info("🔍 Contrôle qualité...")

missing_iso3 = transformed_df.filter(col("id").isNull()).count()
missing_name = transformed_df.filter(col("pays").isNull()).count()

if missing_iso3 > 0:
    raise RuntimeError(f"❌ DATA QUALITY : {missing_iso3} lignes avec id NULL")
if missing_name > 0:
    raise RuntimeError(f"❌ DATA QUALITY : {missing_name} lignes avec country_name / pays NULL")

log.info("✅ Contrôle qualité OK — aucune valeur critique manquante")


# ══════════════════════════════════════════════════════════════════════
# STEP 4 — DELTA DETECTION & WRITE (fichier unique, pas de partition year)
# ══════════════════════════════════════════════════════════════════════
print(f"\n[STEP 4] Delta Detection & Write — {TRANSFORMED_PATH}")

log.info(f"💾 Écriture Parquet vers : {TRANSFORMED_PATH}")

_transformed_path = "memory://spark_df/world_bank/countries/transformed"
_ns_transformed   = resolve_namespace(_transformed_path)

history_exists = False
new_count      = 0

try:
    existing_df    = spark.read.parquet(TRANSFORMED_PATH).cache()
    existing_count = existing_df.count()
    history_exists = True
    log.info(f"📂 Fichier existant trouvé ({existing_count} lignes) — détection des nouveautés...")

    compare_cols = ["id", "iso2_code", "pays", "region_id", "region_iso2", "region_name"]

    new_rows  = transformed_df.join(
        existing_df.select(compare_cols),
        on=compare_cols,
        how="left_anti",
    )
    new_count = new_rows.count()

    if new_count == 0:
        log.info("✅ Aucune nouvelle ligne détectée — fichier Parquet inchangé")
        existing_df.unpersist()

        # 📡 MARQUEZ — STEP 4 (no-op)
        emit_marquez_step(
            spark_df=transformed_df,
            step_name="04_Delta_Write_Countries",
            description=(
                f"Delta detection for worldbank_countries. "
                f"History existed: True ({existing_count} rows). "
                f"No new rows detected via left_anti join on "
                f"(id, iso2_code, pays, region_id, region_iso2, region_name). "
                f"File left untouched."
            ),
            trans_type="LOAD",
            inputs=[_transformed_path],
            outputs=[TRANSFORMED_PATH],
            column_lineage={
                field.name: {
                    "inputFields": [
                        {"namespace": _ns_transformed, "name": _transformed_path, "field": field.name}
                    ],
                    "transformationDescription": "No-op — no changes detected, file unchanged.",
                    "transformationType": "DIRECT",
                }
                for field in transformed_df.schema.fields
            },
        )

    else:
        log.info(f"🆕 {new_count} nouvelles lignes détectées — mise à jour du Parquet")
        final_df = existing_df.unionByName(new_rows)
        existing_df.unpersist()

        # ── Column lineage ────────────────────────────────────────────
        column_lineage_t4 = {
            field.name: {
                "inputFields": [
                    {"namespace": _ns_transformed, "name": _transformed_path, "field": field.name}
                ],
                "transformationDescription": (
                    f"Passed through to Parquet output. "
                    f"Delta detected via left_anti join on (id, iso2_code, pays, region_id, region_iso2, region_name). "
                    f"{new_count} new row(s) appended to existing history ({existing_count} rows) via unionByName."
                ),
                "transformationType": "DIRECT",
            }
            for field in final_df.schema.fields
        }

        # 📡 MARQUEZ — STEP 4 (update)
        emit_marquez_step(
            spark_df=final_df,
            step_name="04_Delta_Write_Countries",
            description=(
                f"Delta detection + Parquet write for worldbank_countries. "
                f"History existed: True ({existing_count} rows). "
                f"New rows detected: {new_count} (left_anti join on compare_cols). "
                f"Appended to history via unionByName. "
                f"Total rows written: {final_df.count()}."
            ),
            trans_type="LOAD",
            inputs=[_transformed_path],
            outputs=[TRANSFORMED_PATH],
            column_lineage=column_lineage_t4,
        )

        (
            final_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(TRANSFORMED_PATH)
        )
        log.info(f"✅ Parquet mis à jour ({final_df.count()} lignes au total)")

except AnalysisException:
    log.info("🆕 Aucun fichier existant — première écriture complète")
    final_df = transformed_df

    # ── Column lineage ────────────────────────────────────────────────
    column_lineage_t4_new = {
        field.name: {
            "inputFields": [
                {"namespace": _ns_transformed, "name": _transformed_path, "field": field.name}
            ],
            "transformationDescription": "New dataset — full write, no history existed.",
            "transformationType": "DIRECT",
        }
        for field in final_df.schema.fields
    }

    # 📡 MARQUEZ — STEP 4 (first write)
    emit_marquez_step(
        spark_df=final_df,
        step_name="04_Delta_Write_Countries",
        description=(
            f"First write for worldbank_countries — no history existed. "
            f"Full write of {total_transformed} rows to {TRANSFORMED_PATH}."
        ),
        trans_type="LOAD",
        inputs=[_transformed_path],
        outputs=[TRANSFORMED_PATH],
        column_lineage=column_lineage_t4_new,
    )

    (
        final_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(TRANSFORMED_PATH)
    )
    log.info(f"✅ Parquet écrit ({total_transformed} lignes)")


# =====================================================
# SCHEMA FINAL
# =====================================================
log.info("📊 Schéma final du Parquet :")
for field in transformed_df.schema.fields:
    log.info(
        f"  Column: {field.name:<25} | "
        f"Type: {field.dataType.simpleString():<12} | "
        f"Nullable: {field.nullable}"
    )

# =====================================================
# STOP SPARK
# =====================================================
log.info("🎉 Job ETL World Bank Countries terminé avec succès")
stop_spark_session(spark)
"""
Spark Job : Transformation HDR UNDP
Conventions du projet :
  - Args    : --year, --month, --day
  - Config  : variables d'environnement MINIO_ENDPOINT / MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
  - Source  : s3a://01-raw/<year>/<month>/PNUD/hdr/hdr_composite_indices.csv
  - Output  : s3a://02-transformed/PNUD/hdr/hdr_hdi_by_country/  (Parquet, partitionné par year)
  - Colonnes finales :
      periode, Variable, version, base, valeur, date_chargement, source, version_active,
      pays, code_secteur, lib_secteur, dim_id, dim_key
  - Delta   : SCD Type 2 — clé (pays, periode, Variable)
              version_active=1 (actif) / version_active=0 (historique)
"""

import argparse
import os
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

# ─── Constantes ────────────────────────────────────────────────────────────────
RAW_BUCKET         = "s3a://01-raw"
TRANSFORMED_BUCKET = "s3a://02-transformed"
LOCAL_FILE_NAME    = "hdr_composite_indices.csv"
SOURCE_LABEL       = "PNUD/hdi"
HDI_COL_PREFIX     = "hdi_"
ISO3_COL           = "iso3"
COUNTRY_COL        = "country"

VARIABLE_LABEL  = "Human Development Index (HDI)"
BASE_VALUE      = "N/A"
VERSION_VALUE   = "N/A"
VERSION_ACTIVE  = "1"
CODE_SECTEUR    = "N/A"
LIB_SECTEUR     = "N/A"
DIM_ID_VALUE    = "HDI"

# ─── Marquez Config ────────────────────────────────────────────────────────────
MARQUEZ_URL     = "http://marquez:5000"
PIPELINE_RUN_ID = str(uuid.uuid4())

NS_DATA_LAKE      = "data_lake"
NS_DATA_WAREHOUSE = "data_warehouse"


# ─── Marquez Helpers ───────────────────────────────────────────────────────────
def resolve_namespace(path: str) -> str:
    if path.startswith("s3a://03-refined") or path.startswith("s3://03-refined"):
        return NS_DATA_WAREHOUSE
    return NS_DATA_LAKE


def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs,
                      input_schema_fields=None, column_lineage=None):
    try:
        client = OpenLineageClient(url=MARQUEZ_URL)

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

        if column_lineage:
            output_dataset_facets["columnLineage"] = {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json#",
                "fields": column_lineage,
            }

        input_dataset_facets = {}
        if input_schema_fields:
            input_dataset_facets["schema"] = {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
                "fields": input_schema_fields,
            }

        run_facets = {
            "processing_engine": {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ProcessingEngineRunFacet.json#",
                "version": "3.5.1",
                "name": "Apache Spark",
                "openlineageAdapterVersion": "itceq-1.0",
            }
        }

        job_facets = {
            "sql": {
                "_producer": "itceq-spark-producer",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json#",
                "query": description,
            }
        }

        input_datasets = [
            Dataset(namespace=resolve_namespace(i), name=i, facets=input_dataset_facets)
            for i in inputs
        ]
        output_datasets = [
            Dataset(namespace=resolve_namespace(o), name=o, facets=output_dataset_facets)
            for o in outputs
        ]

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


# ─── Args ──────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="HDR UNDP Transformation")
    parser.add_argument("--year",  required=True, help="Année d'exécution  (ex: 2026)")
    parser.add_argument("--month", required=True, help="Mois d'exécution   (ex: 03)")
    parser.add_argument("--day",   required=True, help="Jour d'exécution   (ex: 10)")
    return parser.parse_args()


# ─── Spark Session ─────────────────────────────────────────────────────────────
def get_spark_session() -> SparkSession:
    endpoint = os.environ["MINIO_ENDPOINT"]
    access   = os.environ["MINIO_ROOT_USER"]
    secret   = os.environ["MINIO_ROOT_PASSWORD"]

    return (
        SparkSession.builder
        .appName("HDR_UNDP_Transform")
        .config("spark.hadoop.fs.s3a.endpoint",               endpoint)
        .config("spark.hadoop.fs.s3a.access.key",             access)
        .config("spark.hadoop.fs.s3a.secret.key",             secret)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions",               "8")
        .getOrCreate()
    )


# ─── Main Run ──────────────────────────────────────────────────────────────────
def run(spark: SparkSession, input_path: str, output_path: str) -> None:

    _RAW_PATH   = "memory://spark_df/pnud/hdr/raw"
    _LONG_PATH  = "memory://spark_df/pnud/hdr/long"
    _FINAL_PATH = "memory://spark_df/pnud/hdr/final"

    # ══════════════════════════════════════════════════════════════════════
    # STEP 1 — READ CSV
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print(f"[STEP 1] CSV Ingestion — Reading: {input_path.split('/')[-1]}")
    print(f"{'='*80}")

    df_raw = (
        spark.read
        .option("header",   "true")
        .option("encoding", "UTF-8")
        .csv(input_path)
    )

    raw_count = df_raw.count()
    print(f"📥 Lignes lues : {raw_count}  |  Colonnes : {len(df_raw.columns)}")

    hdi_columns = sorted([
        c for c in df_raw.columns
        if c.startswith(HDI_COL_PREFIX) and c[len(HDI_COL_PREFIX):].isdigit()
    ])

    if not hdi_columns:
        raise ValueError(
            f"Aucune colonne '{HDI_COL_PREFIX}<year>' trouvée. "
            f"Colonnes disponibles : {df_raw.columns}"
        )
    print(f"📊 Colonnes HDI ({len(hdi_columns)}) : {hdi_columns[0]} … {hdi_columns[-1]}")

    raw_schema_fields = [
        {
            "name": c,
            "type": "string",
            "description": (
                f"HDI value column for year {c[len(HDI_COL_PREFIX):]} — wide format"
                if c.startswith(HDI_COL_PREFIX)
                else f"Column '{c}' read directly from HDR composite indices CSV"
            ),
        }
        for c in df_raw.columns
    ]

    column_lineage_t1 = {
        c: {
            "inputFields": [
                {"namespace": resolve_namespace(input_path), "name": input_path, "field": c}
            ],
            "transformationDescription": (
                f"Wide HDI column for year {c[len(HDI_COL_PREFIX):]} — will be unpivoted in STEP 2"
                if c.startswith(HDI_COL_PREFIX)
                else f"Directly mapped from CSV column '{c}'"
            ),
            "transformationType": "DIRECT",
        }
        for c in df_raw.columns
    }

    emit_marquez_step(
        spark_df=df_raw,
        step_name="01_Read_CSV_HDR",
        description=(
            f"Read HDR UNDP CSV '{input_path.split('/')[-1]}' ({raw_count} rows). "
            f"Options: header=true, encoding=UTF-8. "
            f"Detected {len(hdi_columns)} HDI year columns: {hdi_columns[0]} → {hdi_columns[-1]}. "
            f"Total columns: {len(df_raw.columns)}."
        ),
        trans_type="EXTRACT",
        inputs=[input_path],
        outputs=[_RAW_PATH],
        input_schema_fields=raw_schema_fields,
        column_lineage=column_lineage_t1,
    )
    print(f"  ✅ STEP 1 complete — {raw_count} rows, {len(hdi_columns)} HDI year columns detected")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2 — UNPIVOT wide → long via stack()
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 2] Unpivot — Wide HDI columns → (periode, valeur)")

    stack_expr = ", ".join(f"'{col}', `{col}`" for col in hdi_columns)
    stack_sql  = f"stack({len(hdi_columns)}, {stack_expr}) as (raw_year_col, hdi_value)"

    df_long = df_raw.select(
        F.col(ISO3_COL).alias("country_code"),
        F.col(COUNTRY_COL).alias("country_name"),
        F.expr(stack_sql),
    )

    df_long = (
        df_long
        .withColumn(
            "periode",
            F.regexp_extract(F.col("raw_year_col"), r"(\d{4})$", 1).cast("int")
        )
        .withColumn("valeur", F.col("hdi_value").cast(FloatType()))
        # .withColumn("valeur", F.col("hdi_value").cast(FloatType()).cast(StringType()))
        .drop("raw_year_col", "hdi_value")
    )

    long_count = df_long.count()
    print(f"  📊 Rows after unpivot : {long_count}")

    _ns_raw = resolve_namespace(_RAW_PATH)
    column_lineage_t2 = {
        "country_code": {
            "inputFields": [{"namespace": _ns_raw, "name": _RAW_PATH, "field": ISO3_COL}],
            "transformationDescription": f"Renamed from '{ISO3_COL}' (ISO3 country code) via .alias('country_code').",
            "transformationType": "DIRECT",
        },
        "country_name": {
            "inputFields": [{"namespace": _ns_raw, "name": _RAW_PATH, "field": COUNTRY_COL}],
            "transformationDescription": f"Renamed from '{COUNTRY_COL}' (country name) via .alias('country_name').",
            "transformationType": "DIRECT",
        },
        "periode": {
            "inputFields": [
                {"namespace": _ns_raw, "name": _RAW_PATH, "field": c}
                for c in hdi_columns
            ],
            "transformationDescription": (
                f"Year extracted from column name via stack() unpivot on {len(hdi_columns)} HDI columns "
                f"({hdi_columns[0]} → {hdi_columns[-1]}). "
                f"regexp_extract(raw_year_col, r'(\\d{{4}})$', 1).cast('int')."
            ),
            "transformationType": "AGGREGATE",
        },
        "valeur": {
            "inputFields": [
                {"namespace": _ns_raw, "name": _RAW_PATH, "field": c}
                for c in hdi_columns
            ],
            "transformationDescription": (
                f"HDI value from cell content via stack() unpivot on {len(hdi_columns)} HDI columns. "
                f"Cast chain: string → FloatType() → StringType()."
            ),
            "transformationType": "AGGREGATE",
        },
    }

    emit_marquez_step(
        spark_df=df_long,
        step_name="02_Unpivot_HDI_Columns",
        description=(
            f"Unpivoted {len(hdi_columns)} wide HDI columns → long format (periode, valeur) "
            f"using Spark stack() expression. "
            f"iso3→country_code, country→country_name. "
            f"periode extracted via regexp_extract(raw_year_col, r'(\\d{{4}})$', 1).cast('int'). "
            f"valeur cast: string → FloatType() → StringType(). "
            f"Result: {long_count} rows."
        ),
        trans_type="TRANSFORMATION",
        inputs=[_RAW_PATH],
        outputs=[_LONG_PATH],
        column_lineage=column_lineage_t2,
    )
    print(f"  ✅ STEP 2 complete — {long_count} rows in long format")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 3 — ENRICHISSEMENT + colonnes standardisées
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[STEP 3] Enrichment — Filtering, renaming and adding standardised columns")

    df_final = (
        df_long
        .filter(F.col("valeur").isNotNull())
        .filter(F.col("country_code").isNotNull())
        .withColumn("dim_key",          F.lit("NA").cast("string"))
        .withColumn("Variable",         F.lit("Human Development Index (HDI)"))
        .withColumn("version",          F.lit("NA").cast("string"))
        .withColumn("version_active",   F.lit("1"))
        .withColumn("base",             F.lit("NA").cast("string"))
        .withColumn("source",           F.lit("PNUD"))
        .withColumn("code_secteur",     F.lit("NA").cast("string"))
        .withColumn("lib_secteur",      F.lit("NA").cast("string"))
        .withColumn("dim_id",           F.lit("NA").cast("string"))
        .withColumn("dim_key",          F.lit("NA").cast("string"))
        .withColumn("periode",          F.lit("NA").cast("string"))
        .withColumn("date_chargement",  F.current_timestamp())
        .withColumnRenamed("country_name", "pays")
        .select(
            "periode",
            "Variable",
            "version",
            "base",
            "valeur",
            "date_chargement",
            "source",
            "version_active",
            "pays",
            "code_secteur",
            "lib_secteur",
            "dim_id",
            "dim_key",
        )
        .orderBy("pays", "periode")
    )

    df_final.cache()
    final_count = df_final.count()
    print(f"✅ Lignes après transformation : {final_count}")
    df_final.show(10, truncate=False)

    _ns_long = resolve_namespace(_LONG_PATH)

    injected_cols_t3 = {
        "Variable":        f"Pipeline-injected: hardcoded constant = '{VARIABLE_LABEL}'",
        "version":         f"Pipeline-injected: hardcoded constant = '{VERSION_VALUE}'",
        "version_active":  f"Pipeline-injected: hardcoded constant = '{VERSION_ACTIVE}' (will be managed by SCD2 in STEP 4)",
        "base":            f"Pipeline-injected: hardcoded constant = '{BASE_VALUE}'",
        "source":          f"Pipeline-injected: hardcoded constant = '{SOURCE_LABEL}'",
        "code_secteur":    f"Pipeline-injected: hardcoded constant = '{CODE_SECTEUR}'",
        "lib_secteur":     f"Pipeline-injected: hardcoded constant = '{LIB_SECTEUR}'",
        "dim_id":          f"Pipeline-injected: hardcoded constant = '{DIM_ID_VALUE}' cast to StringType()",
        "dim_key":         "Pipeline-injected: hardcoded null (no dimension key for HDI global indicator)",
        "date_chargement": "Pipeline-injected: job execution timestamp (using F.current_timestamp()) — overwritten at write time in STEP 4",
    }

    column_lineage_t3 = {}
    for field in df_final.schema.fields:
        col_name = field.name
        if col_name in injected_cols_t3:
            column_lineage_t3[col_name] = {
                "inputFields": [],
                "transformationDescription": injected_cols_t3[col_name],
                "transformationType": "IDENTITY",
            }
        elif col_name == "pays":
            column_lineage_t3[col_name] = {
                "inputFields": [{"namespace": _ns_long, "name": _LONG_PATH, "field": "country_name"}],
                "transformationDescription": (
                    "Renamed from 'country_name' via withColumnRenamed(). "
                    "Rows where country_code IS NULL are filtered out before this rename."
                ),
                "transformationType": "DIRECT",
            }
        elif col_name == "periode":
            column_lineage_t3[col_name] = {
                "inputFields": [{"namespace": _ns_long, "name": _LONG_PATH, "field": "periode"}],
                "transformationDescription": (
                    "Passed through from long format. "
                    "Rows where valeur IS NULL or country_code IS NULL are filtered out."
                ),
                "transformationType": "DIRECT",
            }
        else:
            column_lineage_t3[col_name] = {
                "inputFields": [{"namespace": _ns_long, "name": _LONG_PATH, "field": col_name}],
                "transformationDescription": "Directly passed through from long format DataFrame.",
                "transformationType": "DIRECT",
            }

    emit_marquez_step(
        spark_df=df_final,
        step_name="03_Enrichment_HDR",
        description=(
            f"Enriched and standardised HDR data ({final_count} rows after filtering). "
            f"Filters applied: valeur IS NOT NULL, country_code IS NOT NULL. "
            f"Renamed: country_name→pays. "
            f"Added constants: Variable='{VARIABLE_LABEL}', version='{VERSION_VALUE}', "
            f"version_active='{VERSION_ACTIVE}', base='{BASE_VALUE}', source='{SOURCE_LABEL}', "
            f"code_secteur='{CODE_SECTEUR}', lib_secteur='{LIB_SECTEUR}', "
            f"dim_id='{DIM_ID_VALUE}', dim_key=null, date_chargement=current_timestamp(). "
            f"Ordered by pays, periode."
        ),
        trans_type="TRANSFORMATION",
        inputs=[_LONG_PATH],
        outputs=[_FINAL_PATH],
        column_lineage=column_lineage_t3,
    )
    print(f"  ✅ STEP 3 complete — {final_count} rows, {len(df_final.columns)} columns")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 4 — DELTA DETECTION SCD2 + WRITE PARQUET per year
    #
    # Clé de déduplication : (pays, periode, Variable)
    #   • Première exécution (pas d'historique) → écriture complète, version_active=1
    #   • Exécutions suivantes :
    #       - Détection des lignes nouvelles ou modifiées sur valeur
    #       - Ancien enregistrement correspondant → version_active=0
    #       - Nouvelle ligne → version_active=1, date_chargement=CURRENT_TIMESTAMP
    #       - Lignes inchangées → conservées telles quelles
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print(f"[STEP 4] Delta Detection SCD2 + Write Parquet — Processing by year")
    print(f"         Clé de déduplication : (pays, periode, Variable)")
    print(f"{'='*80}")

    # spark_df_new = df_final.withColumn("valeur", F.col("valeur").cast("string"))
    spark_df_new = df_final.withColumn("valeur", F.col("valeur").cast("double"))
    distinct_years = [row["periode"] for row in spark_df_new.select("periode").distinct().collect()]
    base_path      = output_path.rstrip("/")

    sc         = spark.sparkContext
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    Path       = sc._jvm.org.apache.hadoop.fs.Path
    conf       = sc._jsc.hadoopConfiguration()

    _ns_final = resolve_namespace(_FINAL_PATH)

    for year in sorted(distinct_years):
        print(f"\n  📅 Processing Year  : {year}")

        df_year_new = spark_df_new.filter(F.col("periode") == year)
        df_year_new.createOrReplaceTempView("v_new_data")

        output_year_path = f"{base_path}/{year}"
        temp_output_path = output_year_path + "_temp_write"
        print(f"  📂 Output path      : {output_year_path}")

        history_exists = False
        change_count   = 0

        # ── Tentative de lecture de l'historique existant ─────────────────
        try:
            df_history = spark.read.parquet(output_year_path)
            df_history.createOrReplaceTempView("v_history")
            history_exists = True
            history_count  = df_history.count()
            print(f"  ✅ History found for year {year} — {history_count} existing rows")
        except Exception:
            print(f"  ℹ️  No history for {year}. Full write (all rows version_active=1).")

        # ─────────────────────────────────────────────────────────────────
        # CAS 1 : Pas d'historique → écriture initiale complète
        # ─────────────────────────────────────────────────────────────────
        if not history_exists:
            df_to_write = (
                df_year_new
                .withColumn("date_chargement", F.current_timestamp())
                .withColumn("version_active",  F.lit(1).cast("int"))
            )

        # ─────────────────────────────────────────────────────────────────
        # CAS 2 : Historique présent → delta SCD2
        #         Clé : (pays, periode, Variable)
        #         Comparaison : valeur (après ROUND pour les numériques)
        # ─────────────────────────────────────────────────────────────────
        else:
            delta_query = """
                SELECT
                    n.periode,
                    n.Variable,
                    n.version,
                    n.base,
                    n.valeur,
                    CURRENT_TIMESTAMP AS date_chargement,
                    n.source,
                    n.version_active,
                    n.pays,
                    n.code_secteur,
                    n.lib_secteur,
                    n.dim_id,
                    n.dim_key
                FROM v_new_data n
                LEFT JOIN (
                    SELECT pays, periode, Variable, valeur
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY
                                    LOWER(TRIM(pays)),
                                    periode,
                                    LOWER(TRIM(Variable))
                                ORDER BY date_chargement DESC
                            ) AS rn
                        FROM v_history
                    )
                    WHERE rn = 1
                ) h
                ON  LOWER(TRIM(n.pays))     = LOWER(TRIM(h.pays))
                AND n.periode                 = h.periode
                AND LOWER(TRIM(n.Variable)) = LOWER(TRIM(h.Variable))
                WHERE
                    h.pays IS NULL
                    OR (n.valeur IS NULL AND h.valeur IS NOT NULL)
                    OR (n.valeur IS NOT NULL AND h.valeur IS NULL)
                    OR (n.valeur <> h.valeur)
            """

            df_changes   = spark.sql(delta_query)
            change_count = df_changes.cache().count()

            if change_count > 0:
                print(f"  🔄 {change_count} changed / new row(s) detected")

                # Nouvelles lignes actives
                df_changes_latest = df_changes.withColumn("version_active", F.lit(1).cast("int"))
                df_changes_latest.createOrReplaceTempView("v_changes")

                # Clés impactées (pour désactiver les anciens enregistrements)
                df_outdated_keys = spark.sql("""
                    SELECT
                        LOWER(TRIM(pays))     AS pays_key,
                        periode                 AS periode_key,
                        LOWER(TRIM(Variable)) AS variable_key
                    FROM v_changes
                """)
                df_outdated_keys.createOrReplaceTempView("v_outdated_keys")

                # Mise à jour de l'historique : version_active=0 pour les lignes remplacées
                df_history_updated = (
                    df_history.alias("h")
                    .join(
                        df_outdated_keys.alias("k"),
                        (F.lower(F.trim(F.col("h.pays")))     == F.col("k.pays_key"))
                        & (F.col("h.periode")                   == F.col("k.periode_key"))
                        & (F.lower(F.trim(F.col("h.Variable"))) == F.col("k.variable_key")),
                        how="left",
                    )
                    .withColumn(
                        "version_active",
                        F.when(F.col("k.pays_key").isNotNull(), F.lit(0).cast("int"))
                         .otherwise(F.col("h.version_active").cast("int"))
                    )
                    .drop("pays_key", "periode_key", "variable_key")
                )

                # Union : historique mis à jour + nouvelles lignes actives
                df_to_write = df_history_updated.unionByName(
                    df_changes_latest, allowMissingColumns=True
                )

            else:
                print(f"  ✅ No changes detected for year {year}. Skipping write.")
                df_to_write = None

            df_changes.unpersist()

        # ── Écriture Parquet (via chemin temporaire pour atomicité) ───────
        if df_to_write is not None:

            # ── Column lineage pour l'étape d'écriture ────────────────────
            column_lineage_t4 = {}
            for field in df_to_write.schema.fields:
                if field.name == "date_chargement":
                    column_lineage_t4[field.name] = {
                        "inputFields": [],
                        "transformationDescription": (
                            "Pipeline-injected: CURRENT_TIMESTAMP() stamped at write time. "
                            "Only updated on new/changed rows; historical rows retain their original timestamp."
                        ),
                        "transformationType": "IDENTITY",
                    }
                elif field.name == "version_active":
                    column_lineage_t4[field.name] = {
                        "inputFields": [
                            {"namespace": _ns_final, "name": _FINAL_PATH, "field": "valeur"}
                        ],
                        "transformationDescription": (
                            "SCD Type 2 flag. "
                            + (
                                f"New/changed rows: version_active=1. "
                                f"Old rows matching changed keys (pays, periode, Variable): version_active=0 "
                                f"(LEFT JOIN on v_outdated_keys). "
                                f"{change_count} changed rows detected for year {year}."
                                if history_exists
                                else f"Initial load for year {year}: all rows set to version_active=1."
                            )
                        ),
                        "transformationType": "AGGREGATE",
                    }
                else:
                    column_lineage_t4[field.name] = {
                        "inputFields": [
                            {"namespace": _ns_final, "name": _FINAL_PATH, "field": field.name}
                        ],
                        "transformationDescription": (
                            f"Passed through to Parquet output. "
                            f"coalesce(1) — single output file per year folder."
                        ),
                        "transformationType": "DIRECT",
                    }

            # 📡 MARQUEZ — STEP 4 (per year)
            emit_marquez_step(
                spark_df=df_to_write,
                step_name=f"04_Delta_Write_HDR_{year}",
                description=(
                    f"SCD2 delta detection + Parquet write for HDR UNDP, year {year}. "
                    f"Key: (pays, periode, Variable). "
                    f"History existed: {history_exists}. "
                    + (
                        f"Changed rows detected: {change_count}. "
                        f"Old matching rows set version_active=0 via LEFT JOIN on (pays, periode, Variable). "
                        f"New/changed rows set version_active=1. "
                        f"Final write = history_updated UNION changed_rows (unionByName)."
                        if history_exists
                        else f"No history — full write. All rows version_active=1."
                    )
                    + f" Output: {output_year_path}."
                ),
                trans_type="LOAD",
                inputs=[_FINAL_PATH],
                outputs=[output_year_path],
                column_lineage=column_lineage_t4,
            )

            # Écriture atomique via chemin temporaire
            df_to_write.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

            print(f"\n  📊 FINAL DATAFRAME SCHEMA (year {year})")
            for field in df_to_write.schema.fields:
                print(
                    f"     Column: {field.name:20s} | "
                    f"Type: {field.dataType.simpleString():10s} | "
                    f"Nullable: {field.nullable}"
                )
            print(f"     📊 Total columns: {len(df_to_write.columns)}")

            try:
                target_uri = sc._jvm.java.net.URI(output_year_path)
                fs = FileSystem.get(target_uri, conf)
                if fs.exists(Path(output_year_path)):
                    fs.delete(Path(output_year_path), True)
                fs.rename(Path(temp_output_path), Path(output_year_path))
                print(f"  ✅ Year {year} successfully saved → {output_year_path}")
            except Exception as e:
                print(f"  ❌ FS Error for year {year}: {e}")

        # ── Nettoyage des vues temporaires ────────────────────────────────
        spark.catalog.dropTempView("v_new_data")
        if history_exists:
            spark.catalog.dropTempView("v_history")
        for view in ["v_changes", "v_outdated_keys"]:
            try:
                spark.catalog.dropTempView(view)
            except Exception:
                pass

    print(f"\n🚀 Données écrites sous : {base_path}/<année>/")
    df_final.unpersist()


# ─── Entry-point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()

    input_path  = f"{RAW_BUCKET}/{args.year}/{args.month}/PNUD/hdr/{LOCAL_FILE_NAME}"
    output_path = f"{TRANSFORMED_BUCKET}/PNUD/hdi/"

    print(f"📂 Input  : {input_path}")
    print(f"📂 Output : {output_path}")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        run(spark, input_path, output_path)
    finally:
        spark.stop()
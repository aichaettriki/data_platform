from pyspark.sql import SparkSession, functions as F

from pyspark.sql.window import Window

from datetime import datetime

import uuid

import logging
 
from refined_utils import *

from openlineage.client import OpenLineageClient

from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

# --------------------------------------------------

# CONFIG

# --------------------------------------------------

COL_NAME = "source"

ID_NAME  = "id_source"

MARQUEZ_URL     = "http://marquez:5000"

PIPELINE_RUN_ID = str(uuid.uuid4())

NS_DATA_LAKE      = "data_lake"

NS_DATA_WAREHOUSE = "data_warehouse"
 
logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

# --------------------------------------------------

# MARQUEZ HELPERS

# --------------------------------------------------

def resolve_namespace(path: str) -> str:

    if "03-refined" in path or "dimensions/" in path:

        return NS_DATA_WAREHOUSE

    return NS_DATA_LAKE

def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs, column_lineage=None, extra_metrics=None):

    try:

        client = OpenLineageClient(url=MARQUEZ_URL)

        output_fields = [{"name": f.name, "type": f.dataType.simpleString()} for f in spark_df.schema.fields]

        output_facets = {

            "schema": {"_producer": "dim-source-producer", "fields": output_fields},

            "documentation": {"_producer": "dim-source-producer", "description": f"[{trans_type}] {description}"},

        }

        if column_lineage: output_facets["columnLineage"] = {"_producer": "dim-source-producer", "fields": column_lineage}

        if extra_metrics: output_facets["dataQualityMetrics"] = {"_producer": "dim-source-producer", "metrics": extra_metrics}

        event = RunEvent(

            eventType=RunState.COMPLETE,

            eventTime=datetime.now().isoformat() + "Z",

            run=Run(runId=PIPELINE_RUN_ID),

            job=Job(namespace="refined_dims", name=step_name),

            inputs=[Dataset(namespace=resolve_namespace(i), name=i) for i in inputs],

            outputs=[Dataset(namespace=resolve_namespace(o), name=o, facets=output_facets) for o in outputs],

            producer="dim-source-producer",

        )

        client.emit(event)

    except Exception as e:

        log.warning(f"⚠️ Marquez error: {e}")

# --------------------------------------------------

# MAIN

# --------------------------------------------------

if __name__ == "__main__":

    spark = create_spark("Dim_Source_Process")

    cfg = MinIOConfig()

    SOURCE_PATH  = f"s3a://{cfg.bucket_transformed}/"

    DIM_FOLDER   = f"dimensions/dim_{COL_NAME}"

    DIM_PATH     = f"s3a://{cfg.bucket_refined}/{DIM_FOLDER}/"

    MEM_DISTINCT = "memory://dim_source/distinct"

    MEM_NEW      = "memory://dim_source/new_values"

    MEM_ENRICHED = "memory://dim_source/enriched"

    log.info("🚀 DIM SOURCE JOB START")

    # ==================================================

    # STEP 1 — EXTRACT DISTINCT

    # ==================================================

    df_src_raw = read_transformed(spark, cfg)

    snap = get_tech_snapshot(df_src_raw)

    # Extract distinct business keys

    vals = df_src_raw.select(COL_NAME).distinct().filter(F.col(COL_NAME).isNotNull())

    total_sources = vals.count()

    emit_marquez_step(vals, "01_Extract_Distinct_Source", f"Detected {total_sources} unique sources", "EXTRACT", [SOURCE_PATH], [MEM_DISTINCT])

    # ==================================================

    # STEP 2 — DETECT NEW VALUES (ANTI-JOIN)

    # ==================================================

    try:

        # Optimization: Only read the business key column from the target

        existing = spark.read.parquet(DIM_PATH).select(COL_NAME)

        new_v = vals.join(existing, on=COL_NAME, how="left_anti")

        history_exists = True

    except Exception:

        new_v = vals

        history_exists = False

    new_count = new_v.count()

    emit_marquez_step(new_v, "02_Detect_New_Source", f"New records: {new_count}", "TRANSFORMATION", [MEM_DISTINCT, DIM_PATH] if history_exists else [MEM_DISTINCT], [MEM_NEW])

    # ==================================================

    # STEP 3 — GENERATE IDS & AUDIT COLS

    # ==================================================

    if new_count > 0:

        # Get starting ID from current Max in Refined

        start_id = get_next_id(spark, cfg, DIM_FOLDER, ID_NAME)

        start_id = 1 if start_id <= 0 else start_id

        window_spec = Window.orderBy(COL_NAME)

        new_v_enriched = new_v.withColumn(

            ID_NAME,

            F.row_number().over(window_spec) + (start_id - 1)

        ).withColumn(

            "date_chargement", F.lit(snap['date_chargement'])

        ).withColumn(

            "version_active", F.lit(snap['version_active'])

        )

        emit_marquez_step(new_v_enriched, "03_Generate_IDs", "Assigning sequential IDs", "TRANSFORMATION", [MEM_NEW], [MEM_ENRICHED])

        # ==================================================

        # STEP 4 — WRITE (TRUE APPEND)

        # ==================================================

        # 1. We NO LONGER union with 'existing'. 

        # 2. We bypass 'write_refined' because it is hardcoded to Overwrite.

        log.info(f"Appending {new_count} records to {DIM_PATH}")

        # Coalesce(1) prevents generating many small files

        new_v_enriched.coalesce(1).write.mode("append").parquet(DIM_PATH)

        # Lineage facet for the load

        ns_mem_s3 = resolve_namespace(MEM_ENRICHED)

        ns_dim_phys = resolve_namespace(DIM_PATH)

        col_lineage_final = {

            c: {"inputFields": [{"namespace": ns_mem_s3, "name": MEM_ENRICHED, "field": c}]}

            for c in new_v_enriched.columns

        }

        emit_marquez_step(

            new_v_enriched,

            "04_Append_Dim_Source",

            f"Appended {new_count} new records.",

            "LOAD",

            [MEM_ENRICHED],

            [DIM_PATH],

            column_lineage=col_lineage_final,

            extra_metrics={"inserted_rows": new_count}

        )

        log.info(f"✅ Dim_Source updated: {new_count} records inserted.")

    else:

        log.info("ℹ️ No new source values found. Skipping write.")

    spark.stop()
 
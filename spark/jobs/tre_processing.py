# import sys
# import os
# import logging
# import re
# import uuid
# import pandas as pd
# import openpyxl
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from common.spark_session import create_spark_session, stop_spark_session
# from pyspark.sql.types import StringType
# from pyspark.sql.functions import current_timestamp

# # --- LINEAGE IMPORTS ---
# from openlineage.client import OpenLineageClient
# from openlineage.client.run import Job, Run, RunEvent, Dataset, RunState


# # ──────────────────────────────────────────────────────────────────────────────
# # LOGGING
# # ──────────────────────────────────────────────────────────────────────────────
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )
# logger = logging.getLogger(__name__)


# # ──────────────────────────────────────────────────────────────────────────────
# # MARQUEZ LINEAGE EMITTER
# # ──────────────────────────────────────────────────────────────────────────────
# PIPELINE_RUN_ID = str(uuid.uuid4())
# MARQUEZ_URL = os.getenv("MARQUEZ_URL", "http://marquez:5000")


# def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs,
#                       input_schema_fields=None, column_lineage=None):
#     """
#     Sends rich lineage metadata to Marquez for a given pipeline step.

#     Parameters:
#         spark_df            : Spark DataFrame — used to extract output schema
#         step_name           : Logical name of the step (e.g. '03_Spark_DF_Creation')
#         description         : Human-readable description of what this step does
#         trans_type          : Transformation type label (e.g. 'EXTRACT', 'TRANSFORM', 'LOAD')
#         inputs              : List of input dataset paths/names (source)
#         outputs             : List of output dataset paths/names (destination)
#         input_schema_fields : Optional list of dicts describing the input schema
#                               (used for raw files like Excel where we have no Spark DF)
#         column_lineage      : Optional dict describing per-column lineage
#                               (which input field each output field comes from)
#     """
#     try:
#         client = OpenLineageClient(url=MARQUEZ_URL)

#         # ── Output schema facet ───────────────────────────────────────────────
#         output_fields = [
#             {
#                 "name": field.name,
#                 "type": field.dataType.simpleString(),
#                 "description": f"nullable={field.nullable}",
#             }
#             for field in spark_df.schema.fields
#         ]

#         output_dataset_facets = {
#             "schema": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
#                 "fields": output_fields,
#             },
#             "documentation": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json#",
#                 "description": f"[{trans_type}] {description}",
#             },
#         }

#         # ── Column lineage facet (if provided) ───────────────────────────────
#         if column_lineage:
#             output_dataset_facets["columnLineage"] = {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json#",
#                 "fields": column_lineage,
#             }

#         # ── Input schema facet (if provided — e.g. raw Excel file) ───────────
#         input_dataset_facets = {}
#         if input_schema_fields:
#             input_dataset_facets["schema"] = {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json#",
#                 "fields": input_schema_fields,
#             }

#         # ── Run facets (processing engine info) ──────────────────────────────
#         run_facets = {
#             "processing_engine": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ProcessingEngineRunFacet.json#",
#                 "version": "3.5.1",
#                 "name": "Apache Spark",
#                 "openlineageAdapterVersion": "itceq-1.0",
#             }
#         }

#         # ── Job facets (step description as SQL-like query) ───────────────────
#         job_facets = {
#             "sql": {
#                 "_producer": "itceq-spark-producer",
#                 "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json#",
#                 "query": description,
#             }
#         }

#         event = RunEvent(
#             eventType=RunState.COMPLETE,
#             eventTime=datetime.now().isoformat() + "Z",
#             run=Run(runId=PIPELINE_RUN_ID, facets=run_facets),
#             job=Job(namespace="itceq_prod", name=step_name, facets=job_facets),
#             inputs=[
#                 Dataset(namespace="itceq_prod", name=i, facets=input_dataset_facets)
#                 for i in inputs
#             ],
#             outputs=[
#                 Dataset(namespace="itceq_prod", name=o, facets=output_dataset_facets)
#                 for o in outputs
#             ],
#             producer="itceq-spark-producer",
#         )

#         client.emit(event)
#         print(f"📡 Marquez Updated: [{trans_type}] {step_name} | {len(output_fields)} output columns")

#     except Exception as e:
#         print(f"⚠️  Marquez metadata error at step '{step_name}': {e}")


# # ──────────────────────────────────────────────────────────────────────────────
# # S3 FILE DISCOVERY
# # ──────────────────────────────────────────────────────────────────────────────
# def find_latest_files_recursive(spark, base_path, specific_folder=""):
#     """
#     Handles the structure: s3a://01-raw/YEAR/MONTH/SOURCE_FOLDER/...
#     Returns a list of .xlsx file paths from the most recent YEAR/MONTH folder.
#     """
#     print(f"Searching for latest data starting at: {base_path}")
#     sc = spark.sparkContext
#     try:
#         Path = sc._jvm.org.apache.hadoop.fs.Path
#         FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#         conf = sc._jsc.hadoopConfiguration()
#         fs = FileSystem.get(sc._jvm.java.net.URI(base_path), conf)

#         def get_sorted_subdirs(parent_path_str):
#             p = Path(parent_path_str)
#             if not fs.exists(p):
#                 return []
#             stats = fs.listStatus(p)
#             dirs = [s.getPath() for s in stats if s.isDirectory()]
#             dirs.sort(key=lambda x: x.getName(), reverse=True)
#             return dirs

#         years = get_sorted_subdirs(base_path)
#         if not years:
#             print("  ❌ No year folders found!")
#             return []
#         latest_year = years[0]
#         print(f"  > Latest Year: {latest_year.getName()}")

#         months = get_sorted_subdirs(latest_year.toString())
#         if not months:
#             print("  ❌ No month folders found!")
#             return []
#         latest_month = months[0]
#         print(f"  > Latest Month: {latest_month.getName()}")

#         search_start_path = Path(latest_month.toString())

#         if specific_folder:
#             target_path = Path(f"{latest_month.toString()}/{specific_folder}")
#             if fs.exists(target_path):
#                 search_start_path = target_path
#                 print(f"  > Found target folder: {search_start_path.toString()}")
#             else:
#                 print(f"  ⚠️  Warning: Specific folder '{specific_folder}' not found, searching entire month...")

#         print(f"  > Searching from: {search_start_path.toString()}")

#         files_found = []
#         stack = [search_start_path]
#         while stack:
#             current_path = stack.pop()
#             try:
#                 contents = fs.listStatus(current_path)
#                 for item in contents:
#                     if item.isDirectory():
#                         stack.append(item.getPath())
#                     elif item.isFile():
#                         fname = item.getPath().getName()
#                         if fname.endswith(".xlsx") and not fname.startswith("~"):
#                             full_path = item.getPath().toString()
#                             files_found.append(full_path)
#                             print(f"  ✓ Found file: {full_path}")
#             except Exception as e:
#                 print(f"  ! Error reading {current_path}: {e}")

#         return files_found
#     except Exception as e:
#         print(f"❌ Error traversing directory: {e}")
#         import traceback
#         traceback.print_exc()
#         return []


# def download_from_s3(spark, full_s3_path, local_path):
#     safe_s3_path = full_s3_path.replace(" ", "%20")
#     print(f"  Downloading {full_s3_path}...")
#     sc = spark.sparkContext
#     try:
#         Path = sc._jvm.org.apache.hadoop.fs.Path
#         FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#         conf = sc._jsc.hadoopConfiguration()
#         URI = sc._jvm.java.net.URI
#         uri = URI(safe_s3_path)
#         fs = FileSystem.get(uri, conf)
#         fs.copyToLocalFile(False, Path(uri), Path(local_path), True)
#         return True
#     except Exception as e:
#         print(f"  Error downloading: {e}")
#         return False


# def extract_tables_from_sheet(wb, sheet_name):
#     ws = wb[sheet_name]
#     max_row = ws.max_row
#     max_col = ws.max_column

#     data_start_row = None
#     header_row = None

#     for row_idx in range(1, min(15, max_row + 1)):
#         first_cell = ws.cell(row=row_idx, column=1).value
#         if first_cell:
#             first_cell_str = str(first_cell).strip()
#             if any(k in first_cell_str for k in ["TABLEAUX", "Unité", "millions", "source", "MATRICE"]):
#                 continue
#             if len(first_cell_str) <= 3 and (
#                 first_cell_str.isdigit() or first_cell_str.upper() in ["CR1", "CR2", "CR3", "CR"]
#             ):
#                 data_start_row = row_idx
#                 header_row = row_idx - 1
#                 break

#     if data_start_row is None:
#         return []

#     all_data = []
#     for row_idx in range(header_row, max_row + 1):
#         first_cell = ws.cell(row=row_idx, column=1).value
#         if first_cell and row_idx > data_start_row:
#             first_str = str(first_cell).strip().lower()
#             if any(
#                 keyword in first_str
#                 for keyword in ["total", "totaux", "somme", "equilibre", "emplois totaux", "ressources totales"]
#             ):
#                 break

#         row_data = []
#         for col_idx in range(1, max_col + 1):
#             cell_val = ws.cell(row=row_idx, column=col_idx).value
#             row_data.append(cell_val)
#         all_data.append(row_data)

#     return [{"data": all_data, "sheet": sheet_name}]


# def is_numeric_string(s):
#     """Check if a string represents a number (int or float)"""
#     if not s or not isinstance(s, str):
#         return False
#     s = s.strip()
#     try:
#         float(s)
#         return True
#     except (ValueError, OverflowError):
#         return False


# def clean_table(data_rows):
#     if not data_rows:
#         return None

#     # --- 1. FIND DATA START ---
#     data_start_idx = 0
#     for i in range(min(len(data_rows), 15)):
#         row = data_rows[i]
#         if not row:
#             continue
#         first_cell = str(row[0]).strip() if row[0] else ""
#         second_cell = str(row[1]).strip() if len(row) > 1 and row[1] else ""

#         if any(k in first_cell for k in ["Unité", "TABLEAUX", "millions", "source"]):
#             continue

#         if len(first_cell) <= 4 and (
#             first_cell.isdigit() or first_cell.upper() in ["CR1", "CR2", "CR3", "CR"]
#         ) and len(second_cell) > 5:
#             data_start_idx = i
#             break

#     header_row_idx = data_start_idx - 1 if data_start_idx > 0 else 0
#     raw_headers = data_rows[header_row_idx] if header_row_idx < len(data_rows) else data_rows[0]

#     # --- DYNAMIC CI DETECTION ---
#     print(f"  🔍 Detecting CI matrix region from header structure...")
#     matrice_start_col = None

#     for row_idx in range(min(data_start_idx, len(data_rows))):
#         row = data_rows[row_idx]
#         if not row:
#             continue
#         for col_idx, cell in enumerate(row):
#             if cell and "MATRICE" in str(cell).strip().upper():
#                 matrice_start_col = col_idx
#                 print(f"     ✓ Found 'MATRICE' label at row {row_idx}, column {col_idx} → all columns from here are CI")
#                 break
#         if matrice_start_col is not None:
#             break

#     matrice_ci_columns = set()

#     if matrice_start_col is not None:
#         for col_idx in range(matrice_start_col, len(raw_headers)):
#             h = raw_headers[col_idx]
#             header_str = str(h).strip() if h else ""
#             if not header_str or is_numeric_string(header_str):
#                 matrice_ci_columns.add(col_idx)
#             else:
#                 if len(matrice_ci_columns) >= 2:
#                     break
#     else:
#         print(f"     ⚠️  No 'MATRICE' label found — falling back to numeric header detection")
#         non_id_seen = False
#         ci_started = False
#         consecutive = 0

#         for col_idx, h in enumerate(raw_headers):
#             if col_idx < 2:
#                 continue
#             header_str = str(h).strip() if h else ""
#             if is_numeric_string(header_str):
#                 try:
#                     num_val = float(header_str)
#                     if 1 <= num_val < 100 and float(num_val).is_integer():
#                         if non_id_seen:
#                             ci_started = True
#                         if ci_started:
#                             matrice_ci_columns.add(col_idx)
#                             consecutive += 1
#                     else:
#                         if ci_started and consecutive >= 2:
#                             break
#                 except (ValueError, OverflowError):
#                     pass
#             elif header_str:
#                 non_id_seen = True
#                 if ci_started and consecutive >= 2:
#                     break
#                 elif ci_started:
#                     matrice_ci_columns.clear()
#                     ci_started = False
#                     consecutive = 0

#     print(f"     → Found {len(matrice_ci_columns)} CI columns")

#     # --- 2. GENERATE HEADERS WITH CI_ PREFIX ---
#     headers = []
#     header_counts = {}

#     for i, h in enumerate(raw_headers):
#         if h is None or str(h).strip() == "" or h is False:
#             base_name = f"Col_{i}"
#         else:
#             base_name = " ".join(str(h).replace("\n", " ").split())
#             if "Unité" in base_name or "millions" in base_name:
#                 base_name = f"Col_{i}"

#         if i in matrice_ci_columns:
#             header_str = str(h).strip() if h else ""
#             try:
#                 num_val = float(header_str)
#                 base_name = f"CI_{int(num_val):02d}"
#                 print(f"     → Renaming column {i}: '{header_str}' → '{base_name}'")
#             except (ValueError, OverflowError):
#                 base_name = f"CI_col{i}"

#         if base_name in header_counts:
#             header_counts[base_name] += 1
#             unique_name = f"{base_name}_{header_counts[base_name]}"
#         else:
#             header_counts[base_name] = 0
#             unique_name = base_name

#         headers.append(unique_name)

#     # --- 3. EXTRACT BODY ---
#     body = []
#     for i in range(data_start_idx, len(data_rows)):
#         row = data_rows[i]
#         if not row:
#             continue
#         first_cell = str(row[0]).strip().lower() if row[0] else ""
#         if "totaux" in first_cell or "total" in first_cell:
#             break
#         body.append(row)

#     if not body:
#         return None

#     df = pd.DataFrame(body, columns=headers)

#     # --- 4. STANDARD CLEANUP ---
#     if df.shape[1] >= 2:
#         headers[0] = "code_secteur"
#         headers[1] = "lib_secteur"
#         df.columns = headers

#         df = df[df["code_secteur"].astype(str) != "None"]
#         df = df[
#             ~df["code_secteur"]
#             .astype(str)
#             .str.contains("Unité|TABLEAUX|millions|source", case=False, na=False)
#         ]

#         df["code_secteur"] = df["code_secteur"].astype(str).str.strip()
#         df["lib_secteur"] = df["lib_secteur"].astype(str).str.strip()

#         df = df.dropna(how="all")

#     df = df.dropna(axis=1, how="all")
#     df = df.reset_index(drop=True)

#     # --- 5. SPACER REMOVAL ---
#     cols_to_drop = []
#     if "code_secteur" in df.columns and "lib_secteur" in df.columns:
#         ref_code = df["code_secteur"]
#         ref_label = df["lib_secteur"]

#         for col in df.columns:
#             if col in ["code_secteur", "lib_secteur"]:
#                 continue
#             if col.startswith("CI_"):
#                 continue

#             curr_col_data = df[col].astype(str).str.strip()
#             if curr_col_data.equals(ref_code):
#                 cols_to_drop.append(col)
#             elif curr_col_data.equals(ref_label):
#                 cols_to_drop.append(col)
#             elif (
#                 str(col).startswith("Col_")
#                 and len(df) > 0
#                 and str(df[col].iloc[0]) == str(df["code_secteur"].iloc[0])
#             ):
#                 cols_to_drop.append(col)

#     if cols_to_drop:
#         print(f"     → Removing {len(cols_to_drop)} spacer columns")
#         df = df.drop(columns=cols_to_drop)

#     if not df.columns.is_unique:
#         df.columns = pd.io.parsers.ParserBase({"names": df.columns})._maybe_dedup_names(df.columns)

#     return df


# def merge_sheet_tables_horizontally(table_list):
#     if not table_list:
#         return None
#     return clean_table(table_list[0]["data"])


# def to_long_format_spark(df):
#     """
#     Pivote le DataFrame en format long.
#     Les colonnes Pays, Source, Base, Rang et date_chargement sont conservées comme identifiants.
#     """
#     id_vars = [
#         "code_secteur", "lib_secteur", "Annee", "Version",
#         "date_chargement", "Pays", "Source", "Base", "Rang",
#     ]

#     value_columns = [c for c in df.columns if c not in id_vars]

#     if not value_columns:
#         return df

#     stack_parts = [f"'{c}', CAST(`{c}` AS STRING)" for c in value_columns]
#     stack_expr = f"stack({len(value_columns)}, {', '.join(stack_parts)}) as (Variable, Valeur)"

#     long_df = df.select(
#         F.col("code_secteur"),
#         F.col("lib_secteur"),
#         F.col("Annee"),
#         F.col("Version"),
#         F.col("date_chargement"),
#         F.col("Pays"),
#         F.col("Source"),
#         F.col("Base"),
#         F.col("Rang"),
#         F.expr(stack_expr),
#     )
#     return long_df.where(F.col("Valeur").isNotNull())


# def extract_year_version_from_sheet(sheet_name):
#     clean_name = sheet_name.strip()
#     match_year = re.search(r"(20\d{2})", clean_name)
#     if match_year:
#         year = match_year.group(1)
#         parts = clean_name.split(year)
#         version = parts[-1].strip() if len(parts) > 1 and parts[-1].strip() else "C"
#         return year, version
#     return "Unknown", "Unknown"


# def extract_category_path(full_s3_path, raw_bucket_root):
#     """
#     Extrait le chemin de catégorie depuis le chemin S3 complet.
#     Exemple : s3a://01-raw/2026/02/INS/TRE/file.xlsx → INS/TRE
#     """
#     clean_path = full_s3_path.replace("s3a://", "").strip("/")
#     clean_root = raw_bucket_root.replace("s3a://", "").strip("/")

#     if clean_path.startswith(clean_root):
#         relative_path = clean_path[len(clean_root) :].strip("/")
#         parts = relative_path.split("/")
#         if len(parts) > 3:
#             category_parts = parts[2:-1]
#             return "/".join(category_parts)

#     return "UNKNOWN_CATEGORY"


# def format_code_secteur(val):
#     """Formate code_secteur en préservant les zéros initiaux (ex: '01', '06')."""
#     if val is None or pd.isna(val) or str(val).strip().lower() in ["nan", "none", ""]:
#         return "0"
#     s_val = str(val).strip()

#     try:
#         f = float(s_val)
#         if f.is_integer():
#             if s_val.startswith("0") or (s_val.replace(".", "").replace("-", "").startswith("0")):
#                 return f"{int(f):02d}"
#             else:
#                 return str(int(f))
#     except (ValueError, OverflowError):
#         pass

#     return s_val


# def format_excel_value(val):
#     """Formate les valeurs Excel en supprimant le .0 des entiers."""
#     if val is None or pd.isna(val) or str(val).strip().lower() in ["nan", "none", ""]:
#         return "0"
#     s_val = str(val).strip()

#     try:
#         f = float(s_val)
#         if f.is_integer():
#             return str(int(f))
#     except (ValueError, OverflowError):
#         pass

#     return s_val


# # ──────────────────────────────────────────────────────────────────────────────
# # CALCUL DE version_active
# # ──────────────────────────────────────────────────────────────────────────────
# def compute_version_active(df_new, df_history, history_exists):
#     return df_new.withColumn("version_active", F.lit(1).cast("int"))


# # ──────────────────────────────────────────────────────────────────────────────
# # TRAITEMENT D'UN FICHIER
# # ──────────────────────────────────────────────────────────────────────────────
# def process_single_file(spark, full_s3_file_path, raw_bucket_root, target_bucket):

#     file_name = full_s3_file_path.split("/")[-1]
#     local_tmp_path = f"/tmp/{file_name}"
#     category_path = extract_category_path(full_s3_file_path, raw_bucket_root)

#     print(f"\n{'='*80}")
#     print(f"📁 Processing File : {file_name}")
#     print(f"{'='*80}")

#     if not download_from_s3(spark, full_s3_file_path, local_tmp_path):
#         return

#     try:
#         # ══════════════════════════════════════════════════════════════════════
#         # TASK 1 — EXCEL INGESTION
#         # ══════════════════════════════════════════════════════════════════════
#         print(f"\n[TASK 1] Excel Ingestion — Reading sheets from: {file_name}")

#         wb = openpyxl.load_workbook(local_tmp_path, data_only=True)
#         all_sheets_data = []

#         for sheet_name in wb.sheetnames:
#             tables = extract_tables_from_sheet(wb, sheet_name)
#             if not tables:
#                 continue
#             sheet_df = merge_sheet_tables_horizontally(tables)
#             if sheet_df is not None and not sheet_df.empty:
#                 year, version = extract_year_version_from_sheet(sheet_name)
#                 sheet_df["Annee"] = year
#                 sheet_df["Version"] = version
#                 sheet_df = sheet_df.astype(str)
#                 sheet_df["Annee"] = sheet_df["Annee"].astype(int)
#                 all_sheets_data.append(sheet_df)

#         if not all_sheets_data:
#             print(f"  ⚠️  WARNING: No data found in {file_name}")
#             return

#         final_pd_df = pd.concat(all_sheets_data, ignore_index=True)
#         print(f"  ✅ TASK 1 complete — {len(all_sheets_data)} sheet(s) ingested, {len(final_pd_df)} rows total")

#         # ══════════════════════════════════════════════════════════════════════
#         # TASK 2 — NORMALIZATION
#         # ══════════════════════════════════════════════════════════════════════
#         print(f"\n[TASK 2] Normalization — Formatting values and sector codes")

#         id_cols = ["code_secteur", "lib_secteur", "Annee", "Version"]
#         value_cols = [c for c in final_pd_df.columns if c not in id_cols]

#         print(f"  🔧 Normalizing {len(value_cols)} value columns...")
#         for col_name in value_cols:
#             final_pd_df[col_name] = final_pd_df[col_name].apply(format_excel_value)

#         print(f"  🔧 Normalizing code_secteur (preserving leading zeros)...")
#         final_pd_df["code_secteur"] = final_pd_df["code_secteur"].apply(format_code_secteur)

#         print(f"  ✅ TASK 2 complete")

#         # ══════════════════════════════════════════════════════════════════════
#         # TASK 3 — SPARK DATAFRAME CREATION + BUSINESS COLUMNS
#         # ══════════════════════════════════════════════════════════════════════
#         print(f"\n[TASK 3] Spark DataFrame Creation — Adding metadata columns")

#         final_pd_df.columns = [str(col) for col in final_pd_df.columns]
#         spark_df = spark.createDataFrame(final_pd_df)

#         spark_df = (
#             spark_df
#             .withColumn("date_chargement", current_timestamp())
#             .withColumn("Pays",   F.lit("Tunisie"))
#             .withColumn("Rang",   F.lit(None).cast("int"))
#             .withColumn("Source", F.lit("TRE"))
#             .withColumn("Base",   F.lit(None).cast("int"))
#             .withColumn("code_secteur", F.trim(F.col("code_secteur")))
#             .withColumn("lib_secteur",  F.trim(F.col("lib_secteur")))
#         )

#         # ── Build raw Excel schema for Marquez input ──────────────────────────
#         # These are the columns as they exist in the Excel file (all strings)
#         excel_source_cols = list(final_pd_df.columns)
#         raw_schema_fields = []
#         for col in excel_source_cols:
#             if col in ["code_secteur", "lib_secteur"]:
#                 desc = "Sector identifier column — read directly from Excel row"
#             elif col in ["Annee", "Version"]:
#                 desc = "Extracted from Excel sheet name (e.g. '2021 C' → Annee=2021, Version=C)"
#             elif col.startswith("CI_"):
#                 desc = f"CI matrix column — intermediate consumption value from Excel header '{col}'"
#             else:
#                 desc = f"Value column from Excel file — row data for variable '{col}'"
#             raw_schema_fields.append({
#                 "name": col,
#                 "type": "string",
#                 "description": desc,
#             })

#         # ── Build column lineage: source vs pipeline-injected columns ─────────
#         added_cols_t3 = {
#             "date_chargement": "Pipeline-injected: current timestamp at load time (current_timestamp())",
#             "Pays":            "Pipeline-injected: hardcoded constant = 'Tunisie'",
#             "Source":          "Pipeline-injected: hardcoded constant = 'TRE'",
#             "Rang":            "Pipeline-injected: hardcoded null (no rank available at this stage)",
#             "Base":            "Pipeline-injected: hardcoded null (no base year available at this stage)",
#         }

#         column_lineage_task3 = {}
#         for field in spark_df.schema.fields:
#             if field.name in added_cols_t3:
#                 column_lineage_task3[field.name] = {
#                     "inputFields": [],
#                     "transformationDescription": added_cols_t3[field.name],
#                     "transformationType": "IDENTITY",
#                 }
#             else:
#                 col_desc = "Trimmed with TRIM()" if field.name in ["code_secteur", "lib_secteur"] else "Directly mapped from Excel"
#                 column_lineage_task3[field.name] = {
#                     "inputFields": [
#                         {
#                             "namespace": "itceq_prod",
#                             "name": full_s3_file_path,
#                             "field": field.name,
#                         }
#                     ],
#                     "transformationDescription": col_desc,
#                     "transformationType": "DIRECT",
#                 }

#         # 📡 MARQUEZ — TASK 3
#         emit_marquez_step(
#             spark_df=spark_df,
#             step_name="03_Spark_DF_Creation",
#             description=(
#                 f"Read Excel file '{file_name}' ({len(all_sheets_data)} sheets, {len(final_pd_df)} rows). "
#                 f"Converted to Spark DataFrame ({len(spark_df.columns)} columns). "
#                 f"Added pipeline columns: date_chargement=now(), Pays='Tunisie', Source='TRE', Rang=null, Base=null. "
#                 f"Applied TRIM() on code_secteur and lib_secteur."
#             ),
#             trans_type="EXTRACT",
#             inputs=[full_s3_file_path],
#             outputs=[f"memory://spark_df/{file_name}"],
#             input_schema_fields=raw_schema_fields,
#             column_lineage=column_lineage_task3,
#         )
#         print(f"  ✅ TASK 3 complete — Spark DataFrame created with {len(spark_df.columns)} columns")

#         # ══════════════════════════════════════════════════════════════════════
#         # TASK 4 — LONG FORMAT PIVOT
#         # ══════════════════════════════════════════════════════════════════════
#         print(f"\n[TASK 4] Long Format Pivot — Unpivoting wide columns to (Variable, Valeur)")

#         # Track which columns are value columns (will be unpivoted)
#         id_vars_wide = ["code_secteur", "lib_secteur", "Annee", "Version",
#                         "date_chargement", "Pays", "Source", "Base", "Rang"]
#         value_cols_wide = [c for c in spark_df.columns if c not in id_vars_wide]

#         long_df = to_long_format_spark(spark_df)

#         long_df = long_df.withColumn(
#             "Variable",
#             F.regexp_replace(F.col("Variable"), r"^(\d+)\.0$", "$1"),
#         )
#         long_df = long_df.withColumn("Variable", F.trim(F.col("Variable")))

#         print("  🔧 Converting all columns to lowercase...")
#         long_df = long_df.toDF(*[c.lower() for c in long_df.columns])

#         long_df = (
#             long_df
#             .withColumn("dim_id",  F.lit("None"))
#             .withColumn("dim_key", F.lit("None"))
#         )

#         # ── Build column lineage for the pivot ────────────────────────────────
#         id_cols_lower = ["code_secteur", "lib_secteur", "annee", "version",
#                          "date_chargement", "pays", "source", "base", "rang"]

#         column_lineage_task4 = {}

#         # Identity columns — passed through unchanged
#         for col in id_cols_lower:
#             column_lineage_task4[col] = {
#                 "inputFields": [
#                     {
#                         "namespace": "itceq_prod",
#                         "name": f"memory://spark_df/{file_name}",
#                         "field": col,
#                     }
#                 ],
#                 "transformationDescription": "Identity column — preserved as-is during stack() unpivot. Renamed to lowercase.",
#                 "transformationType": "DIRECT",
#             }

#         # Variable — derived from wide column names
#         column_lineage_task4["variable"] = {
#             "inputFields": [
#                 {
#                     "namespace": "itceq_prod",
#                     "name": f"memory://spark_df/{file_name}",
#                     "field": col,
#                 }
#                 for col in value_cols_wide
#             ],
#             "transformationDescription": (
#                 f"Generated by Spark stack() unpivot: column name from one of "
#                 f"{len(value_cols_wide)} wide columns ({', '.join(value_cols_wide[:5])}...). "
#                 f"Trailing '.0' cleaned with regexp_replace(). Whitespace trimmed."
#             ),
#             "transformationType": "AGGREGATE",
#         }

#         # Valeur — derived from wide column values
#         column_lineage_task4["valeur"] = {
#             "inputFields": [
#                 {
#                     "namespace": "itceq_prod",
#                     "name": f"memory://spark_df/{file_name}",
#                     "field": col,
#                 }
#                 for col in value_cols_wide
#             ],
#             "transformationDescription": (
#                 f"Generated by Spark stack() unpivot: cell value from one of "
#                 f"{len(value_cols_wide)} wide columns. CAST to STRING then filtered: Valeur IS NOT NULL."
#             ),
#             "transformationType": "AGGREGATE",
#         }

#         # Surrogate keys — injected by pipeline
#         column_lineage_task4["dim_id"] = {
#             "inputFields": [],
#             "transformationDescription": "Pipeline-injected placeholder: hardcoded 'None' (dimension mapping not yet available)",
#             "transformationType": "IDENTITY",
#         }
#         column_lineage_task4["dim_key"] = {
#             "inputFields": [],
#             "transformationDescription": "Pipeline-injected placeholder: hardcoded 'None' (dimension key not yet resolved)",
#             "transformationType": "IDENTITY",
#         }

#         # 📡 MARQUEZ — TASK 4
#         emit_marquez_step(
#             spark_df=long_df,
#             step_name="04_Long_Format_Pivot",
#             description=(
#                 f"Unpivoted {len(value_cols_wide)} wide columns → (variable, valeur) pairs "
#                 f"using Spark stack() expression. "
#                 f"Rows with null Valeur filtered out. "
#                 f"Variable names cleaned (regexp_replace trailing '.0', TRIM). "
#                 f"All column names lowercased. "
#                 f"Added dim_id='None' and dim_key='None' as placeholder surrogate keys."
#             ),
#             trans_type="TRANSFORMATION",
#             inputs=[f"memory://spark_df/{file_name}"],
#             outputs=[f"memory://spark_df/{file_name}/long"],
#             column_lineage=column_lineage_task4,
#         )
#         print(f"  ✅ TASK 4 complete — Long format DataFrame ready ({len(long_df.columns)} columns)")

#         # ══════════════════════════════════════════════════════════════════════
#         # TASK 5 — DELTA DETECTION & PARQUET WRITE (per year)
#         # ══════════════════════════════════════════════════════════════════════
#         print(f"\n[TASK 5] Delta Detection & Write — Processing by year")

#         unique_years_rows = long_df.select("annee").distinct().collect()
#         unique_years = [row.annee for row in unique_years_rows]

#         sc = spark.sparkContext
#         Path = sc._jvm.org.apache.hadoop.fs.Path
#         FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
#         conf = sc._jsc.hadoopConfiguration()

#         for year in unique_years:
#             if year == "Unknown":
#                 continue

#             output_path = f"{target_bucket.rstrip('/')}/{category_path}/{year}"
#             print(f"\n  📅 Processing Year  : {year}")
#             print(f"  📂 Output path      : {output_path}")

#             df_new_batch = long_df.filter(F.col("annee") == year)

#             history_exists = False
#             df_history = None
#             change_count = 0

#             try:
#                 df_history = spark.read.parquet(output_path)
#                 if df_history.rdd.isEmpty():
#                     raise Exception("Empty")
#                 history_exists = True
#                 print("  ✅ History found.")
#             except Exception:
#                 print("  ℹ️  No history found. Treating as new dataset.")

#             df_new_batch = compute_version_active(
#                 df_new=df_new_batch,
#                 df_history=df_history,
#                 history_exists=history_exists,
#             )

#             ordered_cols = [
#                 "annee", "variable", "version", "rang", "base", "valeur",
#                 "date_chargement", "source", "version_active", "pays",
#                 "code_secteur", "lib_secteur",
#                 "dim_id", "dim_key",
#             ]
#             cols_to_select = [c for c in ordered_cols if c in df_new_batch.columns]
#             df_new_batch = df_new_batch.select(cols_to_select)

#             df_new_batch.createOrReplaceTempView("v_new_data")

#             df_to_write = None
#             if not history_exists:
#                 print("  🆕 New Data: Writing all rows.")
#                 df_to_write = spark.sql("SELECT * FROM v_new_data")
#             else:
#                 df_history.createOrReplaceTempView("v_history")

#                 delta_query = """
#                 SELECT n.*
#                 FROM v_new_data n
#                 LEFT JOIN (
#                     SELECT code_secteur, lib_secteur, variable, version, valeur
#                     FROM (
#                         SELECT *,
#                             ROW_NUMBER() OVER (
#                                 PARTITION BY code_secteur, lib_secteur, variable, version
#                                 ORDER BY date_chargement DESC, version_active DESC
#                             ) as rn
#                         FROM v_history
#                     )
#                     WHERE rn = 1
#                 ) h
#                 ON  TRIM(n.code_secteur) = TRIM(h.code_secteur)
#                 AND TRIM(n.lib_secteur)  = TRIM(h.lib_secteur)
#                 AND TRIM(n.variable)     = TRIM(h.variable)
#                 AND TRIM(n.version)      = TRIM(h.version)
#                 WHERE
#                     h.code_secteur IS NULL
#                     OR CAST(n.valeur AS DECIMAL(38,12)) <> CAST(h.valeur AS DECIMAL(38,12))
#                     OR (n.valeur IS NOT NULL AND h.valeur IS NULL)
#                     OR (n.valeur IS NULL     AND h.valeur IS NOT NULL)
#                 """

#                 df_changes = spark.sql(delta_query)
#                 change_count = df_changes.cache().count()

#                 if change_count > 0:
#                     print(f"  🔄 {change_count} changed row(s) detected.")
#                     df_to_write = df_changes
#                 else:
#                     print("  ✅ No changes detected. File left untouched.")
#                     df_to_write = None

#                 df_changes.unpersist()

#             if df_to_write is not None:
#                 temp_output_path = output_path + "_temp_write"
#                 if history_exists and change_count > 0:
#                     df_history_full = spark.read.parquet(output_path)

#                     key_cols = ["code_secteur", "lib_secteur", "variable", "version", "annee"]
#                     keys_changed = df_to_write.select(key_cols).distinct()

#                     df_history_updated = (
#                         df_history_full.alias("h")
#                         .join(keys_changed.alias("k"), on=key_cols, how="left")
#                         .withColumn(
#                             "version_active",
#                             F.when(
#                                 F.col("k.code_secteur").isNotNull(),
#                                 F.lit(0),
#                             ).otherwise(F.col("h.version_active")),
#                         )
#                         .select("h.*", "version_active")
#                     )

#                     df_new_clean = df_to_write.withColumn("version_active", F.lit(1))

#                     df_final = df_history_updated.unionByName(
#                         df_new_clean, allowMissingColumns=True
#                     )
#                 else:
#                     df_final = df_to_write

#                 from pyspark.sql.types import LongType, DoubleType, StringType

#                 df_final = (
#                     df_final
#                     .withColumn("annee",          F.col("annee").cast("int"))
#                     .withColumn("rang",            F.col("rang").cast("int"))
#                     .withColumn("base",            F.col("base").cast(StringType()))
#                     .withColumn("version_active",  F.col("version_active").cast("int"))
#                     .withColumn("valeur",          F.col("valeur").cast("double"))
#                     .withColumn("variable",        F.col("variable").cast(StringType()))
#                     .withColumn("version",         F.col("version").cast(StringType()))
#                     .withColumn("source",          F.col("source").cast(StringType()))
#                     .withColumn("pays",            F.col("pays").cast(StringType()))
#                     .withColumn("code_secteur",    F.col("code_secteur").cast(StringType()))
#                     .withColumn("lib_secteur",     F.col("lib_secteur").cast(StringType()))
#                     .withColumn("dim_id",          F.col("dim_id").cast(StringType()))
#                     .withColumn("dim_key",         F.col("dim_key").cast(StringType()))
#                 )

#                 df_final.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

#                 # ── Build column lineage for the final write ──────────────────
#                 column_lineage_task5 = {}
#                 for field in df_final.schema.fields:
#                     if field.name == "version_active":
#                         column_lineage_task5[field.name] = {
#                             "inputFields": [
#                                 {
#                                     "namespace": "itceq_prod",
#                                     "name": f"memory://spark_df/{file_name}/long",
#                                     "field": "version_active",
#                                 }
#                             ],
#                             "transformationDescription": (
#                                 "SCD Type 2 flag: new rows → version_active=1 (compute_version_active). "
#                                 "Old rows matching changed keys → version_active=0 (LEFT JOIN on key_cols + WHEN isNotNull THEN 0)."
#                                 if history_exists and change_count > 0
#                                 else "New dataset: all rows set to version_active=1."
#                             ),
#                             "transformationType": "AGGREGATE",
#                         }
#                     else:
#                         column_lineage_task5[field.name] = {
#                             "inputFields": [
#                                 {
#                                     "namespace": "itceq_prod",
#                                     "name": f"memory://spark_df/{file_name}/long",
#                                     "field": field.name,
#                                 }
#                             ],
#                             "transformationDescription": f"CAST to {field.dataType.simpleString()} for Parquet schema enforcement.",
#                             "transformationType": "DIRECT",
#                         }

#                 # 📡 MARQUEZ — TASK 5
#                 emit_marquez_step(
#                     spark_df=df_final,
#                     step_name=f"05_Delta_Write_{category_path.replace('/','_')}_{year}",
#                     description=(
#                         f"Delta detection + Parquet write for file '{file_name}', year {year}. "
#                         f"History existed: {history_exists}. "
#                         + (
#                             f"Changed rows detected: {change_count}. "
#                             f"Old matching rows set version_active=0. New rows set version_active=1. "
#                             f"Final write = history_updated UNION new_rows."
#                             if history_exists
#                             else "No history — full write. All rows version_active=1."
#                         )
#                     ),
#                     trans_type="LOAD",
#                     inputs=[f"memory://spark_df/{file_name}/long"],
#                     outputs=[output_path],
#                     column_lineage=column_lineage_task5,
#                 )

#                 print(f"  📊 FINAL DATAFRAME SCHEMA (Detailed)")
#                 for field in df_final.schema.fields:
#                     print(
#                         f"     Column: {field.name} | "
#                         f"Type: {field.dataType.simpleString()} | "
#                         f"Nullable: {field.nullable}"
#                     )
#                 print(f"     📊 Total columns: {len(df_final.columns)}")

#                 try:
#                     target_uri = sc._jvm.java.net.URI(output_path)
#                     fs = FileSystem.get(target_uri, conf)
#                     dst_path = Path(output_path)
#                     src_path = Path(temp_output_path)
#                     if fs.exists(dst_path):
#                         fs.delete(dst_path, True)
#                     success = fs.rename(src_path, dst_path)
#                     if success:
#                         print("  ✅ File updated.")
#                 except Exception as fs_e:
#                     print(f"  ❌ FileSystem Error: {fs_e}")

#             spark.catalog.dropTempView("v_new_data")
#             if history_exists:
#                 spark.catalog.dropTempView("v_history")

#         print(f"\n{'='*80}")
#         print(f"✅ Finished processing: {file_name}")
#         print(f"{'='*80}\n")

#     except Exception as e:
#         print(f"❌ ERROR: {e}")
#         import traceback
#         traceback.print_exc()
#     finally:
#         if os.path.exists(local_tmp_path):
#             os.remove(local_tmp_path)


# # ──────────────────────────────────────────────────────────────────────────────
# # PIPELINE PRINCIPAL
# # ──────────────────────────────────────────────────────────────────────────────
# def run_etl_pipeline(spark, raw_bucket, target_bucket, target_folder="INS/TRE"):
#     print(f"\n{'#'*80}")
#     print(f"# ETL PIPELINE STARTED")
#     print(f"# Target Folder : {target_folder}")
#     print(f"# Marquez Run ID: {PIPELINE_RUN_ID}")
#     print(f"# Marquez URL   : {MARQUEZ_URL}")
#     print(f"{'#'*80}\n")

#     print(f"[TASK 0] File Discovery — Scanning S3 for latest .xlsx files")
#     files = find_latest_files_recursive(spark, raw_bucket, specific_folder=target_folder)

#     if not files:
#         print(f"❌ No .xlsx files found in the target folder.")
#         return

#     print(f"\n✅ Found {len(files)} file(s) to process.\n")

#     for full_path in files:
#         process_single_file(spark, full_path, raw_bucket, target_bucket)

#     print(f"\n{'#'*80}")
#     print(f"# ETL PIPELINE COMPLETED")
#     print(f"{'#'*80}\n")


# # ──────────────────────────────────────────────────────────────────────────────
# # MAIN
# # ──────────────────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     _spark = create_spark_session("ETL INS TRE MERGE")
#     try:
#         run_etl_pipeline(
#             spark=_spark,
#             raw_bucket="s3a://01-raw/",
#             target_bucket="s3a://02-transformed",
#             target_folder="INS/TRE",
#         )
#     finally:
#         stop_spark_session(_spark)



import sys
import os
import logging
import re
import uuid
import pandas as pd
import openpyxl
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common.spark_session import create_spark_session, stop_spark_session
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp

# --- LINEAGE IMPORTS ---
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, Dataset, RunState


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# MARQUEZ LINEAGE EMITTER
# ──────────────────────────────────────────────────────────────────────────────

# Global pipeline run ID — used only as fallback / for the "big" global view
PIPELINE_RUN_ID = str(uuid.uuid4())
MARQUEZ_URL = os.getenv("MARQUEZ_URL", "http://marquez:5000")


def emit_marquez_step(spark_df, step_name, description, trans_type, inputs, outputs,
                      input_schema_fields=None, column_lineage=None, run_id=None):
    """
    Sends rich lineage metadata to Marquez for a given pipeline step.

    Parameters:
        spark_df            : Spark DataFrame — used to extract output schema
        step_name           : Logical name of the step (e.g. '03_Spark_DF_Creation_2021')
        description         : Human-readable description of what this step does
        trans_type          : Transformation type label (e.g. 'EXTRACT', 'TRANSFORM', 'LOAD')
        inputs              : List of input dataset paths/names (source)
        outputs             : List of output dataset paths/names (destination)
        input_schema_fields : Optional list of dicts describing the input schema
        column_lineage      : Optional dict describing per-column lineage
        run_id              : Optional run UUID — if provided, scopes this event to that run
                              (use per-year run IDs for year-isolated lineage graphs)
    """
    try:
        client = OpenLineageClient(url=MARQUEZ_URL)

        # Use the provided run_id (year-scoped) or fall back to the global one
        effective_run_id = run_id if run_id else PIPELINE_RUN_ID

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

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat() + "Z",
            run=Run(runId=effective_run_id, facets=run_facets),
            job=Job(namespace="itceq_prod", name=step_name, facets=job_facets),
            inputs=[
                Dataset(namespace="itceq_prod", name=i, facets=input_dataset_facets)
                for i in inputs
            ],
            outputs=[
                Dataset(namespace="itceq_prod", name=o, facets=output_dataset_facets)
                for o in outputs
            ],
            producer="itceq-spark-producer",
        )

        client.emit(event)
        print(f"📡 Marquez Updated: [{trans_type}] {step_name} | {len(output_fields)} output columns | run_id={effective_run_id[:8]}...")

    except Exception as e:
        print(f"⚠️  Marquez metadata error at step '{step_name}': {e}")


# ──────────────────────────────────────────────────────────────────────────────
# S3 FILE DISCOVERY
# ──────────────────────────────────────────────────────────────────────────────
def find_latest_files_recursive(spark, base_path, specific_folder=""):
    """
    Handles the structure: s3a://01-raw/YEAR/MONTH/SOURCE_FOLDER/...
    Returns a list of .xlsx file paths from the most recent YEAR/MONTH folder.
    """
    print(f"Searching for latest data starting at: {base_path}")
    sc = spark.sparkContext
    try:
        Path = sc._jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        conf = sc._jsc.hadoopConfiguration()
        fs = FileSystem.get(sc._jvm.java.net.URI(base_path), conf)

        def get_sorted_subdirs(parent_path_str):
            p = Path(parent_path_str)
            if not fs.exists(p):
                return []
            stats = fs.listStatus(p)
            dirs = [s.getPath() for s in stats if s.isDirectory()]
            dirs.sort(key=lambda x: x.getName(), reverse=True)
            return dirs

        years = get_sorted_subdirs(base_path)
        if not years:
            print("  ❌ No year folders found!")
            return []
        latest_year = years[0]
        print(f"  > Latest Year: {latest_year.getName()}")

        months = get_sorted_subdirs(latest_year.toString())
        if not months:
            print("  ❌ No month folders found!")
            return []
        latest_month = months[0]
        print(f"  > Latest Month: {latest_month.getName()}")

        search_start_path = Path(latest_month.toString())

        if specific_folder:
            target_path = Path(f"{latest_month.toString()}/{specific_folder}")
            if fs.exists(target_path):
                search_start_path = target_path
                print(f"  > Found target folder: {search_start_path.toString()}")
            else:
                print(f"  ⚠️  Warning: Specific folder '{specific_folder}' not found, searching entire month...")

        print(f"  > Searching from: {search_start_path.toString()}")

        files_found = []
        stack = [search_start_path]
        while stack:
            current_path = stack.pop()
            try:
                contents = fs.listStatus(current_path)
                for item in contents:
                    if item.isDirectory():
                        stack.append(item.getPath())
                    elif item.isFile():
                        fname = item.getPath().getName()
                        if fname.endswith(".xlsx") and not fname.startswith("~"):
                            full_path = item.getPath().toString()
                            files_found.append(full_path)
                            print(f"  ✓ Found file: {full_path}")
            except Exception as e:
                print(f"  ! Error reading {current_path}: {e}")

        return files_found
    except Exception as e:
        print(f"❌ Error traversing directory: {e}")
        import traceback
        traceback.print_exc()
        return []


def download_from_s3(spark, full_s3_path, local_path):
    safe_s3_path = full_s3_path.replace(" ", "%20")
    print(f"  Downloading {full_s3_path}...")
    sc = spark.sparkContext
    try:
        Path = sc._jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        conf = sc._jsc.hadoopConfiguration()
        URI = sc._jvm.java.net.URI
        uri = URI(safe_s3_path)
        fs = FileSystem.get(uri, conf)
        fs.copyToLocalFile(False, Path(uri), Path(local_path), True)
        return True
    except Exception as e:
        print(f"  Error downloading: {e}")
        return False


def extract_tables_from_sheet(wb, sheet_name):
    ws = wb[sheet_name]
    max_row = ws.max_row
    max_col = ws.max_column

    data_start_row = None
    header_row = None

    for row_idx in range(1, min(15, max_row + 1)):
        first_cell = ws.cell(row=row_idx, column=1).value
        if first_cell:
            first_cell_str = str(first_cell).strip()
            if any(k in first_cell_str for k in ["TABLEAUX", "Unité", "millions", "source", "MATRICE"]):
                continue
            if len(first_cell_str) <= 3 and (
                first_cell_str.isdigit() or first_cell_str.upper() in ["CR1", "CR2", "CR3", "CR"]
            ):
                data_start_row = row_idx
                header_row = row_idx - 1
                break

    if data_start_row is None:
        return []

    all_data = []
    for row_idx in range(header_row, max_row + 1):
        first_cell = ws.cell(row=row_idx, column=1).value
        if first_cell and row_idx > data_start_row:
            first_str = str(first_cell).strip().lower()
            if any(
                keyword in first_str
                for keyword in ["total", "totaux", "somme", "equilibre", "emplois totaux", "ressources totales"]
            ):
                break

        row_data = []
        for col_idx in range(1, max_col + 1):
            cell_val = ws.cell(row=row_idx, column=col_idx).value
            row_data.append(cell_val)
        all_data.append(row_data)

    return [{"data": all_data, "sheet": sheet_name}]


def is_numeric_string(s):
    """Check if a string represents a number (int or float)"""
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    try:
        float(s)
        return True
    except (ValueError, OverflowError):
        return False


def clean_table(data_rows):
    if not data_rows:
        return None

    # --- 1. FIND DATA START ---
    data_start_idx = 0
    for i in range(min(len(data_rows), 15)):
        row = data_rows[i]
        if not row:
            continue
        first_cell = str(row[0]).strip() if row[0] else ""
        second_cell = str(row[1]).strip() if len(row) > 1 and row[1] else ""

        if any(k in first_cell for k in ["Unité", "TABLEAUX", "millions", "source"]):
            continue

        if len(first_cell) <= 4 and (
            first_cell.isdigit() or first_cell.upper() in ["CR1", "CR2", "CR3", "CR"]
        ) and len(second_cell) > 5:
            data_start_idx = i
            break

    header_row_idx = data_start_idx - 1 if data_start_idx > 0 else 0
    raw_headers = data_rows[header_row_idx] if header_row_idx < len(data_rows) else data_rows[0]

    # --- DYNAMIC CI DETECTION ---
    print(f"  🔍 Detecting CI matrix region from header structure...")
    matrice_start_col = None

    for row_idx in range(min(data_start_idx, len(data_rows))):
        row = data_rows[row_idx]
        if not row:
            continue
        for col_idx, cell in enumerate(row):
            if cell and "MATRICE" in str(cell).strip().upper():
                matrice_start_col = col_idx
                print(f"     ✓ Found 'MATRICE' label at row {row_idx}, column {col_idx} → all columns from here are CI")
                break
        if matrice_start_col is not None:
            break

    matrice_ci_columns = set()

    if matrice_start_col is not None:
        for col_idx in range(matrice_start_col, len(raw_headers)):
            h = raw_headers[col_idx]
            header_str = str(h).strip() if h else ""
            if not header_str or is_numeric_string(header_str):
                matrice_ci_columns.add(col_idx)
            else:
                if len(matrice_ci_columns) >= 2:
                    break
    else:
        print(f"     ⚠️  No 'MATRICE' label found — falling back to numeric header detection")
        non_id_seen = False
        ci_started = False
        consecutive = 0

        for col_idx, h in enumerate(raw_headers):
            if col_idx < 2:
                continue
            header_str = str(h).strip() if h else ""
            if is_numeric_string(header_str):
                try:
                    num_val = float(header_str)
                    if 1 <= num_val < 100 and float(num_val).is_integer():
                        if non_id_seen:
                            ci_started = True
                        if ci_started:
                            matrice_ci_columns.add(col_idx)
                            consecutive += 1
                    else:
                        if ci_started and consecutive >= 2:
                            break
                except (ValueError, OverflowError):
                    pass
            elif header_str:
                non_id_seen = True
                if ci_started and consecutive >= 2:
                    break
                elif ci_started:
                    matrice_ci_columns.clear()
                    ci_started = False
                    consecutive = 0

    print(f"     → Found {len(matrice_ci_columns)} CI columns")

    # --- 2. GENERATE HEADERS WITH CI_ PREFIX ---
    headers = []
    header_counts = {}

    for i, h in enumerate(raw_headers):
        if h is None or str(h).strip() == "" or h is False:
            base_name = f"Col_{i}"
        else:
            base_name = " ".join(str(h).replace("\n", " ").split())
            if "Unité" in base_name or "millions" in base_name:
                base_name = f"Col_{i}"

        if i in matrice_ci_columns:
            header_str = str(h).strip() if h else ""
            try:
                num_val = float(header_str)
                base_name = f"CI_{int(num_val):02d}"
                print(f"     → Renaming column {i}: '{header_str}' → '{base_name}'")
            except (ValueError, OverflowError):
                base_name = f"CI_col{i}"

        if base_name in header_counts:
            header_counts[base_name] += 1
            unique_name = f"{base_name}_{header_counts[base_name]}"
        else:
            header_counts[base_name] = 0
            unique_name = base_name

        headers.append(unique_name)

    # --- 3. EXTRACT BODY ---
    body = []
    for i in range(data_start_idx, len(data_rows)):
        row = data_rows[i]
        if not row:
            continue
        first_cell = str(row[0]).strip().lower() if row[0] else ""
        if "totaux" in first_cell or "total" in first_cell:
            break
        body.append(row)

    if not body:
        return None

    df = pd.DataFrame(body, columns=headers)

    # --- 4. STANDARD CLEANUP ---
    if df.shape[1] >= 2:
        headers[0] = "code_secteur"
        headers[1] = "lib_secteur"
        df.columns = headers

        df = df[df["code_secteur"].astype(str) != "None"]
        df = df[
            ~df["code_secteur"]
            .astype(str)
            .str.contains("Unité|TABLEAUX|millions|source", case=False, na=False)
        ]

        df["code_secteur"] = df["code_secteur"].astype(str).str.strip()
        df["lib_secteur"] = df["lib_secteur"].astype(str).str.strip()

        df = df.dropna(how="all")

    df = df.dropna(axis=1, how="all")
    df = df.reset_index(drop=True)

    # --- 5. SPACER REMOVAL ---
    cols_to_drop = []
    if "code_secteur" in df.columns and "lib_secteur" in df.columns:
        ref_code = df["code_secteur"]
        ref_label = df["lib_secteur"]

        for col in df.columns:
            if col in ["code_secteur", "lib_secteur"]:
                continue
            if col.startswith("CI_"):
                continue

            curr_col_data = df[col].astype(str).str.strip()
            if curr_col_data.equals(ref_code):
                cols_to_drop.append(col)
            elif curr_col_data.equals(ref_label):
                cols_to_drop.append(col)
            elif (
                str(col).startswith("Col_")
                and len(df) > 0
                and str(df[col].iloc[0]) == str(df["code_secteur"].iloc[0])
            ):
                cols_to_drop.append(col)

    if cols_to_drop:
        print(f"     → Removing {len(cols_to_drop)} spacer columns")
        df = df.drop(columns=cols_to_drop)

    if not df.columns.is_unique:
        df.columns = pd.io.parsers.ParserBase({"names": df.columns})._maybe_dedup_names(df.columns)

    return df


def merge_sheet_tables_horizontally(table_list):
    if not table_list:
        return None
    return clean_table(table_list[0]["data"])


def to_long_format_spark(df):
    """
    Pivote le DataFrame en format long.
    Les colonnes Pays, Source, Base, Rang et date_chargement sont conservées comme identifiants.
    """
    id_vars = [
        "code_secteur", "lib_secteur", "Annee", "Version",
        "date_chargement", "Pays", "Source", "Base", "Rang",
    ]

    value_columns = [c for c in df.columns if c not in id_vars]

    if not value_columns:
        return df

    stack_parts = [f"'{c}', CAST(`{c}` AS STRING)" for c in value_columns]
    stack_expr = f"stack({len(value_columns)}, {', '.join(stack_parts)}) as (Variable, Valeur)"

    long_df = df.select(
        F.col("code_secteur"),
        F.col("lib_secteur"),
        F.col("Annee"),
        F.col("Version"),
        F.col("date_chargement"),
        F.col("Pays"),
        F.col("Source"),
        F.col("Base"),
        F.col("Rang"),
        F.expr(stack_expr),
    )
    return long_df.where(F.col("Valeur").isNotNull())


def extract_year_version_from_sheet(sheet_name):
    clean_name = sheet_name.strip()
    match_year = re.search(r"(20\d{2})", clean_name)
    if match_year:
        year = match_year.group(1)
        parts = clean_name.split(year)
        version = parts[-1].strip() if len(parts) > 1 and parts[-1].strip() else "C"
        return year, version
    return "Unknown", "Unknown"


def extract_category_path(full_s3_path, raw_bucket_root):
    """
    Extrait le chemin de catégorie depuis le chemin S3 complet.
    Exemple : s3a://01-raw/2026/02/INS/TRE/file.xlsx → INS/TRE
    """
    clean_path = full_s3_path.replace("s3a://", "").strip("/")
    clean_root = raw_bucket_root.replace("s3a://", "").strip("/")

    if clean_path.startswith(clean_root):
        relative_path = clean_path[len(clean_root) :].strip("/")
        parts = relative_path.split("/")
        if len(parts) > 3:
            category_parts = parts[2:-1]
            return "/".join(category_parts)

    return "UNKNOWN_CATEGORY"


def format_code_secteur(val):
    """Formate code_secteur en préservant les zéros initiaux (ex: '01', '06')."""
    if val is None or pd.isna(val) or str(val).strip().lower() in ["nan", "none", ""]:
        return "0"
    s_val = str(val).strip()

    try:
        f = float(s_val)
        if f.is_integer():
            if s_val.startswith("0") or (s_val.replace(".", "").replace("-", "").startswith("0")):
                return f"{int(f):02d}"
            else:
                return str(int(f))
    except (ValueError, OverflowError):
        pass

    return s_val


def format_excel_value(val):
    """Formate les valeurs Excel en supprimant le .0 des entiers."""
    if val is None or pd.isna(val) or str(val).strip().lower() in ["nan", "none", ""]:
        return "0"
    s_val = str(val).strip()

    try:
        f = float(s_val)
        if f.is_integer():
            return str(int(f))
    except (ValueError, OverflowError):
        pass

    return s_val


# ──────────────────────────────────────────────────────────────────────────────
# CALCUL DE version_active
# ──────────────────────────────────────────────────────────────────────────────
def compute_version_active(df_new, df_history, history_exists):
    return df_new.withColumn("version_active", F.lit(1).cast("int"))


# ──────────────────────────────────────────────────────────────────────────────
# TRAITEMENT D'UN FICHIER
# ──────────────────────────────────────────────────────────────────────────────
def process_single_file(spark, full_s3_file_path, raw_bucket_root, target_bucket):

    file_name = full_s3_file_path.split("/")[-1]
    local_tmp_path = f"/tmp/{file_name}"
    category_path = extract_category_path(full_s3_file_path, raw_bucket_root)

    print(f"\n{'='*80}")
    print(f"📁 Processing File : {file_name}")
    print(f"{'='*80}")

    if not download_from_s3(spark, full_s3_file_path, local_tmp_path):
        return

    try:
        # ══════════════════════════════════════════════════════════════════════
        # TASK 1 — EXCEL INGESTION
        # ══════════════════════════════════════════════════════════════════════
        print(f"\n[TASK 1] Excel Ingestion — Reading sheets from: {file_name}")

        wb = openpyxl.load_workbook(local_tmp_path, data_only=True)
        all_sheets_data = []

        for sheet_name in wb.sheetnames:
            tables = extract_tables_from_sheet(wb, sheet_name)
            if not tables:
                continue
            sheet_df = merge_sheet_tables_horizontally(tables)
            if sheet_df is not None and not sheet_df.empty:
                year, version = extract_year_version_from_sheet(sheet_name)
                sheet_df["Annee"] = year
                sheet_df["Version"] = version
                sheet_df = sheet_df.astype(str)
                sheet_df["Annee"] = sheet_df["Annee"].astype(int)
                all_sheets_data.append(sheet_df)

        if not all_sheets_data:
            print(f"  ⚠️  WARNING: No data found in {file_name}")
            return

        final_pd_df = pd.concat(all_sheets_data, ignore_index=True)
        print(f"  ✅ TASK 1 complete — {len(all_sheets_data)} sheet(s) ingested, {len(final_pd_df)} rows total")

        # ══════════════════════════════════════════════════════════════════════
        # TASK 2 — NORMALIZATION
        # ══════════════════════════════════════════════════════════════════════
        print(f"\n[TASK 2] Normalization — Formatting values and sector codes")

        id_cols = ["code_secteur", "lib_secteur", "Annee", "Version"]
        value_cols = [c for c in final_pd_df.columns if c not in id_cols]

        print(f"  🔧 Normalizing {len(value_cols)} value columns...")
        for col_name in value_cols:
            final_pd_df[col_name] = final_pd_df[col_name].apply(format_excel_value)

        print(f"  🔧 Normalizing code_secteur (preserving leading zeros)...")
        final_pd_df["code_secteur"] = final_pd_df["code_secteur"].apply(format_code_secteur)

        print(f"  ✅ TASK 2 complete")

        # ══════════════════════════════════════════════════════════════════════
        # TASK 3 — SPARK DATAFRAME CREATION + BUSINESS COLUMNS
        # (emitted once globally — covers all years in this file)
        # ══════════════════════════════════════════════════════════════════════
        print(f"\n[TASK 3] Spark DataFrame Creation — Adding metadata columns")

        final_pd_df.columns = [str(col) for col in final_pd_df.columns]
        spark_df = spark.createDataFrame(final_pd_df)

        spark_df = (
            spark_df
            .withColumn("date_chargement", current_timestamp())
            .withColumn("Pays",   F.lit("Tunisie"))
            .withColumn("Rang",   F.lit(None).cast("int"))
            .withColumn("Source", F.lit("TRE"))
            .withColumn("Base",   F.lit(None).cast("int"))
            .withColumn("code_secteur", F.trim(F.col("code_secteur")))
            .withColumn("lib_secteur",  F.trim(F.col("lib_secteur")))
        )

        # ── Build raw Excel schema for Marquez input ──────────────────────────
        excel_source_cols = list(final_pd_df.columns)
        raw_schema_fields = []
        for col in excel_source_cols:
            if col in ["code_secteur", "lib_secteur"]:
                desc = "Sector identifier column — read directly from Excel row"
            elif col in ["Annee", "Version"]:
                desc = "Extracted from Excel sheet name (e.g. '2021 C' → Annee=2021, Version=C)"
            elif col.startswith("CI_"):
                desc = f"CI matrix column — intermediate consumption value from Excel header '{col}'"
            else:
                desc = f"Value column from Excel file — row data for variable '{col}'"
            raw_schema_fields.append({
                "name": col,
                "type": "string",
                "description": desc,
            })

        # ── Build column lineage: source vs pipeline-injected columns ─────────
        added_cols_t3 = {
            "date_chargement": "Pipeline-injected: current timestamp at load time (current_timestamp())",
            "Pays":            "Pipeline-injected: hardcoded constant = 'Tunisie'",
            "Source":          "Pipeline-injected: hardcoded constant = 'TRE'",
            "Rang":            "Pipeline-injected: hardcoded null (no rank available at this stage)",
            "Base":            "Pipeline-injected: hardcoded null (no base year available at this stage)",
        }

        column_lineage_task3 = {}
        for field in spark_df.schema.fields:
            if field.name in added_cols_t3:
                column_lineage_task3[field.name] = {
                    "inputFields": [],
                    "transformationDescription": added_cols_t3[field.name],
                    "transformationType": "IDENTITY",
                }
            else:
                col_desc = "Trimmed with TRIM()" if field.name in ["code_secteur", "lib_secteur"] else "Directly mapped from Excel"
                column_lineage_task3[field.name] = {
                    "inputFields": [
                        {
                            "namespace": "itceq_prod",
                            "name": full_s3_file_path,
                            "field": field.name,
                        }
                    ],
                    "transformationDescription": col_desc,
                    "transformationType": "DIRECT",
                }

        # 📡 MARQUEZ — TASK 3 (global — no year scope, uses PIPELINE_RUN_ID)
        emit_marquez_step(
            spark_df=spark_df,
            step_name=f"03_Spark_DF_Creation_{file_name}",
            description=(
                f"Read Excel file '{file_name}' ({len(all_sheets_data)} sheets, {len(final_pd_df)} rows). "
                f"Converted to Spark DataFrame ({len(spark_df.columns)} columns). "
                f"Added pipeline columns: date_chargement=now(), Pays='Tunisie', Source='TRE', Rang=null, Base=null. "
                f"Applied TRIM() on code_secteur and lib_secteur."
            ),
            trans_type="EXTRACT",
            inputs=[full_s3_file_path],
            outputs=[f"memory://spark_df/{file_name}/all_years/wide"],
            input_schema_fields=raw_schema_fields,
            column_lineage=column_lineage_task3,
            run_id=PIPELINE_RUN_ID,  # global run — appears in the "big" full lineage view
        )
        print(f"  ✅ TASK 3 complete — Spark DataFrame created with {len(spark_df.columns)} columns")

        # ══════════════════════════════════════════════════════════════════════
        # TASK 4 — LONG FORMAT PIVOT
        # (emitted once globally — covers all years in this file)
        # ══════════════════════════════════════════════════════════════════════
        print(f"\n[TASK 4] Long Format Pivot — Unpivoting wide columns to (Variable, Valeur)")

        id_vars_wide = ["code_secteur", "lib_secteur", "Annee", "Version",
                        "date_chargement", "Pays", "Source", "Base", "Rang"]
        value_cols_wide = [c for c in spark_df.columns if c not in id_vars_wide]

        long_df = to_long_format_spark(spark_df)

        long_df = long_df.withColumn(
            "Variable",
            F.regexp_replace(F.col("Variable"), r"^(\d+)\.0$", "$1"),
        )
        long_df = long_df.withColumn("Variable", F.trim(F.col("Variable")))

        print("  🔧 Converting all columns to lowercase...")
        long_df = long_df.toDF(*[c.lower() for c in long_df.columns])

        long_df = (
            long_df
            .withColumn("dim_id",  F.lit("None"))
            .withColumn("dim_key", F.lit("None"))
        )

        # ── Build column lineage for the pivot ────────────────────────────────
        id_cols_lower = ["code_secteur", "lib_secteur", "annee", "version",
                         "date_chargement", "pays", "source", "base", "rang"]

        column_lineage_task4 = {}

        for col in id_cols_lower:
            column_lineage_task4[col] = {
                "inputFields": [
                    {
                        "namespace": "itceq_prod",
                        "name": f"memory://spark_df/{file_name}/all_years/wide",
                        "field": col,
                    }
                ],
                "transformationDescription": "Identity column — preserved as-is during stack() unpivot. Renamed to lowercase.",
                "transformationType": "DIRECT",
            }

        column_lineage_task4["variable"] = {
            "inputFields": [
                {
                    "namespace": "itceq_prod",
                    "name": f"memory://spark_df/{file_name}/all_years/wide",
                    "field": col,
                }
                for col in value_cols_wide
            ],
            "transformationDescription": (
                f"Generated by Spark stack() unpivot: column name from one of "
                f"{len(value_cols_wide)} wide columns ({', '.join(value_cols_wide[:5])}...). "
                f"Trailing '.0' cleaned with regexp_replace(). Whitespace trimmed."
            ),
            "transformationType": "AGGREGATE",
        }

        column_lineage_task4["valeur"] = {
            "inputFields": [
                {
                    "namespace": "itceq_prod",
                    "name": f"memory://spark_df/{file_name}/all_years/wide",
                    "field": col,
                }
                for col in value_cols_wide
            ],
            "transformationDescription": (
                f"Generated by Spark stack() unpivot: cell value from one of "
                f"{len(value_cols_wide)} wide columns. CAST to STRING then filtered: Valeur IS NOT NULL."
            ),
            "transformationType": "AGGREGATE",
        }

        column_lineage_task4["dim_id"] = {
            "inputFields": [],
            "transformationDescription": "Pipeline-injected placeholder: hardcoded 'None' (dimension mapping not yet available)",
            "transformationType": "IDENTITY",
        }
        column_lineage_task4["dim_key"] = {
            "inputFields": [],
            "transformationDescription": "Pipeline-injected placeholder: hardcoded 'None' (dimension key not yet resolved)",
            "transformationType": "IDENTITY",
        }

        # 📡 MARQUEZ — TASK 4 (global — no year scope, uses PIPELINE_RUN_ID)
        emit_marquez_step(
            spark_df=long_df,
            step_name=f"04_Long_Format_Pivot_{file_name}",
            description=(
                f"Unpivoted {len(value_cols_wide)} wide columns → (variable, valeur) pairs "
                f"using Spark stack() expression. "
                f"Rows with null Valeur filtered out. "
                f"Variable names cleaned (regexp_replace trailing '.0', TRIM). "
                f"All column names lowercased. "
                f"Added dim_id='None' and dim_key='None' as placeholder surrogate keys."
            ),
            trans_type="TRANSFORMATION",
            inputs=[f"memory://spark_df/{file_name}/all_years/wide"],
            outputs=[f"memory://spark_df/{file_name}/all_years/long"],
            column_lineage=column_lineage_task4,
            run_id=PIPELINE_RUN_ID,  # global run — appears in the "big" full lineage view
        )
        print(f"  ✅ TASK 4 complete — Long format DataFrame ready ({len(long_df.columns)} columns)")

        # ══════════════════════════════════════════════════════════════════════
        # TASK 5 — DELTA DETECTION & PARQUET WRITE (per year)
        # Each year gets its own run_id → isolated lineage graph in Marquez
        # ══════════════════════════════════════════════════════════════════════
        print(f"\n[TASK 5] Delta Detection & Write — Processing by year")

        unique_years_rows = long_df.select("annee").distinct().collect()
        unique_years = [row.annee for row in unique_years_rows]

        sc = spark.sparkContext
        Path = sc._jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        conf = sc._jsc.hadoopConfiguration()

        for year in unique_years:
            if year == "Unknown":
                continue

            # ── Generate a unique run ID scoped to this year ──────────────────
            # This is the key change: every year gets its own UUID so that when
            # you open job "05_Delta_Write_INS_TRE_2015" in Marquez, you see
            # only the lineage chain for 2015 data, not for all years combined.
            year_run_id = str(uuid.uuid4())
            print(f"\n  📅 Processing Year  : {year} | Marquez Run ID: {year_run_id[:8]}...")

            output_path = f"{target_bucket.rstrip('/')}/{category_path}/{year}"
            print(f"  📂 Output path      : {output_path}")

            # Filter long_df to only this year's rows
            df_year_long = long_df.filter(F.col("annee") == year)

            history_exists = False
            df_history = None
            change_count = 0

            try:
                df_history = spark.read.parquet(output_path)
                if df_history.rdd.isEmpty():
                    raise Exception("Empty")
                history_exists = True
                print("  ✅ History found.")
            except Exception:
                print("  ℹ️  No history found. Treating as new dataset.")

            df_new_batch = compute_version_active(
                df_new=df_year_long,
                df_history=df_history,
                history_exists=history_exists,
            )

            ordered_cols = [
                "annee", "variable", "version", "rang", "base", "valeur",
                "date_chargement", "source", "version_active", "pays",
                "code_secteur", "lib_secteur",
                "dim_id", "dim_key",
            ]
            cols_to_select = [c for c in ordered_cols if c in df_new_batch.columns]
            df_new_batch = df_new_batch.select(cols_to_select)

            df_new_batch.createOrReplaceTempView("v_new_data")

            df_to_write = None
            if not history_exists:
                print("  🆕 New Data: Writing all rows.")
                df_to_write = spark.sql("SELECT * FROM v_new_data")
            else:
                df_history.createOrReplaceTempView("v_history")

                delta_query = """
                SELECT n.*
                FROM v_new_data n
                LEFT JOIN (
                    SELECT code_secteur, lib_secteur, variable, version, valeur
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY code_secteur, lib_secteur, variable, version
                                ORDER BY date_chargement DESC, version_active DESC
                            ) as rn
                        FROM v_history
                    )
                    WHERE rn = 1
                ) h
                ON  TRIM(n.code_secteur) = TRIM(h.code_secteur)
                AND TRIM(n.lib_secteur)  = TRIM(h.lib_secteur)
                AND TRIM(n.variable)     = TRIM(h.variable)
                AND TRIM(n.version)      = TRIM(h.version)
                WHERE
                    h.code_secteur IS NULL
                    OR CAST(n.valeur AS DECIMAL(38,12)) <> CAST(h.valeur AS DECIMAL(38,12))
                    OR (n.valeur IS NOT NULL AND h.valeur IS NULL)
                    OR (n.valeur IS NULL     AND h.valeur IS NOT NULL)
                """

                df_changes = spark.sql(delta_query)
                change_count = df_changes.cache().count()

                if change_count > 0:
                    print(f"  🔄 {change_count} changed row(s) detected.")
                    df_to_write = df_changes
                else:
                    print("  ✅ No changes detected. File left untouched.")
                    df_to_write = None

                df_changes.unpersist()

            if df_to_write is not None:
                temp_output_path = output_path + "_temp_write"
                if history_exists and change_count > 0:
                    df_history_full = spark.read.parquet(output_path)

                    key_cols = ["code_secteur", "lib_secteur", "variable", "version", "annee"]
                    keys_changed = df_to_write.select(key_cols).distinct()

                    df_history_updated = (
                        df_history_full.alias("h")
                        .join(keys_changed.alias("k"), on=key_cols, how="left")
                        .withColumn(
                            "version_active",
                            F.when(
                                F.col("k.code_secteur").isNotNull(),
                                F.lit(0),
                            ).otherwise(F.col("h.version_active")),
                        )
                        .select("h.*", "version_active")
                    )

                    df_new_clean = df_to_write.withColumn("version_active", F.lit(1))

                    df_final = df_history_updated.unionByName(
                        df_new_clean, allowMissingColumns=True
                    )
                else:
                    df_final = df_to_write

                from pyspark.sql.types import LongType, DoubleType, StringType

                df_final = (
                    df_final
                    .withColumn("annee",          F.col("annee").cast("int"))
                    .withColumn("rang",            F.col("rang").cast("int"))
                    .withColumn("base",            F.col("base").cast(StringType()))
                    .withColumn("version_active",  F.col("version_active").cast("int"))
                    .withColumn("valeur",          F.col("valeur").cast("double"))
                    .withColumn("variable",        F.col("variable").cast(StringType()))
                    .withColumn("version",         F.col("version").cast(StringType()))
                    .withColumn("source",          F.col("source").cast(StringType()))
                    .withColumn("pays",            F.col("pays").cast(StringType()))
                    .withColumn("code_secteur",    F.col("code_secteur").cast(StringType()))
                    .withColumn("lib_secteur",     F.col("lib_secteur").cast(StringType()))
                    .withColumn("dim_id",          F.col("dim_id").cast(StringType()))
                    .withColumn("dim_key",         F.col("dim_key").cast(StringType()))
                )

                df_final.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

                # ── Tasks 3 & 4 re-emitted scoped to this year ───────────────
                # These year-scoped emissions link the per-year run back to the
                # original source file, so when you open "2015" in Marquez you
                # see the full chain: Excel → wide → long → parquet/2015.
                # The year-scoped dataset names (e.g. .../2015/wide) keep each
                # year's graph isolated from the others.

                # Year-scoped Task 3
                df_year_wide = spark_df.filter(F.col("Annee") == year)
                emit_marquez_step(
                    spark_df=df_year_wide,
                    step_name=f"03_Spark_DF_Creation_{file_name}_{year}",
                    description=(
                        f"[Year-scoped] Read Excel file '{file_name}', filtered to year {year}. "
                        f"Converted to Spark DataFrame. "
                        f"Added pipeline columns: date_chargement=now(), Pays='Tunisie', Source='TRE', Rang=null, Base=null. "
                        f"Applied TRIM() on code_secteur and lib_secteur."
                    ),
                    trans_type="EXTRACT",
                    inputs=[full_s3_file_path],
                    outputs=[f"memory://spark_df/{file_name}/{year}/wide"],
                    input_schema_fields=raw_schema_fields,
                    column_lineage=column_lineage_task3,
                    run_id=year_run_id,  # ← year-scoped run
                )

                # Year-scoped Task 4
                emit_marquez_step(
                    spark_df=df_year_long,
                    step_name=f"04_Long_Format_Pivot_{file_name}_{year}",
                    description=(
                        f"[Year-scoped] Unpivoted {len(value_cols_wide)} wide columns → (variable, valeur) pairs "
                        f"for year {year}. Rows with null Valeur filtered out. "
                        f"Variable names cleaned. All column names lowercased. "
                        f"Added dim_id='None' and dim_key='None'."
                    ),
                    trans_type="TRANSFORMATION",
                    inputs=[f"memory://spark_df/{file_name}/{year}/wide"],
                    outputs=[f"memory://spark_df/{file_name}/{year}/long"],
                    column_lineage=column_lineage_task4,
                    run_id=year_run_id,  # ← year-scoped run
                )

                # ── Build column lineage for the final write ──────────────────
                column_lineage_task5 = {}
                for field in df_final.schema.fields:
                    if field.name == "version_active":
                        column_lineage_task5[field.name] = {
                            "inputFields": [
                                {
                                    "namespace": "itceq_prod",
                                    "name": f"memory://spark_df/{file_name}/{year}/long",
                                    "field": "version_active",
                                }
                            ],
                            "transformationDescription": (
                                "SCD Type 2 flag: new rows → version_active=1 (compute_version_active). "
                                "Old rows matching changed keys → version_active=0 (LEFT JOIN on key_cols + WHEN isNotNull THEN 0)."
                                if history_exists and change_count > 0
                                else "New dataset: all rows set to version_active=1."
                            ),
                            "transformationType": "AGGREGATE",
                        }
                    else:
                        column_lineage_task5[field.name] = {
                            "inputFields": [
                                {
                                    "namespace": "itceq_prod",
                                    "name": f"memory://spark_df/{file_name}/{year}/long",
                                    "field": field.name,
                                }
                            ],
                            "transformationDescription": f"CAST to {field.dataType.simpleString()} for Parquet schema enforcement.",
                            "transformationType": "DIRECT",
                        }

                # 📡 MARQUEZ — TASK 5 (year-scoped run)
                emit_marquez_step(
                    spark_df=df_final,
                    step_name=f"05_Delta_Write_{category_path.replace('/','_')}_{year}",
                    description=(
                        f"Delta detection + Parquet write for file '{file_name}', year {year}. "
                        f"History existed: {history_exists}. "
                        + (
                            f"Changed rows detected: {change_count}. "
                            f"Old matching rows set version_active=0. New rows set version_active=1. "
                            f"Final write = history_updated UNION new_rows."
                            if history_exists
                            else "No history — full write. All rows version_active=1."
                        )
                    ),
                    trans_type="LOAD",
                    inputs=[f"memory://spark_df/{file_name}/{year}/long"],
                    outputs=[output_path],
                    column_lineage=column_lineage_task5,
                    run_id=year_run_id,  # ← year-scoped run
                )

                print(f"  📊 FINAL DATAFRAME SCHEMA (Detailed)")
                for field in df_final.schema.fields:
                    print(
                        f"     Column: {field.name} | "
                        f"Type: {field.dataType.simpleString()} | "
                        f"Nullable: {field.nullable}"
                    )
                print(f"     📊 Total columns: {len(df_final.columns)}")

                try:
                    target_uri = sc._jvm.java.net.URI(output_path)
                    fs = FileSystem.get(target_uri, conf)
                    dst_path = Path(output_path)
                    src_path = Path(temp_output_path)
                    if fs.exists(dst_path):
                        fs.delete(dst_path, True)
                    success = fs.rename(src_path, dst_path)
                    if success:
                        print("  ✅ File updated.")
                except Exception as fs_e:
                    print(f"  ❌ FileSystem Error: {fs_e}")

            spark.catalog.dropTempView("v_new_data")
            if history_exists:
                spark.catalog.dropTempView("v_history")

        print(f"\n{'='*80}")
        print(f"✅ Finished processing: {file_name}")
        print(f"{'='*80}\n")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(local_tmp_path):
            os.remove(local_tmp_path)


# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────
def run_etl_pipeline(spark, raw_bucket, target_bucket, target_folder="INS/TRE"):
    print(f"\n{'#'*80}")
    print(f"# ETL PIPELINE STARTED")
    print(f"# Target Folder       : {target_folder}")
    print(f"# Global Run ID       : {PIPELINE_RUN_ID}  ← full lineage graph")
    print(f"# Per-year Run IDs    : generated dynamically inside year loop")
    print(f"# Marquez URL         : {MARQUEZ_URL}")
    print(f"{'#'*80}\n")

    print(f"[TASK 0] File Discovery — Scanning S3 for latest .xlsx files")
    files = find_latest_files_recursive(spark, raw_bucket, specific_folder=target_folder)

    if not files:
        print(f"❌ No .xlsx files found in the target folder.")
        return

    print(f"\n✅ Found {len(files)} file(s) to process.\n")

    for full_path in files:
        process_single_file(spark, full_path, raw_bucket, target_bucket)

    print(f"\n{'#'*80}")
    print(f"# ETL PIPELINE COMPLETED")
    print(f"{'#'*80}\n")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _spark = create_spark_session("ETL INS TRE MERGE")
    try:
        run_etl_pipeline(
            spark=_spark,
            raw_bucket="s3a://01-raw/",
            target_bucket="s3a://02-transformed",
            target_folder="INS/TRE",
        )
    finally:
        stop_spark_session(_spark)
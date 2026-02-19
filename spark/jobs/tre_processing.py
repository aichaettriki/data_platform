import sys
import os
import logging
import re

import pandas as pd
import openpyxl

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common.spark_session import create_spark_session, stop_spark_session

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
# S3 FILE DISCOVERY
# ──────────────────────────────────────────────────────────────────────────────
def find_latest_files_recursive(spark, base_path, specific_folder=""):
    """
    FIXED: This function now handles the structure: s3a://01-raw/YEAR/MONTH/SOURCE_FOLDER/...
    Instead of the previous assumption: s3a://01-raw/YEAR/MONTH/DAY/...
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
            if not fs.exists(p): return []
            stats = fs.listStatus(p)
            dirs = [s.getPath() for s in stats if s.isDirectory()]
            dirs.sort(key=lambda x: x.getName(), reverse=True)
            return dirs
 
        # Get latest year
        years = get_sorted_subdirs(base_path)
        if not years:
            print("  ❌ No year folders found!")
            return []
        latest_year = years[0]
        print(f"  > Latest Year: {latest_year.getName()}")
       
        # Get latest month
        months = get_sorted_subdirs(latest_year.toString())
        if not months:
            print("  ❌ No month folders found!")
            return []
        latest_month = months[0]
        print(f"  > Latest Month: {latest_month.getName()}")
       
        # Now we're at: s3a://01-raw/2026/02/
        # The next level contains source folders like: INS, ITCEQ, etc.
        # We need to search for the specific_folder path within this structure
       
        search_start_path = Path(latest_month.toString())
       
        if specific_folder:
            # If specific_folder is "INS/TRE", we navigate to that path
            target_path = Path(f"{latest_month.toString()}/{specific_folder}")
            if fs.exists(target_path):
                search_start_path = target_path
                print(f"  > Found target folder: {search_start_path.toString()}")
            else:
                print(f"  ⚠️  Warning: Specific folder '{specific_folder}' not found, searching entire month...")
       
        print(f"  > Searching from: {search_start_path.toString()}")
 
        # Recursively find all .xlsx files
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
                pass
       
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
   
    # Find the data start row (skip title and unit rows)
    data_start_row = None
    header_row = None
   
    for row_idx in range(1, min(15, max_row + 1)):
        first_cell = ws.cell(row=row_idx, column=1).value
        if first_cell:
            first_cell_str = str(first_cell).strip()
            if any(k in first_cell_str for k in ["TABLEAUX", "Unité", "millions", "Source", "MATRICE"]):
                continue
            if len(first_cell_str) <= 3 and (first_cell_str.isdigit() or first_cell_str.upper() in ["CR1", "CR2", "CR3", "CR"]):
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
            if any(keyword in first_str for keyword in ["total", "totaux", "somme", "equilibre", "emplois totaux", "ressources totales"]):
                break
       
        row_data = []
        for col_idx in range(1, max_col + 1):
            cell_val = ws.cell(row=row_idx, column=col_idx).value
            row_data.append(cell_val)
        all_data.append(row_data)
   
    return [{'data': all_data, 'sheet': sheet_name}]
 
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
    if not data_rows: return None
 
    # --- 1. FIND DATA START ---
    data_start_idx = 0
    for i in range(min(len(data_rows), 15)):
        row = data_rows[i]
        if not row: continue
        first_cell = str(row[0]).strip() if row[0] else ""
        second_cell = str(row[1]).strip() if len(row) > 1 and row[1] else ""
 
        if any(k in first_cell for k in ["Unité", "TABLEAUX", "millions", "Source"]):
            continue
 
        if (len(first_cell) <= 4 and (first_cell.isdigit() or first_cell.upper() in ["CR1", "CR2", "CR3", "CR"]) and len(second_cell) > 5):
            data_start_idx = i
            break
 
    header_row_idx = data_start_idx - 1 if data_start_idx > 0 else 0
    raw_headers = data_rows[header_row_idx] if header_row_idx < len(data_rows) else data_rows[0]
 
    # --- DYNAMIC CI DETECTION: scan ALL rows above data for a "MATRICE" label ---
    # The Excel has a merged cell saying "MATRICE CI" or "MATRICE" somewhere above the header row
    # We find which column index it appears at, then every column from that index onward is CI
    print(f"  🔍 Detecting CI matrix region from header structure...")
 
    matrice_start_col = None
 
    # Scan every row above the data row looking for a cell containing "MATRICE"
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
 
    # Build the CI column set from the header row
    matrice_ci_columns = set()
 
    if matrice_start_col is not None:
        # Every column from matrice_start_col onward whose header is a number (sector code) is CI
        # If header is empty (merged cell spill), also include it — it's under the MATRICE span
        for col_idx in range(matrice_start_col, len(raw_headers)):
            h = raw_headers[col_idx]
            header_str = str(h).strip() if h else ""
            # Include if numeric sector code OR empty (merged cell) OR looks like a sector code
            if not header_str or is_numeric_string(header_str):
                matrice_ci_columns.add(col_idx)
            else:
                # Stop if we hit a clearly named non-CI column (e.g. "Emplois totaux")
                # but only after we've collected at least a few CI columns
                if len(matrice_ci_columns) >= 2:
                    break
 
    else:
        # Fallback: if no "MATRICE" label found, detect purely by numeric header in range [1,99]
        # This handles files where the merged cell label wasn't captured by openpyxl
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
                    # false start
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
                # Header is empty or non-numeric (merged cell spill) — use position
                base_name = f"CI_col{i}"
 
        # Handle duplicate names
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
        if not row: continue
        first_cell = str(row[0]).strip().lower() if row[0] else ""
        if "totaux" in first_cell or "total" in first_cell:
            break
        body.append(row)
 
    if not body: return None
 
    df = pd.DataFrame(body, columns=headers)
 
    # --- 4. STANDARD CLEANUP ---
    if df.shape[1] >= 2:
        headers[0] = "code_secteur"
        headers[1] = "lib_secteur"
        df.columns = headers
 
        df = df[df["code_secteur"].astype(str) != "None"]
        df = df[~df["code_secteur"].astype(str).str.contains("Unité|TABLEAUX|millions|Source", case=False, na=False)]
 
        df["code_secteur"] = df["code_secteur"].astype(str).str.strip()
        df["lib_secteur"] = df["lib_secteur"].astype(str).str.strip()
 
        df = df.dropna(how='all')
 
    df = df.dropna(axis=1, how='all')
    df = df.reset_index(drop=True)
 
    # --- 5. SPACER REMOVAL ---
    cols_to_drop = []
    if "code_secteur" in df.columns and "lib_secteur" in df.columns:
        ref_code = df["code_secteur"]
        ref_label = df["lib_secteur"]
 
        for col in df.columns:
            if col in ["code_secteur", "lib_secteur"]: continue
            if col.startswith("CI_"): continue  # never remove CI columns
 
            curr_col_data = df[col].astype(str).str.strip()
            if curr_col_data.equals(ref_code): cols_to_drop.append(col)
            elif curr_col_data.equals(ref_label): cols_to_drop.append(col)
            elif str(col).startswith("Col_") and len(df) > 0 and str(df[col].iloc[0]) == str(df["code_secteur"].iloc[0]):
                cols_to_drop.append(col)
 
    if cols_to_drop:
        print(f"     → Removing {len(cols_to_drop)} spacer columns")
        df = df.drop(columns=cols_to_drop)
 
    if not df.columns.is_unique:
        df.columns = pd.io.parsers.ParserBase({'names': df.columns})._maybe_dedup_names(df.columns)
 
    return df
 
def merge_sheet_tables_horizontally(table_list):
    if not table_list: return None
    return clean_table(table_list[0]['data'])
 
def to_long_format_spark(df):
    id_vars = ["code_secteur", "lib_secteur", "Annee", "Version", "Date_de_Chargement"]
    value_columns = [c for c in df.columns if c not in id_vars]
   
    if not value_columns: return df
   
    stack_parts = [f"'{c}', CAST(`{c}` AS STRING)" for c in value_columns]
    stack_expr = f"stack({len(value_columns)}, {', '.join(stack_parts)}) as (Variable, Valeur)"
   
    long_df = df.select(
        F.col("code_secteur"),
        F.col("lib_secteur"),
        F.col("Annee"),
        F.col("Version"),
        F.col("Date_de_Chargement"),
        F.expr(stack_expr)
    )
    return long_df.where(F.col("Valeur").isNotNull())
 
def extract_year_version_from_sheet(sheet_name):
    clean_name = sheet_name.strip()
    match_year = re.search(r'(20\d{2})', clean_name)
    if match_year:
        year = match_year.group(1)
        parts = clean_name.split(year)
        version = parts[-1].strip() if len(parts) > 1 and parts[-1].strip() else "C"
        return year, version
    return "Unknown", "Unknown"
 
def extract_category_path(full_s3_path, raw_bucket_root):
    """
    FIXED: Extract category from the actual S3 structure
    Example: s3a://01-raw/2026/02/INS/TRE/file.xlsx → INS/TRE
    """
    clean_path = full_s3_path.replace("s3a://", "").strip("/")
    clean_root = raw_bucket_root.replace("s3a://", "").strip("/")
   
    if clean_path.startswith(clean_root):
        relative_path = clean_path[len(clean_root):].strip("/")
        parts = relative_path.split("/")
       
        # Structure: YEAR/MONTH/SOURCE/SUBSOURCE/.../filename.xlsx
        # We want: SOURCE/SUBSOURCE/...
        # Skip YEAR (parts[0]) and MONTH (parts[1]), take everything except the last part (filename)
        if len(parts) > 3:
            category_parts = parts[2:-1]  # Skip year, month, and filename
            return "/".join(category_parts)
   
    return "UNKNOWN_CATEGORY"
 
def format_code_secteur(val):
    """Format code_secteur preserving leading zeros like '01', '06', etc."""
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
    """Format regular Excel values, removing .0 from whole numbers."""
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
 
 
def process_single_file(spark, full_s3_file_path, raw_bucket_root, target_bucket):
    file_name = full_s3_file_path.split("/")[-1]
    local_tmp_path = f"/tmp/{file_name}"
    category_path = extract_category_path(full_s3_file_path, raw_bucket_root)
   
    print(f"\n{'='*80}")
    print(f"📁 Processing File: {file_name}")
    print(f"📂 Category: {category_path}")
    print(f"📂 Full S3 Path: {full_s3_file_path}")
    print(f"{'='*80}")
 
    if not download_from_s3(spark, full_s3_file_path, local_tmp_path): return
 
    try:
        # --- A. READ EXCEL ---
        wb = openpyxl.load_workbook(local_tmp_path, data_only=True)
        all_sheets_data = []
 
        for sheet_name in wb.sheetnames:
            print(f"\n  📄 Processing sheet: {sheet_name}")
            tables = extract_tables_from_sheet(wb, sheet_name)
            if not tables: continue
            sheet_df = merge_sheet_tables_horizontally(tables)
            if sheet_df is not None and not sheet_df.empty:
                year, version = extract_year_version_from_sheet(sheet_name)
                sheet_df['Annee'] = year
                sheet_df['Version'] = version
                sheet_df = sheet_df.astype(str)
                all_sheets_data.append(sheet_df)
               
                ci_cols = [col for col in sheet_df.columns if col.startswith('CI_')]
                if ci_cols:
                    print(f"     ✅ Found {len(ci_cols)} CI columns: {ci_cols[:5]}{'...' if len(ci_cols) > 5 else ''}")
 
        if not all_sheets_data:
            print(f"  ⚠️  WARNING: No data found in {file_name}")
            return
 
        final_pd_df = pd.concat(all_sheets_data, ignore_index=True)
       
        # --- B. SHOW COLUMN SUMMARY ---
        print(f"\n  🔧 Final column summary:")
        ci_cols = [col for col in final_pd_df.columns if col.startswith('CI_')]
        other_cols = [col for col in final_pd_df.columns if not col.startswith('CI_') and col not in ['code_secteur', 'lib_secteur', 'Annee', 'Version']]
        print(f"     - ID columns: code_secteur, lib_secteur")
        print(f"     - Economic columns: {len(other_cols)}")
        print(f"     - CI matrix columns: {len(ci_cols)}")
        if ci_cols:
            print(f"     - CI columns sample: {ci_cols[:10]}")
       
        # --- C. NORMALIZE DATA ---
        id_cols = ["code_secteur", "lib_secteur", "Annee", "Version"]
        value_cols = [c for c in final_pd_df.columns if c not in id_cols]
 
        print(f"\n  🔧 Normalizing {len(value_cols)} value columns...")
        for col_name in value_cols:
            final_pd_df[col_name] = final_pd_df[col_name].apply(format_excel_value)
       
        print(f"  🔧 Normalizing code_secteur (preserving leading zeros)...")
        final_pd_df["code_secteur"] = final_pd_df["code_secteur"].apply(format_code_secteur)
 
        # --- D. CREATE SPARK DATAFRAME ---
        final_pd_df.columns = [str(col) for col in final_pd_df.columns]
        spark_df = spark.createDataFrame(final_pd_df)
        spark_df = spark_df.withColumn("Date_de_Chargement", F.current_date())
        spark_df = spark_df.withColumn("code_secteur", F.trim(F.col("code_secteur"))) \
                           .withColumn("lib_secteur", F.trim(F.col("lib_secteur")))
 
        long_df = to_long_format_spark(spark_df)
       
        long_df = long_df.withColumn(
            "Variable",
            F.regexp_replace(F.col("Variable"), r"^(\d+)\.0$", "$1")
        )
        long_df = long_df.withColumn("Variable", F.trim(F.col("Variable")))
 
        # --- E. PROCESS PER YEAR ---
        unique_years_rows = long_df.select("Annee").distinct().collect()
        unique_years = [row.Annee for row in unique_years_rows]
       
        sc = spark.sparkContext
        Path = sc._jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
        conf = sc._jsc.hadoopConfiguration()
       
        for year in unique_years:
            if year == "Unknown": continue
           
            output_path = f"{target_bucket.rstrip('/')}/{category_path}/{year}"
            print(f"\n  📅 Processing Year: {year}")
            print(f"  📂 Output path: {output_path}")
 
            df_new_batch = long_df.filter(F.col("Annee") == year)
            df_new_batch.createOrReplaceTempView("v_new_data")
           
            print("  🔍 Sample of NEW data (first 5 rows):")
            spark.sql("SELECT * FROM v_new_data LIMIT 5").show(truncate=False)
 
            history_exists = False
            change_count = 0
 
            try:
                df_history = spark.read.parquet(output_path)
                if df_history.rdd.isEmpty(): raise Exception("Empty")
                df_history.createOrReplaceTempView("v_history")
                history_exists = True
                print("  ✅ History found. Running strict change detection...")
                print("  🔍 Sample of HISTORY data (first 5 rows):")
                spark.sql("SELECT * FROM v_history ORDER BY Date_de_Chargement DESC LIMIT 5").show(truncate=False)
            except Exception:
                print("  ℹ️  No history found. Treating as new dataset.")
 
            df_to_write = None
 
            if not history_exists:
                # First load — write everything as-is
                print("  🆕 New Data: Writing all rows.")
                df_to_write = spark.sql("SELECT * FROM v_new_data")
 
            else:
                # Detect only rows where the value changed vs the latest known value
                delta_query = """
                SELECT n.*
                FROM v_new_data n
                LEFT JOIN (
                    SELECT code_secteur, lib_secteur, Variable, Version, Valeur
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY code_secteur, lib_secteur, Variable, Version
                                ORDER BY Date_de_Chargement DESC
                            ) as rn
                        FROM v_history
                    )
                    WHERE rn = 1
                ) h
                ON  TRIM(n.code_secteur) = TRIM(h.code_secteur)
                AND TRIM(n.lib_secteur)  = TRIM(h.lib_secteur)
                AND TRIM(n.Variable)     = TRIM(h.Variable)
                AND TRIM(n.Version)      = TRIM(h.Version)
                WHERE
                    h.code_secteur IS NULL
                    OR CAST(n.Valeur AS DECIMAL(38,12)) <> CAST(h.Valeur AS DECIMAL(38,12))
                    OR (n.Valeur IS NOT NULL AND h.Valeur IS NULL)
                    OR (n.Valeur IS NULL AND h.Valeur IS NOT NULL)
                """
               
                df_changes = spark.sql(delta_query)
                change_count = df_changes.cache().count()
               
                if change_count > 0:
                    print(f"  🔄 {change_count} changed row(s) detected.")
                    print("  🔍 Sample of CHANGES (first 10 rows):")
                    df_changes.orderBy("Variable", "code_secteur").show(10, truncate=False)
                    df_to_write = df_changes
                else:
                    print("  ✅ No changes detected. Minio file left untouched.")
                    df_to_write = None
 
                df_changes.unpersist()
 
            # ── WRITE ──
            if df_to_write is not None:
                temp_output_path = output_path + "_temp_write"
 
                if history_exists and change_count > 0:
                    print(f"  💾 Inserting {change_count} new row(s) into existing file...")
 
                    # ✅ Keep ALL existing rows untouched + append the changed rows at the bottom
                    # Nothing is deleted or modified — pure insert
                    df_final = spark.read.parquet(output_path).unionAll(df_to_write)
 
                else:
                    # First-time load
                    print(f"  💾 Writing initial load...")
                    df_final = df_to_write
 
                # Write merged result to temp then atomically replace the original file
                df_final.coalesce(1).write.mode("overwrite").parquet(temp_output_path)
 
                try:
                    target_uri = sc._jvm.java.net.URI(output_path)
                    fs = FileSystem.get(target_uri, conf)
                    dst_path = Path(output_path)
                    src_path = Path(temp_output_path)
 
                    if fs.exists(dst_path):
                        fs.delete(dst_path, True)
 
                    success = fs.rename(src_path, dst_path)
                    if success:
                        print("  ✅ File updated successfully — existing rows preserved, changes inserted.")
                    else:
                        print("  ❌ ERROR: Rename failed.")
                except Exception as fs_e:
                    print(f"  ❌ FileSystem Error: {fs_e}")
 
            # ── Cleanup temp views ──
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
 
 
def run_etl_pipeline(spark, raw_bucket, target_bucket, target_folder="INS/TRE"):
    print(f"\n{'#'*80}")
    print(f"# ETL PIPELINE STARTED - IMPROVED CI COLUMN DETECTION")
    print(f"# Target Folder: {target_folder}")
    print(f"{'#'*80}\n")
   
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
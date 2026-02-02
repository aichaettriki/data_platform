
import sys
import os
import shutil
import pandas as pd
import openpyxl
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, expr, col, lit

def create_spark_session(app_name="ETL INS TRE MERGE"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    return spark

# --- UPDATED: ADDED specific_folder ARGUMENT ---
def find_latest_files_recursive(spark, base_path, specific_folder=""):
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

        # 1. Find Year/Month (ignorer le jour)
        years = get_sorted_subdirs(base_path)
        if not years: return []
        latest_year = years[0]
        
        months = get_sorted_subdirs(latest_year.toString())
        if not months: return []
        latest_month = months[0]
        
        print(f"  > Found Latest Year/Month: {latest_year.getName()}/{latest_month.getName()}")

        # 2. CONSTRUCT TARGET PATH
        # On ignore le jour, donc on part directement du mois
        if specific_folder:
            search_start_path = Path(f"{latest_month.toString()}/{specific_folder}")
        else:
            search_start_path = latest_month

        print(f"  > Targeting specific folder: {search_start_path.toString()}")

        if not fs.exists(search_start_path):
            print(f"  ! Warning: The specific folder {specific_folder} does not exist for this month.")
            return []

        # 3. Recursive search ONLY inside that specific folder
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
                            files_found.append(item.getPath().toString())
            except Exception:
                pass
        return files_found
    except Exception as e:
        print(f"Error traversing directory: {e}")
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
    """
    Extract the entire sheet as a single wide table.
    TRE tables have multiple sections side-by-side (Resources, MATRICE CI, Final Uses)
    that should be kept together as one table.
    """
    ws = wb[sheet_name]
    max_row = ws.max_row
    max_col = ws.max_column
    
    print(f"    Sheet dimensions: {max_row} rows x {max_col} columns")
    
    # Find the data start row (skip title and unit rows)
    data_start_row = None
    header_row = None
    
    for row_idx in range(1, min(15, max_row + 1)):
        first_cell = ws.cell(row=row_idx, column=1).value
        if first_cell:
            first_cell_str = str(first_cell).strip()
            # Skip title rows, unit rows, etc.
            if any(k in first_cell_str for k in ["TABLEAUX", "Unité", "millions", "Source", "MATRICE"]):
                continue
            # Check if this looks like a sector code (2-digit or specific code)
            if len(first_cell_str) <= 3 and (first_cell_str.isdigit() or first_cell_str in ["CR2", "CR3"]):
                data_start_row = row_idx
                header_row = row_idx - 1
                break
    
    if data_start_row is None:
        print(f"    Could not find data start row in sheet {sheet_name}")
        return []
    
    print(f"    Header row: {header_row}, Data starts at row: {data_start_row}")
    
    # Extract ALL data as a single table (rows from 1 to max_row)
    all_data = []
    for row_idx in range(1, max_row + 1):
        row_data = []
        for col_idx in range(1, max_col + 1):
            cell_val = ws.cell(row=row_idx, column=col_idx).value
            row_data.append(cell_val)
        all_data.append(row_data)
    
    # Return as a single "table" with all data
    return [{'data': all_data, 'sheet': sheet_name}]


# --- FUNCTION 2: CLEAN TABLE WITH ALL FIXES ---
def clean_table(data_rows):
    """
    Generic version with Spacer Removal.
    1. Reads headers dynamically.
    2. Cleans basic structure.
    3. DETECTS AND REMOVES columns that are duplicates of the 'Code' or 'Label'.
    """
    if not data_rows: 
        return None
    
    # --- 1. FIND DATA START ---
    data_start_idx = 0
    for i in range(min(len(data_rows), 15)):
        row = data_rows[i]
        if not row: continue
        first_cell = str(row[0]).strip() if row[0] else ""
        second_cell = str(row[1]).strip() if len(row) > 1 and row[1] else ""
        
        if any(k in first_cell for k in ["Unité", "TABLEAUX", "millions", "Source", "MATRICE"]): 
            continue
        
        # Look for the pattern: Short Code (col 0) + Long Text (col 1)
        if (len(first_cell) <= 4 and (first_cell.isdigit() or first_cell in ["CR2", "CR3"]) and len(second_cell) > 5):
            data_start_idx = i
            break
    
    header_row_idx = data_start_idx - 1 if data_start_idx > 0 else 0
    raw_headers = data_rows[header_row_idx] if header_row_idx < len(data_rows) else data_rows[0]
    
    # --- 2. GENERATE HEADERS ---
    headers = []
    header_counts = {}

    for i, h in enumerate(raw_headers):
        if h is None or str(h).strip() == "" or h is False:
            base_name = f"Col_{i}"
        else:
            base_name = " ".join(str(h).replace("\n", " ").split())
            if "Unité" in base_name or "millions" in base_name:
                base_name = f"Col_{i}"

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
        new_cols = list(df.columns)
        new_cols[0] = "code_secteur"
        new_cols[1] = "lib_secteur"
        df.columns = new_cols
        
        df = df[df["code_secteur"].astype(str) != "None"]
        df = df[~df["code_secteur"].astype(str).str.contains("Unité|TABLEAUX|millions|Source", case=False, na=False)]
        df = df.dropna(how='all')

    # Remove completely empty columns (fixes "DCF Ménages_1" issue)
    df = df.dropna(axis=1, how='all')
    df = df.reset_index(drop=True)

    # --- 5. SPACER DETECTION & REMOVAL (THE FIX) ---
    # We compare every column against column 0 (Code) and column 1 (Label).
    # If the content matches, it's a spacer (like Col_13 or Col_14).
    
    cols_to_drop = []
    
    # Ensure we have reference columns to compare against
    if "code_secteur" in df.columns and "lib_secteur" in df.columns:
        ref_code = df["code_secteur"].astype(str).str.strip()
        ref_label = df["lib_secteur"].astype(str).str.strip()
        
        for col in df.columns:
            # Skip the actual first two columns
            if col in ["code_secteur", "lib_secteur"]:
                continue
            
            # Get current column content as string
            curr_col_data = df[col].astype(str).str.strip()
            
            # Check 1: Does this column duplicate the SECTOR CODE?
            # We assume it's a duplicate if >90% of rows match
            if curr_col_data.equals(ref_code):
                print(f"    -> Detected spacer column (Duplicate Code): {col}")
                cols_to_drop.append(col)
                continue

            # Check 2: Does this column duplicate the SECTOR LABEL?
            if curr_col_data.equals(ref_label):
                print(f"    -> Detected spacer column (Duplicate Label): {col}")
                cols_to_drop.append(col)
                continue
            
            # Check 3: Is it named 'Col_XX' and contains the code '01' in the first row?
            # This catches partial spacers
            if str(col).startswith("Col_") and len(df) > 0:
                first_val = str(df[col].iloc[0])
                if first_val == str(df["code_secteur"].iloc[0]):
                     print(f"    -> Detected spacer column by value: {col}")
                     cols_to_drop.append(col)

    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Final uniqueness check
    if not df.columns.is_unique:
        df.columns = pd.io.parsers.ParserBase({'names': df.columns})._maybe_dedup_names(df.columns)

    return df

# --- FUNCTION 3: SIMPLIFIED MERGE (NO HORIZONTAL MERGING NEEDED) ---
def merge_sheet_tables_horizontally(table_list):
    """
    Since extract_tables_from_sheet now returns the entire sheet as ONE table,
    this function simply cleans that single table and returns it.
    No horizontal merging is needed.
    """
    if not table_list: 
        return None
    
    # Should only have one table now
    if len(table_list) != 1:
        print(f"    Warning: Expected 1 table, got {len(table_list)} tables")
    
    # Clean the single table
    table_info = table_list[0]
    df = clean_table(table_info['data'])
    
    if df is not None and not df.empty:
        print(f"    Final merged table: {len(df)} rows x {len(df.columns)} columns")
        # Show column names for verification
        print(f"    Columns: {list(df.columns[:10])}... (showing first 10)")
    
    return df




def to_long_format_spark(df):
    id_vars = ["code_secteur", "lib_secteur", "Annee", "Version", "Date_de_Chargement"]
    value_columns = [c for c in df.columns if c not in id_vars]
    
    if not value_columns: return df
    
    stack_parts = [f"'{c}', CAST(`{c}` AS STRING)" for c in value_columns]
    
    stack_expr = f"stack({len(value_columns)}, {', '.join(stack_parts)}) as (Variable, Valeur)"
    
    long_df = df.select(
        col("code_secteur"), 
        col("lib_secteur"), 
        col("Annee"), 
        col("Version"), 
        col("Date_de_Chargement"), 
        expr(stack_expr)
    )
    return long_df.where(col("Valeur").isNotNull())

def extract_year_version_from_sheet(sheet_name):
    clean_name = sheet_name.strip()
    match_year = re.search(r'(20\d{2})', clean_name)
    
    if match_year:
        year = match_year.group(1)
        parts = clean_name.split(year)
        if len(parts) > 1:
            version = parts[-1].strip() 
            if not version: version = "C" 
        else:
            version = "C"
        return year, version
    else:
        return "Unknown", "Unknown"

# --- DYNAMIC PATH EXTRACTION ---
def extract_category_path(full_s3_path, raw_bucket_root):
    # This logic still works to get "INS/TRE" for the output folder name
    clean_path = full_s3_path.replace("s3a://", "").strip("/")
    clean_root = raw_bucket_root.replace("s3a://", "").strip("/")
    
    if clean_path.startswith(clean_root):
        relative_path = clean_path[len(clean_root):].strip("/")
        parts = relative_path.split("/")
        # Year/Month/Day are indexes 0,1,2.
        if len(parts) > 4:
            category_parts = parts[2:-1]
            return "/".join(category_parts)
    return "UNKNOWN_CATEGORY"


def process_single_file(spark, full_s3_file_path, raw_bucket_root, target_bucket):
    file_name = full_s3_file_path.split("/")[-1]
    local_tmp_path = f"/tmp/{file_name}"
    
    category_path = extract_category_path(full_s3_file_path, raw_bucket_root)
    
    print(f"\n--- Processing File: {file_name} ---")
    print(f"    Category: {category_path}")

    if not download_from_s3(spark, full_s3_file_path, local_tmp_path):
        return

    try:
        wb = openpyxl.load_workbook(local_tmp_path, data_only=True)
        all_sheets_data = []

        for sheet_name in wb.sheetnames:
            print(f"  > Reading Sheet: {sheet_name}")
            tables = extract_tables_from_sheet(wb, sheet_name)
            if not tables: continue
            
            sheet_df = merge_sheet_tables_horizontally(tables)
            
            if sheet_df is not None and not sheet_df.empty:
                year, version = extract_year_version_from_sheet(sheet_name)
                sheet_df['Annee'] = year
                sheet_df['Version'] = version
                # Convert to string to prevent initial type mismatch errors
                sheet_df = sheet_df.astype(str)
                all_sheets_data.append(sheet_df)

        if not all_sheets_data:
            print(f"  WARNING: No data found in {file_name}")
            return

        final_pd_df = pd.concat(all_sheets_data, ignore_index=True)
        
        # 1. First, normalize "nan"/"None" strings back to actual Python None objects
        final_pd_df.replace(["nan", "None"], None, inplace=True)
        
        # --- NEW CODE BLOCK: FILL EMPTY VALUES WITH 0 ---
        # Identify columns that are NOT identifiers. 
        # (Date_de_Chargement is added later in Spark, so we don't check it here)
        id_cols = ["code_secteur", "lib_secteur", "Annee", "Version"]
        value_cols = [c for c in final_pd_df.columns if c not in id_cols]

        print(f"    Filling empty cells with 0 in {len(value_cols)} value columns...")

        # Fill None/NaN with 0
        final_pd_df[value_cols] = final_pd_df[value_cols].fillna(0)
        # Also catch empty strings if they exist
        final_pd_df[value_cols] = final_pd_df[value_cols].replace("", 0)
        # -----------------------------------------------

        spark_df = spark.createDataFrame(final_pd_df)
        spark_df = spark_df.withColumn("Date_de_Chargement", current_date())

        long_df = to_long_format_spark(spark_df)

        unique_years_rows = long_df.select("Annee").distinct().collect()
        unique_years = [row.Annee for row in unique_years_rows]
        
        for year in unique_years:
            if year == "Unknown": continue
            year_df = long_df.filter(col("Annee") == year)
            
            output_path = f"{target_bucket.rstrip('/')}/{category_path}/{year}"
            
            print(f"    -> Writing to: {output_path}")

            # year_df.coalesce(1).write.mode("append") \
            #     .option("header", True).option("sep", ";").csv(f"{output_path}/csv")
            
            year_df.coalesce(1).write.mode("append").parquet(f"{output_path}")

        print(f"  ✓ Finished {file_name}")

    except Exception as e:
        print(f"  ✗ ERROR processing {file_name}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(local_tmp_path):
            os.remove(local_tmp_path)

# --- UPDATED RUN FUNCTION ---
def run_etl_pipeline(spark, raw_bucket, target_bucket, target_folder="INS/TRE"):
    """
    Added 'target_folder' argument to specify strict filtering (e.g., 'INS/TRE').
    """
    files = find_latest_files_recursive(spark, raw_bucket, specific_folder=target_folder)
    
    if not files:
        print(f"No .xlsx files found in latest date under folder '{target_folder}'.")
        return

    for full_path in files:
        process_single_file(spark, full_path, raw_bucket, target_bucket)

if __name__ == "__main__":
    spark = create_spark_session()
    # Now we specifically tell the pipeline to only look into INS/TRE
    run_etl_pipeline(spark, "s3a://01-raw/", "s3a://02-transformed", target_folder="INS/TRE")



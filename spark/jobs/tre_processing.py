import sys
import os
import logging
import re

import pandas as pd
import openpyxl

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType

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
# SPARK SESSION
# ──────────────────────────────────────────────────────────────────────────────
def create_spark_session(app_name: str = "ETL INS TRE MERGE") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key",
                os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.endpoint",
                os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Dynamic partition overwrite so we only rewrite the partitions we touch
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ──────────────────────────────────────────────────────────────────────────────
# S3 FILE DISCOVERY
# ──────────────────────────────────────────────────────────────────────────────
def _get_hadoop_fs(spark: SparkSession, base_uri: str):
    """Return a (FileSystem, Path class, URI class) tuple."""
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._jvm.java.net.URI
    conf = sc._jsc.hadoopConfiguration()
    fs = FileSystem.get(URI(base_uri), conf)
    return fs, Path, URI


def _sorted_subdirs(fs, Path, parent_str: str) -> list:
    """Return subdirectories of *parent_str* sorted descending by name."""
    p = Path(parent_str)
    if not fs.exists(p):
        return []
    stats = fs.listStatus(p)
    dirs = [s.getPath() for s in stats if s.isDirectory()]
    dirs.sort(key=lambda x: x.getName(), reverse=True)
    return dirs


def find_latest_xlsx_files(spark: SparkSession, base_path: str,
                           specific_folder: str = "") -> list:
    """
    Discover the latest .xlsx files under:
        {base_path}/{YEAR}/{MONTH}/{specific_folder}/...

    Returns a list of fully-qualified s3a:// paths.
    """
    logger.info(f"🔍 Scanning for latest files under: {base_path}")
    fs, Path, _ = _get_hadoop_fs(spark, base_path)

    years = _sorted_subdirs(fs, Path, base_path)
    if not years:
        logger.error("No year folders found.")
        return []
    latest_year = years[0]
    logger.info(f"  Latest year : {latest_year.getName()}")

    months = _sorted_subdirs(fs, Path, latest_year.toString())
    if not months:
        logger.error("No month folders found.")
        return []
    latest_month = months[0]
    logger.info(f"  Latest month: {latest_month.getName()}")

    search_root = latest_month.toString()
    if specific_folder:
        candidate = f"{search_root}/{specific_folder}"
        if fs.exists(Path(candidate)):
            search_root = candidate
            logger.info(f"  Scoping search to: {search_root}")
        else:
            logger.warning(
                f"  Folder '{specific_folder}' not found – scanning full month.")

    # BFS to collect every .xlsx that is not a temp file
    files_found = []
    stack = [Path(search_root)]
    while stack:
        current = stack.pop()
        try:
            for item in fs.listStatus(current):
                if item.isDirectory():
                    stack.append(item.getPath())
                elif item.isFile():
                    name = item.getPath().getName()
                    if name.endswith(".xlsx") and not name.startswith("~"):
                        full = item.getPath().toString()
                        files_found.append(full)
                        logger.info(f"  ✓ {full}")
        except Exception as exc:
            logger.warning(f"  Could not read {current}: {exc}")

    logger.info(f"  Total files found: {len(files_found)}")
    return files_found


# ──────────────────────────────────────────────────────────────────────────────
# S3 FILE DOWNLOAD (Hadoop copyToLocalFile)
# ──────────────────────────────────────────────────────────────────────────────
def download_from_s3(spark: SparkSession, s3_path: str, local_path: str) -> bool:
    safe_path = s3_path.replace(" ", "%20")
    logger.info(f"  ⬇  Downloading {s3_path}")
    fs, Path, URI = _get_hadoop_fs(spark, safe_path)
    try:
        uri = URI(safe_path)
        fs.copyToLocalFile(False, Path(uri), Path(local_path), True)
        return True
    except Exception as exc:
        logger.error(f"  Download failed: {exc}")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# EXCEL PARSING HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def _is_numeric_string(s) -> bool:
    if not isinstance(s, str):
        return False
    try:
        float(s.strip())
        return True
    except (ValueError, OverflowError):
        return False


def extract_tables_from_sheet(wb, sheet_name: str) -> list:
    """
    Locate the data block inside *sheet_name* and return it as
    [{'data': [[...], ...], 'sheet': sheet_name}].
    """
    ws = wb[sheet_name]
    max_row, max_col = ws.max_row, ws.max_column

    data_start_row = None
    header_row = None

    for row_idx in range(1, min(15, max_row + 1)):
        first = ws.cell(row=row_idx, column=1).value
        if not first:
            continue
        first_str = str(first).strip()
        second = ws.cell(row=row_idx, column=2).value
        second_str = str(second).strip() if second else ""

        if any(k in first_str for k in
               ["TABLEAUX", "Unité", "millions", "Source", "MATRICE"]):
            continue

        if (len(first_str) <= 4
                and (first_str.isdigit()
                     or first_str.upper() in ["CR1", "CR2", "CR3", "CR"])
                and len(second_str) > 5):
            data_start_row = row_idx
            header_row = row_idx - 1
            break

    if data_start_row is None:
        return []

    all_data = []
    for row_idx in range(header_row, max_row + 1):
        first = ws.cell(row=row_idx, column=1).value
        if first and row_idx > data_start_row:
            first_low = str(first).strip().lower()
            if any(kw in first_low for kw in
                   ["total", "totaux", "somme", "equilibre",
                    "emplois totaux", "ressources totales"]):
                break
        all_data.append(
            [ws.cell(row=row_idx, column=c).value for c in range(1, max_col + 1)]
        )

    return [{"data": all_data, "sheet": sheet_name}] if all_data else []


def clean_table(data_rows: list):
    """
    Parse raw rows into a tidy Pandas DataFrame.
    CI (Matrice) columns are renamed to CI_XX.
    """
    if not data_rows:
        return None

    # --- Find data start ---
    data_start_idx = 0
    for i in range(min(len(data_rows), 15)):
        row = data_rows[i]
        if not row:
            continue
        first = str(row[0]).strip() if row[0] else ""
        second = str(row[1]).strip() if len(row) > 1 and row[1] else ""
        if any(k in first for k in ["Unité", "TABLEAUX", "millions", "Source"]):
            continue
        if (len(first) <= 4
                and (first.isdigit() or first.upper() in ["CR1", "CR2", "CR3", "CR"])
                and len(second) > 5):
            data_start_idx = i
            break

    header_row_idx = max(0, data_start_idx - 1)
    raw_headers = (data_rows[header_row_idx]
                   if header_row_idx < len(data_rows) else data_rows[0])

    # --- Locate MATRICE CI region ---
    matrice_start_col = None
    for row_idx in range(min(data_start_idx, len(data_rows))):
        row = data_rows[row_idx]
        if not row:
            continue
        for col_idx, cell in enumerate(row):
            if cell and "MATRICE" in str(cell).strip().upper():
                matrice_start_col = col_idx
                break
        if matrice_start_col is not None:
            break

    matrice_ci_cols: set = set()
    if matrice_start_col is not None:
        for col_idx in range(matrice_start_col, len(raw_headers)):
            h = raw_headers[col_idx]
            h_str = str(h).strip() if h else ""
            if not h_str or _is_numeric_string(h_str):
                matrice_ci_cols.add(col_idx)
            elif len(matrice_ci_cols) >= 2:
                break
    else:
        # Fallback: purely numeric headers in [1, 99]
        non_id_seen = ci_started = False
        consecutive = 0
        for col_idx, h in enumerate(raw_headers):
            if col_idx < 2:
                continue
            h_str = str(h).strip() if h else ""
            if _is_numeric_string(h_str):
                try:
                    num = float(h_str)
                    if 1 <= num < 100 and float(num).is_integer():
                        if non_id_seen:
                            ci_started = True
                        if ci_started:
                            matrice_ci_cols.add(col_idx)
                            consecutive += 1
                    else:
                        if ci_started and consecutive >= 2:
                            break
                except (ValueError, OverflowError):
                    pass
            elif h_str:
                non_id_seen = True
                if ci_started and consecutive >= 2:
                    break
                elif ci_started:
                    matrice_ci_cols.clear()
                    ci_started = False
                    consecutive = 0

    # --- Build header names ---
    headers = []
    header_counts: dict = {}
    for i, h in enumerate(raw_headers):
        if h is None or str(h).strip() == "" or h is False:
            base = f"Col_{i}"
        else:
            base = " ".join(str(h).replace("\n", " ").split())
            if "Unité" in base or "millions" in base:
                base = f"Col_{i}"

        if i in matrice_ci_cols:
            h_str = str(h).strip() if h else ""
            try:
                base = f"CI_{int(float(h_str)):02d}"
            except (ValueError, OverflowError):
                base = f"CI_col{i}"

        if base in header_counts:
            header_counts[base] += 1
            unique = f"{base}_{header_counts[base]}"
        else:
            header_counts[base] = 0
            unique = base
        headers.append(unique)

    # --- Extract body rows ---
    body = []
    for i in range(data_start_idx, len(data_rows)):
        row = data_rows[i]
        if not row:
            continue
        first = str(row[0]).strip().lower() if row[0] else ""
        if "totaux" in first or "total" in first:
            break
        body.append(row)

    if not body:
        return None

    df = pd.DataFrame(body, columns=headers)

    if df.shape[1] >= 2:
        headers[0] = "code_secteur"
        headers[1] = "lib_secteur"
        df.columns = headers
        df = df[df["code_secteur"].astype(str) != "None"]
        df = df[~df["code_secteur"].astype(str).str.contains(
            "Unité|TABLEAUX|millions|Source", case=False, na=False)]
        df["code_secteur"] = df["code_secteur"].astype(str).str.strip()
        df["lib_secteur"] = df["lib_secteur"].astype(str).str.strip()
        df = df.dropna(how="all")

    df = df.dropna(axis=1, how="all").reset_index(drop=True)

    # Drop spacer columns (columns that mirror code_secteur or lib_secteur)
    cols_to_drop = []
    if "code_secteur" in df.columns and "lib_secteur" in df.columns:
        ref_code = df["code_secteur"].astype(str).str.strip()
        ref_label = df["lib_secteur"].astype(str).str.strip()
        for col in df.columns:
            if col in ("code_secteur", "lib_secteur") or col.startswith("CI_"):
                continue
            curr = df[col].astype(str).str.strip()
            if curr.equals(ref_code) or curr.equals(ref_label):
                cols_to_drop.append(col)
            elif (str(col).startswith("Col_") and len(df) > 0
                  and str(df[col].iloc[0]) == str(df["code_secteur"].iloc[0])):
                cols_to_drop.append(col)

    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    if not df.columns.is_unique:
        from pandas.io.parsers.readers import ParserBase
        df.columns = ParserBase({"names": df.columns})._maybe_dedup_names(
            df.columns)

    return df


# ──────────────────────────────────────────────────────────────────────────────
# METADATA HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def extract_year_version(sheet_name: str) -> tuple:
    clean = sheet_name.strip()
    m = re.search(r"(20\d{2})", clean)
    if m:
        year = m.group(1)
        suffix = clean.split(year)[-1].strip()
        version = suffix if suffix else "C"
        return year, version
    return "Unknown", "Unknown"


def extract_category_path(full_s3_path: str, raw_bucket_root: str) -> str:
    """
    s3a://01-raw/2026/02/INS/TRE/file.xlsx → INS/TRE
    """
    clean_path = full_s3_path.replace("s3a://", "").strip("/")
    clean_root = raw_bucket_root.replace("s3a://", "").strip("/")
    if clean_path.startswith(clean_root):
        relative = clean_path[len(clean_root):].strip("/")
        parts = relative.split("/")
        if len(parts) > 3:
            return "/".join(parts[2:-1])  # skip YEAR, MONTH, filename
    return "UNKNOWN_CATEGORY"


def _format_code_secteur(val) -> str:
    if val is None or pd.isna(val) or str(val).strip().lower() in ("nan", "none", ""):
        return "0"
    s = str(val).strip()
    try:
        f = float(s)
        if f.is_integer():
            return f"{int(f):02d}" if s.startswith("0") else str(int(f))
    except (ValueError, OverflowError):
        pass
    return s


def _format_value(val) -> str:
    if val is None or pd.isna(val) or str(val).strip().lower() in ("nan", "none", ""):
        return "0"
    s = str(val).strip()
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except (ValueError, OverflowError):
        pass
    return s


# ──────────────────────────────────────────────────────────────────────────────
# PIVOT  →  LONG FORMAT  (pure Spark stack())
# ──────────────────────────────────────────────────────────────────────────────
def to_long_format(spark_df):
    id_vars = {"code_secteur", "lib_secteur", "Annee", "Version",
               "Date_de_Chargement"}
    value_cols = [c for c in spark_df.columns if c not in id_vars]
    if not value_cols:
        return spark_df

    stack_expr = (
        f"stack({len(value_cols)}, "
        + ", ".join(f"'{c}', CAST(`{c}` AS STRING)" for c in value_cols)
        + ") as (Variable, Valeur)"
    )

    return (
        spark_df
        .select(
            "code_secteur", "lib_secteur", "Annee", "Version",
            "Date_de_Chargement",
            F.expr(stack_expr),
        )
        .where(F.col("Valeur").isNotNull())
        # Clean up residual ".0" suffixes on numeric variable names
        .withColumn(
            "Variable",
            F.trim(F.regexp_replace(F.col("Variable"), r"^(\d+)\.0$", "$1")),
        )
    )


# ──────────────────────────────────────────────────────────────────────────────
# WRITE HELPERS  –  100 % Spark, no manual Parquet / rename tricks
# ──────────────────────────────────────────────────────────────────────────────
def _path_exists(spark: SparkSession, path: str) -> bool:
    """Return True if *path* exists in the object store (even if empty-ish)."""
    try:
        spark.read.parquet(path).limit(1).count()
        return True
    except Exception:
        return False


def _write_year(spark: SparkSession,
                df_to_write,
                output_path: str,
                compression: str = "snappy") -> None:
    """
    Write *df_to_write* to *output_path* as Parquet.
    Spark decides the number of output files based on its own parallelism.
    """
    (
        df_to_write
        .write
        .mode("overwrite")
        .option("compression", compression)
        .parquet(output_path)
    )
    logger.info(f"  ✅ Written → {output_path}")


# ──────────────────────────────────────────────────────────────────────────────
# SINGLE-FILE PROCESSOR
# ──────────────────────────────────────────────────────────────────────────────
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
                year, version = extract_year_version(sheet_name)
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
 
 


# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
def run_etl_pipeline(spark: SparkSession,
                     raw_bucket: str,
                     target_bucket: str,
                     target_folder: str = "INS/TRE") -> None:
    logger.info("#" * 72)
    logger.info("# ETL PIPELINE START")
    logger.info(f"# Source     : {raw_bucket}")
    logger.info(f"# Destination: {target_bucket}")
    logger.info(f"# Folder     : {target_folder}")
    logger.info("#" * 72)

    files = find_latest_xlsx_files(spark, raw_bucket,
                                   specific_folder=target_folder)
    if not files:
        logger.error("❌ No .xlsx files found – pipeline aborted.")
        return

    logger.info(f"\n✅ {len(files)} file(s) to process.\n")
    for path in files:
        process_single_file(spark, path, raw_bucket, target_bucket)

    logger.info("#" * 72)
    logger.info("# ETL PIPELINE COMPLETE")
    logger.info("#" * 72)


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _spark = create_spark_session()
    run_etl_pipeline(
        spark=_spark,
        raw_bucket="s3a://01-raw/",
        target_bucket="s3a://02-transformed",
        target_folder="INS/TRE",
    )
    _spark.stop()
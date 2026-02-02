import sys
import io
import re
import pandas as pd
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def make_columns_unique(cols):
    seen = {}
    new_cols = []
    for col in cols:
        col_str = str(col).strip() if pd.notna(col) else "unnamed"
        if col_str in seen:
            seen[col_str] += 1
            new_cols.append(f"{col_str}_{seen[col_str]}")
        else:
            seen[col_str] = 0
            new_cols.append(col_str)
    return new_cols

def process_sheet_to_df(file_content, sheet_name):
    """Lecture Excel sur le Driver pour éviter les erreurs de worker"""
    try:
        # On lit avec Pandas sur le driver (là où Python 3.11 et openpyxl sont ok)
        df_raw = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, header=None, engine='openpyxl')
        
        # Détecter la ligne de header
        header_idx = -1
        for i, row in df_raw.iterrows():
            row_str = " ".join([str(x).lower() for x in row.values if pd.notna(x)])
            if "production" in row_str:
                header_idx = i
                break
        
        if header_idx == -1: return None

        df_clean = df_raw.iloc[header_idx:].reset_index(drop=True)
        df_clean.columns = make_columns_unique(df_clean.iloc[0].tolist())
        df_clean = df_clean.iloc[1:].reset_index(drop=True).dropna(how='all')
        
        return df_clean.astype(str) # On convertit tout en string ici
    except Exception as e:
        print(f"Erreur lecture onglet {sheet_name}: {e}")
        return None

def run(endpoint, access_key, secret_key):
    spark = SparkSession.builder.appName("TRE_Orchestrator").getOrCreate()
    
    clean_endpoint = endpoint.replace("http://", "").replace("https://", "")
    client = Minio(clean_endpoint, access_key, secret_key, secure=False)
    
    objects = client.list_objects("01-raw", recursive=True)
    
    for obj in objects:
        if not obj.object_name.endswith('.xlsx'): continue
        
        print(f"\n>>> TRAITEMENT : {obj.object_name}")
        path_parts = obj.object_name.split('/')
        source = path_parts[-2] if len(path_parts) > 1 else "source"
        filename = path_parts[-1].replace('.xlsx', '').replace(' ', '_')
        
        response = client.get_object("01-raw", obj.object_name)
        file_content = response.read()
        xls = pd.ExcelFile(io.BytesIO(file_content))
        
        for sheet in xls.sheet_names:
            year_match = re.search(r'(\d{4})', sheet)
            year = year_match.group(1) if year_match else "unknown"
            
            # 1. On transforme l'Excel en Pandas sur le Driver
            pdf = process_sheet_to_df(file_content, sheet)
            
            if pdf is not None and not pdf.empty:
                # 2. On crée le DataFrame Spark
                df_spark = spark.createDataFrame(pdf)
                
                # 3. UNPIVOT (Format Long)
                id_cols = [df_spark.columns[0], df_spark.columns[1]]
                value_cols = [c for c in df_spark.columns if c not in id_cols]
                
                stack_expr = ", ".join([f"'{c}', `{c}`" for c in value_cols])
                df_long = df_spark.select(
                    F.col(id_cols[0]).alias("code_ligne"),
                    F.col(id_cols[1]).alias("libelle_ligne"),
                    F.expr(f"stack({len(value_cols)}, {stack_expr}) as (indicateur, valeur)")
                ).filter("valeur != 'nan' AND valeur != '0' AND valeur != '0.0'")
                
                # 4. Écriture
                output_path = f"s3a://02-transformed/{source}/{filename}/{year}/{sheet.replace(' ', '_')}"
                print(f"      [OK] Écriture vers {output_path}")
                df_long.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], sys.argv[3])
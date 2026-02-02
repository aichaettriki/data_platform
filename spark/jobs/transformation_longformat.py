import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import io
from minio import Minio

def clean_excel_for_spark(file_content, sheet_name):
    """Nettoyage initial avec Pandas pour identifier le tableau"""
    df_raw = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, header=None)
    
    # Détection dynamique du header
    header_idx = 0
    for i, row in df_raw.iterrows():
        row_str = " ".join([str(x).lower() for x in row.values if pd.notna(x)])
        if any(keyword in row_str for keyword in ['production', 'importation', 'produit', 'p.1']):
            header_idx = i
            break
            
    df_clean = df_raw.iloc[header_idx:].reset_index(drop=True)
    # Fusion des colonnes si multi-index ou nettoyage simple
    df_clean.columns = [str(c).strip() if pd.notna(c) else f"Col_{i}" for i, c in enumerate(df_clean.iloc[0])]
    df_clean = df_clean.iloc[1:].reset_index(drop=True)
    return df_clean.dropna(how='all')

def run_transformation(endpoint, access_key, secret_key, bucket_raw, bucket_dest):
    spark = SparkSession.builder.appName("Transformation_TRE_Excel").getOrCreate()
    
    client = Minio(endpoint.replace("http://", ""), access_key=access_key, secret_key=secret_key, secure=False)
    
    objects = client.list_objects(bucket_raw, recursive=True)
    
    for obj in objects:
        if not obj.object_name.endswith('.xlsx'): continue
        
        # Lecture Excel
        response = client.get_object(bucket_raw, obj.object_name)
        file_bytes = response.read()
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        
        for sheet in xls.sheet_names:
            # 1. Nettoyage et conversion en Spark
            pdf = clean_excel_for_spark(file_bytes, sheet)
            if pdf.empty: continue
            
            df_spark = spark.createDataFrame(pdf.astype(str))
            
            # 2. Logic UNPIVOT (Format Long) sans mapping
            # On considère : Col 0 = Code, Col 1 = Libellé, le reste = Opérations
            cols = df_spark.columns
            id_vars = cols[:2]
            val_vars = cols[2:]
            
            stack_expr = ", ".join([f"'{c}', `{c}`" for c in val_vars])
            df_long = df_spark.select(
                F.col(id_vars[0]).alias("code"),
                F.col(id_vars[1]).alias("libelle"),
                F.expr(f"stack({len(val_vars)}, {stack_expr}) as (operation, valeur)")
            )
            
            # Nettoyage final
            df_long = df_long.filter("valeur != '0' AND valeur != '0.0' AND valeur != 'nan'")
            
            # 3. Écriture vers MinIO (Transformed)
            output_path = f"s3a://{bucket_dest}/{obj.object_name.split('.')[0]}/{sheet}"
            df_long.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

if __name__ == "__main__":
    # Récupération des arguments passés par Airflow
    run_transformation(
        endpoint=sys.argv[1],
        access_key=sys.argv[2],
        secret_key=sys.argv[3],
        bucket_raw="01-raw",
        bucket_dest="02-transformed"
    )
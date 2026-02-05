from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import requests
import xml.etree.ElementTree as ET
import boto3
import json
import os

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
BUCKET_NAME = "raw"

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ETL INS API via Spark + MinIO")
        .getOrCreate()
    )
    
    # Config pour MinIO
    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", ACCESS_KEY)
    hadoopConf.set("fs.s3a.secret.key", SECRET_KEY)
    hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark

def upload_to_minio(data_rows, filename="temp_ins_data.json"):
    """
    Sauvegarde en JSON local puis envoie vers MinIO (Landing Zone).
    Cela contourne le problème de version Python entre Airflow et Spark.
    """
    local_path = f"/tmp/{filename}"
    
    # Écriture en JSON pour que Spark puisse lire les colonnes dynamiquement
    with open(local_path, 'w') as f:
        for row in data_rows:
            f.write(json.dumps(row) + "\n")
            
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    s3_path = f"landing/{filename}"
    try:
        s3.upload_file(local_path, BUCKET_NAME, s3_path)
        print(f"✅ Fichier intermédiaire uploadé : s3a://{BUCKET_NAME}/{s3_path}")
    except Exception as e:
        print(f"❌ Erreur upload MinIO: {e}")
        raise e
        
    return f"s3a://{BUCKET_NAME}/{s3_path}"

def fetch_ins_data():
    """Récupère et parse le XML dynamiquement"""
    url = "http://dataportal.ins.tn/WebApi/GetData"
    headers = {'Content-Type': 'application/xml'}
    body = """<QueryMessage SourceId='OBJ4325069'>
        <Period From='2014' To='2014' Frequency='Y'/>
        <DataWhere></DataWhere>
    </QueryMessage>"""

    print("📡 Appel API INS...")
    response = requests.post(url, data=body, headers=headers)
    response.raise_for_status()

    root = ET.fromstring(response.text)
    rows = []
    
    # On récupère tous les attributs tels quels (OBJ..., RDS..., Period)
    for set_elem in root.findall('Set'):
        row = set_elem.attrib.copy() 
        row['value'] = set_elem.text # On ajoute la valeur
        rows.append(row)
        
    return rows

def main():
    # 1. Récupération (Python pur)
    data = fetch_ins_data()
    if not data:
        print("❌ Pas de données.")
        return

    # 2. Upload JSON intermédiaire sur MinIO
    input_s3_path = upload_to_minio(data)

    # 3. Traitement Spark
    spark = create_spark_session()
    
    print(f"📖 Lecture Spark depuis {input_s3_path}")
    # Spark détecte automatiquement les colonnes présentes dans le JSON
    df = spark.read.json(input_s3_path)

    # Ajout timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    print("Aperçu des données et du schéma détecté :")
    df.printSchema()
    df.show(5)

    # 4. Écriture en CSV
    output_path = "s3a://raw/ins_data_2014/"
    
    # coalesce(1) pour n'avoir qu'un seul fichier CSV en sortie
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .option("delimiter", ",") \
        .csv(output_path)

    print(f"✅ Données stockées en CSV dans {output_path}")

if __name__ == "__main__":
    main()
    
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_timestamp
# import requests
# import xml.etree.ElementTree as ET
# import boto3
# import json
# import os

# # Configuration MinIO
# MINIO_ENDPOINT = "http://minio:9000"
# ACCESS_KEY = "minio"
# SECRET_KEY = "minio123"
# BUCKET_NAME = "raw"

# def create_spark_session():
#     spark = (
#         SparkSession.builder
#         .appName("ETL INS API via Spark + MinIO")
#         .getOrCreate()
#     )
    
#     hadoopConf = spark._jsc.hadoopConfiguration()
#     hadoopConf.set("fs.s3a.access.key", ACCESS_KEY)
#     hadoopConf.set("fs.s3a.secret.key", SECRET_KEY)
#     hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
#     hadoopConf.set("fs.s3a.path.style.access", "true")
#     hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     return spark

# def upload_to_minio(data_rows, filename="temp_ins_data.json"):
#     """
#     Sauvegarde les données en JSON localement puis les envoie sur MinIO.
#     """
#     local_path = f"/tmp/{filename}"
    
#     # Écriture en NDJSON (Newline Delimited JSON)
#     # Chaque attribut du XML devient une clé du JSON, donc une colonne Spark
#     with open(local_path, 'w') as f:
#         for row in data_rows:
#             f.write(json.dumps(row) + "\n")
            
#     s3 = boto3.client('s3',
#                       endpoint_url=MINIO_ENDPOINT,
#                       aws_access_key_id=ACCESS_KEY,
#                       aws_secret_access_key=SECRET_KEY)
    
#     s3_path = f"landing/{filename}"
#     try:
#         s3.upload_file(local_path, BUCKET_NAME, s3_path)
#         print(f"✅ Fichier temporaire uploadé : s3a://{BUCKET_NAME}/{s3_path}")
#     except Exception as e:
#         print(f"❌ Erreur upload MinIO: {e}")
#         raise e
        
#     return f"s3a://{BUCKET_NAME}/{s3_path}"

# def fetch_ins_data():
#     url = "http://dataportal.ins.tn/WebApi/GetData"
#     headers = {'Content-Type': 'application/xml'}
#     body = """<QueryMessage SourceId='OBJ4325069'>
#         <Period From='2014' To='2014' Frequency='Y'/>
#         <DataWhere></DataWhere>
#     </QueryMessage>"""

#     print("📡 Appel API INS...")
#     response = requests.post(url, data=body, headers=headers)
#     response.raise_for_status()

#     root = ET.fromstring(response.text)
#     rows = []
    
#     # Parsing dynamique : 
#     # On prend TOUS les attributs du XML (Period, RDS_..., OBJ...) et on les met dans le dict.
#     # On ajoute juste 'value' pour le contenu texte.
#     for set_elem in root.findall('Set'):
#         row = set_elem.attrib.copy() # Copie tous les attributs tels quels
#         row['value'] = set_elem.text # Ajoute la valeur numérique
#         rows.append(row)
        
#     return rows

# def main():
#     # 1. Récupération API
#     data = fetch_ins_data()
#     if not data:
#         print("❌ Pas de données.")
#         return

#     # 2. Upload vers Landing zone (JSON)
#     input_s3_path = upload_to_minio(data)

#     # 3. Traitement Spark
#     spark = create_spark_session()
    
#     print(f"📖 Lecture Spark (Schema inferré automatiquement) depuis {input_s3_path}")
#     df = spark.read.json(input_s3_path)

#     # Ajout du timestamp technique d'ingestion uniquement
#     df = df.withColumn("ingestion_timestamp", current_timestamp())

#     # Affichage pour vérification (Tu verras les colonnes OBJ... et RDS...)
#     print("Aperçu des données :")
#     df.show(5)
#     df.printSchema()

#     # 4. Écriture en PARQUET
#     output_path = "s3a://raw/ins_data_2014_parquet/"
    
#     df.write \
#         .mode("overwrite") \
#         .parquet(output_path)

#     print(f"✅ Données stockées au format PARQUET dans {output_path}")

# if __name__ == "__main__":
#     main()
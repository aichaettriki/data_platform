from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import xml.etree.ElementTree as ET
from datetime import datetime


INS_URL = "http://dataportal.ins.tn/WebApi/GetDimensionElements"

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ETL Dimension INS via Spark + MinIO")
        .getOrCreate()
    )

    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "minio")
    hadoopConf.set("fs.s3a.secret.key", "minio123")
    hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoopConf.set("fs.s3a.path.style.access", "true")

    return spark


def fetch_dimension_xml(dimension_id):
    xml_body = f"""
    <QueryMessage>
        <DataWhere>
            <DimensionId WithData="true">{dimension_id}</DimensionId>
        </DataWhere>
    </QueryMessage>
    """
    headers = {"Content-Type": "text/xml"}

    print("📡 Fetching dimension XML from INS:", dimension_id)
    res = requests.post(INS_URL, data=xml_body, headers=headers)

    print("🔎 Status API =", res.status_code)
    return res.text


def save_raw_xml(xml_text, dimension_id, spark):
    """Enregistrer l’XML brut dans MinIO sous raw/ins/..."""
    now = datetime.now()
    raw_path = f"s3a://raw/ins/dimensions/{dimension_id}/year={now.year}/"

    (spark.sparkContext
         .parallelize([xml_text])
         .saveAsTextFile(raw_path))

    print("💾 RAW XML saved to:", raw_path)


def parse_dimension(xml_text):
    root = ET.fromstring(xml_text)
    elements = []

    def traverse(elem, parent_key=None, level=0):
        key = elem.attrib.get("KEY")
        name = elem.attrib.get("NAME")
        iso = elem.attrib.get("ISO", "")

        elements.append({
            "key": key,
            "name": name,
            "iso": iso,
            "parent_key": parent_key,
            "level": level
        })

        for child in elem.findall("Element"):
            traverse(child, parent_key=key, level=level + 1)

    for elem in root.find("Elements").findall("Element"):
        traverse(elem)

    return elements


def save_dim_parquet(spark, elements, dimension_id):
    schema = StructType([
        StructField("key", StringType()),
        StructField("name", StringType()),
        StructField("iso", StringType()),
        StructField("parent_key", StringType()),
        StructField("level", IntegerType())
    ])

    df = spark.createDataFrame(elements, schema)

    out_path = f"s3a://dim/ins/{dimension_id}/"

    print("💾 Saving parquet to:", out_path)
    df.write.mode("overwrite").parquet(out_path)

    return df


if __name__ == "__main__":
    spark = create_spark_session()


    DIM_ID = "RDS_DICT_REGIONS_NSO"

    xml_data = fetch_dimension_xml(DIM_ID)
    save_raw_xml(xml_data, DIM_ID, spark)

    elements = parse_dimension(xml_data)
    df = save_dim_parquet(spark, elements, DIM_ID)

    df.show(20, truncate=False)
    spark.stop()

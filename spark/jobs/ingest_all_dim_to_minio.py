import requests
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, StringType

# =====================================================
# 🔧 CONFIG
# =====================================================
BASE_URL = "http://dataportal.ins.tn/WebApi/"
TARGET_PATH = "s3a://02-transformed/INS/API/dimensions"

# =====================================================
# 🔧 Spark session
# =====================================================
def create_spark_session():
    return (
        SparkSession.builder
        .appName("INS_API_Dimensions_Ingestion")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

# =====================================================
# 🔧 API helpers
# =====================================================
def post_xml(url, body):
    headers = {"Content-Type": "application/xml"}
    response = requests.post(url, data=body, headers=headers, timeout=60)
    response.raise_for_status()
    return response.text

# =====================================================
# 🔽 1. Get all dimensions
# =====================================================
def get_dimensions():
    print("📡 Calling INS API → GetStructure")

    xml = post_xml(BASE_URL + "GetStructure", "<QueryMessage></QueryMessage>")
    root = ET.fromstring(xml)

    dims = {}

    for source in root.findall(".//Source"):
        for dim in source.findall("./Dimensions/Dimension"):
            dim_id = dim.attrib["Id"]

            if dim_id not in dims:
                dims[dim_id] = {
                    "dimension_id": dim_id,
                    "dimension_name": dim.attrib.get("Name"),
                }

    dimensions = list(dims.values())
    print(f"✅ {len(dimensions)} unique dimensions detected")

    return dimensions

# =====================================================
# 🔽 2. Get elements for one dimension
# =====================================================
def get_dimension_elements(dim):
    dim_id = dim["dimension_id"]
    dim_name = dim["dimension_name"]

    print(f"\n🔎 Dimension {dim_id} | {dim_name}")

    body = f"""
    <QueryMessage>
        <DataWhere>
            <DimensionId WithData='true'>{dim_id}</DimensionId>
        </DataWhere>
    </QueryMessage>
    """

    xml = post_xml(BASE_URL + "GetDimensionElements", body)
    root = ET.fromstring(xml)

    elements = []
    for elem in root.findall(".//Element"):
        elements.append((
            dim_id,
            dim_name,
            elem.attrib.get("KEY"),
            elem.attrib.get("NAME")
        ))

    print(f"   ➕ {len(elements)} elements retrieved")

    # Log preview
    for e in elements[:5]:
        print(f"   • {e[2]} → {e[3]}")

    return elements

# =====================================================
# 🚀 MAIN
# =====================================================
if __name__ == "__main__":

    spark = create_spark_session()

    schema = StructType([
        StructField("dimension_id", StringType(), False),
        StructField("dimension_name", StringType(), True),
        StructField("element_key", StringType(), True),
        StructField("element_name", StringType(), True),
    ])

    dimensions = get_dimensions()

    all_rows = []

    for dim in dimensions:
        rows = get_dimension_elements(dim)
        all_rows.extend(rows)

    if not all_rows:
        print("❌ No data retrieved from INS API")
        spark.stop()
        exit(1)

    print(f"\n📦 Total elements collected: {len(all_rows)}")

    df = spark.createDataFrame(all_rows, schema=schema)
    df = df.withColumn("ingestion_date", current_date())

    print("\n📊 Sample data:")
    df.show(10, truncate=False)

    print(f"\n✍️ Writing data to {TARGET_PATH}")

    (
        df
        .repartition("dimension_id")  # best practice
        .write
        .mode("overwrite")
        .partitionBy("dimension_id")
        .parquet(TARGET_PATH)
    )

    print("✅ INS dimensions ingestion completed successfully")

    spark.stop()
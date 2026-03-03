from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from common.dag_helpers import (
    make_spark_conf,
    create_zip_task,
    SPARK_COMMON_ZIP
)

# =====================================================
# DAG
# =====================================================

with DAG(
    dag_id="02-TRANS__FMI_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["FMI", "SPARK", "TRANSFORM"]
) as dag:

    # 1️⃣ créer common.zip
    zip_common = create_zip_task(dag)

    # 2️⃣ lancer spark
    transform_fmi = SparkSubmitOperator(

        task_id="spark_transform_fmi",

        application="/opt/spark/jobs/etl_fmi.py",

        conn_id="spark_standalone",

        name="FMI-RAW-to-SILVER",

        packages="org.apache.hadoop:hadoop-aws:3.3.4",

        py_files=SPARK_COMMON_ZIP,   # ⭐ TRÈS IMPORTANT

        conf=make_spark_conf(),

        verbose=True,
    )

    zip_common >> transform_fmi
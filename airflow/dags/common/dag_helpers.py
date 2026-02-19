# dags/common/dag_helpers.py
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import zipfile, os, tempfile, shutil
from airflow.operators.python import PythonOperator

RAW_BUCKET = os.getenv("RAW_BUCKET", "s3a://01-raw")
SPARK_COMMON_ZIP = "/tmp/common.zip"
# dags/common/dag_helpers.py
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.endpoint":   os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
    "spark.hadoop.fs.s3a.path.style.access":      "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

def make_spark_conf(extra: dict = None) -> dict:
    conf = dict(SPARK_S3_CONF)
    if extra:
        conf.update(extra)
    return conf


def create_zip_task(dag):

    def _zip_common():
        import zipfile, os

        common_dir = "/opt/spark/common"
        zip_path   = "/tmp/common.zip"
        tmp_path   = zip_path + ".tmp"

        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(common_dir):
                for f in files:
                    if f.endswith(".py"):
                        abs_path = os.path.join(root, f)
                        arc_name = os.path.relpath(abs_path, "/opt/spark").replace(os.sep, "/")
                        zf.write(abs_path, arc_name)
                        print(f"  + {arc_name}")

        os.replace(tmp_path, zip_path)
        print(f"ZIP OK → {zip_path}")

    return PythonOperator(
        task_id="zip_common_package",
        python_callable=_zip_common,
        dag=dag,
    )

def create_cleanup_task(dag, source_bucket: str, triggered_by: str):
    return TriggerDagRunOperator(
        task_id="trigger_cleanup_minio",
        trigger_dag_id="Cleanup_Minio",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        conf={
            "triggered_by": triggered_by,
            "source_bucket": source_bucket,
        },
        dag=dag,
    )
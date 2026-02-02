"""
Airflow DAG: Transform INS TRE Data
Description: Orchestrates the transformation of INS TRE Excel files from raw to transformed format
Schedule: On-demand (manual trigger)
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
# UTILITY FUNCTIONS
# ======================
def validate_environment(**context):
    """Validate that all required environment variables are set"""
    logger.info("="*60)
    logger.info("Validating Environment Variables")
    logger.info("="*60)
    
    # Check if job file exists
    job_path = "/opt/spark/jobs/job_spark_transform_2.py"
    if not os.path.exists(job_path):
        error_msg = f"Spark job file not found: {job_path}"
        logger.error(error_msg)
        logger.error("Please copy the job file to /opt/spark/jobs/")
        raise FileNotFoundError(error_msg)
    
    logger.info(f"✔ Spark job file found: {job_path}")
    logger.info("✔ Environment validation complete")
    return True

def log_job_start(**context):
    """Log job start information"""
    execution_date = context['execution_date']
    logger.info("="*60)
    logger.info(f"Starting TRE Transform Job")
    logger.info(f"Execution Date: {execution_date}")
    logger.info(f"DAG Run ID: {context['dag_run'].run_id}")
    logger.info("="*60)

def log_job_complete(**context):
    """Log job completion information"""
    logger.info("="*60)
    logger.info("TRE Transform Job Completed Successfully")
    logger.info("="*60)

# ======================
# CONFIGURATION
# ======================
# Spark Configuration
SPARK_CONN_ID = "spark_standalone"  # Based on your logs

# IMPORTANT: The actual path where Spark finds the job
# From your logs: /opt/spark-3.5.1-bin-hadoop3/jobs/
SPARK_JOB_PATH = "/opt/spark/jobs/job_spark_transform_2.py"

# Spark packages for S3A support
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# MinIO Configuration
MINIO_ENDPOINT =  "minio:9000"
MINIO_ROOT_USER = get_env_var("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = get_env_var("MINIO_ROOT_PASSWORD")

# Bucket Configuration
RAW_BUCKET = "01-raw"
TRANS_BUCKET =  "02-transformed"
RAW_OBJECT = "2026/01/22/INS/TRE 2015-2023.xlsx"

# ======================
# DAG DEFAULT ARGS
# ======================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 22),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ======================
# DAG DEFINITION
# ======================
with DAG(
    dag_id="transform_ins_tre_v2",
    default_args=default_args,
    description="Transform INS TRE Excel files from raw to long format CSV",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["INS", "TRE", "TRANSFORMED", "SPARK", "V2"],
    doc_md=__doc__,
) as dag:

    # Task 1: Validate Environment
    validate_env = PythonOperator(
        task_id="validate_environment",
        python_callable=validate_environment,
        provide_context=True,
        doc_md="""
        ## Validate Environment
        Checks that the Spark job file exists before execution.
        """
    )

    # Task 2: Log Job Start
    log_start = PythonOperator(
        task_id="log_job_start",
        python_callable=log_job_start,
        provide_context=True,
    )

    # Task 3: Execute Spark Transformation
    transform_tre = SparkSubmitOperator(
        task_id="transform_excel_sheets",
        application=SPARK_JOB_PATH,
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        verbose=True,
        
        # Spark Configuration
        conf={
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "2",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.access.key": MINIO_ROOT_USER,
            "spark.hadoop.fs.s3a.secret.key": MINIO_ROOT_PASSWORD,
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        },
        
        # Environment variables passed to Spark driver
        env_vars={
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "MINIO_ROOT_USER": MINIO_ROOT_USER,
            "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
            "RAW_BUCKET": RAW_BUCKET,
            "TRANS_BUCKET": TRANS_BUCKET,
            "RAW_OBJECT": RAW_OBJECT,
        },
        
        # NO application_args - the improved job doesn't need them
        application_args=[],
        
        # Driver and executor settings
        driver_memory="2g",
        executor_memory="2g",
        executor_cores=2,
        num_executors=2,
        
        # Application name
        name="TRE_Transform_Job",
        
        doc_md="""
        ## Transform Excel Sheets
        Executes the Spark job to transform INS TRE Excel files.
        
        **Process:**
        1. Download Excel file from MinIO (raw bucket)
        2. Process each sheet individually
        3. Transform to long format
        4. Save as CSV to MinIO (transformed bucket)
        
        **Output Location:** s3a://02-transformed/INS/TRE/{year}/{sheet_name}
        """
    )

    # Task 4: Log Job Complete
    log_complete = PythonOperator(
        task_id="log_job_complete",
        python_callable=log_job_complete,
        provide_context=True,
        trigger_rule="all_success",
    )

    # Define task dependencies
    validate_env >> log_start >> transform_tre >> log_complete
# airflow/init_connections.py
from airflow import settings
from airflow.models import Connection

session = settings.Session()

# MinIO
if not session.query(Connection).filter(Connection.conn_id=='minio_default').first():
    conn = Connection(
        conn_id='minio_default',
        conn_type='S3',
        host='http://minio:9000',
        login='minio',
        password='minio123',
        extra='{"secure": false}'
    )
    session.add(conn)

# Spark Standalone
if not session.query(Connection).filter(Connection.conn_id=='spark_standalone').first():
    conn = Connection(
        conn_id='spark_standalone',
        conn_type='spark',
        host='spark://spark-master:7077',
        extra='{"deploy_mode": "client"}'
    )
    session.add(conn)


# PostgreSQL
if not session.query(Connection).filter(Connection.conn_id=='postgres_airflow').first():
    conn = Connection(
        conn_id='postgres_airflow',
        conn_type='postgres',
        host='postgres-airflow',
        schema='airflow',
        login='airflow',
        password='airflow',
        port=5432
    )
    session.add(conn)

session.commit()
session.close()
print("Connections initialized ✅")

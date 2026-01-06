# Data Platform

A comprehensive data processing platform built with Apache Airflow, Apache Spark, MinIO object storage, and integrated monitoring solutions. This platform provides end-to-end data pipeline orchestration, processing, storage, and visualization capabilities.

## 🏗️ Architecture Overview

This platform integrates multiple open-source tools to create a complete data engineering ecosystem:

This folder contains everything required to run Metabase inside the Data Platform.

## 📁 Project Structure

```
data-platform/
├── airflow/
│   ├── dags/                    # Airflow DAGs
│   │   ├── ingest_api_to_minio.py
│   │   ├── load_csv_to_minio.py
│   │   ├── metadata_sync.py
│   │   ├── spark_marquez.py
│   │   └── spark_openlineage_demo.py
│   ├── plugins/                 # Airflow plugins
│   ├── airflow-exporter/        # Prometheus metrics exporter
│   ├── logs/                    # Airflow logs
│   ├── Dockerfile
│   └── requirements.txt
├── spark/
│   ├── jobs/                    # Spark job scripts
│   │   ├── etl_job.py
│   │   ├── etl_equipes.py
│   │   ├── nettoyage_job.py
│   │   └── atlas_job.py
│   ├── notebooks/               # Jupyter notebooks
│   ├── spark-exporter/          # Prometheus metrics exporter
│   ├── conf/                    # Spark configuration
│   ├── events/                  # Spark event logs
│   ├── Dockerfile
│   └── requirements.txt
├── minio/
│   ├── raw/                     # Raw data bucket
│   ├── transformed/             # Transformed data bucket
│   ├── refined/                 # Refined data bucket
│   └── sandbox/                 # Sandbox bucket
├── monitoring/
│   ├── prometheus/              # Prometheus configuration
│   └── grafana/                 # Grafana dashboards & provisioning
├── metabase/
│   ├── build/                   # Metabase Dockerfile
│   ├── metabase-exporter/       # Prometheus metrics exporter
│   ├── scripts/                 # Deployment scripts
│   ├── saved_states/            # Metabase saved configurations
│   └── env/                     # Environment configuration
├── nginx/                       # Nginx configuration (Marquez proxy)
├── pgadmin/                     # pgAdmin server configuration
├── postgres-init/               # PostgreSQL initialization scripts
├── ingestion/                   # Ingestion configurations
│   ├── airflow_ingest.yaml
│   └── spark_ingest.yaml
├── data/                        # Sample data files
├── docker-compose.yaml          # Main Docker Compose configuration
├── marquez.yml                  # Marquez configuration
├── loki-config.yaml             # Loki configuration
├── promtail-config.yaml         # Promtail configuration
├── start.ps1                    # Windows startup script
├── update-hosts.ps1             # Hosts file updater
└── README.md
```

## 🚀 Prerequisites

- **Docker** and **Docker Compose** installed
- **PowerShell** (for Windows startup scripts)
- **Administrator privileges** (for hosts file updates on Windows)

## 📦 Installation & Setup

### Quick Start (Windows)

1. **Run the startup script** (requires administrator privileges):
   ```powershell
   .\start.ps1
   ```
   This script will:
   - Update your hosts file with domain mappings
   - Start all Docker containers

### Manual Setup

1. **Update hosts file** (Windows):
   ```powershell
   .\update-hosts.ps1
   ```

2. **Build Docker images**:
   ```bash
   docker-compose build
   ```

3. **Start all services**:
   ```bash
   docker-compose up -d
   ```

4. **Check service status**:
   ```bash
   docker-compose ps
   ```

5. **View logs**:
   ```bash
   # Airflow webserver
   docker-compose logs -f airflow-webserver
   
   # Spark master
   docker-compose logs -f spark-master
   
   # MinIO
   docker-compose logs -f minio
   
   # All services
   docker-compose logs -f
   ```

## 🌐 Access URLs

### Main Services

| Service | URL | Description |
|---------|-----|-------------|
| **Airflow** | http://localhost:8085<br>http://airflow.itceq.tn | Workflow orchestration UI |
| **Spark Master** | http://localhost:8080<br>http://spark.itceq.tn | Spark cluster management |
| **Spark History** | http://localhost:18080<br>http://spark-history.itceq.tn | Spark job history |
| **MinIO Console** | http://localhost:9001<br>http://minio.itceq.tn | Object storage management |
| **Prometheus** | http://localhost:9090<br>http://prometheus.itceq.tn | Metrics collection |
| **Grafana** | http://localhost:3000<br>http://grafana.itceq.tn | Metrics visualization |
| **Metabase** | http://localhost:3001<br>http://metabase.itceq.tn | Business intelligence |
| **Marquez** | http://localhost:3002<br>http://marquez.itceq.tn | Data lineage UI |
| **Traefik** | http://localhost:8082 | Reverse proxy dashboard |
| **pgAdmin** | http://localhost:5050<br>http://pgadmin.itceq.tn | PostgreSQL administration |


## 🔄 Data Flow

1. **Ingestion**: Data ingested via Airflow DAGs (API, CSV, etc.) → MinIO `raw` bucket
2. **Transformation**: Spark jobs process raw data → MinIO `transformed` bucket
3. **Refinement**: Additional processing → MinIO `refined` bucket
4. **Visualization**: Metabase connects to refined data for analytics
5. **Lineage**: Marquez tracks data flow across all stages
6. **Monitoring**: Prometheus collects metrics, Grafana visualizes them


### Environment Variables

Create a `.env` file in the project root with required environment variables. See `docker-compose.yaml` for all required variables.

## 🐛 Troubleshooting

### Services not starting
```bash
docker-compose down
docker-compose up -d
```

### Check service health
```bash
docker-compose ps
docker-compose logs [service-name]
```

### Reset everything
```bash
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

### Port conflicts
Ensure all required ports are available. Check `docker-compose.yaml` for port mappings.

# Data Platform

Ce projet contient une plateforme de traitement de données avec Airflow, Spark, MinIO et un monitoring avec Prometheus + Grafana.

## Structure du projet
```bash
data-platform/
├─ airflow/
│   ├─ dags/
│   ├─ plugins/
│   └─ Dockerfile
├─ spark/
│   ├─ jobs/
│   ├─ notebooks/
│   └─ Dockerfile
├─ minio/
│   └─ raw/             
│   └─ transformed/    
├─ monitoring/
│    ├─ prometheus/
│    └─ grafana/
├─ metabase/
│      ├─ env/
│      │   └─ metabase.env
│      └─ Dockerfile
├─ logs/
├─ docker-compose.yml
└─ README.md
```
# Metabase Module

This folder contains everything required to run Metabase inside the Data Platform.

## Components
- Dockerfile : custom build of Metabase
- env/metabase.env : environment variables
- init/init.sql : initialization script that creates the Metabase PostgreSQL database

## How to access Metabase

Metabase UI will be available on:

http://localhost:3000

Default database connection is configured to PostgreSQL using the environment file.

## Prérequis

- Docker et Docker Compose installés
- Ports 8085, 8080, 9000, 9001, 9090, 3000 libres


## Installation et exécution

1. Nettoyer les anciens conteneurs Docker si nécessaire.
2. Construire les images :
   ```bash
   docker-compose build
   ```
3. Lancer les services :
   ```bash
   docker-compose up -d
   ```
4. Vérifier les logs :
   ```bash
    docker-compose logs -f airflow-webserver
    docker-compose logs -f spark
    docker-compose logs -f minio
   ```
5. Accéder aux interfaces : 
   
    - Airflow : http://localhost:8085

    - Spark UI : http://localhost:8080

    - MinIO console : http://localhost:9001

    - Prometheus : http://localhost:9090

    - Grafana : http://localhost:3000



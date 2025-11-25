from flask import Flask, Response, jsonify
from prometheus_client import (
    Gauge, Summary, generate_latest, CONTENT_TYPE_LATEST
)
import requests
import os
app = Flask(__name__)


AIRFLOW_WEBSERVER = "http://airflow-webserver:8080/api/v1"
AIRFLOW_AUTH = ("admin", "admin123")


# Pour les logs scheduler
LOGS_PATH = "/app/airflow/logs/scheduler"  # adapter selon ton volume Docker

# Nombre de lignes dans le log par DAG et date
dag_log_lines_gauge = Gauge(
    "airflow_scheduler_log_lines",
    "Nombre de lignes dans le log du scheduler par DAG et date",
    ["date", "dag_id"]
)

# Nombre d'erreurs dans le log par DAG et date
dag_log_errors_gauge = Gauge(
    "airflow_scheduler_log_errors",
    "Nombre d'erreurs dans le log du scheduler par DAG et date",
    ["date", "dag_id"]
)

dag_run_status_gauge = Gauge(
    'airflow_dag_run_status',
    'Number of DAG runs per DAG and state',
    ['dag_id', 'state']
)

dag_run_duration_summary = Summary(
    'airflow_dag_run_duration_seconds',
    'Duration of DAG runs in seconds',
    ['dag_id']
)

task_status_gauge = Gauge(
    'airflow_task_status',
    'Number of tasks per DAG run and state',
    ['dag_id', 'task_id', 'state']
)

def parse_scheduler_logs():
    dag_log_lines_gauge.clear()
    dag_log_errors_gauge.clear()

    if not os.path.exists(LOGS_PATH):
        print(f"Le dossier des logs n'existe pas: {LOGS_PATH}")
        return

    for date_folder in os.listdir(LOGS_PATH):
        date_path = os.path.join(LOGS_PATH, date_folder)
        if not os.path.isdir(date_path):
            continue

        for log_file in os.listdir(date_path):
            if not log_file.endswith(".log"):
                continue

            dag_id = log_file.replace(".log", "")
            file_path = os.path.join(date_path, log_file)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                    dag_log_lines_gauge.labels(date=date_folder, dag_id=dag_id).set(len(lines))
                    errors = sum(1 for line in lines if "ERROR" in line or "CRITICAL" in line)
                    dag_log_errors_gauge.labels(date=date_folder, dag_id=dag_id).set(errors)
            except Exception as e:
                print(f"Erreur lecture log {file_path}: {e}")


@app.route("/airflow_metrics")
def get_metrics():
    try:
        # Récupérer les DAGs
        dags_resp = requests.get(f"{AIRFLOW_WEBSERVER}/dags", auth=AIRFLOW_AUTH, timeout=5)
        dags_resp.raise_for_status()
        dags = dags_resp.json().get("dags", [])

        # Reset des métriques
        dag_run_status_gauge.clear()
        dag_run_duration_summary.clear()
        task_status_gauge.clear()

        for dag in dags:
            dag_id = dag['dag_id']

            # Récupérer les DAG runs
            runs_resp = requests.get(f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/dagRuns", auth=AIRFLOW_AUTH, timeout=5)
            runs_resp.raise_for_status()
            runs = runs_resp.json().get("dag_runs", [])

            counts = {}
            for run in runs:
                state = run['state']
                counts[state] = counts.get(state, 0) + 1

                # Calculer la durée si start_date et end_date disponibles
                start = run.get('start_date')
                end = run.get('end_date')
                if start and end:
                    from datetime import datetime
                    fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
                    start_dt = datetime.strptime(start, fmt)
                    end_dt = datetime.strptime(end, fmt)
                    duration = (end_dt - start_dt).total_seconds()
                    dag_run_duration_summary.labels(dag_id=dag_id).observe(duration)

                # Récupérer les tasks
                tasks_resp = requests.get(f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances", auth=AIRFLOW_AUTH, timeout=5)
                tasks_resp.raise_for_status()
                tasks = tasks_resp.json().get("task_instances", [])
                for task in tasks:
                    task_status_gauge.labels(dag_id=dag_id, task_id=task['task_id'], state=task['state']).set(1)

            # Mettre à jour le gauge DAG run
            for state, count in counts.items():
                dag_run_status_gauge.labels(dag_id=dag_id, state=state).set(count)
        parse_scheduler_logs()
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    except requests.RequestException as e:
        print(f"Error fetching metrics: {e}")
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/health")
def health():
    return jsonify({"status": "UP"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9112)
    
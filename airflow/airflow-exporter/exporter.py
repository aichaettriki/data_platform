from flask import Flask, Response, jsonify
from prometheus_client import Gauge, Counter, Summary, generate_latest, CONTENT_TYPE_LATEST
import requests
import os
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil import parser

app = Flask(__name__)

AIRFLOW_WEBSERVER = os.getenv("AIRFLOW_WEBSERVER", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin123")
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100")
LOGS_PATH = os.getenv("LOGS_PATH", "/opt/airflow/logs")

# ---------------- Metrics ---------------- #
dag_log_lines_gauge = Gauge(
    "airflow_scheduler_log_lines",
    "Nombre de lignes dans le log du scheduler par DAG et date",
    ["date", "dag_id"]
)

dag_log_errors_gauge = Gauge(
    "airflow_scheduler_log_errors",
    "Nombre d'erreurs dans le log du scheduler par DAG et date",
    ["date", "dag_id"]
)

dag_run_status_gauge = Gauge(
    "airflow_dag_run_status",
    "Nombre de DAG runs par DAG et état",
    ["dag_id", "state"]
)

dag_run_duration_summary = Summary(
    "airflow_dag_run_duration_seconds",
    "Durée des DAG runs en secondes",
    ["dag_id"]
)

dag_run_avg_duration_gauge = Gauge(
    "airflow_dag_avg_duration_seconds",
    "Durée moyenne des DAG runs par DAG",
    ["dag_id"]
)

task_status_gauge = Gauge(
    "airflow_task_status",
    "Nombre de tasks par DAG, task_id et état",
    ["dag_id", "task_id", "state"]
)

total_dags_gauge = Gauge(
    "airflow_total_dags",
    "Nombre total de DAGs"
)

task_error_count_gauge = Gauge(
    "airflow_task_error_count",
    "Nombre d'erreurs détectées dans les logs d'une task échouée",
    ["dag_id", "task_id", "execution_date"]
)

task_last_error_message_gauge = Gauge(
    "airflow_task_last_error_message",
    "Dernier message d'erreur extrait du log d'une task échouée (longueur)",
    ["dag_id", "task_id", "execution_date"]
)

task_failed_duration_gauge = Gauge(
    "airflow_task_failed_duration_seconds",
    "Durée en secondes du task échoué (d'après le log Airflow)",
    ["dag_id", "task_id", "execution_date"]
)

airflow_error_count = Gauge(
    "airflow_error_count",
    "Number of ERROR logs by DAG/task",
    ["dag_id", "task_id", "run_id"]
)

airflow_critical_count = Gauge(
    "airflow_critical_count",
    "Number of CRITICAL logs by DAG/task",
    ["dag_id", "task_id", "run_id"]
)

airflow_timeout_count = Gauge(
    "airflow_timeout_count",
    "Number of timeout errors by DAG",
    ["dag_id"]
)

airflow_import_error_count = Gauge(
    "airflow_import_error_count",
    "Number of import errors by DAG file",
    ["dag_file"]
)

# Scheduler Health
scheduler_heartbeat_gauge = Gauge(
    "airflow_scheduler_heartbeat",
    "État du scheduler (1=actif, 0=inactif)"
)

# Task Duration
task_duration_gauge = Gauge(
    "airflow_task_duration_seconds",
    "Durée d'exécution d'une task en secondes",
    ["dag_id", "task_id", "state"]
)

# Pool metrics
pool_slots_open_gauge = Gauge(
    "airflow_pool_open_slots",
    "Nombre de slots disponibles dans un pool",
    ["pool_name"]
)

pool_slots_used_gauge = Gauge(
    "airflow_pool_used_slots",
    "Nombre de slots utilisés dans un pool",
    ["pool_name"]
)

pool_slots_queued_gauge = Gauge(
    "airflow_pool_queued_slots",
    "Nombre de slots en attente dans un pool",
    ["pool_name"]
)

# Database connections
db_connections_gauge = Gauge(
    "airflow_database_connections",
    "Nombre de connexions actives à la base de données"
)

# DAG import errors
dagbag_import_errors_gauge = Gauge(
    "airflow_dagbag_import_errors",
    "Nombre total d'erreurs d'import dans les DAG files"
)

# Executor metrics
executor_queued_tasks_gauge = Gauge(
    "airflow_executor_queued_tasks",
    "Nombre de tasks en queue dans l'executor"
)

executor_running_tasks_gauge = Gauge(
    "airflow_executor_running_tasks",
    "Nombre de tasks en cours d'exécution"
)

# Task retries
task_retry_count_gauge = Gauge(
    "airflow_task_retry_count",
    "Nombre de retries par task",
    ["dag_id", "task_id"]
)

# SLA misses
sla_misses_gauge = Gauge(
    "airflow_sla_misses",
    "Nombre de violations de SLA",
    ["dag_id", "task_id"]
)

# DAG processing
dag_processing_last_duration_gauge = Gauge(
    "airflow_dag_processing_last_duration_seconds",
    "Durée du dernier parsing de DAG en secondes",
    ["dag_id"]
)

# Zombie tasks
zombie_tasks_killed_gauge = Gauge(
    "airflow_zombies_killed",
    "Nombre de zombie tasks détectées et tuées"
)


# Cache pour garder l'historique des DAG runs
dag_run_history = defaultdict(lambda: {"success": 0, "failed": 0, "running": 0})

# Métriques persistantes
dag_runs_total = Gauge('airflow_dag_runs_total', 'Total DAG runs by state', ['dag_id', 'state'])
dag_duration_seconds = Gauge('airflow_dag_duration_seconds', 'DAG run duration', ['dag_id'])
task_failures_total = Gauge('airflow_task_failures_total', 'Total task failures', ['dag_id', 'task_id'])

# ---------------- Functions ---------------- #

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


def query_loki_errors(hours=24, level="ERROR"):
    """Query Loki for error logs in the last N hours"""
    now = datetime.utcnow()
    start = int((now - timedelta(hours=hours)).timestamp() * 1e9)
    end = int(now.timestamp() * 1e9)
    
    query = f'{{job="airflow"}} |= "{level}"'
    
    try:
        response = requests.get(
            f"{LOKI_URL}/loki/api/v1/query_range",
            params={
                "query": query,
                "start": start,
                "end": end,
                "limit": 5000
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json().get("data", {}).get("result", [])
    except Exception as e:
        print(f"Error querying Loki: {e}")
        return []


def extract_error_metrics():
    """Extract error metrics from Loki logs"""
    error_counts = defaultdict(int)
    critical_counts = defaultdict(int)
    timeout_counts = defaultdict(int)
    import_error_counts = defaultdict(int)
    
    # Get ERROR logs (90 days to match retention)
    error_results = query_loki_errors(hours=2160, level="ERROR")
    
    for result in error_results:
        labels = result.get("stream", {})
        dag_id = labels.get("dag_id", "unknown")
        task_id = labels.get("task_id", "unknown")
        run_id = labels.get("run_id", "unknown")
        dag_file = labels.get("dag_file", "")
        
        values = result.get("values", [])
        for timestamp, log_line in values:
            # Count task errors
            if dag_id != "unknown" and task_id != "unknown":
                error_counts[(dag_id, task_id, run_id)] += 1
            
            # Count timeout errors
            if "timed out" in log_line.lower() or "timeout" in log_line.lower():
                timeout_counts[dag_id] += 1
            
            # Count import errors
            if "Failed to import" in log_line and dag_file:
                import_error_counts[dag_file] += 1
    
    # Get CRITICAL logs (90 days to match retention)
    critical_results = query_loki_errors(hours=2160, level="CRITICAL")
    
    for result in critical_results:
        labels = result.get("stream", {})
        dag_id = labels.get("dag_id", "unknown")
        task_id = labels.get("task_id", "unknown")
        run_id = labels.get("run_id", "unknown")
        
        values = result.get("values", [])
        for timestamp, log_line in values:
            if dag_id != "unknown" and task_id != "unknown":
                critical_counts[(dag_id, task_id, run_id)] += 1
    
    return error_counts, critical_counts, timeout_counts, import_error_counts


def get_scheduler_health():
    """Vérifier l'état du scheduler"""
    try:
        health_resp = requests.get(
            f"{AIRFLOW_WEBSERVER}/health",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=5
        )
        health_resp.raise_for_status()
        health_data = health_resp.json()
        
        # Le scheduler est OK si le status est "healthy"
        scheduler_status = health_data.get("scheduler", {}).get("status")
        scheduler_heartbeat_gauge.set(1 if scheduler_status == "healthy" else 0)
        
    except Exception as e:
        print(f"Erreur récupération santé scheduler: {e}")
        scheduler_heartbeat_gauge.set(0)


def get_pool_metrics():
    """Récupérer les métriques des pools"""
    try:
        pools_resp = requests.get(
            f"{AIRFLOW_WEBSERVER}/pools",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=5
        )
        pools_resp.raise_for_status()
        pools = pools_resp.json().get("pools", [])
        
        pool_slots_open_gauge.clear()
        pool_slots_used_gauge.clear()
        pool_slots_queued_gauge.clear()
        
        for pool in pools:
            pool_name = pool["name"]
            pool_slots_open_gauge.labels(pool_name=pool_name).set(pool.get("open_slots", 0))
            pool_slots_used_gauge.labels(pool_name=pool_name).set(pool.get("used_slots", 0))
            pool_slots_queued_gauge.labels(pool_name=pool_name).set(pool.get("queued_slots", 0))
            
    except Exception as e:
        print(f"Erreur récupération pools: {e}")


def get_import_errors():
    """Récupérer les erreurs d'import de DAG"""
    try:
        import_errors_resp = requests.get(
            f"{AIRFLOW_WEBSERVER}/importErrors",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=5
        )
        import_errors_resp.raise_for_status()
        errors = import_errors_resp.json().get("import_errors", [])
        dagbag_import_errors_gauge.set(len(errors))
        
    except Exception as e:
        print(f"Erreur récupération import errors: {e}")
        dagbag_import_errors_gauge.set(0)


def collect_dag_metrics():
    """Collecte et persiste les métriques de tous les DAG runs historiques"""
    try:
        # Récupérer TOUS les DAG runs (pas seulement les actifs)
        response = requests.get(
            f"{AIRFLOW_WEBSERVER}/dags",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            params={"limit": 1000}  # Augmenter la limite
        )
        
        for dag in response.json().get('dags', []):
            dag_id = dag['dag_id']
            
            # Récupérer l'historique complet des runs
            runs_response = requests.get(
                f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/dagRuns",
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                params={
                    "limit": 1000,
                    "order_by": "-execution_date"
                }
            )
            
            # Compter par état
            states = {"success": 0, "failed": 0, "running": 0}
            for run in runs_response.json().get('dag_runs', []):
                state = run['state']
                if state in states:
                    states[state] += 1
                
                # Durée
                if run.get('duration'):
                    dag_duration_seconds.labels(dag_id=dag_id).set(run['duration'])
            
            # Mettre à jour les métriques
            for state, count in states.items():
                dag_runs_total.labels(dag_id=dag_id, state=state).set(count)
                
    except Exception as e:
        print(f"Error collecting DAG metrics: {e}")


# ---------------- Routes ---------------- #

@app.route("/airflow_metrics")
def airflow_metrics():
    """Endpoint pour les métriques globales Airflow (API Airflow)"""
    try:
        # Récupérer les DAGs (limiter à 100)
        dags_resp = requests.get(
            f"{AIRFLOW_WEBSERVER}/dags",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            params={"limit": 100},
            timeout=5
        )
        dags_resp.raise_for_status()
        dags = dags_resp.json().get("dags", [])

        # Metrics globales
        total_dags_gauge.set(len(dags))
        dag_run_status_gauge.clear()
        dag_run_avg_duration_gauge.clear()
        task_status_gauge.clear()
        task_duration_gauge.clear()
        task_retry_count_gauge.clear()

        for dag in dags:
            dag_id = dag["dag_id"]
            durations = []

            # Récupérer seulement les 10 derniers runs
            runs_resp = requests.get(
                f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/dagRuns",
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                params={"limit": 10},
                timeout=5
            )
            runs_resp.raise_for_status()
            runs = runs_resp.json().get("dag_runs", [])

            # Compteurs par état
            run_counts = defaultdict(int)
            task_counts = defaultdict(int)

            for run in runs:
                state = run.get("state", "unknown")
                run_counts[state] += 1

                # Durée DAG run
                start = run.get("start_date")
                end = run.get("end_date")
                if start and end:
                    try:
                        start_dt = parser.isoparse(start)
                        end_dt = parser.isoparse(end)
                        duration = (end_dt - start_dt).total_seconds()
                        durations.append(duration)
                        dag_run_duration_summary.labels(dag_id=dag_id).observe(duration)
                    except Exception as e:
                        print(f"Erreur parsing dates DAG {dag_id}: {e}")

                # Récupérer les tasks
                try:
                    tasks_resp = requests.get(
                        f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances",
                        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                        params={"limit": 100},  # Augmenté de 50 à 100
                        timeout=3
                    )
                    tasks_resp.raise_for_status()
                    tasks = tasks_resp.json().get("task_instances", [])

                    for task in tasks:
                        task_id = task["task_id"]
                        task_state = task.get("state", "unknown")
                        key = (dag_id, task_id, task_state)
                        task_counts[key] += 1
                        
                        # Durée de la task
                        task_start = task.get("start_date")
                        task_end = task.get("end_date")
                        if task_start and task_end:
                            try:
                                task_start_dt = parser.isoparse(task_start)
                                task_end_dt = parser.isoparse(task_end)
                                task_duration = (task_end_dt - task_start_dt).total_seconds()
                                task_duration_gauge.labels(
                                    dag_id=dag_id,
                                    task_id=task_id,
                                    state=task_state
                                ).set(task_duration)
                            except Exception as e:
                                print(f"Erreur parsing dates task {dag_id}/{task_id}: {e}")
                        
                        # Nombre de retries
                        try_number = task.get("try_number", 0)
                        if try_number > 1:
                            task_retry_count_gauge.labels(
                                dag_id=dag_id,
                                task_id=task_id
                            ).set(try_number - 1)
                        
                except Exception as e:
                    print(f"Erreur récupération tasks pour {dag_id}/{run['dag_run_id']}: {e}")
                    continue

            # Mettre à jour gauges DAG run - TOUS LES ÉTATS
            for state, count in run_counts.items():
                dag_run_status_gauge.labels(dag_id=dag_id, state=state).set(count)
            
            # S'assurer que les états avec 0 sont aussi exposés (important pour Grafana)
            all_possible_states = ["success", "failed", "running", "queued", "upstream_failed"]
            for state in all_possible_states:
                if state not in run_counts:
                    dag_run_status_gauge.labels(dag_id=dag_id, state=state).set(0)

            # Mettre à jour gauges tasks
            for (dag_id_key, task_id, state), count in task_counts.items():
                task_status_gauge.labels(dag_id=dag_id_key, task_id=task_id, state=state).set(count)

            # Durée moyenne
            if durations:
                avg_duration = sum(durations) / len(durations)
                dag_run_avg_duration_gauge.labels(dag_id=dag_id).set(avg_duration)

        # Nouvelles métriques
        get_scheduler_health()
        get_pool_metrics()
        get_import_errors()
        
        # Logs scheduler
        parse_scheduler_logs()

        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
    except Exception as e:
        print(f"Error in /airflow_metrics: {e}")
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/metrics")
def metrics():
    """Endpoint principal pour Prometheus (erreurs depuis Loki)"""
    try:
        error_counts, critical_counts, timeout_counts, import_error_counts = extract_error_metrics()
        
        # Clear and set error metrics
        airflow_error_count.clear()
        for (dag_id, task_id, run_id), count in error_counts.items():
            airflow_error_count.labels(dag_id=dag_id, task_id=task_id, run_id=run_id).set(count)
        
        # Clear and set critical metrics
        airflow_critical_count.clear()
        for (dag_id, task_id, run_id), count in critical_counts.items():
            airflow_critical_count.labels(dag_id=dag_id, task_id=task_id, run_id=run_id).set(count)
        
        # Clear and set timeout metrics
        airflow_timeout_count.clear()
        for dag_id, count in timeout_counts.items():
            airflow_timeout_count.labels(dag_id=dag_id).set(count)
        
        # Clear and set import error metrics
        airflow_import_error_count.clear()
        for dag_file, count in import_error_counts.items():
            airflow_import_error_count.labels(dag_file=dag_file).set(count)
        
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
    except Exception as e:
        print(f"Error in /metrics: {e}")
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# ---------------- Main ---------------- #
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("EXPORTER_PORT", 9112)))

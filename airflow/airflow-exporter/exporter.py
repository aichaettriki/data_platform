from flask import Flask, Response, jsonify
from prometheus_client import Gauge, Counter, Summary, generate_latest, CONTENT_TYPE_LATEST
import requests
import os
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil import parser
import logging
import sys

app = Flask(__name__)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger(__name__)

# Configuration
AIRFLOW_WEBSERVER = os.getenv("AIRFLOW_WEBSERVER", "http://data_platform-airflow-webserver-1:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin123")
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100")
LOGS_PATH = os.getenv("LOGS_PATH", "/opt/airflow/logs")
METRICS_TIME_WINDOW_DAYS = int(os.getenv("METRICS_TIME_WINDOW_DAYS", "30"))  # Filtre les métriques sur X jours

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

task_count_per_dag_gauge = Gauge(
    "airflow_task_count_per_dag",
    "Nombre de tâches par DAG",
    ["dag_id"]
)

task_duration_gauge = Gauge(
    "airflow_task_duration_seconds",
    "Durée d'exécution d'une task en secondes",
    ["dag_id", "task_id", "state"]
)

task_retry_count_gauge = Gauge(
    "airflow_task_retry_count",
    "Nombre de retries par task",
    ["dag_id", "task_id"]
)

scheduler_heartbeat_gauge = Gauge(
    "airflow_scheduler_heartbeat",
    "État du scheduler (1=actif, 0=inactif)"
)

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

dagbag_import_errors_gauge = Gauge(
    "airflow_dagbag_import_errors",
    "Nombre total d'erreurs d'import dans les DAG files"
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

# Nouvelle métrique pour le nombre de tâches définies
airflow_task_count_gauge = Gauge(
    "airflow_task_count",
    "Nombre de tâches définies par DAG",
    ["dag_id"]
)
airflow_task_count_total_gauge = Gauge(
    "airflow_task_count_total",
    "Nombre total de tâches définies dans Airflow"
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

airflow_error_details = Gauge(
    "airflow_error_details",
    "Detailed error information with message snippets",
    ["dag_id", "task_id", "run_id", "error_type"]
)

airflow_traceback_count = Gauge(
    "airflow_traceback_count",
    "Number of traceback lines in error logs",
    ["dag_id", "task_id", "run_id"]
)

# ---------------- Functions ---------------- #

def parse_scheduler_logs():
    """Parse scheduler logs to extract metrics"""
    dag_log_lines_gauge.clear()
    dag_log_errors_gauge.clear()

    if not os.path.exists(LOGS_PATH):
        logger.error(f"Logs directory does not exist: {LOGS_PATH}")
        return

    try:
        date_folders = [f for f in os.listdir(LOGS_PATH) if os.path.isdir(os.path.join(LOGS_PATH, f))]
        
        for date_folder in date_folders:
            date_path = os.path.join(LOGS_PATH, date_folder)
            
            try:
                log_files = [f for f in os.listdir(date_path) if f.endswith(".log")]
                
                for log_file in log_files:
                    dag_id = log_file.replace(".log", "")
                    file_path = os.path.join(date_path, log_file)

                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            lines = f.readlines()
                            errors = sum(1 for line in lines if "ERROR" in line or "CRITICAL" in line)
                            
                            dag_log_lines_gauge.labels(date=date_folder, dag_id=dag_id).set(len(lines))
                            dag_log_errors_gauge.labels(date=date_folder, dag_id=dag_id).set(errors)
                    except Exception as e:
                        logger.error(f"Error reading {file_path}: {e}")
            except Exception as e:
                logger.error(f"Error listing {date_path}: {e}")
    except Exception as e:
        logger.error(f"Error listing {LOGS_PATH}: {e}")


def query_loki_errors(hours=24, level="ERROR"):
    """Query Loki for error logs"""
    now = datetime.utcnow()
    start_dt = now - timedelta(hours=hours)
    
    start = int(start_dt.timestamp() * 1_000_000_000)
    end = int(now.timestamp() * 1_000_000_000)
    
    if not (1_000_000_000_000_000_000 < end < 2_000_000_000_000_000_000):
        logger.error(f"Invalid end timestamp: {end}")
        return []
    
    if not (1_000_000_000_000_000_000 < start < end):
        logger.error(f"Invalid start timestamp: {start}")
        return []
    
    query = f'{{job="airflow"}} |= "{level}"'
    
    try:
        url = f"{LOKI_URL}/loki/api/v1/query_range"
        params = {
            "query": query,
            "start": start,
            "end": end,
            "limit": 5000
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = data.get("data", {}).get("result", [])
        
        logger.info(f"Loki query returned {len(results)} streams for {level}")
        return results
    except Exception as e:
        logger.error(f"Error querying Loki: {e}")
        return []


def extract_error_metrics():
    """Extract error metrics from Loki logs"""
    error_counts = defaultdict(int)
    critical_counts = defaultdict(int)
    timeout_counts = defaultdict(int)
    import_error_counts = defaultdict(int)
    error_details = defaultdict(lambda: {"tracebacks": 0, "error_types": set()})
    
    error_results = query_loki_errors(hours=2160, level="ERROR")
    
    for result in error_results:
        labels = result.get("stream", {})
        dag_id = labels.get("dag_id", "unknown")
        task_id = labels.get("task_id", "unknown")
        run_id = labels.get("run_id", "unknown")
        dag_file = labels.get("dag_file", "")
        
        values = result.get("values", [])
        for timestamp, log_line in values:
            if dag_id != "unknown" and task_id != "unknown":
                error_counts[(dag_id, task_id, run_id)] += 1
                
                key = (dag_id, task_id, run_id)
                if "traceback" in log_line.lower():
                    error_details[key]["tracebacks"] += 1
                
                error_types = [
                    "AirflowException", "Py4JJavaError", "AWSS3IOException",
                    "XMinioStorageFull", "ImportError"
                ]
                for error_type in error_types:
                    if error_type in log_line:
                        error_details[key]["error_types"].add(error_type)
            
            if "timed out" in log_line.lower() or "timeout" in log_line.lower():
                timeout_counts[dag_id] += 1
            
            if "Failed to import" in log_line and dag_file:
                import_error_counts[dag_file] += 1
    
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
    
    logger.info(f"Extracted {len(error_counts)} error counts, {len(critical_counts)} critical counts")
    
    return error_counts, critical_counts, timeout_counts, import_error_counts, error_details


def get_scheduler_health():
    """Check scheduler health"""
    try:
        health_resp = requests.get(
            f"{AIRFLOW_WEBSERVER}/health",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=5
        )
        health_resp.raise_for_status()
        health_data = health_resp.json()
        
        scheduler_status = health_data.get("scheduler", {}).get("status")
        scheduler_heartbeat_gauge.set(1 if scheduler_status == "healthy" else 0)
        
    except Exception as e:
        logger.error(f"Error fetching scheduler health: {e}")
        scheduler_heartbeat_gauge.set(0)


def get_pool_metrics():
    """Fetch pool metrics"""
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
        logger.error(f"Error fetching pools: {e}")


def get_import_errors():
    """Fetch DAG import errors"""
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
        logger.error(f"Error fetching import errors: {e}")
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
        logger.error(f"Error collecting DAG metrics: {e}")


def expose_task_count_metrics():
    try:
        from airflow.models import DagBag
        dagbag = DagBag()
        airflow_task_count_gauge.clear()
        total = 0
        logger.info(f"DagBag import_errors: {dagbag.import_errors}")
        logger.info(f"Nombre de DAGs trouvés: {len(dagbag.dags)}")
        for dag_id, dag_obj in dagbag.dags.items():
            num_tasks = len(dag_obj.tasks)
            airflow_task_count_gauge.labels(dag_id=dag_id).set(num_tasks)
            logger.info(f"DAG {dag_id}: {num_tasks} tasks")
            total += num_tasks
        airflow_task_count_total_gauge.set(total)
        logger.info(f"Exposé airflow_task_count_total={total}")
    except Exception as e:
        logger.error(f"Erreur lors de l'exposition des métriques de tâches: {e}")

# Fonction pour exposer le nombre de tâches par DAG via l'API Airflow

# ---------------- Routes ---------------- #

@app.route("/airflow_metrics")
def airflow_metrics():
    """Endpoint pour les métriques globales Airflow (API Airflow)"""
    print("\n" + "="*60)
    print("🎯 ENDPOINT: /airflow_metrics appelé")
    print("="*60)
    logger.info("=== Appel /airflow_metrics ===")
    
    try:
        # Récupérer TOUS les DAGs (limite augmentée)
        dags_resp = requests.get(
            f"{AIRFLOW_WEBSERVER}/dags",
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            params={"limit": 500},
            timeout=10
        )
        dags_resp.raise_for_status()
        dags = dags_resp.json().get("dags", [])

        # Metrics globales
        total_dags_gauge.set(len(dags))
        dag_run_status_gauge.clear()
        dag_run_avg_duration_gauge.clear()
        task_status_gauge.clear()
        task_count_per_dag_gauge.clear()
        task_duration_gauge.clear()
        task_retry_count_gauge.clear()

        # Récupérer le nombre de tâches par DAG
        for dag in dags:
            try:
                tasks_resp = requests.get(
                    f"{AIRFLOW_WEBSERVER}/dags/{dag['dag_id']}/tasks",
                    auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                    timeout=10
                )
                tasks_resp.raise_for_status()
                tasks = tasks_resp.json().get("tasks", [])
                logger.info(f"DAG {dag['dag_id']}: {len(tasks)} tasks")
                task_count_per_dag_gauge.labels(dag_id=dag['dag_id']).set(len(tasks))
            except Exception as e:
                logger.error(f"Erreur récupération tasks pour {dag['dag_id']}: {e}")

        # Calculer la date limite pour le filtrage (X jours en arrière) en UTC
        from datetime import timezone
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=METRICS_TIME_WINDOW_DAYS)
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"📅 Filtrage des métriques à partir de: {cutoff_date_str}")
        
        for dag in dags:
            dag_id = dag["dag_id"]
            durations = []

            # Récupérer l'historique des runs avec filtrage par date via API
            runs_resp = requests.get(
                f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/dagRuns",
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                params={
                    "limit": 1000,
                    "execution_date_gte": cutoff_date_str
                },
                timeout=10
            )
            runs_resp.raise_for_status()
            runs = runs_resp.json().get("dag_runs", [])
            
            logger.info(f"DAG {dag_id}: {len(runs)} runs dans les {METRICS_TIME_WINDOW_DAYS} derniers jours")

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
                        logger.error(f"Erreur parsing dates DAG {dag_id}: {e}")

                # Récupérer les tasks
                try:
                    tasks_resp = requests.get(
                        f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances",
                        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                        params={"limit": 500},  # Augmenté pour avoir tout l'historique
                        timeout=5
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
                                logger.error(f"Erreur parsing dates task {dag_id}/{task_id}: {e}")
                        
                        # Nombre de retries
                        try_number = task.get("try_number", 0)
                        if try_number > 1:
                            task_retry_count_gauge.labels(
                                dag_id=dag_id,
                                task_id=task_id
                            ).set(try_number - 1)
                        
                except Exception as e:
                    logger.error(f"Erreur récupération tasks pour {dag_id}/{run['dag_run_id']}: {e}")
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

        logger.info("Airflow metrics generated successfully")
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
    except Exception as e:
        logger.error(f"Error in /airflow_metrics: {e}", exc_info=True)
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/metrics")
def metrics():
    """Main endpoint for Prometheus (Loki errors)"""
    logger.info("[DEBUG] Entrée dans la route /metrics")
    try:
        error_counts, critical_counts, timeout_counts, import_error_counts, error_details = extract_error_metrics()
        
        airflow_error_count.clear()
        for (dag_id, task_id, run_id), count in error_counts.items():
            airflow_error_count.labels(dag_id=dag_id, task_id=task_id, run_id=run_id).set(count)
        
        airflow_critical_count.clear()
        for (dag_id, task_id, run_id), count in critical_counts.items():
            airflow_critical_count.labels(dag_id=dag_id, task_id=task_id, run_id=run_id).set(count)
        
        airflow_timeout_count.clear()
        for dag_id, count in timeout_counts.items():
            airflow_timeout_count.labels(dag_id=dag_id).set(count)
        
        airflow_import_error_count.clear()
        for dag_file, count in import_error_counts.items():
            airflow_import_error_count.labels(dag_file=dag_file).set(count)
        
        airflow_error_details.clear()
        airflow_traceback_count.clear()
        for (dag_id, task_id, run_id), details in error_details.items():
            traceback_count = details.get("tracebacks", 0)
            airflow_traceback_count.labels(
                dag_id=dag_id, 
                task_id=task_id, 
                run_id=run_id
            ).set(traceback_count)
            error_types = details.get("error_types", set())
            for error_type in error_types:
                airflow_error_details.labels(
                    dag_id=dag_id,
                    task_id=task_id,
                    run_id=run_id,
                    error_type=error_type
                ).set(1)
        
        logger.info("[DEBUG] Fin traitement erreurs, début exposition tâches")
        
        # Exposition simplifiée du nombre de tâches via API Airflow
        logger.info("Exposition des métriques de tâches via API")
        try:
            airflow_task_count_gauge.clear()
            total_tasks = 0
            
            # Récupérer la liste des DAGs
            dags_resp = requests.get(
                f"{AIRFLOW_WEBSERVER}/dags",
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                params={"limit": 500},
                timeout=10
            )
            dags_resp.raise_for_status()
            dags = dags_resp.json().get("dags", [])
            
            for dag in dags:
                dag_id = dag["dag_id"]
                try:
                    tasks_resp = requests.get(
                        f"{AIRFLOW_WEBSERVER}/dags/{dag_id}/tasks",
                        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                        timeout=10
                    )
                    tasks_resp.raise_for_status()
                    tasks = tasks_resp.json().get("tasks", [])
                    num_tasks = len(tasks)
                    airflow_task_count_gauge.labels(dag_id=dag_id).set(num_tasks)
                    total_tasks += num_tasks
                except Exception as e:
                    logger.error(f"Erreur API tasks pour DAG {dag_id}: {e}")
            
            airflow_task_count_total_gauge.set(total_tasks)
            logger.info(f"Métriques tâches exposées: {total_tasks} tâches totales")
        except Exception as e:
            logger.error(f"Erreur exposition métriques tâches: {e}")
        
        logger.info("Metrics generated successfully")
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
    except Exception as e:
        logger.error(f"Error in /metrics: {e}", exc_info=True)
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# ---------------- Main ---------------- #
if __name__ == "__main__":
    logger.info("Starting Airflow Exporter on 0.0.0.0:9112")
    app.run(host="0.0.0.0", port=int(os.getenv("EXPORTER_PORT", 9112)))

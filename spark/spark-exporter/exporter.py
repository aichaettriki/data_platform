#!/usr/bin/env python3
"""
Spark REST API Exporter for Prometheus
Exposes Spark cluster metrics from Spark Master REST API
"""


import time
import logging
import requests
from threading import Thread
from flask import Flask, Response
from prometheus_client import generate_latest, Gauge
import os
from dotenv import load_dotenv

# ======================
# Configuration (via .env)
# ======================
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

SPARK_MASTER_URL = get_env_var("SPARK_MASTER_URL")
EXPORTER_PORT = get_env_var("SPARK_EXPORTER_PORT")
SCRAPE_INTERVAL = 15

# ======================
# Logging
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("spark-exporter")

# ======================
# Flask app
# ======================
app = Flask(__name__)

# ======================
# Cluster-level metrics
# ======================
spark_workers_total = Gauge("spark_workers_total", "Total number of workers")
spark_workers_alive = Gauge("spark_workers_alive", "Number of alive workers")

spark_cores_total = Gauge("spark_cores_total", "Total cores in cluster")
spark_cores_used = Gauge("spark_cores_used", "Used cores in cluster")
spark_cores_free = Gauge("spark_cores_free", "Free cores in cluster")

spark_memory_total_mb = Gauge("spark_memory_total_mb", "Total memory in cluster (MB)")
spark_memory_used_mb = Gauge("spark_memory_used_mb", "Used memory in cluster (MB)")
spark_memory_free_mb = Gauge("spark_memory_free_mb", "Free memory in cluster (MB)")

# ======================
# Application metrics
# ======================
spark_applications_running = Gauge("spark_applications_running", "Running applications")
spark_applications_completed = Gauge("spark_applications_completed", "Completed applications")
spark_applications_waiting = Gauge("spark_applications_waiting", "Waiting applications")

spark_app_duration_ms = Gauge(
    "spark_app_duration_ms",
    "Application duration (ms)",
    ["app_id", "app_name", "state"]
)
spark_app_cores = Gauge(
    "spark_app_cores",
    "Cores used by application",
    ["app_id", "app_name", "state"]
)
spark_app_memory_mb = Gauge(
    "spark_app_memory_mb",
    "Memory used by application (MB)",
    ["app_id", "app_name", "state"]
)
spark_app_start_time = Gauge(
    "spark_app_start_time",
    "Application start time (unix timestamp)",
    ["app_id", "app_name", "state"]
)
spark_app_state = Gauge(
    "spark_app_state",
    "Application state (1=RUNNING, 2=FINISHED)",
    ["app_id", "app_name"]
)

# ======================
# Worker metrics
# ======================
spark_worker_cores = Gauge(
    "spark_worker_cores",
    "Worker cores",
    ["worker_id", "worker_host"]
)
spark_worker_cores_used = Gauge(
    "spark_worker_cores_used",
    "Worker used cores",
    ["worker_id", "worker_host"]
)
spark_worker_memory_mb = Gauge(
    "spark_worker_memory_mb",
    "Worker memory (MB)",
    ["worker_id", "worker_host"]
)
spark_worker_memory_used_mb = Gauge(
    "spark_worker_memory_used_mb",
    "Worker used memory (MB)",
    ["worker_id", "worker_host"]
)
spark_worker_state = Gauge(
    "spark_worker_state",
    "Worker state (1=ALIVE, 0=DEAD)",
    ["worker_id", "worker_host"]
)

# ======================
# Exporter health
# ======================
spark_exporter_up = Gauge(
    "spark_exporter_up",
    "Exporter scrape success (1 = up, 0 = down)"
)
spark_exporter_scrape_duration_seconds = Gauge(
    "spark_exporter_scrape_duration_seconds",
    "Duration of last scrape"
)

# ======================
# Spark API helpers
# ======================
def fetch_json(endpoint):
    try:
        r = requests.get(f"{SPARK_MASTER_URL}{endpoint}", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"Spark API error {endpoint}: {e}")
        return None


# ======================
# Metrics updater
# ======================
def update_metrics():
    start = time.time()

    # CLEAR dynamic metrics
    spark_worker_cores.clear()
    spark_worker_cores_used.clear()
    spark_worker_memory_mb.clear()
    spark_worker_memory_used_mb.clear()
    spark_worker_state.clear()

    spark_app_duration_ms.clear()
    spark_app_cores.clear()
    spark_app_memory_mb.clear()
    spark_app_start_time.clear()
    spark_app_state.clear()

    try:
        cluster = fetch_json("/json/")
        if not cluster:
            spark_exporter_up.set(0)
            return

        workers = cluster.get("workers", [])
        alive = [w for w in workers if w.get("state") == "ALIVE"]

        cores_total = cluster.get("cores", 0)
        cores_used = cluster.get("coresused", 0)
        mem_total = cluster.get("memory", 0)
        mem_used = cluster.get("memoryused", 0)

        spark_workers_total.set(len(workers))
        spark_workers_alive.set(len(alive))

        spark_cores_total.set(cores_total)
        spark_cores_used.set(cores_used)
        spark_cores_free.set(cores_total - cores_used)

        spark_memory_total_mb.set(mem_total)
        spark_memory_used_mb.set(mem_used)
        spark_memory_free_mb.set(mem_total - mem_used)

        for w in workers:
            wid = w.get("id", "unknown")
            host = w.get("host", "unknown")
            state = 1 if w.get("state") == "ALIVE" else 0

            spark_worker_cores.labels(wid, host).set(w.get("cores", 0))
            spark_worker_cores_used.labels(wid, host).set(w.get("coresused", 0))
            spark_worker_memory_mb.labels(wid, host).set(w.get("memory", 0))
            spark_worker_memory_used_mb.labels(wid, host).set(w.get("memoryused", 0))
            spark_worker_state.labels(wid, host).set(state)

        running_apps = cluster.get("activeapps", [])
        completed_apps = cluster.get("completedapps", [])

        spark_applications_running.set(len(running_apps))
        spark_applications_completed.set(len(completed_apps))
        spark_applications_waiting.set(0)

        for app in running_apps:
            aid = app.get("id", "unknown")
            name = app.get("name", "unknown")
            duration = app.get("duration", 0)
            start_time = app.get("starttime", 0) / 1000

            spark_app_cores.labels(aid, name, "RUNNING").set(app.get("cores", 0))
            spark_app_memory_mb.labels(aid, name, "RUNNING").set(app.get("memoryperslave", 0))
            spark_app_duration_ms.labels(aid, name, "RUNNING").set(duration)
            spark_app_start_time.labels(aid, name, "RUNNING").set(start_time)
            spark_app_state.labels(aid, name).set(1)

        for app in completed_apps:
            aid = app.get("id", "unknown")
            name = app.get("name", "unknown")
            duration = app.get("duration", 0)
            start_time = app.get("starttime", 0) / 1000

            spark_app_cores.labels(aid, name, "FINISHED").set(app.get("cores", 0))
            spark_app_memory_mb.labels(aid, name, "FINISHED").set(app.get("memoryperslave", 0))
            spark_app_duration_ms.labels(aid, name, "FINISHED").set(duration)
            spark_app_start_time.labels(aid, name, "FINISHED").set(start_time)
            spark_app_state.labels(aid, name).set(2)

        spark_exporter_up.set(1)

    except Exception as e:
        logger.error(f"Metrics update failed: {e}")
        spark_exporter_up.set(0)

    spark_exporter_scrape_duration_seconds.set(time.time() - start)


# ======================
# Background thread
# ======================
def metrics_loop():
    while True:
        update_metrics()
        time.sleep(SCRAPE_INTERVAL)


# ======================
# HTTP endpoints
# ======================
@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype="text/plain")


@app.route("/health")
def health():
    return {"status": "ok", "spark_master": SPARK_MASTER_URL}, 200


# ======================
# Main
# ======================
if __name__ == "__main__":
    logger.info(f"Starting Spark Exporter for {SPARK_MASTER_URL}")
    Thread(target=metrics_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=EXPORTER_PORT)

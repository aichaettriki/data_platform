#!/usr/bin/env python3
"""
Spark REST API Exporter for Prometheus
Exposes detailed Spark cluster metrics from the Spark Master REST API
"""

import time
import logging
import requests
from flask import Flask, Response
from prometheus_client import generate_latest, CollectorRegistry, Gauge

# Configuration
SPARK_MASTER_URL = "http://spark-master:8080"
SCRAPE_INTERVAL = 15  # seconds

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('spark-exporter')

# Flask app
app = Flask(__name__)

# Prometheus registry
registry = CollectorRegistry()

# Metrics definitions
# Cluster-level metrics
spark_workers_total = Gauge('spark_workers_total', 'Total number of workers', registry=registry)
spark_workers_alive = Gauge('spark_workers_alive', 'Number of alive workers', registry=registry)
spark_cores_total = Gauge('spark_cores_total', 'Total cores in cluster', registry=registry)
spark_cores_used = Gauge('spark_cores_used', 'Used cores in cluster', registry=registry)
spark_cores_free = Gauge('spark_cores_free', 'Free cores in cluster', registry=registry)
spark_memory_total_mb = Gauge('spark_memory_total_mb', 'Total memory in cluster (MB)', registry=registry)
spark_memory_used_mb = Gauge('spark_memory_used_mb', 'Used memory in cluster (MB)', registry=registry)
spark_memory_free_mb = Gauge('spark_memory_free_mb', 'Free memory in cluster (MB)', registry=registry)

# Application metrics
spark_applications_running = Gauge('spark_applications_running', 'Number of running applications', registry=registry)
spark_applications_completed = Gauge('spark_applications_completed', 'Number of completed applications', registry=registry)
spark_applications_waiting = Gauge('spark_applications_waiting', 'Number of waiting applications', registry=registry)

# Application-level detailed metrics (with labels)
spark_app_duration_ms = Gauge('spark_app_duration_ms', 'Application duration in milliseconds', ['app_id', 'app_name', 'state'], registry=registry)
spark_app_cores = Gauge('spark_app_cores', 'Cores used by application', ['app_id', 'app_name', 'state'], registry=registry)
spark_app_memory_mb = Gauge('spark_app_memory_mb', 'Memory used by application in MB', ['app_id', 'app_name', 'state'], registry=registry)
spark_app_state = Gauge('spark_app_state', 'Application state (1=RUNNING, 2=FINISHED, 3=FAILED, 4=KILLED)', ['app_id', 'app_name'], registry=registry)
spark_app_start_time = Gauge('spark_app_start_time', 'Application start time (Unix timestamp)', ['app_id', 'app_name', 'state'], registry=registry)

# Worker-level metrics (with labels)
spark_worker_cores = Gauge('spark_worker_cores', 'Cores per worker', ['worker_id', 'worker_host'], registry=registry)
spark_worker_cores_used = Gauge('spark_worker_cores_used', 'Used cores per worker', ['worker_id', 'worker_host'], registry=registry)
spark_worker_memory_mb = Gauge('spark_worker_memory_mb', 'Memory per worker (MB)', ['worker_id', 'worker_host'], registry=registry)
spark_worker_memory_used_mb = Gauge('spark_worker_memory_used_mb', 'Used memory per worker (MB)', ['worker_id', 'worker_host'], registry=registry)
spark_worker_state = Gauge('spark_worker_state', 'Worker state (1=ALIVE, 0=DEAD)', ['worker_id', 'worker_host'], registry=registry)

# Exporter health
spark_exporter_up = Gauge('spark_exporter_up', 'Exporter is successfully scraping metrics', registry=registry)
spark_exporter_scrape_duration_seconds = Gauge('spark_exporter_scrape_duration_seconds', 'Duration of last scrape', registry=registry)


def fetch_cluster_info():
    """Fetch cluster information from Spark Master API"""
    try:
        response = requests.get(f"{SPARK_MASTER_URL}/json/", timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch cluster info: {e}")
        return None


def fetch_applications():
    """Fetch applications from Spark Master API"""
    try:
        response = requests.get(f"{SPARK_MASTER_URL}/api/v1/applications", timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch applications: {e}")
        return []


def update_metrics():
    """Update all Prometheus metrics"""
    start_time = time.time()
    
    try:
        # Fetch cluster data
        cluster_info = fetch_cluster_info()
        if not cluster_info:
            spark_exporter_up.set(0)
            return
        
        # Parse cluster-level metrics
        status = cluster_info.get('status', 'UNKNOWN')
        workers = cluster_info.get('workers', [])
        alive_workers = [w for w in workers if w.get('state') == 'ALIVE']
        
        cores_total = cluster_info.get('cores', 0)
        cores_used = cluster_info.get('coresused', 0)
        memory_total = cluster_info.get('memory', 0)
        memory_used = cluster_info.get('memoryused', 0)
        
        # Update cluster metrics
        spark_workers_total.set(len(workers))
        spark_workers_alive.set(len(alive_workers))
        spark_cores_total.set(cores_total)
        spark_cores_used.set(cores_used)
        spark_cores_free.set(cores_total - cores_used)
        spark_memory_total_mb.set(memory_total)
        spark_memory_used_mb.set(memory_used)
        spark_memory_free_mb.set(memory_total - memory_used)
        
        # Update worker-level metrics
        for worker in workers:
            worker_id = worker.get('id', 'unknown')
            worker_host = worker.get('host', 'unknown')
            worker_state = 1 if worker.get('state') == 'ALIVE' else 0
            
            spark_worker_cores.labels(worker_id=worker_id, worker_host=worker_host).set(worker.get('cores', 0))
            spark_worker_cores_used.labels(worker_id=worker_id, worker_host=worker_host).set(worker.get('coresused', 0))
            spark_worker_memory_mb.labels(worker_id=worker_id, worker_host=worker_host).set(worker.get('memory', 0))
            spark_worker_memory_used_mb.labels(worker_id=worker_id, worker_host=worker_host).set(worker.get('memoryused', 0))
            spark_worker_state.labels(worker_id=worker_id, worker_host=worker_host).set(worker_state)
        
        # Fetch and parse applications
        running_apps = cluster_info.get('activeapps', [])
        completed_apps = cluster_info.get('completedapps', [])
        
        logger.info(f"Found {len(running_apps)} running apps and {len(completed_apps)} completed apps")
        logger.info(f"Running apps data: {running_apps}")
        logger.info(f"Completed apps data: {completed_apps[:2]}")  # Log first 2 for brevity
        
        spark_applications_running.set(len(running_apps))
        spark_applications_completed.set(len(completed_apps))
        spark_applications_waiting.set(0)  # Not available in REST API
        
        # Update detailed metrics for running applications
        for app in running_apps:
            app_id = app.get('id', 'unknown')
            app_name = app.get('name', 'unknown')
            cores = app.get('cores', 0)
            memory_per_node = app.get('memoryperslave', 0)
            duration = app.get('duration', 0)
            start_time = app.get('starttime', 0) / 1000  # Convert to seconds
            
            logger.info(f"Running app: {app_name} (ID: {app_id}), Cores: {cores}, Memory: {memory_per_node}MB, Duration: {duration}ms")
            spark_app_cores.labels(app_id=app_id, app_name=app_name, state='RUNNING').set(cores)
            spark_app_memory_mb.labels(app_id=app_id, app_name=app_name, state='RUNNING').set(memory_per_node)
            spark_app_duration_ms.labels(app_id=app_id, app_name=app_name, state='RUNNING').set(duration)
            spark_app_start_time.labels(app_id=app_id, app_name=app_name, state='RUNNING').set(start_time)
            spark_app_state.labels(app_id=app_id, app_name=app_name).set(1)  # 1 = RUNNING
        
        # Update detailed metrics for completed applications
        for app in completed_apps:
            app_id = app.get('id', 'unknown')
            app_name = app.get('name', 'unknown')
            cores = app.get('cores', 0)
            memory_per_node = app.get('memoryperslave', 0)
            duration = app.get('duration', 0)
            start_time = app.get('starttime', 0) / 1000  # Convert to seconds
            
            logger.info(f"Completed app: {app_name} (ID: {app_id}), Cores: {cores}, Memory: {memory_per_node}MB, Duration: {duration}ms")
            spark_app_cores.labels(app_id=app_id, app_name=app_name, state='FINISHED').set(cores)
            spark_app_memory_mb.labels(app_id=app_id, app_name=app_name, state='FINISHED').set(memory_per_node)
            spark_app_duration_ms.labels(app_id=app_id, app_name=app_name, state='FINISHED').set(duration)
            spark_app_start_time.labels(app_id=app_id, app_name=app_name, state='FINISHED').set(start_time)
            spark_app_state.labels(app_id=app_id, app_name=app_name).set(2)  # 2 = FINISHED
        
        # Mark exporter as healthy
        spark_exporter_up.set(1)
        
        # Record scrape duration
        duration = time.time() - start_time
        spark_exporter_scrape_duration_seconds.set(duration)
        
        logger.info(f"Metrics updated successfully in {duration:.2f}s - Workers: {len(alive_workers)}/{len(workers)}, "
                   f"Cores: {cores_used}/{cores_total}, Memory: {memory_used}/{memory_total}MB, "
                   f"Apps: {len(running_apps)} running, {len(completed_apps)} completed")
        
    except Exception as e:
        logger.error(f"Failed to update metrics: {e}")
        spark_exporter_up.set(0)


@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint"""
    update_metrics()
    return Response(generate_latest(registry), mimetype='text/plain')


@app.route('/health')
def health():
    """Health check endpoint"""
    return {'status': 'healthy', 'spark_master_url': SPARK_MASTER_URL}, 200


if __name__ == '__main__':
    logger.info(f"Starting Spark Exporter - Monitoring: {SPARK_MASTER_URL}")
    app.run(host='0.0.0.0', port=9091)

import time
import requests
from prometheus_client import start_http_server, Gauge
 
# ================= CONFIG =================
MARQUEZ_URL = "http://marquez:5000/api/v1"
SCRAPE_INTERVAL = 30
 
# ================= METRICS =================
 
# Inventory
datasets_total = Gauge(
    "marquez_datasets_total",
    "Total number of datasets",
    ["namespace"]
)
 
jobs_total = Gauge(
    "marquez_jobs_total",
    "Total number of jobs",
    ["namespace"]
)
 
namespaces_total = Gauge(
    "marquez_namespaces_total",
    "Total number of namespaces"
)
 
# Activity
runs_total = Gauge(
    "marquez_runs_total",
    "Total runs",
    ["namespace"]
)
 
runs_running = Gauge(
    "marquez_runs_running",
    "Running runs",
    ["namespace"]
)
 
runs_failed = Gauge(
    "marquez_runs_failed_total",
    "Failed runs",
    ["namespace"]
)
 
runs_completed = Gauge(
    "marquez_runs_completed_total",
    "Completed runs",
    ["namespace"]
)
 
# Freshness
dataset_last_update = Gauge(
    "marquez_dataset_last_updated_timestamp",
    "Last update timestamp of dataset",
    ["namespace", "dataset"]
)
 
# ================= FUNCTIONS =================
 
def safe_request(url):
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"Error {r.status_code} for {url}")
            return None
    except Exception as e:
        print(f"Request failed: {e}")
        return None
 
 
def collect_namespaces():
    url = f"{MARQUEZ_URL}/namespaces"
    data = safe_request(url)
    if not data:
        return []
 
    namespaces = data.get("namespaces", [])
    namespaces_total.set(len(namespaces))
 
    return [ns["name"] for ns in namespaces]
 
 
def collect_datasets(namespace):
    url = f"{MARQUEZ_URL}/namespaces/{namespace}/datasets"
    data = safe_request(url)
    if not data:
        datasets_total.labels(namespace=namespace).set(0)
        return []
 
    datasets = data.get("datasets", [])
    datasets_total.labels(namespace=namespace).set(len(datasets))
 
    return datasets
 
 
def collect_jobs(namespace):
    url = f"{MARQUEZ_URL}/namespaces/{namespace}/jobs"
    data = safe_request(url)
    if not data:
        jobs_total.labels(namespace=namespace).set(0)
        return []
 
    jobs = data.get("jobs", [])
    jobs_total.labels(namespace=namespace).set(len(jobs))
 
    return jobs
 
def collect_runs(namespace, jobs):
    total = running = failed = completed = 0
 
    # On itère sur chaque job du namespace
    for job in jobs:
        job_name = job.get("name")
        if not job_name:
            continue

        offset = 0
        limit = 100
 
        while True:
            # L'URL correcte pour l'API Marquez
            url = f"{MARQUEZ_URL}/namespaces/{namespace}/jobs/{job_name}/runs?limit={limit}&offset={offset}"
            data = safe_request(url)
            
            if not data or "runs" not in data:
                break
 
            runs = data["runs"]
            if not runs:
                break
 
            for r in runs:
                state = r.get("state", "").upper()
                total += 1
                if state == "RUNNING":
                    running += 1
                elif state == "FAILED":
                    failed += 1
                elif state == "COMPLETED":
                    completed += 1
 
            if len(runs) < limit:
                break
            offset += limit
 
    # On met à jour les métriques Prometheus une fois tous les jobs analysés
    runs_total.labels(namespace=namespace).set(total)
    runs_running.labels(namespace=namespace).set(running)
    runs_failed.labels(namespace=namespace).set(failed)
    runs_completed.labels(namespace=namespace).set(completed)
 
def collect_freshness(namespace, datasets):
    now = time.time()
 
    for ds in datasets:
        name = ds.get("name")
 
        # last update via facets if exists
        last_update = ds.get("lastModifiedAt")
 
        if last_update:
            timestamp = last_update / 1000  # ms -> sec
            dataset_last_update.labels(
                namespace=namespace,
                dataset=name
            ).set(timestamp)
 
 
# ================= MAIN LOOP =================
 
def collect():
    namespaces = collect_namespaces()
 
    for ns in namespaces:
        datasets = collect_datasets(ns)
        jobs = collect_jobs(ns)             # Stockez les jobs ici
        collect_runs(ns, jobs)              # Passez les jobs à collect_runs
        collect_freshness(ns, datasets)
 
 
if __name__ == "__main__":
    print("Starting Marquez Exporter on :8000")
    start_http_server(8000)
 
    while True:
        collect()
        time.sleep(SCRAPE_INTERVAL)
 
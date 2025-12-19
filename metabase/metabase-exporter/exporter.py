#!/usr/bin/env python3
"""
Metabase Prometheus Exporter
Expose Metabase metrics for Prometheus monitoring
"""

import os
import time
import logging
import requests
import psutil
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Info, Counter
from requests.exceptions import RequestException

# =========================
# Configuration
# =========================
METABASE_URL = os.getenv('METABASE_URL', 'http://metabase:3000')
METABASE_USERNAME = os.getenv('METABASE_USERNAME', 'admin@example.com')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin')
EXPORTER_PORT = int(os.getenv('EXPORTER_PORT', '9101'))
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '30'))

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("metabase_exporter")

# =========================
# Prometheus metrics
# =========================
metabase_up = Gauge('metabase_up', 'Metabase is up and running')
metabase_info = Info('metabase_info', 'Metabase version and build information')

metabase_database_count = Gauge('metabase_database_count', 'Number of configured databases')

metabase_user_count = Gauge('metabase_user_count', 'Total number of users')
metabase_active_users = Gauge('metabase_active_users', 'Active users in last 30 days')

# Re-added dashboard count metric
metabase_dashboard_count = Gauge(
    'metabase_dashboard_count',
    'Total number of dashboards',
)

metabase_question_count = Gauge(
    'metabase_question_count',
    'Total number of questions/cards',
    ['database_name']
)

metabase_failed_cards = Gauge(
    'metabase_failed_cards',
    'Number of cards with SQL errors',
    ['database_name']
)

metabase_scrape_duration = Gauge(
    'metabase_scrape_duration_seconds',
    'Time taken to scrape metrics'
)


metabase_scrape_errors = Counter(
    'metabase_scrape_errors_total',
    'Total number of scrape errors'
)

# =========================
# Process resource metrics
# =========================
metabase_cpu_percent = Gauge('metabase_cpu_percent', 'CPU usage percent of Metabase process')
metabase_memory_mb = Gauge('metabase_memory_mb', 'Memory usage (MB) of Metabase process')

# =========================
# SQL / Application errors
# =========================
metabase_application_errors = Counter(
    'metabase_application_errors_total',
    'Total number of Metabase application errors'
)

# ⚠️ EVENEMENT UNIQUE (1 seule fois)
metabase_last_sql_error = Gauge(
    'metabase_last_sql_error',
    'Unique SQL error detected',
    ['card_id', 'database_name', 'error']
)

# 🔢 COMPTEUR (combien de fois)
metabase_sql_error_total = Counter(
    'metabase_sql_error_total',
    'Total number of SQL errors',
    ['database_name', 'card_id']
)

# =========================
# Database health metrics
# =========================
metabase_database_status = Gauge(
    'metabase_database_status',
    'Database connection status (1=up, 0=down)',
    ['database_name', 'database_id', 'engine']
)

metabase_database_latency_ms = Gauge(
    'metabase_database_latency_ms',
    'Database latency in milliseconds',
    ['database_name', 'database_id']
)

metabase_database_missing = Gauge(
    'metabase_database_missing',
    'Database configured but missing/unreachable',
    ['database_name', 'database_id', 'engine']
)

# =========================
# 🔒 Anti-duplication cache
# =========================
SEEN_SQL_ERRORS = set()


class MetabaseExporter:
    def __init__(self):
        self.session = requests.Session()
        self.session_token = None
        self.last_login = 0
        self.login_ttl = 3600

    # =========================
    # Authentication
    # =========================
    def login(self):
        try:
            r = self.session.post(
                f"{METABASE_URL}/api/session",
                json={
                    "username": METABASE_USERNAME,
                    "password": METABASE_PASSWORD
                },
                timeout=10
            )
            r.raise_for_status()
            self.session_token = r.json().get("id")
            self.session.headers.update({'X-Metabase-Session': self.session_token})
            self.last_login = time.time()
            logger.info("Logged in to Metabase")
            return True
        except RequestException as e:
            logger.error(f"Login failed: {e}")
            metabase_up.set(0)
            return False

    def ensure_authenticated(self):
        if not self.session_token or (time.time() - self.last_login) > self.login_ttl:
            return self.login()
        return True

    def get(self, endpoint):
        if not self.ensure_authenticated():
            return None
        try:
            r = self.session.get(f"{METABASE_URL}/api{endpoint}", timeout=15)
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            logger.error(f"GET {endpoint} failed: {e}")
            metabase_application_errors.inc()
            return None

    # =========================
    # Collectors
    # =========================
    def collect_health_metrics(self):
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            metabase_up.set(1 if r.status_code == 200 else 0)
            return r.status_code == 200
        except RequestException:
            metabase_up.set(0)
            return False

    def collect_version_info(self):
        data = self.get('/session/properties')
        if data:
            v = data.get('version', {})
            metabase_info.info({
                'version': v.get('tag', 'unknown'),
                'build_date': v.get('date', 'unknown')
            })

    def collect_user_metrics(self):
        users = self.get('/user')
        if not isinstance(users, list):
            return

        metabase_user_count.set(len(users))

        active = 0
        now = datetime.now()
        for u in users:
            last = u.get('last_login')
            if last:
                try:
                    d = datetime.fromisoformat(last.replace('Z', '+00:00'))
                    if (now - d.replace(tzinfo=None)).days <= 30:
                        active += 1
                except Exception:
                    pass

        metabase_active_users.set(active)

    def collect_content_metrics(self):
        dbs = self.get('/database')
        db_id_to_name = {}

        if isinstance(dbs, dict) and 'data' in dbs:
            for db in dbs['data']:
                db_id_to_name[str(db['id'])] = db.get('name', 'unknown')

        # Collect dashboard count
        dashboards = self.get('/dashboard')
        if isinstance(dashboards, list):
            metabase_dashboard_count.set(len(dashboards))
        else:
            metabase_dashboard_count.set(0)

        cards = self.get('/card')
        if not isinstance(cards, list):
            return

        questions_per_db = {}
        failed_cards_per_db = {}

        for card in cards:
            card_id = card.get('id')
            db_id = str(card.get('database_id'))
            db_name = db_id_to_name.get(db_id, 'unknown')

            questions_per_db[db_name] = questions_per_db.get(db_name, 0) + 1
            failed_cards_per_db.setdefault(db_name, 0)

            if not card_id:
                continue

            try:
                result = self.session.post(
                    f"{METABASE_URL}/api/card/{card_id}/query/json",
                    timeout=20
                ).json()

                if isinstance(result, dict) and result.get("error"):
                    error_msg = result.get("error")[:200]

                    error_key = (str(card_id), db_name, error_msg)

                    failed_cards_per_db[db_name] += 1
                    metabase_application_errors.inc()

                    # 🔢 compteur
                    metabase_sql_error_total.labels(
                        database_name=db_name,
                        card_id=str(card_id)
                    ).inc()

                    # 🔔 événement UNIQUE
                    if error_key not in SEEN_SQL_ERRORS:
                        logger.warning(
                            f"NEW SQL ERROR | card={card_id} | db={db_name} | {error_msg}"
                        )

                        metabase_last_sql_error.labels(
                            card_id=str(card_id),
                            database_name=db_name,
                            error=error_msg
                        ).set(1)

                        SEEN_SQL_ERRORS.add(error_key)

            except Exception as e:
                logger.error(f"Card {card_id} execution failed: {e}")
                failed_cards_per_db[db_name] += 1
                metabase_application_errors.inc()

        for db, c in questions_per_db.items():
            metabase_question_count.labels(database_name=db).set(c)

        for db, c in failed_cards_per_db.items():
            metabase_failed_cards.labels(database_name=db).set(c)

    def collect_database_metrics(self):
        databases = self.get('/database')
        if not databases:
            return

        if isinstance(databases, dict) and 'data' in databases:
            databases = databases['data']

        metabase_database_count.set(len(databases))

        for db in databases:
            db_id = str(db.get('id'))
            db_name = db.get('name', 'unknown')
            engine = db.get('engine', 'unknown')

            start = time.time()
            ok = False

            try:
                r = self.session.get(
                    f"{METABASE_URL}/api/database/{db_id}",
                    timeout=15
                )
                ok = r.status_code == 200
            except Exception:
                ok = False

            latency = (time.time() - start) * 1000
            metabase_database_latency_ms.labels(
                database_name=db_name,
                database_id=db_id
            ).set(latency)

            metabase_database_status.labels(
                database_name=db_name,
                database_id=db_id,
                engine=engine
            ).set(1 if ok else 0)

    def collect_missing_database_metrics(self):
        databases = self.get('/database')
        if not databases:
            return

        if isinstance(databases, dict) and 'data' in databases:
            databases = databases['data']

        for db in databases:
            db_id = str(db.get('id'))
            db_name = db.get('name', 'unknown')
            engine = db.get('engine', 'unknown')

            missing = 0
            try:
                r = self.session.get(
                    f"{METABASE_URL}/api/database/{db_id}",
                    timeout=15
                )
                if r.status_code != 200:
                    missing = 1
            except Exception:
                missing = 1

            metabase_database_missing.labels(
                database_name=db_name,
                database_id=db_id,
                engine=engine
            ).set(missing)

    def collect_process_metrics(self):
        # Get Metabase process details
        try:
            for proc in psutil.process_iter(['pid', 'name', 'exe', 'cmdline']):
                if 'java' in proc.info['name']:
                    # Assuming Metabase runs on Java
                    try:
                        # Get CPU and Memory usage
                        cpu_usage = proc.cpu_percent(interval=1)
                        memory_usage = proc.memory_info().rss / (1024 * 1024)  # Convert to MB

                        metabase_cpu_percent.set(cpu_usage)
                        metabase_memory_mb.set(memory_usage)
                    except Exception as e:
                        logger.warning(f"Error collecting process metrics: {e}")
        except Exception as e:
            logger.warning(f"Error iterating processes: {e}")

    def collect_all_metrics(self):
        start = time.time()
        try:
            if not self.collect_health_metrics():
                return
            self.collect_version_info()
            self.collect_user_metrics()
            self.collect_content_metrics()
            self.collect_database_metrics()
            self.collect_missing_database_metrics()
            self.collect_process_metrics()
            metabase_scrape_duration.set(time.time() - start)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            metabase_scrape_errors.inc()
            metabase_application_errors.inc()


def main():
    logger.info(f"Starting Metabase exporter on port {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)
    exporter = MetabaseExporter()

    while True:
        exporter.collect_all_metrics()
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()

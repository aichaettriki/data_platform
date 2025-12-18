#!/usr/bin/env python3
"""
Metabase Prometheus Exporter
Expose Metabase metrics for Prometheus monitoring
"""

import os
import time
import logging
import requests
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Info, Counter
from requests.exceptions import RequestException

# Configuration
METABASE_URL = os.getenv('METABASE_URL', 'http://metabase:3000')
METABASE_USERNAME = os.getenv('METABASE_USERNAME', 'admin@example.com')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin')
EXPORTER_PORT = int(os.getenv('EXPORTER_PORT', '9101'))
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '30'))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
metabase_up = Gauge('metabase_up', 'Metabase is up and running')
metabase_info = Info('metabase_info', 'Metabase version and build information')
metabase_database_count = Gauge('metabase_database_count', 'Number of configured databases')
metabase_user_count = Gauge('metabase_user_count', 'Total number of users')
metabase_active_users = Gauge('metabase_active_users', 'Number of active users in last 30 days')
metabase_question_count = Gauge('metabase_question_count', 'Total number of questions/cards', ['database_name'])
metabase_dashboard_count = Gauge('metabase_dashboard_count', 'Total number of dashboards', ['database_name'])
metabase_collection_count = Gauge('metabase_collection_count', 'Total number of collections', ['database_name'])
metabase_failed_cards = Gauge('metabase_failed_cards', 'Current number of cards with SQL errors', ['database_name'])
metabase_scrape_duration = Gauge('metabase_scrape_duration_seconds', 'Time taken to scrape metrics')
metabase_scrape_errors = Counter('metabase_scrape_errors_total', 'Total number of scrape errors')

# Global SQL / application errors
metabase_application_errors = Counter(
    'metabase_application_errors_total',
    'Total number of Metabase SQL / application errors'
)
# Expose last SQL error as a Gauge metric with labels
metabase_last_sql_error = Gauge(
    'metabase_last_sql_error',
    'Last SQL error detected in a card',
    ['card_id', 'database_name', 'error']
)
# Counter for SQL errors per card/database
metabase_sql_error_counter = Counter(
    'metabase_sql_error_total',
    'Total number of SQL errors per card and database',
    ['database_name', 'card_id']
)
# Database health metrics (CORRECT)
metabase_database_status = Gauge(
    'metabase_database_status',
    'Database connection status as seen by Metabase (1=up, 0=down)',
    ['database_name', 'database_id', 'engine']
)

metabase_database_latency_ms = Gauge(
    'metabase_database_latency_ms',
    'Database connection latency in milliseconds measured by Metabase',
    ['database_name', 'database_id']
)

metabase_database_missing = Gauge(
    "metabase_database_missing",
    "Database configured in Metabase but not reachable or not existing",
    ["database_name", "database_id", "engine"]
)


class MetabaseExporter:
    def __init__(self):
        self.session = requests.Session()
        self.session_token = None
        self.last_login = 0
        self.login_ttl = 3600

    def login(self):
        try:
            logger.info(f"Attempting to login to Metabase at {METABASE_URL}")
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
            logger.info("Successfully logged in to Metabase")
            return True
        except RequestException as e:
            logger.error(f"Failed to login: {e}")
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
            r = self.session.get(f"{METABASE_URL}/api{endpoint}", timeout=10)
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            logger.error(f"Failed to fetch {endpoint}: {e}")
            metabase_application_errors.inc()
            return None

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
        if isinstance(users, list):
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
                    except:
                        pass
            metabase_active_users.set(active)

    def collect_content_metrics(self):
        dbs = self.get('/database')
        db_id_to_name = {}
        if isinstance(dbs, dict) and 'data' in dbs:
            for db in dbs['data']:
                db_id_to_name[str(db['id'])] = db.get('name', 'unknown')

        cards = self.get('/card')

        questions_per_db = {}
        failed_cards_per_db = {}

        if isinstance(cards, list):
            for card in cards:
                db_id = str(card.get('database_id', 'unknown'))
                db_name = db_id_to_name.get(db_id, 'unknown')
                questions_per_db.setdefault(db_name, 0)
                failed_cards_per_db.setdefault(db_name, 0)
                questions_per_db[db_name] += 1

                card_id = card.get('id')
                if not card_id:
                    continue

                try:
                    result = self.session.post(
                        f"{METABASE_URL}/api/card/{card_id}/query/json",
                        timeout=20
                    ).json()

                    if isinstance(result, dict) and result.get("error"):
                        error_msg = result.get('error')
                        logger.warning(f"SQL error detected in card {card_id} (DB: {db_name}): {error_msg}")
                        failed_cards_per_db[db_name] += 1
                        metabase_application_errors.inc()
                        # Set last SQL error as Gauge metric (value always 1)
                        metabase_last_sql_error.labels(
                            card_id=str(card_id),
                            database_name=db_name,
                            error=error_msg[:200]  # Truncate if too long
                        ).set(1)
                        # Increment SQL error counter per card/database
                        metabase_sql_error_counter.labels(database_name=db_name, card_id=str(card_id)).inc()

                except Exception as e:
                    logger.error(f"Exception during SQL execution in card {card_id} (DB: {db_name}): {e}")
                    failed_cards_per_db[db_name] += 1
                    metabase_application_errors.inc()

            for db, c in questions_per_db.items():
                metabase_question_count.labels(database_name=db).set(c)
            for db, c in failed_cards_per_db.items():
                metabase_failed_cards.labels(database_name=db).set(c)

            logger.info(f"Questions/Cards by DB: {questions_per_db}")

        dashboards = self.get('/dashboard')
        if isinstance(dashboards, list):
            counts = {}
            for d in dashboards:
                counts.setdefault('unknown', 0)
                counts['unknown'] += 1
            for db, c in counts.items():
                metabase_dashboard_count.labels(database_name=db).set(c)
            logger.info(f"Dashboards by DB: {counts}")

        collections = self.get('/collection')
        if isinstance(collections, list):
            counts = {}
            for c in collections:
                counts.setdefault('unknown', 0)
                counts['unknown'] += 1
            for db, c in counts.items():
                metabase_collection_count.labels(database_name=db).set(c)
            logger.info(f"Collections by DB: {counts}")



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
            db_engine = db.get('engine', 'unknown')

            start = time.time()
            db_ok = False



            try:
                response = self.session.get(
                    f"{METABASE_URL}/api/database/{db_id}",
                    timeout=20
                )

                latency_ms = (time.time() - start) * 1000
                metabase_database_latency_ms.labels(
                    database_name=db_name,
                    database_id=db_id
                ).set(latency_ms)

                if response.status_code == 200:
                    db_ok = True
                else:
                    db_ok = False

            except Exception as e:
                logger.error(f"Database GET failed for {db_name}: {e}")
                db_ok = False

            metabase_database_status.labels(
                database_name=db_name,
                database_id=db_id,
                engine=db_engine
            ).set(1 if db_ok else 0)


    def collect_missing_database_metrics(self):
        databases = self.get('/database')
        if not databases:
            return

        if isinstance(databases, dict) and 'data' in databases:
            databases = databases['data']

        for db in databases:
            db_id = str(db.get('id'))
            db_name = db.get('name', 'unknown')
            db_engine = db.get('engine', 'unknown')

            missing = 0

            try:
                response = self.session.get(
                    f"{METABASE_URL}/api/database/{db_id}",
                    timeout=15
                )

                logger.info(f"DB MISSING GET {db_name} (id={db_id}): HTTP {response.status_code}")

                if response.status_code != 200:
                    missing = 1

            except Exception as e:
                logger.error(f"Database MISSING GET failed for {db_name}: {e}")
                missing = 1

            metabase_database_missing.labels(
                database_name=db_name,
                database_id=db_id,
                engine=db_engine
            ).set(missing)


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

            metabase_scrape_duration.set(time.time() - start)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            metabase_scrape_errors.inc()
            metabase_application_errors.inc()

def main():
    logger.info(f"Starting exporter on port {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)
    exporter = MetabaseExporter()
    while True:
        exporter.collect_all_metrics()
        time.sleep(SCRAPE_INTERVAL)

if __name__ == "__main__":
    main()

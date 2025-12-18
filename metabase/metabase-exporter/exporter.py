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

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Prometheus metrics (essentielles)
metabase_up = Gauge('metabase_up', 'Metabase is up and running')
metabase_info = Info('metabase_info', 'Metabase version and build information')
metabase_database_count = Gauge('metabase_database_count', 'Number of configured databases')
metabase_user_count = Gauge('metabase_user_count', 'Total number of users')
metabase_active_users = Gauge('metabase_active_users', 'Number of active users in last 30 days')
metabase_question_count = Gauge('metabase_question_count', 'Total number of questions/cards', ['database_name'])
metabase_dashboard_count = Gauge('metabase_dashboard_count', 'Total number of dashboards', ['database_name'])
metabase_collection_count = Gauge('metabase_collection_count', 'Total number of collections', ['database_name'])
metabase_failed_cards = Gauge('metabase_failed_cards', 'Current number of dashboard cards with errors', ['database_name'])
metabase_scrape_duration = Gauge('metabase_scrape_duration_seconds', 'Time taken to scrape metrics')
metabase_scrape_errors = Counter('metabase_scrape_errors_total', 'Total number of scrape errors')

# Authentication/Security metrics
metabase_login_failures = Gauge('metabase_login_failures_total', 'Total number of failed login attempts', ['username'])
metabase_suspicious_logins = Gauge('metabase_suspicious_logins_total', 'Number of suspicious login attempts')
metabase_active_sessions = Gauge('metabase_active_sessions', 'Number of active user sessions')

# Data source health metrics
metabase_database_status = Gauge('metabase_database_status', 'Database connection status (1=healthy, 0=down)', ['database_name', 'database_id', 'engine'])
metabase_database_latency_ms = Gauge('metabase_database_latency_ms', 'Database connection latency in milliseconds', ['database_name', 'database_id'])
metabase_database_errors = Counter('metabase_database_errors_total', 'Total database connection errors', ['database_name', 'database_id'])
metabase_database_timeouts = Counter('metabase_database_timeouts_total', 'Total database connection timeouts', ['database_name', 'database_id'])


class MetabaseExporter:
    def __init__(self):
        self.session = requests.Session()
        self.session_token = None
        self.last_login = 0
        self.login_ttl = 3600  # Re-login every hour
        
    def login(self):
        """Authenticate with Metabase and get session token"""
        try:
            logger.info(f"Attempting to login to Metabase at {METABASE_URL}")
            response = self.session.post(
                f"{METABASE_URL}/api/session",
                json={
                    "username": METABASE_USERNAME,
                    "password": METABASE_PASSWORD
                },
                timeout=10
            )
            response.raise_for_status()
            self.session_token = response.json().get('id')
            self.session.headers.update({'X-Metabase-Session': self.session_token})
            self.last_login = time.time()
            logger.info("Successfully logged in to Metabase")
            return True
        except RequestException as e:
            logger.error(f"Failed to login to Metabase: {e}")
            metabase_up.set(0)
            return False
    
    def ensure_authenticated(self):
        """Ensure we have a valid session token"""
        if not self.session_token or (time.time() - self.last_login) > self.login_ttl:
            return self.login()
        return True
    
    def get(self, endpoint):
        """Make authenticated GET request to Metabase API"""
        if not self.ensure_authenticated():
            return None
        
        try:
            response = self.session.get(
                f"{METABASE_URL}/api{endpoint}",
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            logger.error(f"Failed to fetch {endpoint}: {e}")
            return None
    
    def collect_health_metrics(self):
        """Check if Metabase is healthy"""
        try:
            response = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if response.status_code == 200:
                metabase_up.set(1)
                logger.debug("Metabase health check: OK")
                return True
            else:
                metabase_up.set(0)
                logger.warning(f"Metabase health check failed: {response.status_code}")
                return False
        except RequestException as e:
            metabase_up.set(0)
            logger.error(f"Failed to check Metabase health: {e}")
            return False
    
    def collect_version_info(self):
        """Collect Metabase version information"""
        data = self.get('/session/properties')
        if data:
            version_info = data.get('version', {})
            metabase_info.info({
                'version': version_info.get('tag', 'unknown'),
                'build_date': version_info.get('date', 'unknown')
            })
            logger.debug(f"Metabase version: {version_info.get('tag', 'unknown')}")
    
    def collect_database_metrics(self):
        """Collect database connection metrics, health status, and latency"""
        data = self.get('/database')
        if data:
            if isinstance(data, dict) and 'data' in data:
                databases = data['data']
            elif isinstance(data, list):
                databases = data
            else:
                databases = []
            metabase_database_count.set(len(databases))
            logger.debug(f"Databases configured: {len(databases)}")

            # Check health and latency for each database
            for db in databases:
                db_id = str(db.get('id', 'unknown'))
                db_name = db.get('name', 'unknown')
                db_engine = db.get('engine', 'unknown')
                latency_ms = None
                try:
                    start = time.time()
                    resp = self.get(f'/database/{db_id}')
                    elapsed = (time.time() - start) * 1000  # ms
                    status = 1 if resp else 0
                    latency_ms = elapsed
                except Exception:
                    status = 0
                    latency_ms = None
                metabase_database_status.labels(database_name=db_name, database_id=db_id, engine=db_engine).set(status)
                if latency_ms is not None:
                    metabase_database_latency_ms.labels(database_name=db_name, database_id=db_id).set(latency_ms)
    
    def collect_user_metrics(self):
        """Collect user statistics"""
        data = self.get('/user')
        if data and isinstance(data, list):
            metabase_user_count.set(len(data))
            
            # Count active users (logged in within 30 days)
            active_count = 0
            now = datetime.now()
            for user in data:
                last_login = user.get('last_login')
                if last_login:
                    try:
                        # Metabase returns ISO format datetime
                        login_date = datetime.fromisoformat(last_login.replace('Z', '+00:00'))
                        days_since_login = (now - login_date.replace(tzinfo=None)).days
                        if days_since_login <= 30:
                            active_count += 1
                    except:
                        pass
            
            metabase_active_users.set(active_count)
            logger.debug(f"Total users: {len(data)}, Active users (30d): {active_count}")
    
    def collect_content_metrics(self):
        """Collect metrics about questions, dashboards, and collections, and count failed cards by executing their queries, all by database_name"""
        # Get all databases to map id -> name
        db_data = self.get('/database')
        db_id_to_name = {}
        if db_data:
            if isinstance(db_data, dict) and 'data' in db_data:
                dbs = db_data['data']
            elif isinstance(db_data, list):
                dbs = db_data
            else:
                dbs = []
            for db in dbs:
                db_id = str(db.get('id', 'unknown'))
                db_name = db.get('name', 'unknown')
                db_id_to_name[db_id] = db_name

        # Questions/Cards
        cards = self.get('/card')
        questions_per_db = {}
        failed_cards_per_db = {}
        if cards and isinstance(cards, list):
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
                    query_result = self.get(f'/card/{card_id}/query')
                    if isinstance(query_result, dict):
                        if (
                            query_result.get('error')
                            or query_result.get('status') == 'error'
                            or (query_result.get('message') and isinstance(query_result.get('message'), str))
                        ):
                            failed_cards_per_db[db_name] += 1
                except Exception as e:
                    logger.warning(f"Erreur lors de l'exécution de la card {card_id}: {e}")
                    failed_cards_per_db[db_name] += 1
            for db_name, count in questions_per_db.items():
                metabase_question_count.labels(database_name=db_name).set(count)
            for db_name, count in failed_cards_per_db.items():
                metabase_failed_cards.labels(database_name=db_name).set(count)
            logger.info(f"Questions/Cards by DB: {questions_per_db}")
        else:
            logger.info("No cards/questions found.")

        # Dashboards
        dashboards = self.get('/dashboard')
        dashboards_per_db = {}
        if dashboards and isinstance(dashboards, list):
            for dash in dashboards:
                db_id = str(dash.get('database_id', 'unknown'))
                db_name = db_id_to_name.get(db_id, 'unknown')
                dashboards_per_db.setdefault(db_name, 0)
                dashboards_per_db[db_name] += 1
            for db_name, count in dashboards_per_db.items():
                metabase_dashboard_count.labels(database_name=db_name).set(count)
            logger.info(f"Dashboards by DB: {dashboards_per_db}")

        # Collections
        collections = self.get('/collection')
        collections_per_db = {}
        if collections and isinstance(collections, list):
            for coll in collections:
                db_id = str(coll.get('database_id', 'unknown'))
                db_name = db_id_to_name.get(db_id, 'unknown')
                collections_per_db.setdefault(db_name, 0)
                collections_per_db[db_name] += 1
            for db_name, count in collections_per_db.items():
                metabase_collection_count.labels(database_name=db_name).set(count)
            logger.info(f"Collections by DB: {collections_per_db}")


    
    def collect_all_metrics(self):
        """Collect all Metabase metrics (essentielles)"""
        start_time = time.time()
        try:
            logger.info("Starting metrics collection")
            if not self.collect_health_metrics():
                logger.warning("Metabase is not healthy, skipping detailed metrics")
                return
            self.collect_version_info()
            self.collect_database_metrics()
            self.collect_user_metrics()
            self.collect_content_metrics()
            duration = time.time() - start_time
            metabase_scrape_duration.set(duration)
            logger.info(f"Metrics collection completed in {duration:.2f}s")
        except Exception as e:
            logger.error(f"Error during metrics collection: {e}")
            metabase_scrape_errors.inc()
            duration = time.time() - start_time
            metabase_scrape_duration.set(duration)


def main():
    """Main exporter loop"""
    logger.info(f"Starting Metabase Prometheus Exporter on port {EXPORTER_PORT}")
    logger.info(f"Metabase URL: {METABASE_URL}")
    logger.info(f"Scrape interval: {SCRAPE_INTERVAL}s")
    
    # Start Prometheus HTTP server
    start_http_server(EXPORTER_PORT)
    logger.info(f"Exporter HTTP server started on port {EXPORTER_PORT}")
    
    exporter = MetabaseExporter()
    
    # Main loop
    while True:
        try:
            exporter.collect_all_metrics()
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            metabase_scrape_errors.inc()
        
        time.sleep(SCRAPE_INTERVAL)


if __name__ == '__main__':
    main()

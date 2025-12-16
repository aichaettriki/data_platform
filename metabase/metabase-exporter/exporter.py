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
from prometheus_client import start_http_server, Gauge, Counter, Info
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

# Prometheus metrics
metabase_up = Gauge('metabase_up', 'Metabase is up and running')
metabase_info = Info('metabase_info', 'Metabase version and build information')
metabase_database_count = Gauge('metabase_database_count', 'Number of configured databases')
metabase_user_count = Gauge('metabase_user_count', 'Total number of users')
metabase_active_users = Gauge('metabase_active_users', 'Number of active users in last 30 days')
metabase_question_count = Gauge('metabase_question_count', 'Total number of questions/cards')
metabase_dashboard_count = Gauge('metabase_dashboard_count', 'Total number of dashboards')
metabase_collection_count = Gauge('metabase_collection_count', 'Total number of collections')
metabase_query_executions = Counter('metabase_query_executions_total', 'Total number of query executions')
metabase_query_avg_duration = Gauge('metabase_query_avg_duration_seconds', 'Average query execution time')
metabase_failed_queries = Counter('metabase_failed_queries_total', 'Total number of failed queries')
metabase_failed_cards = Gauge('metabase_failed_cards', 'Current number of dashboard cards with errors')
metabase_cache_hit_rate = Gauge('metabase_cache_hit_rate', 'Query cache hit rate')
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
        """Collect database connection metrics"""
        data = self.get('/database')
        if data:
            # API returns {'data': [...], 'total': N}
            if isinstance(data, dict) and 'data' in data:
                databases = data['data']
                metabase_database_count.set(len(databases))
                logger.debug(f"Databases configured: {len(databases)}")
            elif isinstance(data, list):
                metabase_database_count.set(len(data))
                logger.debug(f"Databases configured: {len(data)}")
    
    def collect_database_health_metrics(self):
        """Collect detailed health metrics for each database"""
        data = self.get('/database')
        if not data:
            return
        
        databases = data.get('data', []) if isinstance(data, dict) else data
        
        for db in databases:
            db_id = str(db.get('id', 'unknown'))
            db_name = db.get('name', 'unknown')
            db_engine = db.get('engine', 'unknown')
            
            # Test database connection and measure latency
            start_time = time.time()
            try:
                # Native query endpoint to test connection
                test_result = self.get(f'/database/{db_id}')
                latency_ms = (time.time() - start_time) * 1000
                
                if test_result:
                    # Database is reachable
                    metabase_database_status.labels(
                        database_name=db_name,
                        database_id=db_id,
                        engine=db_engine
                    ).set(1)
                    
                    metabase_database_latency_ms.labels(
                        database_name=db_name,
                        database_id=db_id
                    ).set(latency_ms)
                    
                    logger.debug(f"Database {db_name} (ID: {db_id}): healthy, latency {latency_ms:.2f}ms")
                else:
                    # Database is down or unreachable
                    metabase_database_status.labels(
                        database_name=db_name,
                        database_id=db_id,
                        engine=db_engine
                    ).set(0)
                    
                    metabase_database_errors.labels(
                        database_name=db_name,
                        database_id=db_id
                    ).inc()
                    metabase_failed_queries.inc()
                    logger.warning(f"Database {db_name} (ID: {db_id}): connection failed")
                    
            except RequestException as e:
                # Connection error or timeout
                metabase_database_status.labels(
                    database_name=db_name,
                    database_id=db_id,
                    engine=db_engine
                ).set(0)
                
                if 'timeout' in str(e).lower():
                    metabase_database_timeouts.labels(
                        database_name=db_name,
                        database_id=db_id
                    ).inc()
                    logger.warning(f"Database {db_name} (ID: {db_id}): timeout")
                else:
                    metabase_database_errors.labels(
                        database_name=db_name,
                        database_id=db_id
                    ).inc()
                    logger.error(f"Database {db_name} (ID: {db_id}): error - {e}")
                metabase_failed_queries.inc()
    
    def collect_auth_security_metrics(self):
        """Collect authentication and security metrics"""
        # Get login history from activity log
        try:
            # Fetch recent audit log events
            audit_log = self.get('/audit')
            
            if audit_log and isinstance(audit_log, list):
                login_failures = {}
                suspicious_count = 0
                
                for event in audit_log:
                    model = event.get('model', '')
                    details = event.get('details', {})
                    
                    # Track failed login attempts
                    if model == 'User' and details.get('success') == False:
                        username = details.get('username', 'unknown')
                        login_failures[username] = login_failures.get(username, 0) + 1
                    
                    # Detect suspicious patterns (multiple failures from same IP, etc.)
                    if details.get('failure_count', 0) > 5:
                        suspicious_count += 1
                
                # Update metrics
                for username, count in login_failures.items():
                    metabase_login_failures.labels(username=username).set(count)
                
                metabase_suspicious_logins.set(suspicious_count)
                logger.debug(f"Login failures tracked: {len(login_failures)} users, {suspicious_count} suspicious")
                
        except Exception as e:
            logger.debug(f"Could not fetch audit log (may require enterprise): {e}")
        
        # Get active sessions count
        try:
            sessions = self.get('/session')
            if sessions and isinstance(sessions, list):
                metabase_active_sessions.set(len(sessions))
                logger.debug(f"Active sessions: {len(sessions)}")
        except Exception as e:
            logger.debug(f"Could not fetch session data: {e}")
    
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
        """Collect metrics about questions, dashboards, and collections"""
        # Questions/Cards
        cards = self.get('/card')
        failed_cards_count = 0
        if cards and isinstance(cards, list):
            metabase_question_count.set(len(cards))
            logger.info(f"Questions/Cards: {len(cards)}")
            # (Suppression de la vérification individuelle des cards/questions échouées ici)
            logger.info(f"Cards/Questions échouées: {failed_cards_count}")
        else:
            logger.info("Aucune card/question trouvée.")
        # Vous pouvez exposer ce nombre via Prometheus si besoin, par exemple :
        metabase_failed_cards.set(failed_cards_count)
        
        # Dashboards
        dashboards = self.get('/dashboard')
        if dashboards and isinstance(dashboards, list):
            metabase_dashboard_count.set(len(dashboards))
            logger.info(f"Dashboards: {len(dashboards)}")
            # Vérifier les cards échouées dans chaque dashboard via query_metadata
            for dashboard in dashboards:
                dashboard_id = dashboard.get('id')
                if not dashboard_id:
                    continue
                try:
                    metadata = self.get(f'/dashboard/{dashboard_id}/query_metadata')
                    logger.info(f"Récupéré query_metadata pour dashboard {dashboard_id}: {metadata}")

                    metadata1 = self.get(f'/dashboard/{dashboard_id}/items')
                    logger.info(f"Récupéré iteeeeeeeeeeeeeeeeeeeeeeeems pour dashboard {dashboard_id}: {metadata1}")

                    metadata2 = self.get(f'/dashboard/{dashboard_id}/related')
                    logger.info(f"Récupéré relateeeeeeeeeeedddddddddddddddddddddddd pour dashboard {dashboard_id}: {metadata2}")
                    # metadata est une liste de dicts, chaque dict = une card/question du dashboard
                    if metadata and isinstance(metadata, list):
                        for card_meta in metadata:
                            # Si la card a une erreur, on incrémente le compteur
                            if card_meta.get('error') or (card_meta.get('status') == 'error'):
                                failed_cards_count += 1
                except Exception as e:
                    logger.debug(f"Erreur lors de la récupération de query_metadata pour dashboard {dashboard_id}: {e}")
            # Mettre à jour la métrique après le passage sur les dashboards
            metabase_failed_cards.set(failed_cards_count)
        
        # Collections
        collections = self.get('/collection')
        if collections and isinstance(collections, list):
            metabase_collection_count.set(len(collections))
            logger.info(f"Collections: {len(collections)}")
    
    def collect_query_metrics(self):
        """Collect query execution metrics from activity logs"""
        # Note: This endpoint might require admin privileges
        # We'll try to get recent activity
        try:
            data = self.get('/activity/recent_views')
            if data and isinstance(data, list):
                # This is a simplified version - in production you'd want to track more detailed metrics
                logger.debug(f"Recent activity items: {len(data)}")
        except Exception as e:
            logger.debug(f"Could not fetch activity metrics: {e}")

    
    def collect_all_metrics(self):
        """Collect all Metabase metrics"""
        start_time = time.time()
        
        try:
            logger.info("Starting metrics collection")
            # Health check first
            if not self.collect_health_metrics():
                logger.warning("Metabase is not healthy, skipping detailed metrics")
                return
            # Collect all metrics
            self.collect_version_info()
            self.collect_database_metrics()
            self.collect_database_health_metrics()
            self.collect_user_metrics()
            self.collect_content_metrics()
            self.collect_query_metrics()
            duration = time.time() - start_time
            metabase_scrape_duration.set(duration)
            logger.info(f"Metrics collection completed in {duration:.2f}s")
        except Exception as e:
            print(f"[DEBUG] Exception pendant la collecte des métriques : {e}")
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

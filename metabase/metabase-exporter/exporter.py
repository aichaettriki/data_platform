#!/usr/bin/env python3
"""
Metabase Prometheus Exporter
Expose Metabase metrics for Prometheus monitoring
"""

import os
import time
import logging
import requests
import re
from datetime import datetime
import dateutil.parser
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

# --- Prometheus metrics ---

# System Health
metabase_up = Gauge('metabase_up', 'Metabase is up and running')
metabase_info = Info('metabase_info', 'Metabase version and build information')

# Counts
metabase_database_count = Gauge('metabase_database_count', 'Number of configured databases')
metabase_user_count = Gauge('metabase_user_count', 'Total number of users')
metabase_active_users = Gauge('metabase_active_users', 'Number of active users in last 30 days')
metabase_question_count = Gauge('metabase_question_count', 'Total number of questions/cards')
metabase_dashboard_count = Gauge('metabase_dashboard_count', 'Total number of dashboards')
metabase_collection_count = Gauge('metabase_collection_count', 'Total number of collections')

# Query Performance & Errors
metabase_query_executions = Counter('metabase_query_executions_total', 'Total number of query executions detected')
metabase_failed_queries = Counter('metabase_failed_queries_total', 'Total number of failed queries detected from logs', ['reason'])
metabase_scrape_duration = Gauge('metabase_scrape_duration_seconds', 'Time taken to scrape metrics')
metabase_scrape_errors = Counter('metabase_scrape_errors_total', 'Total number of scrape errors')

# Authentication/Security metrics
metabase_login_failures = Gauge('metabase_login_failures_total', 'Total number of failed login attempts', ['username'])
metabase_active_sessions = Gauge('metabase_active_sessions', 'Number of active user sessions')

# Data source health metrics
metabase_database_status = Gauge('metabase_database_status', 'Database connection status (1=healthy, 0=down)', ['database_name', 'database_id', 'engine'])
metabase_database_latency_ms = Gauge('metabase_database_latency_ms', 'Database connection latency in milliseconds', ['database_name', 'database_id'])
metabase_database_errors = Counter('metabase_database_errors_total', 'Total database connection errors', ['database_name', 'database_id'])


class MetabaseExporter:
    def __init__(self):
        self.session = requests.Session()
        self.session_token = None
        self.last_login = 0
        self.login_ttl = 3600  # Re-login every hour
        
        # Track the last timestamp of logs we processed to avoid double counting
        self.last_log_check_time = datetime.now().astimezone()

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
                return True
            else:
                metabase_up.set(0)
                return False
        except RequestException:
            metabase_up.set(0)
            return False

    def collect_version_info(self):
        data = self.get('/session/properties')
        if data:
            version_info = data.get('version', {})
            metabase_info.info({
                'version': version_info.get('tag', 'unknown'),
                'build_date': version_info.get('date', 'unknown')
            })

    def collect_database_metrics(self):
        """Collect database connection health and latency"""
        data = self.get('/database')
        if not data:
            return

        databases = data.get('data', []) if isinstance(data, dict) else data
        metabase_database_count.set(len(databases))
        
        for db in databases:
            db_id = str(db.get('id', 'unknown'))
            db_name = db.get('name', 'unknown')
            db_engine = db.get('engine', 'unknown')
            
            # Skip checking the sample dataset to save time if desired, 
            # though it's good to keep for baseline.
            
            start_time = time.time()
            try:
                # We attempt to fetch metadata for the DB. 
                # This ensures the DB connection is active and valid.
                # /api/database/{id}?include=tables triggers a lighter check than a full sync
                self.get(f'/database/{db_id}?include=tables')
                
                latency_ms = (time.time() - start_time) * 1000
                
                metabase_database_status.labels(db_name, db_id, db_engine).set(1)
                metabase_database_latency_ms.labels(db_name, db_id).set(latency_ms)
                
            except Exception:
                # If the metadata fetch fails, the DB is likely unreachable
                metabase_database_status.labels(db_name, db_id, db_engine).set(0)
                metabase_database_errors.labels(db_name, db_id).inc()
                logger.warning(f"Database down: {db_name}")

    def collect_log_errors(self):
        """
        Parse internal Metabase logs to detect query failures.
        This is the most reliable way to find dashboard errors.
        """
        logs = self.get('/util/logs')
        if not logs or not isinstance(logs, list):
            return

        # Sort logs by time to process correctly
        # Metabase logs usually come as objects with 'timestamp' and 'msg'
        # or sometimes raw strings depending on version. 
        
        current_max_time = self.last_log_check_time

        for log_entry in logs:
            try:
                if isinstance(log_entry, dict):
                    timestamp_str = log_entry.get('timestamp')
                    level = log_entry.get('level', 'INFO')
                    msg = log_entry.get('msg', '')
                else:
                    # Handle raw string logs (older versions)
                    continue

                if not timestamp_str:
                    continue

                # Parse timestamp (ISO format)
                log_time = dateutil.parser.parse(timestamp_str)

                # Only process new logs
                if log_time <= self.last_log_check_time:
                    continue

                # Update max time seen
                if log_time > current_max_time:
                    current_max_time = log_time

                # Check for Query Failures
                if level == 'ERROR':
                    # Heuristics for query failures in Metabase logs
                    if 'Error running query' in msg or 'PSQLException' in msg or 'SQLException' in msg:
                        metabase_failed_queries.labels(reason='sql_error').inc()
                        logger.debug(f"Detected SQL Error in logs: {msg[:100]}")
                    elif 'SocketTimeoutException' in msg or 'timed out' in msg:
                        metabase_failed_queries.labels(reason='timeout').inc()
                    elif 'killed by user' in msg:
                        metabase_failed_queries.labels(reason='cancelled').inc()
            
            except Exception as e:
                logger.debug(f"Error parsing log entry: {e}")

        self.last_log_check_time = current_max_time

    def collect_user_stats(self):
        """Collect user counts and active sessions"""
        users = self.get('/user')
        if users and isinstance(users, list):
            metabase_user_count.set(len(users))
            
            # Active users (last 30 days)
            active_count = 0
            now = datetime.now()
            for user in users:
                last_login = user.get('last_login')
                if last_login:
                    try:
                        # Handle Z for UTC
                        login_date = dateutil.parser.parse(last_login).replace(tzinfo=None)
                        if (now - login_date).days <= 30:
                            active_count += 1
                    except Exception:
                        pass
            metabase_active_users.set(active_count)

        # Active sessions
        try:
            sessions = self.get('/session')
            if sessions and isinstance(sessions, list):
                metabase_active_sessions.set(len(sessions))
        except:
            pass

    def collect_counts(self):
        """Collect high level counts"""
        try:
            for endpoint, metric in [
                ('/card', metabase_question_count),
                ('/dashboard', metabase_dashboard_count),
                ('/collection', metabase_collection_count)
            ]:
                data = self.get(endpoint)
                if isinstance(data, list):
                    metric.set(len(data))
        except:
            pass

    def collect_all_metrics(self):
        """Main collection loop"""
        start_time = time.time()
        
        try:
            if not self.collect_health_metrics():
                return

            self.collect_version_info()
            self.collect_database_metrics()
            self.collect_user_stats()
            self.collect_counts()
            
            # The Critical fix for "Query Failures"
            self.collect_log_errors()
            
            duration = time.time() - start_time
            metabase_scrape_duration.set(duration)
            logger.info(f"Scrape completed in {duration:.2f}s")
            
        except Exception as e:
            logger.error(f"Scrape failed: {e}")
            metabase_scrape_errors.inc()

def main():
    logger.info(f"Starting Metabase Exporter on port {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)
    
    exporter = MetabaseExporter()
    
    while True:
        exporter.collect_all_metrics()
        time.sleep(SCRAPE_INTERVAL)

if __name__ == '__main__':
    main()
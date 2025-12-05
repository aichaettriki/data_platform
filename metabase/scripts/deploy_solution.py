import os
import time
import json
import glob
import requests
from metabase_api import Metabase_API
 
# --- CONFIGURATION ---
MB_URL = "http://metabase:3000"
ADMIN_EMAIL = os.getenv("MB_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASS = os.getenv("MB_ADMIN_PASS", "AdminPass123")
 
DWH_NAME = os.getenv("DWH_NAME", "icteq_db")
DWH_HOST = os.getenv("DWH_HOST", "postgres-airflow")
DWH_DB   = os.getenv("DWH_DB", "icteq_db")
DWH_USER = os.getenv("DWH_USER", "airflow")
DWH_PASS = os.getenv("DWH_PASS", "airflow")
 
SAVED_STATES_DIR = "/saved_states"
 
def get_timestamp():
    return time.strftime("%H:%M:%S")
 
def log(msg):
    print(f"[{get_timestamp()}] {msg}", flush=True)
 
def wait_for_metabase():
    log(f"⏳ Waiting for Metabase API...")
    for i in range(120):
        try:
            if requests.get(f"{MB_URL}/api/health", timeout=5).status_code == 200:
                log("✅ Metabase is UP.")
                return
        except: pass
        time.sleep(5)
    exit(1)
 
def setup_admin():
    try:
        r = requests.get(f"{MB_URL}/api/session/properties")
        token = r.json().get("setup-token")
        if token:
            requests.post(f"{MB_URL}/api/setup", json={
                "token": token,
                "user": {"email": ADMIN_EMAIL, "first_name": "Admin", "last_name": "User", "password": ADMIN_PASS},
                "prefs": {"site_name": "BigData Platform", "allow_tracking": False}
            })
            log("   ✅ Admin created.")
    except Exception as e: log(f"⚠️ Setup error: {e}")
 
def get_database_list(mb):
    res = mb.get("/api/database")
    if isinstance(res, dict) and 'data' in res: return res['data']
    return res if isinstance(res, list) else []
 
def add_dwh_connection(mb):
    log(f"🔌 Checking Database '{DWH_NAME}'...")
    dbs = get_database_list(mb)
    if any(db['name'] == DWH_NAME for db in dbs): return
 
    res = requests.post(f"{MB_URL}/api/database",
        headers={'Content-Type': 'application/json', 'X-Metabase-Session': mb.session_id},
        json={
        "engine": "postgres", "name": DWH_NAME,
        "details": {"host": DWH_HOST, "port": 5432, "dbname": DWH_DB, "user": DWH_USER, "password": DWH_PASS, "ssl": False}
    })
    
    if res.status_code != 200: log(f"   ❌ DB Connection Failed: {res.text}")
    else: log(f"   ✅ Database added : { res.text }") 

        
 
def get_or_create_collection(mb, name):
    cols = mb.get("/api/collection")
    if isinstance(cols, list):
        for c in cols:
            if c['name'] == name: return c['id']
    res = mb.post("/api/collection", json={"name": name, "color": "#509EE3"})
    return res['id'] if isinstance(res, dict) else None
 
def deploy_content(mb):
    log("📦 Starting Content Deployment...")
   
    dbs = get_database_list(mb)
    target_db_id = next((db['id'] for db in dbs if db['name'] == DWH_NAME), None)
    if not target_db_id:
        log(f"❌ Critical: DB '{DWH_NAME}' not found.")
        return
 
    files = glob.glob(f"{SAVED_STATES_DIR}/*.json")
    for filepath in files:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
           
            dash_name = data.get('name', 'Unknown')
            log(f"   ... Processing: {dash_name}")
 
            if any(d['name'] == dash_name for d in mb.get('/api/dashboard')):
                log("       ⚠️ Exists. Skipping.")
                continue
 
            col_id = get_or_create_collection(mb, dash_name)
 
            dash = mb.post("/api/dashboard", json={
                "name": dash_name,
                "description": data.get('description'),
                "collection_id": col_id,
                "parameters": []
            })
           
            if not isinstance(dash, dict) or 'id' not in dash:
                log("       ❌ Failed to create dashboard.")
                continue
           
            dash_id = dash['id']
            dash_cards_payload = []
            cards = data.get('cards', [])
 
            for i, card_def in enumerate(cards, 1):
                layout = card_def.get('layout', {'row':0, 'col':0, 'size_x':4, 'size_y':3})
 
                # A. Handle Text Cards
                if card_def.get('is_text'):
                    dash_cards_payload.append({
                        "id": -i,
                        "card_id": None,
                        "row": layout.get('row', 0),
                        "col": layout.get('col', 0),
                        "size_x": layout.get('size_x', 4),
                        "size_y": layout.get('size_y', 1),
                        "visualization_settings": card_def.get('visualization_settings', {}),
                        "parameter_mappings": [],
                        "series": []
                    })
                    continue
 
                # B. Handle Data Cards
                query = card_def['dataset_query']
                query['database'] = target_db_id
               
                card = mb.post("/api/card", json={
                    "name": card_def['name'],
                    "dataset_query": query,
                    "display": card_def['display'],
                    "visualization_settings": card_def['visualization_settings'],
                    "collection_id": col_id
                })
               
                if isinstance(card, dict) and 'id' in card:
                    dash_cards_payload.append({
                        "id": -i,
                        "card_id": card['id'],
                        "row": layout.get('row', 0),
                        "col": layout.get('col', 0),
                        "size_x": layout.get('size_x', 4),
                        "size_y": layout.get('size_y', 3),
                       
                        # --- THE FIX IS HERE ---
                        # We must pass the settings from the JSON, NOT empty {}
                        # This tells Metabase how to color the chart, which columns to use, etc.
                        "visualization_settings": card_def.get('visualization_settings', {}),
                       
                        "parameter_mappings": [],
                        "series": []
                    })
                else:
                    log(f"       ⚠️ Failed to create card: {card_def['name']}")
 
            log(f"       🔗 Linking {len(dash_cards_payload)} cards to dashboard...")
           
            update_payload = {"dashcards": dash_cards_payload}
           
            link_res = requests.put(
                f"{MB_URL}/api/dashboard/{dash_id}",
                headers={'Content-Type': 'application/json', 'X-Metabase-Session': mb.session_id},
                json=update_payload
            )
 
            if link_res.status_code == 200:
                check_res = mb.get(f"/api/dashboard/{dash_id}")
                visible = check_res.get('ordered_cards') or check_res.get('dashcards') or []
                count = len(visible)
                if count == 0 and len(dash_cards_payload) > 0:
                     log(f"       ❌ WARNING: Dashboard still empty.")
                else:
                     log(f"       ✅ Deployed successfully ({count} cards visible).")
            else:
                log(f"       ❌ PUT Failed. Status: {link_res.status_code} Msg: {link_res.text}")
 
        except Exception as e:
            log(f"       ❌ Error processing file {filepath}: {e}")
 
    log("✅ Deployment Finished.")
 
if __name__ == "__main__":
    wait_for_metabase()
    setup_admin()
    time.sleep(2)
    mb = Metabase_API(MB_URL, ADMIN_EMAIL, ADMIN_PASS)
    add_dwh_connection(mb)
    deploy_content(mb)
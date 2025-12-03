import requests
import json
import sys

# ==========================================
# CONFIGURATION
# ==========================================
MB_URL = "http://localhost:3001"  # Your External Docker Port
MB_USER = "skanderbenregaya@gmail.com"     # Your Admin Email
MB_PASS = "Motdepasse@21011990"            # The password you just set/verified

OUTPUT_FILE = "metabase/scripts/setup_metabase.sh" # Save directly to correct folder if path exists

# ==========================================
# LOGIC
# ==========================================
s = requests.Session()

def get_token():
    print(f"[*] Authenticating to {MB_URL}...")
    try:
        res = s.post(f"{MB_URL}/api/session", json={"username": MB_USER, "password": MB_PASS})
        if res.status_code != 200:
            print(f"[!] Login failed: {res.status_code} - {res.text}")
            sys.exit(1)
        return res.json()['id']
    except Exception as e:
        print(f"[!] Connection failed: {e}")
        sys.exit(1)

def fetch_db_id(headers):
    print(f"[*] Fetching Database Metadata...")
    res = s.get(f"{MB_URL}/api/database", headers=headers)
    
    if res.status_code != 200:
        print(f"[!] Failed to fetch DBs: {res.status_code} - {res.text}")
        sys.exit(1)

    data = res.json()

    # --- FIX FOR YOUR ERROR ---
    # If API returns a dict (Error or Wrapper) instead of List
    if isinstance(data, dict):
        print(f"[!] Warning: API returned a dictionary: {data}")
        # Try to find a list inside 'data' key (some versions do this)
        if 'data' in data and isinstance(data['data'], list):
            data = data['data']
        else:
            print("[!] Could not find database list in response. Exiting.")
            sys.exit(1)
    # --------------------------

    for db in data:
        # We look for postgres engine that isn't the sample dataset
        if db.get('engine') == 'postgres' and not db.get('is_sample', False):
            print(f"    > Found DWH: {db['name']} (ID: {db['id']})")
            return db
    
    print("[!] No Postgres DB found connected to Metabase. Please connect your 'postgres-airflow' first in the UI.")
    sys.exit(1)

def fetch_cards(headers):
    res = s.get(f"{MB_URL}/api/card", headers=headers)
    cards = [c for c in res.json() if not c.get('archived', False)]
    print(f"[*] Found {len(cards)} active questions.")
    return cards

def fetch_dashboards(headers):
    res = s.get(f"{MB_URL}/api/dashboard", headers=headers)
    dashboards = [d for d in res.json() if not d.get('archived', False)]
    print(f"[*] Found {len(dashboards)} active dashboards.")
    return dashboards

def fetch_dashboard_cards(dash_id, headers):
    res = s.get(f"{MB_URL}/api/dashboard/{dash_id}", headers=headers)
    return res.json().get('ordered_cards', [])

def generate_bash_script(db_info, cards, dashboards, session_id):
    print(f"[*] Generating Bash Script: {OUTPUT_FILE}...")
    
    script_content = f"""#!/bin/bash
set -e

# ==========================================
# AUTO-GENERATED METABASE SETUP
# ==========================================

MB_URL="http://localhost:3000"
ADMIN_EMAIL="${{MB_USER}}"
ADMIN_PASS="${{MB_PASS}}"

# DWH CONFIG (Matches your dev environment)
DWH_NAME="{db_info['name']}"
DWH_HOST="postgres-airflow"
DWH_PORT="5432"
DWH_DB="airflow"
DWH_USER="airflow"
DWH_PASS="airflow"

echo "[Setup] Waiting for Metabase API..."
until curl -s "$MB_URL/api/health" | grep "ok" > /dev/null; do
    echo "  ...waiting"
    sleep 5
done

# 1. SETUP ADMIN
SETUP_TOKEN=$(curl -s "$MB_URL/api/session/properties" | jq -r '.setup_token')
if [ "$SETUP_TOKEN" != "null" ] && [ -n "$SETUP_TOKEN" ]; then
    echo "[Setup] Creating Admin..."
    PAYLOAD=$(jq -n \\
              --arg token "$SETUP_TOKEN" \\
              --arg email "$ADMIN_EMAIL" \\
              --arg pass "$ADMIN_PASS" \\
              '{{token: $token, user: {{email: $email, first_name: "Admin", last_name: "Super", password: $pass}}, prefs: {{site_name: "Data Platform", allow_tracking: false}}}}')
    curl -s -f -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$MB_URL/api/setup" || echo "Setup skipped"
else
    echo "[Setup] Admin already exists."
fi

# 2. LOGIN
echo "[Setup] Logging in..."
LOGIN_PAYLOAD=$(jq -n --arg u "$ADMIN_EMAIL" --arg p "$ADMIN_PASS" '{{username: $u, password: $p}}')
SESSION_ID=$(curl -s -X POST -H "Content-Type: application/json" -d "$LOGIN_PAYLOAD" "$MB_URL/api/session" | jq -r '.id')

if [ "$SESSION_ID" == "null" ] || [ -z "$SESSION_ID" ]; then echo "[!] Login failed"; exit 1; fi

# 3. SETUP DATABASE
echo "[Setup] Configuring Database..."
EXISTING_DB=$(curl -s -H "X-Metabase-Session: $SESSION_ID" "$MB_URL/api/database" | jq -r '.[] | select(.name=="{db_info['name']}") | .id')

if [ -z "$EXISTING_DB" ]; then
    DB_PAYLOAD=$(jq -n \\
        --arg name "$DWH_NAME" \\
        --arg host "$DWH_HOST" \\
        --arg db "$DWH_DB" \\
        --arg user "$DWH_USER" \\
        --arg pass "$DWH_PASS" \\
        '{{name: $name, engine: "postgres", details: {{host: $host, port: 5432, dbname: $db, user: $user, password: $pass, ssl: false}}}}')
    DB_ID=$(curl -s -f -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" -d "$DB_PAYLOAD" "$MB_URL/api/database" | jq -r '.id')
else
    DB_ID=$EXISTING_DB
fi

declare -A CARD_MAP
"""

    # Generate Card Logic
    for card in cards:
        # SQL Escaping for Bash + JSON
        sql_query = card['dataset_query']['native']['query'].replace("'", "'\\''")
        viz_settings = json.dumps(card.get('visualization_settings', {}))
        
        script_content += f"""
echo "  > Question: {card['name']}"
CARD_PAYLOAD=$(jq -n \\
    --arg db_id "$DB_ID" \\
    --arg name "{card['name']}" \\
    --arg sql '{sql_query}' \\
    --arg display "{card['display']}" \\
    --argjson viz '{viz_settings}' \\
    '{{name: $name, dataset_query: {{database: ($db_id|tonumber), type: "native", native: {{query: $sql}}}}, display: $display, visualization_settings: $viz}}')

CARD_RESP=$(curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" -d "$CARD_PAYLOAD" "$MB_URL/api/card")
NEW_CARD_ID=$(echo $CARD_RESP | jq -r '.id')
CARD_MAP["{card['id']}"]=$NEW_CARD_ID
"""

    # Generate Dashboard Logic
    for dash in dashboards:
        dash_cards = fetch_dashboard_cards(dash['id'], {'X-Metabase-Session': session_id})
        script_content += f"""
echo "  > Dashboard: {dash['name']}"
DASH_ID=$(curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \\
    -d '{{ "name": "{dash['name']}", "description": "{dash.get('description', '')}" }}' \\
    "$MB_URL/api/dashboard" | jq -r '.id')
"""
        for dc in dash_cards:
            if 'card_id' not in dc: continue
            script_content += f"""
MAPPED_CARD_ID=${{CARD_MAP["{dc['card_id']}"]}}
if [ -n "$MAPPED_CARD_ID" ] && [ "$MAPPED_CARD_ID" != "null" ]; then
    DASH_CARD_PAYLOAD=$(jq -n \\
        --arg card_id "$MAPPED_CARD_ID" \\
        --argjson row {dc['row']} \\
        --argjson col {dc['col']} \\
        --argjson sizeX {dc['sizeX']} \\
        --argjson sizeY {dc['sizeY']} \\
        --argjson viz '{json.dumps(dc.get('visualization_settings', {}))}' \\
        '{{cardId: ($card_id|tonumber), row: $row, col: $col, sizeX: $sizeX, sizeY: $sizeY, visualization_settings: $viz}}')
    curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \\
        -d "$DASH_CARD_PAYLOAD" "$MB_URL/api/dashboard/$DASH_ID/cards" > /dev/null
fi
"""

    script_content += '\necho "[Setup] SUCCESS: Metabase Fully Configured!"'
    
    # Try writing to the metabase/scripts folder if possible, else current dir
    try:
        with open(OUTPUT_FILE, "w", encoding='utf-8', newline='\n') as f:
            f.write(script_content)
        print(f"[*] Success! Script saved to {OUTPUT_FILE}")
    except FileNotFoundError:
        print(f"[!] Warning: Could not find folder 'metabase/scripts'. Saving to 'final_setup.sh' in current dir instead.")
        with open("final_setup.sh", "w", encoding='utf-8', newline='\n') as f:
            f.write(script_content)

# ==========================================
# MAIN EXECUTION
# ==========================================
token = get_token()
headers = {'X-Metabase-Session': token}
db_info = fetch_db_id(headers)
cards = fetch_cards(headers)
dashboards = fetch_dashboards(headers)
generate_bash_script(db_info, cards, dashboards, token)
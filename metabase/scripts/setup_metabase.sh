#!/bin/bash
set -e

# ==========================================
# 1. CONFIGURATION
# ==========================================
MB_URL="http://localhost:3000"
ADMIN_EMAIL="${MB_USER}"
ADMIN_PASS="${MB_PASS}"

# Your Database Details (Airflow Postgres)
DWH_NAME="metabase_db"
DWH_HOST="postgres-airflow"
DWH_PORT="5432"
DWH_DB="airflow"
DWH_USER="airflow"
DWH_PASS="airflow"

# Dashboard Name
DASH_NAME="Equipe Analysis"

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

wait_for_metabase() {
    echo "[Setup] Waiting for Metabase API..."
    until curl -s "$MB_URL/api/health" | grep "ok" > /dev/null; do
        sleep 5
    done
    echo "[Setup] Metabase is ready."
}

# Uses jq to handle special characters (like !) in passwords safely
api_post() {
    local endpoint="$1"
    local data="$2"
    curl -s -f -X POST -H "Content-Type: application/json" \
         -H "X-Metabase-Session: $SESSION_ID" \
         -d "$data" "$MB_URL$endpoint"
}

# ==========================================
# 3. SETUP LOGIC
# ==========================================

wait_for_metabase

# --- A. Create Admin (if needed) ---
SETUP_TOKEN=$(curl -s "$MB_URL/api/session/properties" | jq -r '.setup_token')
if [ "$SETUP_TOKEN" != "null" ] && [ -n "$SETUP_TOKEN" ]; then
    echo "[Setup] Creating Admin User..."
    PAYLOAD=$(jq -n \
              --arg token "$SETUP_TOKEN" \
              --arg email "$ADMIN_EMAIL" \
              --arg pass "$ADMIN_PASS" \
              '{token: $token, user: {email: $email, first_name: "Admin", last_name: "Super", password: $pass}, prefs: {site_name: "Data Platform", allow_tracking: false}}')
    
    curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$MB_URL/api/setup" > /dev/null
else
    echo "[Setup] Admin already exists."
fi

# --- B. Login ---
echo "[Setup] Logging in..."
LOGIN_PAYLOAD=$(jq -n --arg u "$ADMIN_EMAIL" --arg p "$ADMIN_PASS" '{username: $u, password: $p}')
SESSION_ID=$(curl -s -X POST -H "Content-Type: application/json" -d "$LOGIN_PAYLOAD" "$MB_URL/api/session" | jq -r '.id')

if [ "$SESSION_ID" == "null" ] || [ -z "$SESSION_ID" ]; then
    echo "[!] Error: Login failed. Check MB_USER/MB_PASS variables."
    exit 1
fi

# --- C. Connect Database ---
echo "[Setup] Connecting to Database..."
EXISTING_DB=$(curl -s -H "X-Metabase-Session: $SESSION_ID" "$MB_URL/api/database" | jq -r '.[] | select(.name=="'"$DWH_NAME"'") | .id')

if [ -z "$EXISTING_DB" ]; then
    DB_PAYLOAD=$(jq -n \
        --arg name "$DWH_NAME" \
        --arg host "$DWH_HOST" \
        --arg db "$DWH_DB" \
        --arg user "$DWH_USER" \
        --arg pass "$DWH_PASS" \
        '{name: $name, engine: "postgres", details: {host: $host, port: 5432, dbname: $db, user: $user, password: $pass, ssl: false}}')
    
    DB_ID=$(api_post "/api/database" "$DB_PAYLOAD" | jq -r '.id')
else
    DB_ID=$EXISTING_DB
fi
echo "   > DB ID: $DB_ID"

# --- D. Create Questions ---
# We use Associative Array to store Card IDs
declare -A CARDS

# 1. Total (Scalar)
echo "[Setup] Creating Question: Total"
SQL_TOTAL="select count(distinct fullname) from equipe"
PAYLOAD_TOTAL=$(jq -n --arg db "$DB_ID" --arg sql "$SQL_TOTAL" \
    '{name: "Total", display: "scalar", dataset_query: {database: ($db|tonumber), type: "native", native: {query: $sql}}}')
CARDS["total"]=$(api_post "/api/card" "$PAYLOAD_TOTAL" | jq -r '.id')

# 2. Person_Nbr (Bar Chart)
echo "[Setup] Creating Question: Person_Nbr"
SQL_BAR="select count(*) nbr , fullname from equipe group by fullname order by count(*) desc"
PAYLOAD_BAR=$(jq -n --arg db "$DB_ID" --arg sql "$SQL_BAR" \
    '{name: "Person_Nbr", display: "bar", dataset_query: {database: ($db|tonumber), type: "native", native: {query: $sql}}, visualization_settings: {"graph.dimensions": ["fullname"], "graph.metrics": ["nbr"]}}')
CARDS["bar"]=$(api_post "/api/card" "$PAYLOAD_BAR" | jq -r '.id')

# 3. Gender Rep (Pie Chart) - Complex SQL handled safely by jq
echo "[Setup] Creating Question: Gender Rep"
# We put the SQL in a variable. jq handles the quotes inside it automatically.
SQL_PIE="select count(*) nbr ,gender from (select fullname , 'Homme' gender from equipe  where fullname like '%Ben rekaya Skander%' Union select fullname , 'Femme' gender from equipe  where fullname not like '%Ben rekaya Skander%' ) tab group by gender"

PAYLOAD_PIE=$(jq -n --arg db "$DB_ID" --arg sql "$SQL_PIE" \
    '{name: "Gender Rep", display: "pie", dataset_query: {database: ($db|tonumber), type: "native", native: {query: $sql}}, visualization_settings: {"pie.dimension": "gender", "pie.metric": "nbr"}}')
CARDS["pie"]=$(api_post "/api/card" "$PAYLOAD_PIE" | jq -r '.id')

# --- E. Create Dashboard ---
# Check if exists first
EXISTING_DASH=$(curl -s -H "X-Metabase-Session: $SESSION_ID" "$MB_URL/api/dashboard" | jq -r '.[] | select(.name=="'"$DASH_NAME"'") | .id')

if [ -z "$EXISTING_DASH" ]; then
    echo "[Setup] Creating Dashboard: $DASH_NAME"
    DASH_ID=$(api_post "/api/dashboard" '{"name": "'"$DASH_NAME"'", "description": "Auto-generated"}' | jq -r '.id')

    # Link Cards
    # 1. Total (Top Left: 0,0)
    api_post "/api/dashboard/$DASH_ID/cards" "$(jq -n --arg id "${CARDS[total]}" '{cardId: ($id|tonumber), row: 0, col: 0, sizeX: 4, sizeY: 4}')" > /dev/null
    
    # 2. Bar (Top Right: 0,4)
    api_post "/api/dashboard/$DASH_ID/cards" "$(jq -n --arg id "${CARDS[bar]}" '{cardId: ($id|tonumber), row: 0, col: 4, sizeX: 14, sizeY: 4}')" > /dev/null
    
    # 3. Pie (Bottom: 4,0)
    api_post "/api/dashboard/$DASH_ID/cards" "$(jq -n --arg id "${CARDS[pie]}" '{cardId: ($id|tonumber), row: 4, col: 0, sizeX: 18, sizeY: 10}')" > /dev/null

    echo "[Setup] SUCCESS! Dashboard created."
else
    echo "[Setup] Dashboard already exists."
fi

echo "[Setup] Script Complete."
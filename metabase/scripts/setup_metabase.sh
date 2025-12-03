#!/bin/bash

# ==========================================
# 1. CONFIGURATION & CREDENTIALS
# ==========================================
MB_URL="http://localhost:3000"
ADMIN_EMAIL="${MB_USER}"
ADMIN_PASS="${MB_PASS}"

# DWH Connection Details
DWH_NAME="Airflow DWH"
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

# Function to wait for Metabase to start
wait_for_metabase() {
    echo "[Setup] Waiting for Metabase API at $MB_URL..."
    until curl -s "$MB_URL/api/health" | grep "ok" > /dev/null; do
        sleep 5
    done
    echo "[Setup] Metabase is ready."
}

# Function to setup Admin user (runs only once)
setup_admin() {
    SETUP_TOKEN=$(curl -s "$MB_URL/api/session/properties" | jq -r '.setup_token')
    if [ "$SETUP_TOKEN" != "null" ] && [ -n "$SETUP_TOKEN" ]; then
        echo "[Setup] Creating Admin User..."
        curl -s -X POST -H "Content-Type: application/json" \
          -d '{
            "token": "'"$SETUP_TOKEN"'",
            "user": { "email": "'"$ADMIN_EMAIL"'", "first_name": "Admin", "last_name": "Super", "password": "'"$ADMIN_PASS"'" },
            "prefs": { "site_name": "Data Platform", "allow_tracking": false }
          }' "$MB_URL/api/setup" > /dev/null
    else
        echo "[Setup] Admin already exists."
    fi
}

# Function to Login and get Session ID
get_session_id() {
    SESSION_ID=$(curl -s -X POST -H "Content-Type: application/json" \
        -d '{ "username": "'"$ADMIN_EMAIL"'", "password": "'"$ADMIN_PASS"'" }' \
        "$MB_URL/api/session" | jq -r '.id')
    
    if [ "$SESSION_ID" == "null" ]; then
        echo "[Error] Could not login. Check credentials."
        exit 1
    fi
}

# ==========================================
# 3. CORE SETUP LOGIC
# ==========================================

# A. Connect Database
setup_database() {
    # Check if DB exists
    EXISTING_DB_ID=$(curl -s -H "X-Metabase-Session: $SESSION_ID" "$MB_URL/api/database" | jq -r '.[] | select(.name=="'"$DWH_NAME"'") | .id')

    if [ -z "$EXISTING_DB_ID" ]; then
        echo "[Setup] Connecting to Database ($DWH_NAME)..."
        RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
            -d '{
              "name": "'"$DWH_NAME"'",
              "engine": "postgres",
              "details": { "host": "'"$DWH_HOST"'", "port": 5432, "dbname": "'"$DWH_DB"'", "user": "'"$DWH_USER"'", "password": "'"$DWH_PASS"'", "ssl": false }
            }' "$MB_URL/api/database")
        DB_ID=$(echo $RESPONSE | jq -r '.id')
    else
        echo "[Setup] Database already connected."
        DB_ID=$EXISTING_DB_ID
    fi
}

# B. Create Questions and Dashboard
create_content() {
    # Check if Dashboard exists
    EXISTING_DASH_ID=$(curl -s -H "X-Metabase-Session: $SESSION_ID" "$MB_URL/api/dashboard" | jq -r '.[] | select(.name=="'"$DASH_NAME"'") | .id')

    if [ -n "$EXISTING_DASH_ID" ]; then
        echo "[Setup] Dashboard '$DASH_NAME' already exists. Skipping creation."
        return
    fi

    echo "[Setup] Creating Dashboard and Questions..."

    # 1. Create Dashboard Container
    DASH_ID=$(curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
        -d '{ "name": "'"$DASH_NAME"'", "description": "Auto-generated via Docker" }' \
        "$MB_URL/api/dashboard" | jq -r '.id')

    # --- CARD 1: TOTAL (Scalar) ---
    # SQL: select count(distinct fullname) from equipe
    echo "  > Creating Card: Total"
    SQL_TOTAL="select count(distinct fullname) from equipe"
    
    CARD_TOTAL_ID=$(curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
        -d '{
            "name": "Total",
            "dataset_query": { "database": '"$DB_ID"', "type": "native", "native": { "query": "'"$SQL_TOTAL"'" } },
            "display": "scalar"
        }' "$MB_URL/api/card" | jq -r '.id')

    # --- CARD 2: Person Nbr (Bar Chart) ---
    # SQL: select count(*) nbr , fullname from equipe group by fullname order by count(*) desc
    echo "  > Creating Card: Person Nbr"
    SQL_BAR="select count(*) as nbr , fullname from equipe group by fullname order by count(*) desc"
    
    CARD_BAR_ID=$(curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
        -d '{
            "name": "Person_Nbr",
            "dataset_query": { "database": '"$DB_ID"', "type": "native", "native": { "query": "'"$SQL_BAR"'" } },
            "display": "bar",
            "visualization_settings": {
                "graph.dimensions": ["fullname"],
                "graph.metrics": ["nbr"]
            }
        }' "$MB_URL/api/card" | jq -r '.id')

    # --- CARD 3: Gender Rep (Pie Chart) ---
    # SQL: Complex Union Query
    # Note: We flatten the SQL to one line and escape single quotes for JSON safety
    echo "  > Creating Card: Gender Rep"
    SQL_PIE="select count(*) as nbr ,gender from (select fullname , '\''Homme'\'' gender from equipe where fullname like '\''%Ben rekaya Skander%'\'' Union select fullname , '\''Femme'\'' gender from equipe where fullname not like '\''%Ben rekaya Skander%'\'') tab group by gender"

    CARD_PIE_ID=$(curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
        -d '{
            "name": "Gender Rep",
            "dataset_query": { "database": '"$DB_ID"', "type": "native", "native": { "query": "'"$SQL_PIE"'" } },
            "display": "pie",
            "visualization_settings": {
                "pie.dimension": "gender",
                "pie.metric": "nbr"
            }
        }' "$MB_URL/api/card" | jq -r '.id')

    # --- LINK CARDS TO DASHBOARD ---
    echo "  > Positioning Cards on Dashboard..."
    
    # 1. Total: Top Left (Row 0, Col 0, Size 4x4)
    curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
        -d '{ "cardId": '"$CARD_TOTAL_ID"', "row": 0, "col": 0, "sizeX": 4, "sizeY": 4 }' \
        "$MB_URL/api/dashboard/$DASH_ID/cards" > /dev/null

    # 2. Bar Chart: Top Right (Row 0, Col 4, Size 14x4 - Stretches to right)
    curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
        -d '{ "cardId": '"$CARD_BAR_ID"', "row": 0, "col": 4, "sizeX": 14, "sizeY": 4 }' \
        "$MB_URL/api/dashboard/$DASH_ID/cards" > /dev/null

    # 3. Pie Chart: Bottom (Row 4, Col 0, Size 18x8 - Full Width, Taller)
    curl -s -X POST -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_ID" \
        -d '{ "cardId": '"$CARD_PIE_ID"', "row": 4, "col": 0, "sizeX": 18, "sizeY": 10 }' \
        "$MB_URL/api/dashboard/$DASH_ID/cards" > /dev/null

    echo "[Setup] SUCCESS! Dashboard is ready at: $MB_URL/dashboard/$DASH_ID"
}

# ==========================================
# 4. EXECUTION
# ==========================================
wait_for_metabase
setup_admin
get_session_id
setup_database
create_content
echo "[Setup] Script Finished."
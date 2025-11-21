#!/bin/bash

# Start Metabase in background
/app/run_metabase.sh &

# Wait until Metabase API responds
echo "⏳ Waiting for Metabase API to be ready..."
until curl -s http://localhost:3000/api/health | grep -q '"status":"ok"'; do
  sleep 5
done

echo "✅ Metabase API ready"

# Try to login with admin
LOGIN_STATUS=1
while [ $LOGIN_STATUS -ne 0 ]; do
    SESSION=$(curl -s -X POST \
      -H "Content-Type: application/json" \
      -d '{"username": "'"$MB_ADMIN_EMAIL"'", "password": "'"$MB_ADMIN_PASSWORD"'"}' \
      http://localhost:3000/api/session | jq -r '.id')

    if [ "$SESSION" != "null" ] && [ -n "$SESSION" ]; then
        LOGIN_STATUS=0
    else
        echo "⏳ Waiting for admin account creation..."
        sleep 5
    fi
done

echo "✅ Logged in. Session token = $SESSION"

# Add Airflow DB
curl -s -X POST http://localhost:3000/api/database \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION" \
  -d '{
        "name": "airflow_db",
        "engine": "postgres",
        "details": {
            "host": "postgres-airflow",
            "port": 5432,
            "dbname": "airflow",
            "user": "airflow",
            "password": "airflow",
            "ssl": false
        }
      }'

echo "🎉 Database airflow_db added to Metabase!"

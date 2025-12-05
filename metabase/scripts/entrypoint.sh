#!/bin/bash

echo "[Docker] Starting Metabase..."

# 1. Start Metabase in the background using the original script
/app/run_metabase.sh &

# Capture the Process ID (PID) of Metabase
METABASE_PID=$!

# 2. Run the setup logic in the background (it will wait for port 3000)
/app/setup_metabase.sh &

# 3. Wait for the Metabase process to ensure the container stays alive
# If Metabase crashes, this wait command finishes, and the container exits (correct behavior)
wait $METABASE_PID
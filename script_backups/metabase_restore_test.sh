#!/bin/bash

# Load config
source metabase_env.conf

# Check input argument
echo $(ls -t "$BACKUP_DIR"/*.dump | head -1)

BACKUP_FILE=$(ls -t "$BACKUP_DIR"/*.dump | head -1)
if [ -z "$BACKUP_FILE" ]; then
    echo "❌ ERROR: No backup files found."
    exit 1  
fi

DUMP_NAME=$(basename "$BACKUP_FILE")

echo "🔵 Starting Metabase TEST restore..."
echo "Using dump: $DUMP_NAME"

# Stop Metabase container
echo "⛔ Stopping Metabase TEST..."
docker stop "$TEST_METABASE_CONTAINER"

# Copy dump into Postgres container
echo "🔵 Copying dump into PostgreSQL container..."

echo cp "$BACKUP_FILE" "$TEST_POSTGRES_CONTAINER:/backup/"

docker cp "$BACKUP_FILE" "$TEST_POSTGRES_CONTAINER:/backup/"
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to copy dump to postgres container."
    exit 1
fi



# Restore database
echo "🔄 Restoring database..."

echo exec -i "$TEST_POSTGRES_CONTAINER" pg_restore -U "$TEST_DB_USER" -d "$TEST_DB_NAME" -c "/backup/$DUMP_NAME"

docker exec -i "$TEST_POSTGRES_CONTAINER" pg_restore -U "$TEST_DB_USER" -d "$TEST_DB_NAME" -c "/backup/$DUMP_NAME"

# Start Metabase container
echo "🚀 Starting Metabase TEST..."
docker start "$TEST_METABASE_CONTAINER"

echo "🎉 Restore completed successfully!"



source metabase_env.conf

TIMESTAMP=$(date +"%Y%m%d_%H%M")
DUMP_FILE="metabase_dev_${TIMESTAMP}.dump"

echo "🔵 Starting Metabase DEV backup..."

echo "$DEV_POSTGRES_CONTAINER" pg_dump -U "$DEV_DB_USER" -d "$DEV_DB_NAME" -Fc -f "/tmp/$DUMP_FILE"

# Create dump inside container
docker exec -i "$DEV_POSTGRES_CONTAINER" pg_dump -U "$DEV_DB_USER" -d "$DEV_DB_NAME" -Fc -f "/tmp/$DUMP_FILE"
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to create database dump."
    exit 1
fi

# Copy dump to host
docker cp "$DEV_POSTGRES_CONTAINER:/tmp/$DUMP_FILE" "$BACKUP_DIR/"
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to copy dump to host."
    exit 1
fi

echo "✅ Backup completed: $BACKUP_DIR/$DUMP_FILE"

# Optional: transfer to PROD
if [ "$COPY_TO_PROD" = true ]; then
    echo "🔵 Copying dump to PROD..."
    scp "$BACKUP_DIR/$DUMP_FILE" "$PROD_USER@$PROD_HOST:$PROD_PATH"
    if [ $? -ne 0 ]; then
        echo "❌ ERROR: Failed to copy file to PROD."
        exit 1
    fi
    echo "✅ File transferred to PROD: $PROD_HOST:$PROD_PATH"
fi

echo "🎉 DEV backup process finished."

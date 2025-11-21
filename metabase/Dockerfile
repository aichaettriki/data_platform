FROM metabase/metabase:latest


USER root
RUN apk add --no-cache jq

# Copy custom environment config
COPY env/metabase.env /app/metabase.env
COPY init/add-database.sh /app/add-database.sh
RUN chmod +x /app/add-database.sh

CMD ["bash", "-c", "/app/add-database.sh & /app/run_metabase.sh"]

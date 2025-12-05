FROM metabase/metabase:latest

# Switch to root to install utilities
USER root

# Install curl, jq, and bash (Alpine uses apk)
RUN apk add --no-cache curl jq bash

# Create directory for scripts (if not exists) and set ownership
WORKDIR /app

# Copy scripts and set ownership to UID 2000 (Metabase User)
# We use UID 2000 because the named user 'metabase' alias is sometimes missing
COPY --chown=2000:2000 scripts/entrypoint.sh /app/custom_entrypoint.sh
COPY --chown=2000:2000 scripts/setup_metabase.sh /app/setup_metabase.sh

# Make them executable
RUN chmod +x /app/custom_entrypoint.sh /app/setup_metabase.sh

# Switch back to the Metabase user by UID
USER 2000

# Override the entrypoint
ENTRYPOINT ["/app/custom_entrypoint.sh"]
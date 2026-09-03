#!/usr/bin/env bash
# Tear down everything provision.sh created. Deleting the environment cascades
# the cluster, Flink pool, statements, connection, and API keys with it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/deploy.env"
[ -f "$ENV_FILE" ] && { set -a; source "$ENV_FILE"; set +a; }
ENV_NAME="${ENV_NAME:-MTA-STREAMING-INTELLIGENCE}"

command -v confluent >/dev/null || { echo "confluent CLI not found"; exit 1; }
command -v jq >/dev/null || { echo "jq not found"; exit 1; }

confluent login --no-browser >/dev/null 2>&1 || true
ENV_ID="$(confluent environment list -o json | jq -r --arg n "$ENV_NAME" '.[] | select(.name==$n) | .id' | head -1)"
[ -z "$ENV_ID" ] && { echo "no environment named $ENV_NAME — nothing to do"; exit 0; }

read -r -p "Delete environment $ENV_NAME ($ENV_ID) and ALL its resources? [y/N] " ok
[ "$ok" = "y" ] || { echo "aborted"; exit 0; }
confluent environment delete "$ENV_ID" --force
echo "deleted $ENV_ID"

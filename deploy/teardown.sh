#!/usr/bin/env bash
# Tear down resources created for Transit Pulse NYC.
# By default, cleans up all application resources (connector, Flink statements,
# Flink models, Flink connections, Flink compute pool, and Kafka topics) while
# preserving the user's environment and Kafka cluster.
#
# Usage:
#   ./deploy/teardown.sh          # interactive confirmation, cleans app resources
#   ./deploy/teardown.sh --yes    # non-interactive confirmation
#   ./deploy/teardown.sh --all    # also deletes dedicated cluster & environment (if not "default")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/deploy.env"
[ -f "$ENV_FILE" ] && { set -a; source "$ENV_FILE"; set +a; }

CLOUD="${CLOUD:-aws}"
REGION="${REGION:-us-east-1}"
ENV_NAME="${ENV_NAME:-default}"
CLUSTER_NAME="${CLUSTER_NAME:-cluster_0}"
POOL_NAME="${POOL_NAME:-MTA-FLINK-POOL}"
CONFIRM=true
DELETE_ALL=false

for arg in "$@"; do
  case "$arg" in
    -y|--yes) CONFIRM=false ;;
    --all) DELETE_ALL=true ;;
    -h|--help)
      echo "Usage: ./deploy/teardown.sh [--yes] [--all]"
      echo "  --yes   Skip confirmation prompt"
      echo "  --all   Also delete Kafka cluster and environment (if not named 'default')"
      exit 0
      ;;
  esac
done

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

command -v confluent >/dev/null || { echo "confluent CLI not found"; exit 1; }
command -v jq >/dev/null || { echo "jq not found"; exit 1; }

# Authenticate or reuse session
if ! confluent environment list -o json >/dev/null 2>&1; then
  confluent login
fi

# Resolve environment
ENV_ID="$(confluent environment list -o json 2>/dev/null | jq -r --arg n "$ENV_NAME" '.[] | select(.name==$n or .id==$n) | .id' | head -1)"
if [ -z "$ENV_ID" ]; then
  ENV_ID="$(confluent environment list -o json 2>/dev/null | jq -r '.[] | select(.is_current==true) | .id' | head -1)"
fi
[ -z "$ENV_ID" ] && { echo "No active Confluent Cloud environment found — nothing to tear down."; exit 0; }

# Resolve Kafka cluster
CLUSTER_ID="$(confluent kafka cluster list --environment "$ENV_ID" -o json 2>/dev/null | jq -r --arg n "$CLUSTER_NAME" '.[] | select(.name==$n or .id==$n) | .id' | head -1)"
if [ -z "$CLUSTER_ID" ]; then
  CLUSTER_ID="$(confluent kafka cluster list --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.[] | select(.is_current==true) | .id' | head -1)"
fi

# Resolve Flink compute pool
POOL_ID="$(confluent flink compute-pool list --environment "$ENV_ID" --region "$REGION" -o json 2>/dev/null | jq -r --arg n "$POOL_NAME" '.[] | select(.name==$n or .id==$n) | .id' | head -1)"

log "Tear Down Plan"
echo "  Environment:   $ENV_NAME ($ENV_ID)"
echo "  Cluster:       ${CLUSTER_ID:-none}"
echo "  Compute Pool:  ${POOL_ID:-none} ($POOL_NAME)"
echo "  Scope:         HTTP Connector, Flink Statements, Flink Connections, Compute Pool, Kafka Topics"

if [ "$CONFIRM" = true ]; then
  read -r -p "Proceed with tearing down Transit Pulse NYC cloud resources? [y/N] " ok
  case "$ok" in
    [yY]|[yY][eE][sS]) ;;
    *) echo "Aborted."; exit 0 ;;
  esac
fi

# 1. Managed Connector
if [ -n "$CLUSTER_ID" ]; then
  log "1. Removing HTTP Source Connector..."
  CONN_IDS="$(confluent connect cluster list --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.[] | select(.name | test("^(mta-|service-alerts)")) | .id')"
  if [ -n "$CONN_IDS" ]; then
    for cid in $CONN_IDS; do
      confluent connect cluster delete "$cid" --cluster "$CLUSTER_ID" --environment "$ENV_ID" --force
      echo "   -> Deleted connector $cid"
    done
  else
    echo "   -> No connector found."
  fi
fi

# 2. Flink Statements
log "2. Deleting Transit Pulse Flink SQL Statements..."
STATEMENTS="$(confluent flink statement list --environment "$ENV_ID" --cloud "$CLOUD" --region "$REGION" -o json 2>/dev/null | jq -r '.[] | select(.name | test("^(mta-|create-mta|create-gemini)")) | .name')"
if [ -n "$STATEMENTS" ]; then
  for s in $STATEMENTS; do
    confluent flink statement delete "$s" --environment "$ENV_ID" --cloud "$CLOUD" --region "$REGION" --force >/dev/null 2>&1 || true
    echo "   -> Deleted statement: $s"
  done
else
  echo "   -> No matching Flink statements found."
fi

# 3. Flink Connections
log "3. Deleting Flink Connections..."
for conn in "llm-dispatcher-connection" "llm-dispatcher-connection-gemini"; do
  if confluent flink connection list --cloud "$CLOUD" --region "$REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -e --arg n "$conn" '.[] | select(.name==$n)' >/dev/null 2>&1; then
    confluent flink connection delete "$conn" --cloud "$CLOUD" --region "$REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
    echo "   -> Deleted connection: $conn"
  fi
done

# 4. Flink Compute Pool (stops CFU billing)
if [ -n "$POOL_ID" ]; then
  log "4. Deleting Flink Compute Pool (stopping CFU billing)..."
  confluent flink compute-pool delete "$POOL_ID" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
  echo "   -> Deleted compute pool: $POOL_ID ($POOL_NAME)"
fi

# 5. Kafka Topics
if [ -n "$CLUSTER_ID" ]; then
  log "5. Deleting Transit Pulse Kafka Topics..."
  MTA_TOPICS=(
    "mta_vehicle_positions"
    "mta_trip_updates"
    "mta_bus_positions"
    "mta_arrival_estimates"
    "mta_headway_alerts"
    "mta_headway_forecast"
    "mta_dispatcher_decisions"
    "mta_service_alerts"
    "mta.vehicle_positions"
    "mta.trip_updates"
    "mta.bus_positions"
    "mta.arrival_estimates"
    "mta.headway_alerts"
    "mta.headway_forecast"
    "mta.dispatcher_decisions"
    "mta.service_alerts"
  )
  for t in "${MTA_TOPICS[@]}"; do
    if confluent kafka topic describe "$t" --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null 2>&1; then
      confluent kafka topic delete "$t" --cluster "$CLUSTER_ID" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
      echo "   -> Deleted topic: $t"
    fi
  done
fi

# 6. Schema Registry Subjects
log "6. Cleaning up Schema Registry subjects..."
SR_SUBJECTS=(
  "mta_vehicle_positions-value"
  "mta_trip_updates-value"
  "mta_bus_positions-value"
  "mta_arrival_estimates-value"
  "mta_headway_alerts-value"
  "mta_headway_forecast-value"
  "mta_dispatcher_decisions-value"
  "mta_service_alerts-value"
  "mta.vehicle_positions-value"
  "mta.trip_updates-value"
  "mta.bus_positions-value"
  "mta.arrival_estimates-value"
  "mta.headway_alerts-value"
  "mta.headway_forecast-value"
  "mta.dispatcher_decisions-value"
  "mta.service_alerts-value"
)
for subj in "${SR_SUBJECTS[@]}"; do
  confluent schema-registry schema delete --subject "$subj" --version all --environment "$ENV_ID" --force >/dev/null 2>&1 || true
  confluent schema-registry schema delete --subject "$subj" --version all --environment "$ENV_ID" --permanent --force >/dev/null 2>&1 || true
done
echo "   -> Deleted Transit Pulse Schema Registry subjects."

# 7. Optional Cluster / Environment deletion
if [ "$DELETE_ALL" = true ]; then
  if [ "$ENV_NAME" != "default" ] && [ "$ENV_ID" != "default" ]; then
    log "6. Deleting Environment $ENV_NAME ($ENV_ID)..."
    confluent environment delete "$ENV_ID" --force
    echo "   -> Deleted environment: $ENV_ID"
  else
    echo "   -> Skipping environment deletion: '$ENV_NAME' is your default environment."
  fi
else
  log "Preserved environment '$ENV_NAME' and cluster '$CLUSTER_NAME'."
fi

log "Transit Pulse NYC teardown complete."

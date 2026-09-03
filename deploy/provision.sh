#!/usr/bin/env bash
# =============================================================================
# One-command Confluent Cloud provisioning for MTA Streaming Intelligence.
#
# Mirrors the F1 demo's self-service path, but driven by the `confluent` CLI
# instead of Terraform. It creates:
#   - a Confluent Cloud environment (+ Schema Registry / Stream Governance)
#   - a Kafka cluster + a Kafka API key
#   - a Schema Registry API key
#   - a Flink compute pool
#   - a Bedrock LLM connection
#   - every Flink statement (tables, pipeline, model, agents) in order
# ...then writes ../.env so the producer and dashboard just work.
#
# Prereqs on your Mac:
#   brew install confluentinc/tap/cli jq
#   confluent version           # v4+ recommended
#   cp deploy/deploy.env.example deploy/deploy.env && edit it
#
# Run from the repo root:  ./deploy/provision.sh
#
# NOTE: Confluent CLI flag names drift between versions. This targets the v4
# surface; if a subcommand rejects a flag, `confluent <cmd> --help` shows the
# current name. Every step echoes what it runs so you can adjust live.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FLINK_DIR="$REPO_DIR/flink"
ENV_FILE="$SCRIPT_DIR/deploy.env"
OUT_ENV="$REPO_DIR/.env"

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
run()  { printf '    $ %s\n' "$*"; eval "$@"; }
die()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

command -v confluent >/dev/null || die "confluent CLI not found (brew install confluentinc/tap/cli)"
command -v jq >/dev/null || die "jq not found (brew install jq)"
[ -f "$ENV_FILE" ] || die "missing $ENV_FILE — copy deploy.env.example and fill it in"
# shellcheck disable=SC1090
source "$ENV_FILE"

# Confluent auth is flexible (see the auth step below): an existing CLI session,
# a Cloud API key in deploy.env, or interactive login all work. The in-Flink LLM
# creds (Bedrock or Google AI / Gemini, per LLM_PROVIDER) are validated at the
# connection step below.

CLOUD="${CLOUD:-aws}"; REGION="${REGION:-us-east-1}"
ENV_NAME="${ENV_NAME:-MTA-STREAMING-INTELLIGENCE}"
CLUSTER_NAME="${CLUSTER_NAME:-MTA-CLUSTER}"
CLUSTER_TYPE="${CLUSTER_TYPE:-basic}"
POOL_NAME="${POOL_NAME:-MTA-FLINK-POOL}"
FLINK_MAX_CFU="${FLINK_MAX_CFU:-10}"
BEDROCK_REGION="${BEDROCK_REGION:-$REGION}"
BEDROCK_MODEL_ID="${BEDROCK_MODEL_ID:-us.anthropic.claude-sonnet-4-5-20250929-v1:0}"
CONNECTION_NAME="llm-dispatcher-connection"

# In-Flink LLM provider: bedrock (default) or googleai (Google AI Studio / Gemini).
LLM_PROVIDER="$(echo "${LLM_PROVIDER:-bedrock}" | tr '[:upper:]' '[:lower:]')"
GEMINI_MODEL_ID="${GEMINI_MODEL_ID:-gemini-2.0-flash}"
GOOGLEAI_ENDPOINT="${GOOGLEAI_ENDPOINT:-https://generativelanguage.googleapis.com/v1/models/${GEMINI_MODEL_ID}:generateContent}"

# ---- find-or-create helpers (idempotent by display name) --------------------

# Auth precedence: (1) reuse an active CLI session if you already ran
# `confluent login` on your Mac; (2) else non-interactive with the Cloud API key
# from deploy.env; (3) else open interactive (browser/SSO) login.
log "Authenticating with Confluent Cloud"
if confluent environment list -o json >/dev/null 2>&1; then
  echo "    reusing your current confluent CLI session"
elif [ -n "${CONFLUENT_CLOUD_API_KEY:-}" ] && [ -n "${CONFLUENT_CLOUD_API_SECRET:-}" ]; then
  echo "    logging in non-interactively with the Cloud API key from deploy.env"
  run "confluent login >/dev/null 2>&1 || true"
else
  echo "    no active session and no Cloud API key set — starting interactive login"
  run "confluent login"
fi
confluent environment list -o json >/dev/null 2>&1 \
  || die "not authenticated — run 'confluent login' (or set the Cloud API key in deploy.env) and re-run"

log "Environment: $ENV_NAME"
ENV_ID="$(confluent environment list -o json | jq -r --arg n "$ENV_NAME" '.[] | select(.name==$n) | .id' | head -1)"
if [ -z "$ENV_ID" ]; then
  # Stream Governance (Schema Registry) is enabled with the environment.
  ENV_ID="$(confluent environment create "$ENV_NAME" --governance-package essentials -o json | jq -r '.id')"
fi
run "confluent environment use $ENV_ID"
echo "    ENV_ID=$ENV_ID"

log "Kafka cluster: $CLUSTER_NAME ($CLUSTER_TYPE, $CLOUD/$REGION)"
CLUSTER_ID="$(confluent kafka cluster list -o json | jq -r --arg n "$CLUSTER_NAME" '.[] | select(.name==$n) | .id' | head -1)"
if [ -z "$CLUSTER_ID" ]; then
  CLUSTER_ID="$(confluent kafka cluster create "$CLUSTER_NAME" --cloud "$CLOUD" --region "$REGION" --type "$CLUSTER_TYPE" -o json | jq -r '.id')"
fi
run "confluent kafka cluster use $CLUSTER_ID"
BOOTSTRAP="$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.endpoint // .bootstrap_endpoint' | sed 's|SASL_SSL://||')"
echo "    CLUSTER_ID=$CLUSTER_ID  BOOTSTRAP=$BOOTSTRAP"

log "Waiting for cluster to be UP"
for _ in $(seq 1 30); do
  st="$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.status // "UNKNOWN"')"
  [ "$st" = "UP" ] && break
  sleep 5
done

log "Kafka API key"
KAFKA_KEY_JSON="$(confluent api-key create --resource "$CLUSTER_ID" --description "mta-producer" -o json)"
KAFKA_API_KEY="$(echo "$KAFKA_KEY_JSON" | jq -r '.api_key // .key')"
KAFKA_API_SECRET="$(echo "$KAFKA_KEY_JSON" | jq -r '.api_secret // .secret')"

log "Schema Registry"
SR_ID="$(confluent schema-registry cluster describe -o json | jq -r '.cluster_id // .id')"
SR_URL="$(confluent schema-registry cluster describe -o json | jq -r '.endpoint_url // .endpoint')"
SR_KEY_JSON="$(confluent api-key create --resource "$SR_ID" --description "mta-sr" -o json)"
SR_API_KEY="$(echo "$SR_KEY_JSON" | jq -r '.api_key // .key')"
SR_API_SECRET="$(echo "$SR_KEY_JSON" | jq -r '.api_secret // .secret')"
echo "    SR_ID=$SR_ID  SR_URL=$SR_URL"

log "Flink compute pool: $POOL_NAME"
POOL_ID="$(confluent flink compute-pool list -o json | jq -r --arg n "$POOL_NAME" '.[] | select(.name==$n) | .id' | head -1)"
if [ -z "$POOL_ID" ]; then
  POOL_ID="$(confluent flink compute-pool create "$POOL_NAME" --cloud "$CLOUD" --region "$REGION" --max-cfu "$FLINK_MAX_CFU" -o json | jq -r '.id')"
fi
run "confluent flink compute-pool use $POOL_ID"
echo "    POOL_ID=$POOL_ID"

log "LLM connection: $CONNECTION_NAME (provider=$LLM_PROVIDER)"
# The in-Flink dispatcher agent (CREATE MODEL -> CREATE AGENT -> AI_RUN_AGENT)
# calls the LLM through this connection — same idea as the F1 demo's
# terraform/modules/llm, created here with the CLI from your deploy.env creds.
# CONN_FLAGS/MODEL_WITH are set per provider, then reused for create + CREATE MODEL.
case "$LLM_PROVIDER" in
  bedrock)
    : "${AWS_BEDROCK_ACCESS_KEY:?set in deploy.env for LLM_PROVIDER=bedrock}"
    : "${AWS_BEDROCK_SECRET_KEY:?set in deploy.env for LLM_PROVIDER=bedrock}"
    : "${BEDROCK_MODEL_ID:?set BEDROCK_MODEL_ID in deploy.env}"
    : "${BEDROCK_REGION:?set BEDROCK_REGION (or REGION) in deploy.env}"
    ENDPOINT="https://bedrock-runtime.${BEDROCK_REGION}.amazonaws.com/model/${BEDROCK_MODEL_ID}/invoke"
    echo "    model=$BEDROCK_MODEL_ID  region=$BEDROCK_REGION"
    echo "    access_key=${AWS_BEDROCK_ACCESS_KEY:0:4}…  secret=***  session_token=$([ -n "${AWS_SESSION_TOKEN:-}" ] && echo present || echo none)"
    TOKEN_FLAG=""
    [ -n "${AWS_SESSION_TOKEN:-}" ] && TOKEN_FLAG="--aws-session-token \"$AWS_SESSION_TOKEN\""
    CONN_FLAGS="--type bedrock --endpoint \"$ENDPOINT\" --aws-access-key \"$AWS_BEDROCK_ACCESS_KEY\" --aws-secret-key \"$AWS_BEDROCK_SECRET_KEY\" $TOKEN_FLAG"
    MODEL_WITH="'provider'='bedrock','task'='text_generation','bedrock.connection'='$CONNECTION_NAME','bedrock.params.max_tokens'='1024'"
    ;;
  googleai|gemini)
    LLM_PROVIDER="googleai"
    : "${GOOGLEAI_API_KEY:?set GOOGLEAI_API_KEY in deploy.env for LLM_PROVIDER=googleai (get one at https://aistudio.google.com/apikey)}"
    ENDPOINT="$GOOGLEAI_ENDPOINT"
    echo "    model=$GEMINI_MODEL_ID"
    echo "    endpoint=$ENDPOINT"
    echo "    api_key=${GOOGLEAI_API_KEY:0:4}…"
    CONN_FLAGS="--type googleai --endpoint \"$ENDPOINT\" --api-key \"$GOOGLEAI_API_KEY\""
    MODEL_WITH="'provider'='googleai','task'='text_generation','googleai.connection'='$CONNECTION_NAME'"
    ;;
  *)
    die "unknown LLM_PROVIDER='$LLM_PROVIDER' (use 'bedrock' or 'googleai')"
    ;;
esac
if ! confluent flink connection list --cloud "$CLOUD" --region "$REGION" -o json 2>/dev/null | jq -e --arg n "$CONNECTION_NAME" '.[] | select(.name==$n)' >/dev/null; then
  run "confluent flink connection create \"$CONNECTION_NAME\" \
    --cloud \"$CLOUD\" --region \"$REGION\" --environment \"$ENV_ID\" \
    $CONN_FLAGS"
else
  echo "    connection already exists"
fi
# The whole AI path depends on this — fail loud now, not later as a model error.
confluent flink connection list --cloud "$CLOUD" --region "$REGION" -o json 2>/dev/null \
  | jq -e --arg n "$CONNECTION_NAME" '.[] | select(.name==$n)' >/dev/null \
  || die "LLM connection '$CONNECTION_NAME' ($LLM_PROVIDER) was not created — check the creds/region in deploy.env and 'confluent flink connection create --help'"

# ---- Flink statement submission ---------------------------------------------
# Strip -- comments, split a .sql file on ';', submit each statement, and wait
# until it reaches COMPLETED (DDL) or RUNNING (continuous INSERT/CTAS).

submit_stmt() {
  local sql="$1" label="$2"
  sql="$(printf '%s' "$sql" | sed -e 's/[[:space:]]*$//')"
  [ -z "$sql" ] && return 0
  log "Flink statement: $label"
  local name="mta-$(echo "$label" | tr '[:upper:] _.' '[:lower:]---')-$RANDOM"
  confluent flink statement create "$name" \
    --sql "$sql" \
    --compute-pool "$POOL_ID" \
    --database "$CLUSTER_NAME" \
    --catalog "$ENV_NAME" \
    -o json >/dev/null
  for _ in $(seq 1 40); do
    local phase
    phase="$(confluent flink statement describe "$name" -o json 2>/dev/null | jq -r '.status.phase // .phase // "PENDING"')"
    case "$phase" in
      RUNNING|COMPLETED) echo "    -> $phase"; return 0 ;;
      FAILED|FAILING)    die "statement '$label' entered $phase — see: confluent flink statement describe $name" ;;
    esac
    sleep 3
  done
  echo "    -> still pending (continuing)"
}

submit_file() {
  local file="$1"
  [ -f "$file" ] || die "missing $file"
  # drop comment lines, then split on semicolons
  local cleaned
  cleaned="$(grep -v '^[[:space:]]*--' "$file" | tr '\n' ' ')"
  local IFS=';'
  local i=0
  for stmt in $cleaned; do
    if [ -n "$(echo "$stmt" | tr -d '[:space:]')" ]; then
      i=$((i+1))
      submit_stmt "$stmt" "$(basename "$file" .sql)#$i"
    fi
  done
}

# Order matters: sources -> pipeline -> forecast -> model -> agent.
submit_file "$FLINK_DIR/01_create_tables.sql"
submit_file "$FLINK_DIR/07_bus_positions.sql"
submit_file "$FLINK_DIR/02_arrival_estimates.sql"
submit_file "$FLINK_DIR/03_headway.sql"
submit_file "$FLINK_DIR/04_headway_alerts.sql"
submit_file "$FLINK_DIR/08_headway_forecast.sql"

# CREATE MODEL (connection made above via the CLI, so we skip 05's CREATE CONNECTION).
# WITH clause is provider-specific ($MODEL_WITH), set in the connection step above.
submit_stmt "CREATE MODEL \`llm_dispatcher_model\` INPUT (\`prompt\` STRING) OUTPUT (\`response\` STRING) WITH ($MODEL_WITH)" "create_model"

submit_file "$FLINK_DIR/06_dispatcher_agent.sql"

# ---- Optional: managed HTTP Source Connector (real JSON feed) ----------------
# Demonstrates the Confluent managed-connector ingest path alongside the live
# producer: polls a JSON endpoint (ALERTS_HTTP_URL, e.g. MTA service alerts) and
# lands records in mta.service_alerts with a JSON Schema in Schema Registry — so
# it shows in Stream Lineage as a real source connector. Gated + non-fatal: a
# config/flag hiccup here must never sink the core Flink pipeline above.
CONN_TEMPLATE="$SCRIPT_DIR/connectors/http_source_service_alerts.json"
if [ "${ENABLE_HTTP_SOURCE:-true}" = "true" ] && [ -n "${ALERTS_HTTP_URL:-}" ] && [ -f "$CONN_TEMPLATE" ]; then
  log "HTTP Source connector: mta-service-alerts-http-source"
  echo "    url=$ALERTS_HTTP_URL  topic=mta.service_alerts"
  CONN_CFG="$(mktemp)"
  # jq (a required dep) safely injects creds + URL, handling any special chars.
  jq --arg k "$KAFKA_API_KEY" --arg s "$KAFKA_API_SECRET" --arg u "$ALERTS_HTTP_URL" \
    '.config."kafka.api.key"=$k | .config."kafka.api.secret"=$s | .config.url=$u' \
    "$CONN_TEMPLATE" > "$CONN_CFG"
  if confluent connect cluster create --config-file "$CONN_CFG" -o json >/dev/null 2>/tmp/mta_conn_err; then
    echo "    -> connector submitted (check: confluent connect cluster list)"
  else
    printf '\033[1;33m    WARN: connector create failed — core pipeline is unaffected.\n    %s\n    Fix flags/config (Console > Connectors > HTTP Source can generate the exact\n    JSON), then re-run, or set ENABLE_HTTP_SOURCE=false to skip.\033[0m\n' \
      "$(tr '\n' ' ' < /tmp/mta_conn_err)" >&2
  fi
  rm -f "$CONN_CFG" /tmp/mta_conn_err
else
  echo "    (skipping HTTP Source connector: set ENABLE_HTTP_SOURCE=true and ALERTS_HTTP_URL in deploy.env to enable)"
fi

# ---- write .env for the producer + dashboard --------------------------------
log "Writing $OUT_ENV"
cat > "$OUT_ENV" <<EOF
# Generated by deploy/provision.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
BOOTSTRAP_SERVERS=$BOOTSTRAP
KAFKA_API_KEY=$KAFKA_API_KEY
KAFKA_API_SECRET=$KAFKA_API_SECRET

SCHEMA_REGISTRY_URL=$SR_URL
SCHEMA_REGISTRY_API_KEY=$SR_API_KEY
SCHEMA_REGISTRY_API_SECRET=$SR_API_SECRET

ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY:-}
DISPATCHER_MODEL=${DISPATCHER_MODEL:-claude-haiku-4-5-20251001}

MTA_FEEDS=all
POLL_INTERVAL_SECONDS=15
BUS_ENABLED=true
BUS_AGENCIES=all

TOPIC_VEHICLE_POSITIONS=mta.vehicle_positions
TOPIC_TRIP_UPDATES=mta.trip_updates
TOPIC_BUS_POSITIONS=mta.bus_positions
TOPIC_ARRIVAL_ESTIMATES=mta.arrival_estimates
TOPIC_HEADWAY_ALERTS=mta.headway_alerts
TOPIC_RECOMMENDATIONS=mta.dispatcher_decisions
DASHBOARD_GROUP_ID=mta-dashboard
EOF

log "Done. Environment=$ENV_ID Cluster=$CLUSTER_ID Pool=$POOL_ID"
cat <<'NEXT'

Next:
  python scripts/fetch_static_gtfs.py && python scripts/build_shapes.py
  python producers/mta_producer.py          # streams subway + buses
  uvicorn dashboard.app:app --port 8000      # dashboard

Then open Confluent Cloud > your environment > Stream Lineage and screenshot it
for the submission form.
NEXT

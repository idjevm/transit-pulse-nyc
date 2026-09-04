# Transit Pulse NYC — Self-Service Walkthrough & Guide

Modeled directly on the architecture of [`demo-confluent-intelligence-f1`](https://github.com/confluentinc/demo-confluent-intelligence-f1) for **Confluent AI Day NYC**. We swap Formula 1 tire telemetry for live NYC subway and bus streams.

```
MTA GTFS-RT Feeds (Subway & Buses)
       │
       ▼ [producers/mta_producer.py]
Confluent Kafka Topics (Avro + Schema Registry)
  ├── mta_vehicle_positions (subway train pings)
  ├── mta_trip_updates      (arrival predictions)
  └── mta_bus_positions     (live bus GPS + heading)
       │
       ▼ [Confluent Cloud Flink SQL (flink/*.sql)]
  01_create_tables.sql       -> Back topics with Avro schemas
  07_bus_positions.sql       -> Live bus GPS table
  02_arrival_estimates.sql   -> Dedupes & computes live ETAs (upsert)
  03_headway.sql             -> MATCH_RECOGNIZE consecutive arrivals -> headway_seconds
  04_headway_alerts.sql      -> BUNCHING / GAP alerts (thresholds & ML_DETECT_ANOMALIES)
  08_headway_forecast.sql    -> Predictive CEP forecasting (PREDICTED_BUNCHING / GAP)
  05_create_model.sql        -> Registers Bedrock / Claude Sonnet connection
  06_dispatcher_agent.sql    -> CREATE AGENT + AI_RUN_AGENT (in-stream LLM copilot)
       │
       ▼
Local Pit Wall / Live Dashboard (FastAPI + WebSocket @ 4Hz + Leaflet)
  ├── Live Map: http://localhost:8000
  ├── Real-Time Context Engine (RTCE): MCP server for coding agents
  └── Interactive Claude Agents: Rider Advisor, Operator Risk, Route Designer
```

---

## 1. Where to See the App Running

Once services are launched, there are two primary destinations:

### A. The Live Map Dashboard (Local)
👉 **[http://localhost:8000](http://localhost:8000)** (or `http://127.0.0.1:8000`)
- **Live Fleet View**: Real MTA subway trains moving on official route lines + over 3,000 live city buses with heading direction arrows.
- **Arrivals Board**: Real-time countdown clocks per station.
- **Alert Feed**: Real-time bunching (`<150s`) and headway gap (`>900s`) pulsing alert markers.
- **In-Stream AI Copilot**: Streaming recommendations generated directly by Flink and Bedrock (`HOLD TRAIN`, `GAP FILL`, `SKIP-STOP`, `MONITOR`).
- **Interactive AI Agents**: Rider advisor, fleet risk predictor, and new-route designer grounded in the live system state.
- **Health check**: [http://localhost:8000/healthz](http://localhost:8000/healthz)

### B. Confluent Cloud Console (Cloud)
👉 **[https://confluent.cloud](https://confluent.cloud)**
- **Stream Lineage**: Open your environment (`default` / `env-3kwon2`) $\to$ **Stream Lineage**. You will see the end-to-end graph connecting your producer topics, continuous Flink queries, and the in-stream LLM agent.
- **Flink SQL Workspace**: Open **Flink** $\to$ **SQL Workspaces**. Select catalog `default` and database `lkc-k8kpr3m` to inspect or run live queries.
- **Topics**: Open cluster `cluster_0` $\to$ **Topics** to view messages flowing into `mta_vehicle_positions`, `mta_headway_alerts`, and `mta_dispatcher_decisions`.
- **Real-Time Context Engine**: Topics $\to$ `mta_vehicle_positions` / `mta_headway_alerts` $\to$ view RTCE MCP enablement.

---

## 2. Before You Start (Prerequisites)

1. **Confluent CLI (v4+) & jq**:
   ```bash
   brew install confluentinc/tap/cli jq
   confluent login
   ```
2. **Python 3.10+**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **AWS Bedrock Credentials**:
   An IAM user with `bedrock:InvokeModel` permissions on `us.anthropic.claude-sonnet-4-5-20250929-v1:0` in `us-east-1` (same as the F1 demo).

---

## 3. Provisioning Confluent Cloud

[`deploy/provision.sh`](file:///Users/idjevm/projects/transit-pulse-nyc/deploy/provision.sh) automatically sets up the environment, compute pool, Bedrock LLM connection, and submits every Flink SQL statement in order:

```bash
# 1. Verify live MTA GTFS-RT feed decoding (no Kafka needed)
python scripts/smoke_test.py

# 2. Configure credentials in deploy/deploy.env
# CLOUD="aws", REGION="us-east-1", LLM_PROVIDER="bedrock"
# Fill in AWS_BEDROCK_ACCESS_KEY and AWS_BEDROCK_SECRET_KEY

# 3. Run the one-command provisioner
./deploy/provision.sh
```

---

## 4. Run the Producer & Local Dashboard

```bash
# 1. Download static GTFS station coordinates and build subway route GeoJSON
python scripts/fetch_static_gtfs.py
python scripts/build_shapes.py

# 2. Start the MTA live stream producer (runs in background or separate tab)
python producers/mta_producer.py

# 3. Start the dashboard web server (runs in separate tab)
uvicorn dashboard.app:app --host 127.0.0.1 --port 8000
```

---

## 5. Inspecting Flink SQL in Confluent Cloud

In the Confluent Cloud Console, open the **Flink SQL Workspace** (catalog `default`, database `lkc-k8kpr3m`), and run these verification queries:

### A. Inspect Live Headway Alerts
```sql
SELECT
  route_id,
  direction,
  stop_name,
  alert_type,
  severity,
  headway_seconds,
  arrival_time
FROM `mta_headway_alerts`
WHERE alert_type IN ('BUNCHING', 'GAP');
```

### B. Inspect In-Stream AI Dispatcher Decisions
```sql
SELECT
  route_id,
  direction,
  stop_name,
  alert_type,
  action,
  dispatcher_note,
  rider_message,
  reasoning
FROM `mta_dispatcher_decisions`;
```

### C. Inspect Headway Predictions
```sql
SELECT
  route_id,
  direction,
  stop_name,
  curr_trip,
  headway_seconds,
  predicted_headway,
  forecast_type,
  arrival_time
FROM `mta_headway_forecast`
WHERE forecast_type <> 'STABLE';
```

---

## 6. Real-Time Context Engine (RTCE)

Transit Pulse NYC supports Confluent's **Real-Time Context Engine (RTCE)** to expose live transit data as an **MCP tool** for AI coding assistants (Claude Code, Cursor, Windsurf, Codex).

To register the RTCE MCP server:
```bash
python scripts/setup_rtce.py
```

Once connected, you can ask your AI agent in plain English:
- *"Which subway lines currently have train bunching in Manhattan?"*
- *"What is the current headway and vehicle status for the 2 train at Times Square?"*
- *"Are there any active service gaps on the A or C lines right now?"*

---

## 7. Teardown

When you are finished with the demo or workshop:
```bash
./deploy/teardown.sh
```
This safely deletes the Flink statements, compute pool, and cluster resources.


# Transit Pulse NYC — Self-Service Walkthrough & Guide

Modeled on the architecture of real-time streaming intelligence platforms for **Confluent AI Day NYC**. We combine live MTA telemetry, weather, and passenger demand with Flink SQL and in-stream AI.

```
MTA GTFS-RT Feeds (Subway & Buses) ──► Kafka Topics (Avro + Schema Registry)
  ├── mta_vehicle_positions (subway train pings)
  ├── mta_trip_updates      (arrival predictions)
  └── mta_bus_positions     (live bus GPS + heading)

External Sources ──► Confluent Managed Connectors
  ├── Open Data JSON Feed ──► [HttpSource]   ──► mta_service_alerts (JSON Schema)
  ├── Open-Meteo NYC      ──► [HttpSource]   ──► nyc_weather_events (JSON Schema)
  └── Synthetic Crowds    ──► [DatagenSource]──► mta_passenger_surges (Avro + SR)
       │
       ▼ [Confluent Cloud Flink SQL (flink/*.sql)]
  01_create_tables.sql       -> Back topics with Avro schemas
  07_bus_positions.sql       -> Live bus GPS table
  02_arrival_estimates.sql   -> Dedupes & computes live ETAs (upsert)
  03_headway.sql             -> MATCH_RECOGNIZE consecutive arrivals -> headway_seconds
  04_headway_alerts.sql      -> BUNCHING / GAP alerts (thresholds & ML_DETECT_ANOMALIES)
  08_headway_forecast.sql    -> Predictive CEP forecasting (PREDICTED_BUNCHING / GAP)
  05_create_model.sql        -> Registers Bedrock or Google AI (Gemini) connection
  06_dispatcher_agent.sql    -> CREATE AGENT + AI_RUN_AGENT (in-stream LLM copilot)
       │
       ├─► [HttpSink Connector] ──► External Webhook (Webhook.site / Slack / Ops bridge)
       │   (mta_dispatcher_decisions)
       ▼
Local Pit Wall / Live Dashboard (FastAPI + WebSocket @ 4Hz + Leaflet)
  ├── Live Map: http://localhost:8000 (subways + ~2,700 GPS buses)
  ├── Live Weather & Station Crowd Surges Indicators
  ├── Next Arrivals Board: Live countdown clocks per station
  ├── AI Dispatcher Card with Live Webhook Stream link
  ├── Real-Time Context Engine (RTCE): MCP server for coding agents
  └── Interactive AI Copilots: Rider Advisor, Operator Risk, Route Designer
      (Gemini 3.1 / Pro with Claude fallback)
```

---

## 1. Where to See the App Running

Once services are launched, there are two primary destinations:

### A. The Live Map Dashboard (Local)
👉 **[http://localhost:8000](http://localhost:8000)** (or `http://127.0.0.1:8000`)
- **Live Fleet View**: Real MTA subway trains moving on official route lines + ~2,700 live city buses with heading direction arrows.
- **Live Weather Badge**: Real-time NYC temperature, precipitation, and conditions in topbar (`⛅ 15°C · Overcast`).
- **Crowd Surges Counter**: Active station turnstile crowd alerts.
- **Arrivals Board**: Real-time countdown clocks across stations and routes.
- **Alert Feed**: Real-time bunching (`<150s`) and headway gap (`>900s`) pulsing alert markers.
- **In-Stream AI Copilot**: Streaming recommendations generated directly by Flink and Bedrock/Gemini (`HOLD TRAIN`, `GAP FILL`, `SKIP-STOP`, `MONITOR`).
- **Live Webhook Stream Link**: Click `webhook live ↗` on the Dispatcher card to watch decisions hit external webhooks in real time.
- **Interactive AI Copilots**: Rider advisor, fleet risk predictor, and new-route designer grounded in fleet headways, weather, and crowd surges.
- **Health check**: [http://localhost:8000/healthz](http://localhost:8000/healthz)

### B. Confluent Cloud Console (Cloud)
👉 **[https://confluent.cloud](https://confluent.cloud)**
- **Stream Lineage**: Open your environment $\to$ **Stream Lineage**. You will see the end-to-end graph connecting your managed connectors (Sources and Sink), producer topics, continuous Flink queries, and the in-stream LLM agent.
- **Connectors**: Open your cluster $\to$ **Connectors** to inspect the 4 running managed connectors (`mta-service-alerts-http-source`, `mta-weather-http-source`, `mta-passenger-surges-datagen`, and `mta-dispatcher-decisions-http-sink`).
- **Flink SQL Workspace**: Open **Flink** $\to$ **SQL Workspaces**. Select catalog `default` and database `<your-cluster-id>` to inspect or run live queries.
- **Topics & Schema Registry**: View messages flowing into topics governed by Avro and JSON Schema in Schema Registry. Full schema listing is available in [`docs/SCHEMAS.md`](SCHEMAS.md).

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
3. **LLM Credentials** (one of):
   - **Google AI / Gemini** (optional): `GEMINI_API_KEY` from [Google AI Studio](https://aistudio.google.com/apikey) for interactive copilots and/or in-Flink model (`gemini-3.1-pro-preview` / `gemini-pro-latest`).
   - **AWS Bedrock** (optional): An IAM user with `bedrock:InvokeModel` on `us.anthropic.claude-sonnet-4-5-20250929-v1:0` in `us-east-1`.

---

## 3. Provisioning Confluent Cloud

[`deploy/provision.sh`](file:///Users/idjevm/projects/transit-pulse-nyc/deploy/provision.sh) automatically sets up the environment, compute pool, LLM connection, managed connectors, and submits every Flink SQL statement in order:

```bash
# 1. Verify live MTA GTFS-RT feed decoding (no Kafka needed)
python scripts/smoke_test.py

# 2. Configure credentials in deploy/deploy.env
# CLOUD="aws", REGION="us-east-1", LLM_PROVIDER="bedrock" (or "googleai")
# Fill in keys + region in deploy/deploy.env

# 3. Run the one-command provisioner
./deploy/provision.sh
```

`provision.sh` prints the IDs you'll need in the Console steps below — `ENV_ID`,
`CLUSTER_ID`, and `POOL_ID`. Use those in place of the `<your-...>` placeholders.

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

In the Confluent Cloud Console, open the **Flink SQL Workspace** (catalog `default`, database `<your-cluster-id>` — the `CLUSTER_ID` printed by `provision.sh`), and run these verification queries:

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

### D. Inspect Managed Connector Topics (Weather & Passenger Surges)
In your terminal or Confluent Console **Topics** tab:
```bash
# Ingested by HttpSource from Open-Meteo
confluent kafka topic consume nyc_weather_events --cluster "$CLUSTER_ID" --value-format json

# Generated by DatagenSource for station turnstiles
confluent kafka topic consume mta_passenger_surges --cluster "$CLUSTER_ID" --value-format avro
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
./deploy/teardown.sh          # default: preserves the environment and Kafka cluster
./deploy/teardown.sh --all    # also deletes the environment (never "default")
```
The default run tears down the HTTP connector, Flink statements, Flink
connections, the compute pool, Kafka topics, Schema Registry subjects, and the
app API keys, and best-effort drops the Flink model/agent. It **preserves** the
environment and the Kafka cluster. `--all` additionally deletes the environment
(only when it isn't named `default`); the Kafka cluster is never deleted
automatically.


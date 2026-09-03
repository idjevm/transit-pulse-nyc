# Confluent AI Day NYC — App Submission

## App name
Transit Pulse NYC — Real-Time MTA Streaming Intelligence & Dispatcher Copilot

## One-line pitch
Live NYC subway feeds stream into Kafka, Flink SQL detects train bunching and
service gaps, and a Flink Streaming Agent turns each alert into a dispatcher action
and a rider message in real time.

## What it does
We ingest the real MTA GTFS-Realtime subway feed (train positions + arrival
predictions, updating every ~5-30s) into Confluent Cloud. Flink SQL computes live
arrival ETAs, reconstructs the headway (time between successive trains) at every
station with `MATCH_RECOGNIZE`, and flags bunching (trains too close) and gaps
(riders waiting too long). Each alert is handed to a Flink Streaming Agent that
calls an LLM through `AI_RUN_AGENT` and returns a concrete dispatcher action (HOLD
TRAIN / GAP FILL / SKIP-STOP / MONITOR) plus a calm rider-facing message. A live
map dashboard shows trains, ETAs, alerts, and the AI recommendations.

The map is a high-precision live view: real route-line geometry (from static GTFS
`shapes.txt`) drawn as colored polylines, with subway trains and **~2,700 live NYC
buses** riding on top — buses stream their true GPS lat/lon + heading, so their
icons sit at real positions and rotate to direction of travel. Riders/dispatchers
can toggle Subway/Bus and filter to a specific line, and bunching/gap alerts appear
as pulsing markers on the map.

## Confluent platform usage
- **Connectors / ingest:** a producer streams the live MTA GTFS-RT feeds into
  Kafka — subway positions + trip updates (`mta.vehicle_positions`,
  `mta.trip_updates`) and ~2,700 live buses with real GPS (`mta.bus_positions`).
  Drop-in replaceable with an HTTP Source Connector.
- **Stream Governance / Schema Registry:** every topic is Avro; schemas are created
  and registered by the Flink `CREATE TABLE` DDL, and the producer serializes
  against them (`use.latest.version=true`, `auto.register.schemas=false`).
- **Stream Processing / Flink SQL:**
  - deduplication (`ROW_NUMBER`) for latest arrival prediction per train/stop,
  - live ETA transform (`UNIX_TIMESTAMP`),
  - `MATCH_RECOGNIZE` CEP to measure headway between consecutive distinct trains,
  - bunching/gap classification, with an optional `ML_DETECT_ANOMALIES` variant
    that learns each station's normal headway.
- **Flink-driven AI:** `CREATE MODEL` + `CREATE AGENT` + `AI_RUN_AGENT` run the LLM
  as a Flink operator inside a `CREATE TABLE ... AS SELECT`, writing decisions to
  `mta.dispatcher_decisions`. The AI is part of the stream, not a side service.
- **Predictive Flink:** a headway-forecast job (`mta_headway_forecast`) projects the
  next headway per station and raises `PREDICTED_BUNCHING` / `PREDICTED_GAP` *before*
  it happens — deterministic by default, with an optional `ML_FORECAST` variant.

## Interactive AI agents
Beyond the streaming dispatcher, the dashboard exposes three on-demand Claude agents,
each grounded in the live snapshot (fleet, alerts, dispatcher decisions):
- **Rider advisor** — "what should I watch for going from X to Y right now?"
- **Operator prediction** — current risk, what will degrade next, actions to take.
- **Route designer** — proposes a new bus route (waypoints + rationale) and draws it
  on the map. The in-Flink LLM uses AWS Bedrock; these interactive agents call Claude
  directly, so both the streaming and interactive AI paths are demonstrated.

## Business impact
Bunching and gaps are the single largest driver of unreliable subway service.
Today, dispatchers react to them manually and riders get little warning. This turns
the raw real-time feed into (1) prioritized, actionable dispatcher instructions and
(2) accurate rider ETAs and service messages — the same decision loop transit
agencies pay for, built entirely on the data streaming platform.

## Most-Flink-Driven notes (iPhone prize)
The heaviest lifting is in Flink SQL: stateful dedup, a `MATCH_RECOGNIZE` pattern
for event-to-event headway, anomaly classification, and the LLM agent itself
running via `AI_RUN_AGENT`, plus a predictive headway-forecast job. The pipeline is
eight chained Flink statements; the only non-Flink code is a thin ingest producer
and the dashboard.

## Tech stack
Confluent Cloud (Kafka, Schema Registry, Flink), Flink SQL + Streaming Agents,
Python (confluent-kafka producer + FastAPI dashboard), Leaflet map. LLM via a Flink
model connection (AWS Bedrock / OpenAI / Azure OpenAI).

## Repo / demo
- Code: this repository (`mta-streaming-intelligence`).
- Live data sources: MTA subway GTFS-Realtime + MTA Bus Time GTFS-Realtime
  (~2.7k buses with real GPS); route-line geometry from static GTFS.
- 2-minute demo flow: smoke test the live feed → producer into Kafka → Flink alerts
  firing → AI dispatcher recommendation → dashboard map + panels.

# Confluent AI Day NYC — App Submission

## App name
Transit Pulse NYC — Real-Time MTA Streaming Intelligence & Dispatcher Copilot

## One-line pitch
Live NYC subway feeds stream into Kafka, Flink SQL detects train bunching and
service gaps, and a Flink Streaming Agent turns each alert into a dispatcher action
and a rider message in real time.

## What it does
We ingest the real MTA GTFS-Realtime subway feed (train positions + arrival
predictions, updating every ~5-30s) into Confluent Cloud alongside ~2,700 live MTA buses
streaming true GPS lat/lon and heading. Flink SQL computes live arrival ETAs, reconstructs
the headway (time between successive trains) at every station with `MATCH_RECOGNIZE`, and
flags bunching (trains too close) and gaps (riders waiting too long). Each alert is handed
to a Flink Streaming Agent that calls an LLM through `AI_RUN_AGENT` and returns a concrete
dispatcher action (HOLD TRAIN / GAP FILL / SKIP-STOP / MONITOR) plus a calm rider-facing
message into `mta_dispatcher_decisions`. A Confluent managed **HTTP Sink Connector** streams
these decisions out to external incident webhooks (e.g. Webhook.site / Slack) in real time.

A high-performance live map dashboard displays subway trains, buses, station countdown
boards, live NYC weather conditions (`nyc_weather_events`), station crowd surge indicators
(`mta_passenger_surges`), and the AI dispatcher stream with a live webhook inspection link.

## Confluent platform usage
- **Connectors / ingest & outgest:** Four Confluent Cloud fully-managed connectors:
  1. **HTTP Source Connector (`mta-service-alerts-http-source`)**: Ingests real-time MTA
     service alerts from NY Open Data into `mta_service_alerts` (JSON Schema in SR).
  2. **HTTP Source Connector (`mta-weather-http-source`)**: Continuously polls live NYC
     meteorological conditions (temp, precipitation, wind) from Open-Meteo into
     `nyc_weather_events` (JSON Schema in SR).
  3. **Datagen Source Connector (`mta-passenger-surges-datagen`)**: Generates real-time
     synthetic station turnstile tap spikes into `mta_passenger_surges` (Avro in SR).
  4. **HTTP Sink Connector (`mta-dispatcher-decisions-http-sink`)**: Continuously streams
     every AI dispatcher decision from `mta_dispatcher_decisions` out to external ops webhooks.
- **Stream Governance / Schema Registry:** Every topic is governed by Schema Registry using
  Avro or JSON Schema. Full schema definitions are documented in [`docs/SCHEMAS.md`](SCHEMAS.md).
- **Stream Processing / Flink SQL:**
  - deduplication (`ROW_NUMBER`) for latest arrival prediction per train/stop,
  - live ETA transform (`UNIX_TIMESTAMP`),
  - `MATCH_RECOGNIZE` CEP to measure headway between consecutive distinct trains,
  - bunching/gap classification, with an optional `ML_DETECT_ANOMALIES` variant
    that learns each station's normal headway.
- **Flink-driven AI:** `CREATE MODEL` + `CREATE AGENT` + `AI_RUN_AGENT` run the LLM
  as a Flink operator inside a continuous query, writing structured decisions to
  `mta_dispatcher_decisions`. The AI is an active operator in the stream, not a sidecar.
- **Predictive Flink:** a headway-forecast job (`mta_headway_forecast`) projects the
  next headway per station and raises `PREDICTED_BUNCHING` / `PREDICTED_GAP` *before*
  it happens — deterministic by default, with an optional `ML_FORECAST` variant.

## Interactive AI copilots
Beyond the streaming dispatcher, the dashboard exposes three on-demand copilots,
each grounded in the live snapshot (fleet headways, alerts, live NYC weather, and crowd surges):
- **Rider advisor** — "what should I watch for going from X to Y right now?"
- **Fleet risk predictor** — current operational bottlenecks, what will degrade next, actions to take.
- **Bus route designer** — proposes an optimized or new bus route (waypoints + rationale) and renders
  it dynamically on the map.
The in-Flink dispatcher runs on AWS Bedrock or Google Gemini; the interactive copilots use
Google Gemini (`gemini-3.1-pro-preview` / `gemini-pro-latest`) when a Google API key is set
and fall back to Anthropic Claude otherwise.

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
Confluent Cloud (Kafka, Schema Registry, Flink, Managed HTTP Source & Sink Connectors, DatagenSource),
Flink SQL + Streaming Agents (`AI_RUN_AGENT`), Python (confluent-kafka producer + FastAPI dashboard),
Leaflet map. In-stream LLM via Flink model connection (AWS Bedrock / Google AI Studio); interactive
copilots via Google Gemini / Anthropic Claude.

## Repo / demo
- Code: this repository (`transit-pulse-nyc`).
- Live schemas: [`docs/SCHEMAS.md`](SCHEMAS.md).
- Live data sources: MTA subway GTFS-Realtime + MTA Bus Time GTFS-Realtime
  (~2.7k buses with real GPS); route-line geometry from static GTFS; Open-Meteo weather; NY Open Data.
- 2-minute demo flow: smoke test the live feed → producer into Kafka → Flink alerts
  firing → AI dispatcher recommendation → HTTP sink webhook outgest → dashboard map + panels.

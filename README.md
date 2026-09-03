# Transit Pulse NYC

*MTA Streaming Intelligence — real-time NYC transit intelligence on Confluent.*

Real-time NYC subway intelligence on the **Confluent Data Streaming Platform**.

Live MTA GTFS-Realtime feeds flow into Kafka; **Flink SQL** computes live arrival
estimates and detects train **bunching** and **service gaps**; and a **Flink
Streaming Agent** (LLM running *inside* Flink via `AI_RUN_AGENT`) turns each alert
into a plain-English **dispatcher action** and a **rider-facing message**.

The dashboard is a high-precision live map: real subway route lines (from static
GTFS geometry) with subway trains and **~2,700 city buses** — buses carry real GPS
lat/lon + heading — moving on top, plus mode/route filters and on-map alert
markers. Below the map, three **interactive Claude agents** (rider advisor,
operator prediction, and a new-bus-route designer) answer questions grounded in
the live fleet state.

Built for **Confluent AI Day NYC**, modeled on the architecture of
[`confluentinc/demo-confluent-intelligence-f1`](https://github.com/confluentinc/demo-confluent-intelligence-f1) —
we keep its shape (topics-as-Flink-tables, `ML_DETECT_ANOMALIES`, `CREATE AGENT` +
`AI_RUN_AGENT`, FastAPI/websocket dashboard) and swap F1 tire telemetry for the NYC
subway.

## Why this wins

| Prize criterion | How we hit it |
|---|---|
| **Business impact** | Bunching and gaps are the #1 driver of unreliable transit. Dispatchers make hold/gap-fill calls in real time; riders need accurate ETAs. |
| **Connectors / ingest** | A producer ingests the *real* MTA GTFS-RT feed (updates every ~5-30s). Swappable for an HTTP Source Connector. |
| **Stream processing (Flink)** | Deduplication, ETA transforms, `MATCH_RECOGNIZE` for consecutive-arrival headway, and anomaly detection on the headway series. |
| **Flink-driven AI** | The dispatcher LLM is a **Flink operator** (`AI_RUN_AGENT`), not a side service — the recommendation is produced by a `CREATE TABLE ... AS SELECT`. |
| **Stream Governance** | Every topic is Avro + Schema Registry; schemas are created by the Flink `CREATE TABLE` DDL. |

## Architecture

```
MTA GTFS-RT ──producers/mta_producer.py──► Kafka (Avro + Schema Registry)
  subway + bus                              mta.vehicle_positions   (train pings)
                                            mta.trip_updates         (arrival predictions)
                                            mta.bus_positions        (live bus GPS + heading)
        │
        ▼  Confluent Cloud Flink SQL (flink/*.sql, run in order)
   01  CREATE TABLE (register schemas / back topics)
   02  mta_arrival_estimates   live ETA per stop/route/direction        ──► rider board
   03  mta_headway             MATCH_RECOGNIZE consecutive arrivals → headway_seconds
   04  mta_headway_alerts      BUNCHING / GAP  (threshold + optional ML_DETECT_ANOMALIES)
   05  llm_dispatcher_model    CREATE MODEL  (Bedrock/Claude connection)
   06  dispatcher_agent + mta_dispatcher_decisions
                               CREATE AGENT + AI_RUN_AGENT  ← the LLM runs IN Flink
   07  mta_bus_positions       live bus GPS source table
   08  mta_headway_forecast    PREDICT next headway → PREDICTED_BUNCHING / GAP
        │
        ▼  dashboard/ (FastAPI + websocket + Leaflet)
   live map (route lines + train/bus icons + alert markers, mode/route filters),
   next-arrivals board, bunching/gap alert feed, AI dispatcher panel,
   + three interactive Claude agents (advisor / operator / route designer)
```

## Topics

| Topic | Produced by | Description |
|-------|-------------|-------------|
| `mta.vehicle_positions` | producer | current train position + status (high-volume stream) |
| `mta.trip_updates` | producer | predicted arrival per stop |
| `mta.bus_positions` | producer | live bus GPS (real lat/lon + heading), ~2.7k buses |
| `mta.arrival_estimates` | Flink | ETA seconds per stop/route/direction |
| `mta.headway` | Flink | headway between consecutive trains at a stop |
| `mta.headway_alerts` | Flink | BUNCHING / GAP alerts |
| `mta.dispatcher_decisions` | Flink Streaming Agent | LLM dispatcher action + rider message |

## Prerequisites

- Python 3.11+ and, for one-command provisioning, the `confluent` CLI (v4+) + `jq`
  (`brew install confluentinc/tap/cli jq`).
- Confluent Cloud: a Cloud API key (the provisioner creates the cluster, Schema
  Registry, Flink pool, and connection for you).
- **AWS Bedrock** (Claude) credentials for the in-Flink dispatcher LLM, and an
  **Anthropic API key** for the interactive agents. See `flink/05_create_model.sql`.

## Provision (one command)

`deploy/provision.sh` stands up the entire Confluent side with the `confluent` CLI
(environment, cluster, API keys, Schema Registry, Flink pool, Bedrock connection,
and every Flink statement in order), then writes `.env`.

```bash
pip install -r requirements.txt
cp deploy/deploy.env.example deploy/deploy.env   # fill in keys + region
./deploy/provision.sh                            # builds everything, writes .env

python scripts/fetch_static_gtfs.py   # stop names + lat/lon for subway trains
python scripts/build_shapes.py        # subway route-line geometry for the map
python producers/mta_producer.py      # stream real MTA feed -> Kafka (subway + buses)
uvicorn dashboard.app:app --port 8000 # dashboard on http://localhost:8000
```

Teardown: `./deploy/teardown.sh`. Full details and the manual (click-through
Flink workspace) alternative are in [`deploy/README.md`](deploy/README.md).

Then open **Confluent Cloud → your environment → Stream Lineage** and screenshot it
for the submission form (that screenshot is required and must be real).

## Layout

```
mta-streaming-intelligence/
├── README.md
├── requirements.txt
├── .env.example
├── deploy/                    # one-command Confluent provisioning (CLI-driven)
│   ├── provision.sh           # build env/cluster/pool/connection + submit Flink
│   ├── teardown.sh            # delete the environment and everything under it
│   ├── deploy.env.example     # keys + region (copy to deploy.env)
│   └── README.md
├── flink/                     # Flink SQL, run in numeric order
│   ├── 01_create_tables.sql
│   ├── 02_arrival_estimates.sql
│   ├── 03_headway.sql
│   ├── 04_headway_alerts.sql
│   ├── 05_create_model.sql
│   ├── 06_dispatcher_agent.sql
│   ├── 07_bus_positions.sql   # live bus GPS source table
│   └── 08_headway_forecast.sql # predictive bunching/gap (operator prediction)
├── producers/
│   ├── config.py              # env-driven settings (mirrors datagen/config.py)
│   ├── feeds.py               # MTA subway GTFS-RT feed URLs
│   ├── bus_feeds.py           # MTA Bus Time GTFS-RT feed + agency helpers
│   ├── stops.py               # stop_id -> name/lat/lon lookup (static GTFS)
│   └── mta_producer.py        # GTFS-RT (subway + bus) -> Avro -> Kafka
├── dashboard/
│   ├── app.py                 # entrypoint (uvicorn dashboard.app:app)
│   ├── server.py              # FastAPI + websocket + /api/shapes
│   ├── consumer.py            # Kafka -> DashboardState
│   ├── state.py               # thread-safe snapshot
│   ├── agents.py              # interactive Claude agents (advisor/operator/route)
│   └── static/                # index.html, app.js, styles.css (Leaflet)
├── scripts/
│   ├── smoke_test.py          # decode the live feed, print stats
│   ├── fetch_static_gtfs.py   # download stops.txt for map coordinates
│   └── build_shapes.py        # build subway route-line geometry (GeoJSON)
└── docs/
    └── SUBMISSION.md          # answers for the AI Day submission form
```

## The map

Modeled on modern live transit view: static GTFS `shapes.txt` gives the real
route-line geometry (drawn as colored polylines, served from `/api/shapes`), and
live vehicles ride on top — subway trains as MTA bullets (snapped to station
coords) and buses as heading-rotated icons at their true GPS position. Controls
let you toggle **Subway / Bus** and filter to a **specific line/route**; bunching
and gap **alerts** render as pulsing markers at the involved train's location.

## AI agents

Two tiers of Claude, matching where the work belongs:

- **In Flink (streaming):** the dispatcher agent (`05`/`06`) runs the LLM *inside*
  Flink via `AI_RUN_AGENT`, turning each bunching/gap alert into a dispatcher
  action + rider message as rows land. Job `08` adds a **predictive** headway
  forecast (flags `PREDICTED_BUNCHING` / `PREDICTED_GAP` before it happens).

- **Interactive (dashboard):** three on-demand agents in `dashboard/agents.py`,
  each grounded in the live snapshot (fleet, alerts, dispatcher decisions):
  - **Rider advisor** — "what should I watch for going from X to Y right now?"
  - **Operator prediction** — current risk, what degrades next, actions to take.
  - **Route designer** — proposes a new bus route (waypoints + rationale) and
    draws it on the map as a dashed cyan line.

  These call Claude directly (`ANTHROPIC_API_KEY`) via `POST /api/advisor`,
  `/api/operator`, `/api/route-designer`.

## Data sources

- **Subway** GTFS-Realtime (no key): feed URLs in [`producers/feeds.py`](producers/feeds.py).
- **Bus** GTFS-Realtime — MTA Bus Time / OneBusAway NYC
  (`https://gtfsrt.prod.obanyc.com/vehiclePositions`), ~2,700 live buses with real
  lat/lon + heading; public feed currently needs no key (an optional free key is
  supported). See [`producers/bus_feeds.py`](producers/bus_feeds.py).
- **Static GTFS** (stop coordinates + route geometry) from
  `http://web.mta.info/developers/data/nyct/subway/google_transit.zip`.

# Provisioning (Confluent Cloud, one command)

`provision.sh` stands up the whole Confluent side with the `confluent` CLI, then
writes `../.env` so the producer and dashboard run immediately. Teardown is one
command too.

## Prerequisites (macOS)

```bash
brew install confluentinc/tap/cli jq
confluent version    # v4+ recommended
confluent login      # browser / SSO — provision.sh reuses this session
```

You also need:
- **Confluent Cloud auth.** Simplest on a Mac: just `confluent login` (browser/SSO)
  and the script reuses that session. For a fully non-interactive run instead, put a
  **Cloud API key** (Console → top-right menu → *Cloud API keys*, created by an
  OrganizationAdmin) in `deploy.env` — the script logs in with it automatically.
- **An in-Flink LLM**, one of:
  - **AWS Bedrock** (`LLM_PROVIDER=bedrock`, the F1-demo default) — AWS credentials
    with `bedrock:InvokeModel` on the model.
  - **Google AI / Gemini** (`LLM_PROVIDER=googleai`) — an API key from
    [Google AI Studio](https://aistudio.google.com/apikey) (works with Gemini Pro).
- An **Anthropic API key** for the interactive agents (rider/operator/route
  designer run Claude as a side service, not in Flink).

## Run

```bash
cp deploy/deploy.env.example deploy/deploy.env
$EDITOR deploy/deploy.env            # fill in keys + region
./deploy/provision.sh
```

What it creates: environment (+ Schema Registry), Kafka cluster, Kafka + SR API
keys, a Flink compute pool, a Bedrock connection, and every Flink statement in
order (source tables → arrival/headway pipeline → predictive forecast → model →
dispatcher agent). Then it writes `../.env`.

```bash
python scripts/fetch_static_gtfs.py && python scripts/build_shapes.py
python producers/mta_producer.py           # subway + ~2.7k live buses
uvicorn dashboard.app:app --port 8000       # dashboard on :8000
```

Verify: `python scripts/smoke_test.py` prints live feed counts (no Kafka needed),
and `curl localhost:8000/healthz` should return `{"status":"ok","live":true,...}`.

Open **Confluent Cloud → your environment → Stream Lineage** and screenshot it for
the submission form (that screenshot is required and must be real, not AI made).

## Managed HTTP Source Connector (real JSON feed)

Alongside the GTFS-RT producer, `provision.sh` can stand up a Confluent
**fully-managed HTTP Source Connector** that polls a JSON endpoint into
`mta_service_alerts` (JSON Schema registered in Schema Registry) — this is the
"Use of Confluent Connector(s)" path, and it shows up as a source in Stream
Lineage.

Enable it in `deploy.env`:

```bash
export ENABLE_HTTP_SOURCE="true"
export ALERTS_HTTP_URL="https://data.ny.gov/resource/<dataset-id>.json?\$limit=200&\$order=:updated_at%20DESC"
```

`ALERTS_HTTP_URL` can be any JSON feed; the example is MTA service alerts on
data.ny.gov (Socrata, no key for modest polling) — open the dataset, *Export →
API Endpoint* to get a real `<dataset-id>`. Leave the URL empty (or set
`ENABLE_HTTP_SOURCE=false`) to skip the connector; the rest of the pipeline is
unaffected either way. Connector creation is **non-fatal** — if a config/flag is
off it warns and continues, so it never blocks the core Flink pipeline.

Config template: [`connectors/http_source_service_alerts.json`](connectors/http_source_service_alerts.json).
The managed-connector config schema is the most version-specific part of this
repo — if `confluent connect cluster create` rejects a field, build the connector
once in **Console → Connectors → HTTP Source**, use *Download connector config* to
get the exact JSON, and drop your values into the template.

To read that topic in Flink, run [`../flink/09_service_alerts.sql`](../flink/09_service_alerts.sql)
by hand once data is flowing — its columns must match your feed's JSON keys, which
is why it isn't auto-submitted.

## Managed HTTP Sink Connector (dispatcher decisions out)

`provision.sh` can also stand up a Confluent **fully-managed HTTP Sink Connector**
that streams every decision the in-Flink dispatcher writes to
`mta_dispatcher_decisions` out to an external endpoint — a second managed
connector (a **Sink**) in Stream Lineage, closing the loop the same governed way
the source ingests. We own the decisions Avro schema, so there's no feed-shape
guesswork.

Enable it in `deploy.env`:

```bash
export ENABLE_HTTP_SINK="true"
export DECISIONS_WEBHOOK_URL="https://webhook.site/<your-id>"   # or a Slack/Teams/ops webhook
```

Point `DECISIONS_WEBHOOK_URL` at a Slack/Teams incoming webhook, an ops bridge, or
a throwaway `https://webhook.site` URL for a live demo. Leave it empty (or set
`ENABLE_HTTP_SINK=false`) to skip it. Like the source connector, creation is
**non-fatal** and it's recreated with the current Kafka API key on each run.

Config template: [`connectors/http_sink_dispatcher_decisions.json`](connectors/http_sink_dispatcher_decisions.json).
Same version caveat as the source: if `confluent connect cluster create` rejects a
field, build it once in **Console → Connectors → HTTP Sink**, *Download connector
config*, and drop your values into the template.

## Managed HTTP Source Connector (NYC Weather Events)

`provision.sh` can also provision an **HTTP Source Connector** that continuously polls
live NYC weather observations (temperature, precipitation, weather conditions, wind speed)
from Open-Meteo into `nyc_weather_events` (governed with JSON Schema in Schema Registry).
The dashboard topbar displays live weather, and the interactive AI copilots use current
weather to diagnose weather-related track or headway disruptions.

Enable it in `deploy.env`:

```bash
export ENABLE_WEATHER_SOURCE="true"
export WEATHER_HTTP_URL="https://api.open-meteo.com/v1/forecast?latitude=40.7128&longitude=-74.0060&current=temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m&timezone=America%2FNew_York"
```

Config template: [`connectors/http_source_weather.json`](connectors/http_source_weather.json).

## Managed Datagen Source Connector (Passenger Turnstile Surges)

To simulate dynamic station crowding without physical sensors, `provision.sh` provisions a
Confluent **DatagenSource Connector** that generates real-time synthetic station turnstile
spikes and passenger crowd events into `mta_passenger_surges` using Avro serialization with
Schema Registry. The dashboard monitors active station surges and alerts operators to crowd
bottlenecks.

Enable it in `deploy.env`:

```bash
export ENABLE_PASSENGER_DATAGEN="true"
```

Config template: [`connectors/datagen_passenger_surges.json`](connectors/datagen_passenger_surges.json).

## Choosing the in-Flink LLM (Bedrock or Gemini)

The dispatcher agent's LLM is set by `LLM_PROVIDER` in `deploy.env`:

- **`bedrock`** (default) — fill `AWS_BEDROCK_ACCESS_KEY` / `AWS_BEDROCK_SECRET_KEY`
  (+ optional `AWS_SESSION_TOKEN`), `BEDROCK_REGION`, `BEDROCK_MODEL_ID`.
- **`googleai`** — get a key at https://aistudio.google.com/apikey, then set
  `GOOGLEAI_API_KEY` and `GEMINI_MODEL_ID` (e.g. `gemini-2.0-flash`,
  `gemini-1.5-pro`). `provision.sh` creates a `--type googleai` connection and a
  `CREATE MODEL ... 'provider'='googleai'` for you.

Only the block matching your `LLM_PROVIDER` needs to be filled in. Doing it by hand
in the Console instead? `flink/05_create_model.sql` has both the Bedrock and Gemini
`CREATE CONNECTION` / `CREATE MODEL` variants to copy.

## Teardown

```bash
./deploy/teardown.sh          # default: tears down the app's Confluent resources
./deploy/teardown.sh --all    # also deletes the environment (never "default")
```

By default, teardown **preserves** the Confluent environment and the Kafka
cluster. It deletes the HTTP connector, Flink statements, Flink connections, the
compute pool, Kafka topics, Schema Registry subjects, and the app API keys, and
best-effort drops the Flink model/agent. `--all` additionally deletes the
environment (only when it isn't named `default`); the Kafka cluster is never
deleted automatically.

## Notes

- **CLI flag drift:** this targets the v4 CLI surface. If a subcommand rejects a
  flag, run `confluent <cmd> --help` — the script echoes every command it runs, so
  adjust and re-run (it's idempotent by resource name).
- The `flink/*.sql` files stay the source of truth and also work by hand in the
  Console Flink workspace if you'd rather click through it. `provision.sh` submits
  the same statements (it creates the Bedrock connection via the CLI, so it skips
  the `CREATE CONNECTION` block in `05_create_model.sql` and submits only the
  `CREATE MODEL`).
- **Manual (no-CLI) path:** if you build the Confluent side by hand instead of
  running `provision.sh`, that script is also what writes `../.env`. So do it
  yourself first — `cp .env.example .env` from the repo root and fill in the
  cluster bootstrap, Schema Registry URL, and both API key/secret pairs from the
  Console — before running the producer or dashboard.

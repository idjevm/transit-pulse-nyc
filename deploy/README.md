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
`mta.service_alerts` (JSON Schema registered in Schema Registry) — this is the
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
./deploy/teardown.sh     # deletes the environment and everything under it
```

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

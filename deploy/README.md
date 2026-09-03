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
- **AWS Bedrock** credentials with `bedrock:InvokeModel` for the Claude model (the
  Flink AI agents call the LLM through a Bedrock connection).
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

"""Configuration for the MTA streaming producer and dashboard.

Env-driven, mirroring the F1 demo's datagen/config.py. Load a local .env first
(python-dotenv) so `python producers/mta_producer.py` works with no exports.
"""

import os

from dotenv import load_dotenv

load_dotenv()

# ---- Kafka (Confluent Cloud) ----
KAFKA_BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY", "")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET", "")

# ---- Schema Registry ----
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "")
SR_API_KEY = os.environ.get("SCHEMA_REGISTRY_API_KEY", "")
SR_API_SECRET = os.environ.get("SCHEMA_REGISTRY_API_SECRET", "")

# ---- Topics (defaults match flink/01_create_tables.sql) ----
TOPIC_VEHICLE_POSITIONS = os.environ.get("TOPIC_VEHICLE_POSITIONS", "mta_vehicle_positions")
TOPIC_TRIP_UPDATES = os.environ.get("TOPIC_TRIP_UPDATES", "mta_trip_updates")
TOPIC_ARRIVAL_ESTIMATES = os.environ.get("TOPIC_ARRIVAL_ESTIMATES", "mta_arrival_estimates")
TOPIC_HEADWAY_ALERTS = os.environ.get("TOPIC_HEADWAY_ALERTS", "mta_headway_alerts")
TOPIC_DECISIONS = os.environ.get("TOPIC_RECOMMENDATIONS", "mta_dispatcher_decisions")
TOPIC_BUS_POSITIONS = os.environ.get("TOPIC_BUS_POSITIONS", "mta_bus_positions")
# Predictive headway forecast (flink/08). Optional job: the consumer tolerates the
# topic not existing yet, so this is safe to wire up whether or not 08 is running.
TOPIC_FORECAST = os.environ.get("TOPIC_FORECAST", "mta_headway_forecast")

# ---- Producer tuning ----
MTA_FEEDS = os.environ.get("MTA_FEEDS", "all")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "15"))

# ---- MTA Bus Time (OneBusAway NYC) GTFS-RT ----
# Buses broadcast real GPS lat/lon + bearing (unlike subway trains, which snap to
# station coordinates). The public GTFS-RT feed currently serves data without a
# key; BUS_API_KEY is appended when set (recommended for stability / rate limits —
# free from https://register.developer.obanyc.com). Set BUS_ENABLED=false to run
# subway-only.
BUS_ENABLED = os.environ.get("BUS_ENABLED", "true").strip().lower() in ("1", "true", "yes")
BUS_API_KEY = os.environ.get("BUS_API_KEY", "")
# Comma-separated agency filter, or "all". Agencies: "MTA NYCT" (the five-borough
# NYCT buses) and "MTABC" (MTA Bus Company). Trims the ~2.7k-bus firehose for demos.
BUS_AGENCIES = os.environ.get("BUS_AGENCIES", "all")

# ---- Consumer groups ----
DASHBOARD_GROUP_ID = os.environ.get("DASHBOARD_GROUP_ID", "mta-dashboard")


def kafka_producer_conf() -> dict:
    # Reliability: the idempotent producer guarantees no duplicate records on
    # retry and preserved per-partition order. Idempotence implies acks=all and
    # bounded in-flight requests (librdkafka enforces this); we set acks
    # explicitly to document intent. lz4 + a small linger batch the burst each
    # poll cycle emits without adding meaningful latency.
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "client.id": "mta-producer",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,
    }


def schema_registry_conf() -> dict:
    return {
        "url": SR_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    }

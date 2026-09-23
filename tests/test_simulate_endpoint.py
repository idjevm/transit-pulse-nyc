"""Tests for the disruption-simulator endpoints (PR: disruption simulator &
forecast).

POST /api/simulate injects a labeled SIMULATED disruption into the shared state,
which the next snapshot (and thus /healthz counts + the websocket) reflects.

dashboard.consumer imports confluent_kafka at load, so we install a minimal stub
when the native package is absent. TestClient is used WITHOUT the context manager
so the app's startup event (which spawns the Kafka consumer thread) does not run.
"""

import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ensure_confluent_kafka_stub() -> None:
    try:
        import confluent_kafka  # noqa: F401
        return
    except Exception:
        pass
    ck = types.ModuleType("confluent_kafka")
    ck.Producer = object
    ck.Consumer = object
    ck.OFFSET_BEGINNING = -2
    ck.OFFSET_END = -1

    class _KafkaError:
        UNKNOWN_TOPIC_OR_PART = 3
        _UNKNOWN_TOPIC = -188
        _UNKNOWN_PARTITION = -190
        _PARTITION_EOF = -191

    ck.KafkaError = _KafkaError
    sr = types.ModuleType("confluent_kafka.schema_registry")
    sr.SchemaRegistryClient = object
    sr_avro = types.ModuleType("confluent_kafka.schema_registry.avro")
    sr_avro.AvroSerializer = object
    sr_avro.AvroDeserializer = object
    ser = types.ModuleType("confluent_kafka.serialization")

    class _MessageField:
        VALUE = "value"

    ser.MessageField = _MessageField
    ser.SerializationContext = lambda *a, **k: None
    ck.schema_registry = sr
    sr.avro = sr_avro
    ck.serialization = ser
    sys.modules.update({
        "confluent_kafka": ck,
        "confluent_kafka.schema_registry": sr,
        "confluent_kafka.schema_registry.avro": sr_avro,
        "confluent_kafka.serialization": ser,
    })


_ensure_confluent_kafka_stub()

from fastapi.testclient import TestClient  # noqa: E402

from dashboard import server  # noqa: E402


def _client() -> TestClient:
    # No context manager: skip the startup event (Kafka consumer thread).
    return TestClient(server.create_app())


def test_simulate_routes_registered():
    app = server.create_app()
    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/api/simulate" in paths
    assert "/api/simulate/clear" in paths


def test_simulate_injects_and_healthz_reflects_it():
    client = _client()
    resp = client.post("/api/simulate", json={"scenario": "bunching"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["scenario"] == "bunching"

    health = client.get("/healthz").json()
    assert health["counts"]["alerts"] == 1


def test_simulate_defaults_scenario_when_missing():
    client = _client()
    resp = client.post("/api/simulate", json={})
    assert resp.json()["scenario"] == "bunching"


def test_simulate_clear_empties_state():
    client = _client()
    client.post("/api/simulate", json={"scenario": "gap"})
    assert client.get("/healthz").json()["counts"]["alerts"] == 1
    clear = client.post("/api/simulate/clear")
    assert clear.json()["ok"] is True
    assert client.get("/healthz").json()["counts"]["alerts"] == 0

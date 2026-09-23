"""Tests for the dashboard server's agent wiring (PR: dashboard-copilot-ux).

The fix that matters here: the blocking LLM calls run in a worker thread with a
timeout, so the websocket push loop never freezes while an agent thinks. We test
the _run_agent wrapper (passthrough + timeout) and that every route is mounted.

dashboard.consumer imports confluent_kafka at module load (and reads KafkaError
attributes), so we install a minimal stub when the native package is absent —
on Mac/CI the real package is used and the stub is skipped.
"""

import asyncio
import os
import sys
import time
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

from dashboard import server  # noqa: E402


def test_run_agent_passthrough():
    result = asyncio.run(server._run_agent(lambda x: {"ok": True, "v": x}, 5))
    assert result == {"ok": True, "v": 5}


def test_run_agent_times_out(monkeypatch):
    monkeypatch.setattr(server, "AGENT_TIMEOUT_SEC", 0.1)

    def slow():
        time.sleep(1.0)
        return {"ok": True}

    result = asyncio.run(server._run_agent(slow))
    assert result["ok"] is False
    assert "did not respond" in result["error"]


def test_all_routes_registered():
    app = server.create_app()
    paths = {getattr(r, "path", None) for r in app.routes}
    for p in ("/", "/ws", "/healthz", "/api/shapes", "/api/agent-info",
              "/api/advisor", "/api/operator", "/api/route-designer"):
        assert p in paths, f"route {p} is not mounted"

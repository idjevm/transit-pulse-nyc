"""Tests for producer reliability (PR: producer-reliability).

Covers the idempotent-producer config, the event-time record timestamp, the
BufferError backpressure retry, and the delivery-report callback.

confluent_kafka ships a native (librdkafka) wheel that isn't installed on every
dev box. These tests only exercise our own logic, so if the real package is
absent we install a minimal stub for the names mta_producer imports at module
load. On a machine where confluent_kafka IS installed (Mac/CI) the real package
is used and the stub is skipped.
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
    sr = types.ModuleType("confluent_kafka.schema_registry")
    sr.SchemaRegistryClient = object
    sr_avro = types.ModuleType("confluent_kafka.schema_registry.avro")
    sr_avro.AvroSerializer = object
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

from producers import config, mta_producer  # noqa: E402


class FakeProducer:
    """Records produce()/poll() calls; can fail the first N produce()s with
    BufferError to exercise the backpressure retry."""

    def __init__(self, fail_times: int = 0):
        self.calls = []
        self.polls = 0
        self._fail = fail_times

    def produce(self, topic, key=None, value=None, timestamp=None, on_delivery=None):
        if self._fail > 0:
            self._fail -= 1
            raise BufferError("Local: Queue full")
        self.calls.append({
            "topic": topic, "key": key, "value": value,
            "timestamp": timestamp, "on_delivery": on_delivery,
        })

    def poll(self, timeout=0):
        self.polls += 1


def test_producer_conf_has_reliability_settings():
    conf = config.kafka_producer_conf()
    assert conf["enable.idempotence"] is True
    assert conf["acks"] == "all"
    assert conf["compression.type"] == "lz4"
    assert conf["linger.ms"] == 50
    assert conf["client.id"] == "mta-producer"
    # SASL/bootstrap wiring must survive the additions.
    assert conf["security.protocol"] == "SASL_SSL"
    assert "bootstrap.servers" in conf


def test_produce_stamps_event_time_and_delivery_callback():
    p = FakeProducer()
    mta_producer._produce(p, "mta_vehicle_positions", b"trip-1", b"payload", 1_700_000_000_123)
    assert len(p.calls) == 1
    call = p.calls[0]
    assert call["timestamp"] == 1_700_000_000_123  # Kafka record timestamp == event-time
    assert call["on_delivery"] is mta_producer._on_delivery
    assert p.polls == 0


def test_produce_retries_once_on_buffererror():
    p = FakeProducer(fail_times=1)
    mta_producer._produce(p, "t", b"k", b"v", 42)
    assert p.polls == 1          # drained the queue before retrying
    assert len(p.calls) == 1     # the retry succeeded


def test_on_delivery_counts_ok_and_failures():
    mta_producer._delivery["ok"] = 0
    mta_producer._delivery["failed"] = 0

    class Msg:
        def topic(self):
            return "t"

    mta_producer._on_delivery(None, Msg())
    mta_producer._on_delivery("boom", Msg())
    assert mta_producer._delivery["ok"] == 1
    assert mta_producer._delivery["failed"] == 1

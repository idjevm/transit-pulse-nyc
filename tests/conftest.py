"""Shared pytest fixtures / test bootstrap.

confluent_kafka ships a native (librdkafka) wheel that isn't installed on every
dev box (notably not on this Windows Python build). Our tests only exercise our
own logic, so when the real package is absent we install ONE complete stub here,
before any test module is collected. Doing it in conftest (rather than per-file)
makes the suite order-independent: otherwise the first test module to import
could install a partial stub that shadows the fuller one a later module needs
(e.g. the producer test only needs Producer, but the consumer needs
OFFSET_BEGINNING / Consumer / KafkaError). On a machine where confluent_kafka IS
installed (Mac/CI) the real package is used and this stub is skipped.
"""

import sys
import types


def _install_confluent_kafka_stub() -> None:
    try:
        import confluent_kafka as _ck  # noqa: F401
        if hasattr(_ck, "OFFSET_BEGINNING"):
            return  # real package (or an already-complete stub) is present
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


_install_confluent_kafka_stub()

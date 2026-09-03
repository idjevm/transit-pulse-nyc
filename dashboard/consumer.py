"""Kafka -> DashboardState consumer.

Tails the four dashboard-facing topics and routes each Avro-decoded record into
the shared DashboardState. Same confluent-kafka + Schema Registry stack the
producer uses, run in reverse (mirrors the F1 pitwall consumer).

  - mta.vehicle_positions : tail from latest (high-volume live view)
  - mta.arrival_estimates : tail from latest (continuously re-emitted)
  - mta.headway_alerts     : from earliest (low-volume; show recent history)
  - mta.dispatcher_decisions : from earliest (low-volume AI output)

Topics that don't exist yet (the Flink jobs may not be running) are handled
gracefully: UNKNOWN_TOPIC is suppressed until Flink creates them.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, KafkaError  # noqa: E402
from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: E402
from confluent_kafka.schema_registry.avro import AvroDeserializer  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from dashboard.state import DashboardState  # noqa: E402
from producers import config  # noqa: E402

logger = logging.getLogger("mta-dashboard.consumer")

EARLIEST = {config.TOPIC_HEADWAY_ALERTS, config.TOPIC_DECISIONS}

BENIGN = frozenset({
    KafkaError.UNKNOWN_TOPIC_OR_PART,
    KafkaError._UNKNOWN_TOPIC,
    KafkaError._UNKNOWN_PARTITION,
    KafkaError._PARTITION_EOF,
})


def _build_consumer() -> Consumer:
    conf = dict(config.kafka_producer_conf())  # reuse bootstrap + SASL
    conf.update({
        "group.id": f"{config.DASHBOARD_GROUP_ID}-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
    })
    return Consumer(conf)


def _on_assign(consumer: Consumer, partitions) -> None:
    for tp in partitions:
        tp.offset = OFFSET_BEGINNING if tp.topic in EARLIEST else OFFSET_END
    consumer.assign(partitions)


def run_consumer(state: DashboardState, stop) -> None:
    topics = [
        config.TOPIC_VEHICLE_POSITIONS,
        config.TOPIC_BUS_POSITIONS,
        config.TOPIC_ARRIVAL_ESTIMATES,
        config.TOPIC_HEADWAY_ALERTS,
        config.TOPIC_DECISIONS,
    ]
    routes = {
        config.TOPIC_VEHICLE_POSITIONS: state.update_vehicle,
        config.TOPIC_BUS_POSITIONS: state.update_bus,
        config.TOPIC_ARRIVAL_ESTIMATES: state.update_arrival,
        config.TOPIC_HEADWAY_ALERTS: state.add_alert,
        config.TOPIC_DECISIONS: state.add_recommendation,
    }

    try:
        consumer = _build_consumer()
        sr = SchemaRegistryClient(config.schema_registry_conf())
        deserialize = AvroDeserializer(sr, schema_str=None)
    except Exception as exc:  # noqa: BLE001
        logger.error("could not start feed: %s", exc)
        state.record_error("FEED_START_FAILED", str(exc))
        return

    consumer.subscribe(topics, on_assign=_on_assign)
    logger.info("consuming %s", ", ".join(topics))
    warned: set = set()

    try:
        while not stop.is_set():
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                code = msg.error().code()
                if code not in BENIGN and code not in warned:
                    warned.add(code)
                    logger.warning("kafka %s: %s", msg.error().name(), msg.error())
                    state.record_error(msg.error().name(), str(msg.error()))
                continue
            topic = msg.topic()
            try:
                value = deserialize(msg.value(), SerializationContext(topic, MessageField.VALUE))
            except Exception as exc:  # noqa: BLE001 - one bad record must not kill the tail
                logger.debug("decode failed on %s: %s", topic, exc)
                continue
            if value is None:
                continue
            routes[topic](value)
            state.clear_error()
    except Exception as exc:  # noqa: BLE001
        logger.error("feed stopped: %s", exc)
        state.record_error("FEED_STOPPED", str(exc))
    finally:
        consumer.close()

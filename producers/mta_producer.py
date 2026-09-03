"""MTA GTFS-Realtime -> Kafka producer.

Polls the NYC subway GTFS-RT feeds on an interval, decodes the protobuf, and
produces Avro records to two topics:

  - mta.vehicle_positions : one record per train (current position + status)
  - mta.trip_updates      : one record per predicted stop arrival

Both are Avro via Schema Registry, produced directly to Confluent Cloud. The Avro
value schemas are the ones registered by flink/01_create_tables.sql, so we run the
serializer with auto.register.schemas=false / use.latest.version=true — the same
approach as the F1 demo's datagen/simulator.py. Run 01_create_tables.sql BEFORE
this producer so the subjects exist.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer  # noqa: E402
from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: E402
from confluent_kafka.schema_registry.avro import AvroSerializer  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from producers import bus_feeds, config, feeds, stops  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mta-producer")

# GTFS-RT VehiclePosition.VehicleStopStatus enum -> string.
_STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}


def _now_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _direction(stop_id: str, trip_id: str) -> str:
    """N/S direction from the stop_id suffix, falling back to the trip_id suffix."""
    if stop_id and stop_id[-1] in ("N", "S"):
        return stop_id[-1]
    if trip_id and trip_id[-1] in ("N", "S"):
        return trip_id[-1]
    return "U"


def _build_serializer() -> tuple[SchemaRegistryClient, AvroSerializer]:
    sr = SchemaRegistryClient(config.schema_registry_conf())
    serializer = AvroSerializer(
        sr,
        schema_str=None,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )
    return sr, serializer


def _vehicle_record(entity, feed_ts_ms: int) -> dict | None:
    v = entity.vehicle
    trip_id = v.trip.trip_id
    route_id = v.trip.route_id
    stop_id = v.stop_id or ""
    if not trip_id or not route_id:
        return None
    name, lat, lon = stops.lookup(stop_id)
    ts = int(v.timestamp * 1000) if v.timestamp else feed_ts_ms
    return {
        "trip_id": trip_id,
        "route_id": route_id,
        "direction": _direction(stop_id, trip_id),
        "stop_id": stop_id,
        "stop_base": stops.base_id(stop_id),
        "stop_name": name,
        "stop_lat": lat,
        "stop_lon": lon,
        "current_status": _STATUS.get(v.current_status, "IN_TRANSIT_TO"),
        "stop_sequence": int(v.current_stop_sequence or 0),
        "event_time": ts,
    }


def _bus_record(entity, feed_ts_ms: int) -> dict | None:
    """A bus VehiclePosition with real GPS lat/lon + bearing.

    Bus route_ids are agency-namespaced (e.g. 'MTA NYCT_M15'); we keep the raw id
    and also emit the rider-facing short name ('M15'). Direction is the GTFS
    direction_id (0/1), not the subway's N/S.
    """
    v = entity.vehicle
    trip_id = v.trip.trip_id
    route_id = v.trip.route_id
    if not route_id or not v.HasField("position"):
        return None
    lat = float(v.position.latitude)
    lon = float(v.position.longitude)
    if not lat or not lon:
        return None
    ts = int(v.timestamp * 1000) if v.timestamp else feed_ts_ms
    return {
        "trip_id": trip_id or f"{route_id}:{v.vehicle.id}",
        "route_id": route_id,
        "route_short": bus_feeds.route_short(route_id),
        "direction": str(v.trip.direction_id) if v.trip.HasField("direction_id") else "",
        "stop_id": v.stop_id or "",
        "vehicle_lat": lat,
        "vehicle_lon": lon,
        "bearing": float(v.position.bearing) if v.position.HasField("bearing") else -1.0,
        "current_status": _STATUS.get(v.current_status, "IN_TRANSIT_TO"),
        "event_time": ts,
    }


def _trip_update_records(entity, feed_ts_ms: int):
    tu = entity.trip_update
    trip_id = tu.trip.trip_id
    route_id = tu.trip.route_id
    if not trip_id or not route_id:
        return
    for stu in tu.stop_time_update:
        stop_id = stu.stop_id or ""
        arrival = int(stu.arrival.time) if stu.HasField("arrival") else 0
        departure = int(stu.departure.time) if stu.HasField("departure") else 0
        if not stop_id or (arrival == 0 and departure == 0):
            continue
        name, lat, lon = stops.lookup(stop_id)
        yield {
            "trip_id": trip_id,
            "route_id": route_id,
            "direction": _direction(stop_id, trip_id),
            "stop_id": stop_id,
            "stop_base": stops.base_id(stop_id),
            "stop_name": name,
            "stop_lat": lat,
            "stop_lon": lon,
            "arrival_epoch": arrival,
            "departure_epoch": departure,
            "event_time": feed_ts_ms,
        }


def _fetch(url: str) -> gtfs_realtime_pb2.FeedMessage | None:
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("fetch failed %s: %s", url, exc)
        return None
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(resp.content)
    except Exception as exc:  # noqa: BLE001 - malformed payload must not crash the loop
        logger.warning("parse failed %s: %s", url, exc)
        return None
    return feed


def run() -> None:
    selected = feeds.resolve(config.MTA_FEEDS)
    logger.info("MTA producer starting | feeds=%s | interval=%ss",
                ",".join(selected), config.POLL_INTERVAL_SECONDS)
    logger.info("bootstrap=%s", config.KAFKA_BOOTSTRAP)

    _, serializer = _build_serializer()
    producer = Producer(config.kafka_producer_conf())

    vp_ctx = SerializationContext(config.TOPIC_VEHICLE_POSITIONS, MessageField.VALUE)
    tu_ctx = SerializationContext(config.TOPIC_TRIP_UPDATES, MessageField.VALUE)
    bus_ctx = SerializationContext(config.TOPIC_BUS_POSITIONS, MessageField.VALUE)

    buses_on = config.BUS_ENABLED
    logger.info("bus feed: %s%s", "ENABLED" if buses_on else "disabled (BUS_ENABLED=false)",
                " (with key)" if buses_on and config.BUS_API_KEY else "")

    while True:
        cycle_start = time.time()
        n_vp = n_tu = n_bus = 0
        for key, url in selected.items():
            feed = _fetch(url)
            if feed is None:
                continue
            feed_ts_ms = int(feed.header.timestamp * 1000) if feed.header.timestamp else _now_millis()
            for entity in feed.entity:
                if entity.HasField("vehicle"):
                    rec = _vehicle_record(entity, feed_ts_ms)
                    if rec:
                        producer.produce(
                            config.TOPIC_VEHICLE_POSITIONS,
                            key=rec["trip_id"].encode("utf-8"),
                            value=serializer(rec, vp_ctx),
                        )
                        n_vp += 1
                if entity.HasField("trip_update"):
                    for rec in _trip_update_records(entity, feed_ts_ms):
                        producer.produce(
                            config.TOPIC_TRIP_UPDATES,
                            key=rec["trip_id"].encode("utf-8"),
                            value=serializer(rec, tu_ctx),
                        )
                        n_tu += 1
            producer.poll(0)

        if buses_on:
            bus_feed = _fetch(bus_feeds.vehicle_positions_url(config.BUS_API_KEY))
            if bus_feed is not None:
                bus_ts_ms = int(bus_feed.header.timestamp * 1000) if bus_feed.header.timestamp else _now_millis()
                for entity in bus_feed.entity:
                    if not entity.HasField("vehicle"):
                        continue
                    # Bus route_id is already the short name (e.g. "M21"); the
                    # agency prefix lives on the vehicle id ("MTA NYCT_9771").
                    if not bus_feeds.agency_selected(entity.vehicle.vehicle.id, config.BUS_AGENCIES):
                        continue
                    rec = _bus_record(entity, bus_ts_ms)
                    if rec:
                        producer.produce(
                            config.TOPIC_BUS_POSITIONS,
                            key=rec["trip_id"].encode("utf-8"),
                            value=serializer(rec, bus_ctx),
                        )
                        n_bus += 1
                producer.poll(0)

        producer.flush(timeout=10)
        logger.info("cycle done | vehicle_positions=%d trip_updates=%d buses=%d | %.1fs",
                    n_vp, n_tu, n_bus, time.time() - cycle_start)

        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, config.POLL_INTERVAL_SECONDS - elapsed))


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        logger.info("shutting down")


if __name__ == "__main__":
    main()

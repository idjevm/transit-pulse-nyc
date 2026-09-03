-- 01: Source tables — mta.vehicle_positions and mta.trip_updates
--
-- These CREATE TABLE statements back Kafka topics and register the Avro value
-- schemas in Schema Registry (value.format = 'avro-registry'). The Python
-- producer then produces against these registered schemas with
-- auto.register.schemas=false / use.latest.version=true (see producers/config.py),
-- exactly like the F1 demo's datagen/simulator.py.
--
-- Run this FIRST, before starting the producer. IF NOT EXISTS makes re-runs a
-- no-op (same reasoning as the F1 topics Terraform module).
--
-- event_time is produced as epoch-millis by the producer and maps to Flink
-- TIMESTAMP(3). The watermark tolerates the feed's ~30s update cadence and
-- out-of-order arrival across the several GTFS-RT feed groups.

CREATE TABLE IF NOT EXISTS `mta_vehicle_positions` (
  `trip_id`        STRING  COMMENT 'GTFS-RT trip id, e.g. 077600_2..N',
  `route_id`       STRING  COMMENT 'Subway line, e.g. 2, A, L',
  `direction`      STRING  COMMENT 'N (uptown) or S (downtown), from stop_id suffix',
  `stop_id`        STRING  COMMENT 'Current/next GTFS stop id, e.g. 221N',
  `stop_base`      STRING  COMMENT 'stop_id without N/S suffix, e.g. 221',
  `stop_name`      STRING  COMMENT 'Station name (from static GTFS), may be empty',
  `stop_lat`       DOUBLE  COMMENT 'Station latitude (from static GTFS), 0 if unknown',
  `stop_lon`       DOUBLE  COMMENT 'Station longitude (from static GTFS), 0 if unknown',
  `current_status` STRING  COMMENT 'INCOMING_AT | STOPPED_AT | IN_TRANSIT_TO',
  `stop_sequence`  INT     COMMENT 'Current stop sequence in the trip',
  `event_time`     TIMESTAMP(3) COMMENT 'Feed timestamp',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
)
DISTRIBUTED INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE IF NOT EXISTS `mta_trip_updates` (
  `trip_id`         STRING COMMENT 'GTFS-RT trip id',
  `route_id`        STRING COMMENT 'Subway line',
  `direction`       STRING COMMENT 'N or S',
  `stop_id`         STRING COMMENT 'GTFS stop id for this predicted arrival',
  `stop_base`       STRING COMMENT 'stop_id without N/S suffix',
  `stop_name`       STRING COMMENT 'Station name (from static GTFS), may be empty',
  `stop_lat`        DOUBLE COMMENT 'Station latitude, 0 if unknown',
  `stop_lon`        DOUBLE COMMENT 'Station longitude, 0 if unknown',
  `arrival_epoch`   BIGINT COMMENT 'Predicted arrival, unix seconds (0 if none)',
  `departure_epoch` BIGINT COMMENT 'Predicted departure, unix seconds (0 if none)',
  `event_time`      TIMESTAMP(3) COMMENT 'Feed timestamp',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
)
DISTRIBUTED INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

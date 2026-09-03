-- 07: Source table — mta.bus_positions (MTA Bus Time GTFS-RT)
--
-- Buses report REAL GPS: vehicle_lat / vehicle_lon / bearing (heading degrees).
-- This is the moving-vehicle layer the dashboard draws on top of the route lines
-- (the high-precision live view). Subway trains, by contrast, snap to station
-- coordinates in mta_vehicle_positions.
--
-- Optional table: only needed if you set BUS_API_KEY and want buses in Flink /
-- the dashboard. Run it (like 01) BEFORE starting the producer so the Avro
-- subject exists; the producer serializes against it (auto.register=false).
--
-- Same downstream headway/bunching logic (02..06) can later be pointed at buses
-- by materializing bus arrivals at stops; for the demo we surface buses live on
-- the map and keep the bunching copilot on the subway.

CREATE TABLE IF NOT EXISTS `mta_bus_positions` (
  `trip_id`        STRING  COMMENT 'GTFS-RT trip id (or route:vehicle if absent)',
  `route_id`       STRING  COMMENT 'Agency-namespaced route, e.g. MTA NYCT_M15',
  `route_short`    STRING  COMMENT 'Rider-facing route, e.g. M15',
  `direction`      STRING  COMMENT 'GTFS direction_id (0/1), empty if unknown',
  `stop_id`        STRING  COMMENT 'Next GTFS stop id, may be empty',
  `vehicle_lat`    DOUBLE  COMMENT 'Real GPS latitude of the bus',
  `vehicle_lon`    DOUBLE  COMMENT 'Real GPS longitude of the bus',
  `bearing`        DOUBLE  COMMENT 'Heading in degrees (0=N, 90=E), -1 if unknown',
  `current_status` STRING  COMMENT 'INCOMING_AT | STOPPED_AT | IN_TRANSIT_TO',
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

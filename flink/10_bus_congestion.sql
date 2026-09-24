-- 10: Bus Corridor Congestion & Velocity Analytics
-- Continuous streaming window aggregation over ~2,700 live buses into corridor-level fleet congestion metrics.

CREATE TABLE IF NOT EXISTS `mta_bus_corridor_speed` (
  `route_short`      STRING COMMENT 'Rider-facing bus route, e.g. M15, B44, Q58',
  `active_buses`     BIGINT COMMENT 'Number of unique active buses in this window',
  `in_transit_buses` BIGINT COMMENT 'Buses currently in transit',
  `stopped_buses`    BIGINT COMMENT 'Buses currently stopped at stations/signals',
  `congestion_level` STRING COMMENT 'LIGHT | MODERATE | HEAVY corridor congestion',
  `window_time`      TIMESTAMP(3) COMMENT 'Window watermark timestamp'
)
DISTRIBUTED INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

INSERT INTO `mta_bus_corridor_speed`
SELECT
  COALESCE(NULLIF(route_short, ''), 'UNKNOWN') AS route_short,
  COUNT(DISTINCT trip_id) AS active_buses,
  COUNT(CASE WHEN current_status = 'IN_TRANSIT_TO' THEN 1 END) AS in_transit_buses,
  COUNT(CASE WHEN current_status = 'STOPPED_AT' THEN 1 END) AS stopped_buses,
  CASE
    WHEN COUNT(DISTINCT trip_id) >= 10 AND (CAST(COUNT(CASE WHEN current_status = 'STOPPED_AT' THEN 1 END) AS DOUBLE) / NULLIF(COUNT(DISTINCT trip_id), 0)) > 0.4 THEN 'HEAVY'
    WHEN COUNT(DISTINCT trip_id) >= 5 AND (CAST(COUNT(CASE WHEN current_status = 'STOPPED_AT' THEN 1 END) AS DOUBLE) / NULLIF(COUNT(DISTINCT trip_id), 0)) > 0.25 THEN 'MODERATE'
    ELSE 'LIGHT'
  END AS congestion_level,
  window_time
FROM TABLE(
  TUMBLE(TABLE `mta_bus_positions`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
)
WHERE route_short IS NOT NULL AND route_short <> ''
GROUP BY window_start, window_end, window_time, COALESCE(NULLIF(route_short, ''), 'UNKNOWN');


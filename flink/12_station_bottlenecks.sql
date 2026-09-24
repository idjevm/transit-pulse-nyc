-- 12: Multi-Modal Station Bottleneck Correlator
-- Joins real-time passenger crowd surges (DatagenSource) with train headway alerts (Flink CEP)
-- to detect platform overcrowding and unserviced rider demand.

CREATE TABLE IF NOT EXISTS `mta_station_bottlenecks` (
  `station_name`              STRING COMMENT 'Station experiencing surge',
  `subway_line`               STRING COMMENT 'Subway route id, e.g. 1, 2, A, L',
  `crowd_level`               STRING COMMENT 'HIGH | SURGE turnstile volume',
  `taps_per_minute`           INT    COMMENT 'Estimated turnstile entries per minute',
  `alert_type`                STRING COMMENT 'BUNCHING or GAP',
  `headway_seconds`           BIGINT COMMENT 'Observed train headway in seconds',
  `bottleneck_severity`       STRING COMMENT 'CRITICAL | ELEVATED | WATCH',
  `dispatcher_recommendation` STRING COMMENT 'Actionable crowd mitigation instruction',
  `detection_time`            TIMESTAMP(3) COMMENT 'Correlation timestamp'
)
DISTRIBUTED INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

INSERT INTO `mta_station_bottlenecks`
SELECT
  s.station_name,
  a.route_id AS subway_line,
  s.crowd_level,
  s.taps_per_minute,
  a.alert_type,
  a.headway_seconds,
  CASE
    WHEN s.crowd_level = 'SURGE' AND a.alert_type = 'GAP' THEN 'CRITICAL'
    WHEN s.crowd_level IN ('HIGH', 'SURGE') OR a.severity = 'HIGH' THEN 'ELEVATED'
    ELSE 'WATCH'
  END AS bottleneck_severity,
  CASE
    WHEN s.crowd_level = 'SURGE' AND a.alert_type = 'GAP' THEN 'DEPLOY GAP FILL & PLATFORM CROWD CONTROL'
    WHEN s.crowd_level = 'SURGE' AND a.alert_type = 'BUNCHING' THEN 'HOLD LEADING TRAIN TO ABSORB PASSENGERS'
    ELSE 'MONITOR PASSENGER SURGE CONGESTION'
  END AS dispatcher_recommendation,
  a.arrival_time AS detection_time
FROM `mta_headway_alerts` AS a
JOIN `mta_passenger_surges` AS s
ON a.route_id = SUBSTRING(s.line, 2)
WHERE s.crowd_level IN ('HIGH', 'SURGE');


-- 03: Headway between consecutive trains — the core stream-processing showcase.
--
-- Input:  mta_vehicle_positions
-- Output: mta_headway  (one row per consecutive pair of distinct trains that
--         stop at the same station/route/direction, with the gap between them)
--
-- "Headway" is the time between successive trains at a stop. Small headway =
-- bunching; large headway = a service gap. This is the transit analog of the F1
-- demo's tire-temperature signal that ML_DETECT_ANOMALIES watched.
--
-- Two steps:
--
-- 1. arrivals: a train reports STOPPED_AT for several consecutive feed frames
--    while it sits in the station. We collapse those to a single arrival event
--    per (trip_id, stop_id) by keeping the FIRST STOPPED_AT row
--    (ROW_NUMBER dedup, rn = 1 ORDER BY event_time ASC).
--
-- 2. MATCH_RECOGNIZE walks the arrivals per (route_id, direction, stop_id) in
--    event-time order and emits a row for each consecutive pair (A then B) where
--    B is a *different* train, measuring the seconds between them. This is Flink
--    CEP — a strong, purpose-built fit for "time between successive events".
--
-- AFTER MATCH SKIP TO NEXT ROW so B of one pair becomes A of the next (a rolling
-- headway series), not disjoint pairs.

CREATE TABLE IF NOT EXISTS `mta_headway` (
  `route_id`        STRING,
  `direction`       STRING,
  `stop_id`         STRING,
  `stop_name`       STRING,
  `stop_lat`        DOUBLE,
  `stop_lon`        DOUBLE,
  `prev_trip`       STRING,
  `curr_trip`       STRING,
  `headway_seconds` BIGINT,
  `arrival_time`    TIMESTAMP(3)
) WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);

INSERT INTO `mta_headway`
WITH arrivals AS (
  SELECT
    trip_id, route_id, direction, stop_id, stop_name, stop_lat, stop_lon, event_time
  FROM (
    SELECT
      trip_id, route_id, direction, stop_id, stop_name, stop_lat, stop_lon, event_time,
      ROW_NUMBER() OVER (PARTITION BY trip_id, stop_id ORDER BY event_time ASC) AS rn
    FROM `mta_vehicle_positions`
    WHERE current_status = 'STOPPED_AT'
  )
  WHERE rn = 1
)
SELECT
  route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
  prev_trip, curr_trip, headway_seconds, arrival_time
FROM arrivals
MATCH_RECOGNIZE (
  PARTITION BY route_id, direction, stop_id
  ORDER BY event_time
  MEASURES
    A.trip_id     AS prev_trip,
    B.trip_id     AS curr_trip,
    B.stop_name   AS stop_name,
    B.stop_lat    AS stop_lat,
    B.stop_lon    AS stop_lon,
    TIMESTAMPDIFF(SECOND, A.event_time, B.event_time) AS headway_seconds,
    B.event_time  AS arrival_time
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (A B)
  DEFINE
    B AS B.trip_id <> A.trip_id
) AS mr;

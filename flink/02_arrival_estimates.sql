-- 02: Live arrival estimates — feeds the rider "Next Arrivals" board.
--
-- Input:  mta_trip_updates (predicted arrivals per stop)
-- Output: mta_arrival_estimates
--
-- Each trip re-predicts every feed cycle, so a (trip_id, stop_id) pair appears
-- many times. We keep only the LATEST prediction per (trip_id, stop_id) using a
-- ROW_NUMBER dedup (rn = 1, ORDER BY event_time DESC) — the standard Flink
-- deduplication pattern — then compute a live ETA in seconds against wall clock.
--
-- UNIX_TIMESTAMP() returns current epoch seconds and is evaluated per row, so
-- eta_seconds counts down as the pipeline runs. We drop already-departed
-- predictions (arrival in the past) and anything more than 30 min out to keep the
-- board tight.

CREATE TABLE IF NOT EXISTS `mta_arrival_estimates` (
  `trip_id`       STRING,
  `stop_id`       STRING,
  `route_id`      STRING,
  `direction`     STRING,
  `stop_name`     STRING,
  `stop_lat`      DOUBLE,
  `stop_lon`      DOUBLE,
  `arrival_epoch` BIGINT,
  `eta_seconds`   BIGINT,
  `event_time`    TIMESTAMP(3),
  PRIMARY KEY (`trip_id`, `stop_id`) NOT ENFORCED
) DISTRIBUTED BY (`trip_id`, `stop_id`) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);

INSERT INTO `mta_arrival_estimates`
WITH latest AS (
  SELECT *
  FROM (
    SELECT
      trip_id, stop_id, route_id, direction, stop_name, stop_lat, stop_lon,
      arrival_epoch, event_time,
      ROW_NUMBER() OVER (PARTITION BY trip_id, stop_id ORDER BY event_time DESC) AS rn
    FROM `mta_trip_updates`
    WHERE arrival_epoch > 0
  )
  WHERE rn = 1
)
SELECT
  trip_id, stop_id, route_id, direction, stop_name, stop_lat, stop_lon,
  arrival_epoch,
  arrival_epoch - UNIX_TIMESTAMP() AS eta_seconds,
  event_time
FROM latest
WHERE arrival_epoch - UNIX_TIMESTAMP() BETWEEN -30 AND 1800;

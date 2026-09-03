-- 08: Predictive bunching — forecast the NEXT headway before it goes bad.
--
-- Input:  mta_headway  (one row per consecutive-train arrival, with headway_seconds)
-- Output: mta_headway_forecast  (predicted next headway + PREDICTED_BUNCHING / PREDICTED_GAP)
--
-- This is the "operator prediction" story: instead of only alerting once bunching
-- has happened (job 04), we watch the headway trend at each station and flag when
-- the NEXT train is on track to bunch or open a gap — early enough to act.
--
-- DEFAULT PATH — linear trend extrapolation. Fully deterministic and valid on any
-- Flink version: predicted_next = current + (current - previous). If the headway
-- is shrinking fast enough that the next one is projected under the bunching bound
-- (while the current is still fine), we raise PREDICTED_BUNCHING; symmetric for
-- gaps. Reliable for a live demo and easy for judges to reason about.
--
-- See the bottom for the OPTIONAL ML_FORECAST variant (Confluent's forecasting
-- model function), the forecasting analogue of the ML_DETECT_ANOMALIES block in 04.

CREATE TABLE IF NOT EXISTS `mta_headway_forecast` (
  `route_id`             STRING,
  `direction`            STRING,
  `stop_id`              STRING,
  `stop_name`            STRING,
  `stop_lat`             DOUBLE,
  `stop_lon`             DOUBLE,
  `curr_trip`            STRING,
  `headway_seconds`      BIGINT,
  `prev_headway_seconds` BIGINT,
  `predicted_headway`    BIGINT,
  `forecast_type`        STRING,
  `arrival_time`         TIMESTAMP(3)
) WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);

INSERT INTO `mta_headway_forecast`
WITH trended AS (
  SELECT
    route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
    curr_trip, headway_seconds, arrival_time,
    LAG(headway_seconds) OVER (
      PARTITION BY route_id, direction, stop_id
      ORDER BY arrival_time
    ) AS prev_headway_seconds
  FROM `mta_headway`
),
projected AS (
  SELECT
    route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
    curr_trip, headway_seconds, prev_headway_seconds, arrival_time,
    -- linear extrapolation of the next headway from the last two observations
    headway_seconds + (headway_seconds - prev_headway_seconds) AS predicted_headway
  FROM trended
  WHERE prev_headway_seconds IS NOT NULL
)
SELECT
  route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
  curr_trip, headway_seconds, prev_headway_seconds, predicted_headway,
  CASE
    WHEN predicted_headway < 150 AND headway_seconds >= 150 THEN 'PREDICTED_BUNCHING'
    WHEN predicted_headway > 900 AND headway_seconds <= 900 THEN 'PREDICTED_GAP'
    ELSE 'STABLE'
  END AS forecast_type,
  arrival_time
FROM projected
-- only surface the trend when it is meaningfully moving toward a problem
WHERE (predicted_headway < 150 AND headway_seconds >= 150)
   OR (predicted_headway > 900 AND headway_seconds <= 900);


-- ============================================================================
-- OPTIONAL — model-based forecast variant (run INSTEAD of the INSERT above).
--
-- ML_FORECAST is Confluent Flink's built-in forecasting model function (the
-- forecasting sibling of ML_DETECT_ANOMALIES). It learns each station's headway
-- pattern and projects it forward, so the prediction adapts to that stop's normal
-- rhythm instead of a straight-line trend. Signature/args can vary by Flink
-- version — check your workspace's function docs and adjust the config keys.
--
-- INSERT INTO `mta_headway_forecast`
-- WITH scored AS (
--   SELECT
--     route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
--     curr_trip, headway_seconds, arrival_time,
--     ML_FORECAST(
--       CAST(headway_seconds AS DOUBLE), arrival_time,
--       JSON_OBJECT('minTrainingSize' VALUE 12,
--                   'maxTrainingSize' VALUE 100,
--                   'horizon' VALUE 1))
--       OVER (PARTITION BY route_id, direction, stop_id
--             ORDER BY arrival_time
--             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS res
--   FROM `mta_headway`
-- )
-- SELECT
--   route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
--   curr_trip, headway_seconds,
--   CAST(NULL AS BIGINT) AS prev_headway_seconds,
--   CAST(res.forecast_value AS BIGINT) AS predicted_headway,
--   CASE WHEN res.forecast_value < 150 THEN 'PREDICTED_BUNCHING'
--        WHEN res.forecast_value > 900 THEN 'PREDICTED_GAP'
--        ELSE 'STABLE' END AS forecast_type,
--   arrival_time
-- FROM scored
-- WHERE res.forecast_value < 150 OR res.forecast_value > 900;
-- ============================================================================

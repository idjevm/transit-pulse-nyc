-- 04: Bunching / gap alerts from the headway stream.
--
-- Input:  mta_headway
-- Output: mta_headway_alerts  (only rows that are actually a problem)
--
-- DEFAULT PATH — deterministic thresholds. Reliable for a live demo and easy for
-- judges to reason about:
--   headway < 150s  -> BUNCHING (trains too close, will bunch further downstream)
--   headway > 900s  -> GAP      (riders waiting too long)
-- Normal headways sit roughly in the 150-600s band across lines, so these bounds
-- flag the real outliers without constant false positives.
--
-- The severity tier gives the AI agent (job 06) something to prioritize on.
--
-- See the bottom of this file for the OPTIONAL ML_DETECT_ANOMALIES variant, which
-- learns the normal headway per station instead of using fixed thresholds — the
-- same GA anomaly function the F1 demo runs on tire temperature.

CREATE TABLE IF NOT EXISTS `mta_headway_alerts` (
  `route_id`        STRING,
  `direction`       STRING,
  `stop_id`         STRING,
  `stop_name`       STRING,
  `stop_lat`        DOUBLE,
  `stop_lon`        DOUBLE,
  `prev_trip`       STRING,
  `curr_trip`       STRING,
  `headway_seconds` BIGINT,
  `alert_type`      STRING,
  `severity`        STRING,
  `arrival_time`    TIMESTAMP(3)
) WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);

INSERT INTO `mta_headway_alerts`
SELECT
  route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
  prev_trip, curr_trip, headway_seconds,
  CASE
    WHEN headway_seconds < 150 THEN 'BUNCHING'
    WHEN headway_seconds > 900 THEN 'GAP'
    ELSE 'NORMAL'
  END AS alert_type,
  CASE
    WHEN headway_seconds < 75  OR headway_seconds > 1500 THEN 'HIGH'
    WHEN headway_seconds < 150 OR headway_seconds > 900  THEN 'MEDIUM'
    ELSE 'LOW'
  END AS severity,
  arrival_time
FROM `mta_headway`
WHERE headway_seconds < 150 OR headway_seconds > 900;


-- ============================================================================
-- OPTIONAL — learned anomaly variant (run INSTEAD of the INSERT above).
--
-- ML_DETECT_ANOMALIES learns each station's normal headway distribution and
-- flags statistical outliers, so a naturally short-headway trunk stop and a
-- long-headway outer-borough stop are each judged against their own baseline.
-- This is the same GA function the F1 demo uses (docs/demo-reference/
-- enrichment_anomaly.sql). It needs a time attribute to ORDER BY; arrival_time
-- carries the rowtime out of MATCH_RECOGNIZE, so keep the ORDER BY on it.
--
-- INSERT INTO `mta_headway_alerts`
-- WITH scored AS (
--   SELECT
--     route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
--     prev_trip, curr_trip, headway_seconds, arrival_time,
--     ML_DETECT_ANOMALIES(
--       CAST(headway_seconds AS DOUBLE), arrival_time,
--       JSON_OBJECT('minTrainingSize' VALUE 12,
--                   'maxTrainingSize' VALUE 100,
--                   'confidencePercentage' VALUE 99.0,
--                   'enableStl' VALUE FALSE))
--       OVER (PARTITION BY route_id, direction, stop_id
--             ORDER BY arrival_time
--             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS res
--   FROM `mta_headway`
-- )
-- SELECT
--   route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
--   prev_trip, curr_trip, headway_seconds,
--   CASE WHEN res.actual_value < res.lower_bound THEN 'BUNCHING'
--        WHEN res.actual_value > res.upper_bound THEN 'GAP'
--        ELSE 'NORMAL' END AS alert_type,
--   'MEDIUM' AS severity,
--   arrival_time
-- FROM scored
-- WHERE res.is_anomaly;
-- ============================================================================

-- 06: AI Dispatcher Copilot — CREATE AGENT + AI_RUN_AGENT.
--
-- This is where the LLM runs INSIDE Flink. For every bunching/gap alert, the
-- agent produces a dispatcher action and a rider-facing message, and we parse its
-- reply into columns with REGEXP_EXTRACT — the exact pattern the F1 demo uses in
-- docs/demo-reference/streaming_agent_pit_decisions.sql.
--
-- Input:  mta_headway_alerts
-- Output: mta_dispatcher_decisions
--
-- Run 05_create_model.sql first (creates llm_dispatcher_model), then run this.
--
-- Two correctness properties this job guarantees:
--
--  1. Effectively-once per alert. Alerts are first-row deduplicated on
--     (route_id, direction, stop_id, curr_trip, prev_trip) ordered by the
--     arrival_time ROWTIME — an append-only dedup — so the (expensive,
--     non-deterministic) agent fires exactly once per distinct alert even if
--     job 04 reprocesses. mta_dispatcher_decisions is an UPSERT table keyed by
--     the same identity, so a reprocessed alert overwrites its own row rather
--     than appending a duplicate decision.
--
--  2. Deterministic control action. The `action` column is computed in SQL from
--     the alert (same thresholds the prompt states), so the operator-facing
--     decision never depends on model output. The model's own suggestion is kept
--     alongside as `llm_action` for audit, and the LLM still writes the
--     natural-language dispatcher_note / rider_message / reasoning.

CREATE AGENT `dispatcher_agent`
USING MODEL `llm_dispatcher_model`
USING PROMPT 'You are the AI dispatcher copilot for the NYC subway. For each service
alert you receive, decide the single best control action and write a short rider
message. Respond with EXACTLY these 4 labeled lines, in this order. No markdown,
no bold, plain text only.

Action: [HOLD TRAIN | GAP FILL | SKIP-STOP | MONITOR]
Dispatcher Note: [one sentence, concrete instruction a dispatcher can act on now]
Rider Message: [one short sentence a rider would see on a countdown clock/app]
Reasoning: [1-2 sentences explaining the choice]

ACTION RULES — apply in order:
1. alert_type = BUNCHING and headway < 90s  -> Action: HOLD TRAIN (hold the trailing train to restore spacing).
2. alert_type = BUNCHING and headway 90-150s -> Action: MONITOR (spacing tightening but not critical yet).
3. alert_type = GAP and headway > 1200s     -> Action: GAP FILL (short-turn or add service to close the gap).
4. alert_type = GAP and headway 900-1200s   -> Action: MONITOR (gap opening, watch the next train).
5. Otherwise                                 -> Action: MONITOR.

Never invent stations, trains, or times not present in the input. Keep the Rider
Message calm and non-alarming. Use the line and direction from the input in both
the note and the message (N = uptown/Bronx-bound, S = downtown/Brooklyn-bound).'
WITH ('max_iterations' = '3');


CREATE TABLE IF NOT EXISTS `mta_dispatcher_decisions` (
  `route_id`        STRING,
  `direction`       STRING,
  `stop_id`         STRING,
  `stop_name`       STRING,
  `stop_lat`        DOUBLE,
  `stop_lon`        DOUBLE,
  `alert_type`      STRING,
  `severity`        STRING,
  `headway_seconds` BIGINT,
  `prev_trip`       STRING,
  `curr_trip`       STRING,
  `arrival_time`    TIMESTAMP(3),
  `action`          STRING,
  `llm_action`      STRING,
  `dispatcher_note` STRING,
  `rider_message`   STRING,
  `reasoning`       STRING,
  `raw_response`    STRING,
  PRIMARY KEY (`route_id`, `direction`, `stop_id`, `curr_trip`, `prev_trip`) NOT ENFORCED
) DISTRIBUTED BY (`route_id`, `direction`, `stop_id`, `curr_trip`, `prev_trip`) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);

INSERT INTO `mta_dispatcher_decisions`
WITH deduped_alerts AS (
  SELECT
    route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
    alert_type, severity, headway_seconds, prev_trip, curr_trip, arrival_time
  FROM (
    SELECT
      route_id, direction, stop_id, stop_name, stop_lat, stop_lon,
      alert_type, severity, headway_seconds, prev_trip, curr_trip, arrival_time,
      ROW_NUMBER() OVER (
        PARTITION BY route_id, direction, stop_id, curr_trip, prev_trip
        ORDER BY arrival_time ASC
      ) AS rn
    FROM `mta_headway_alerts`
    WHERE alert_type IN ('BUNCHING', 'GAP')
  )
  WHERE rn = 1
)
SELECT
  a.route_id,
  a.direction,
  a.stop_id,
  a.stop_name,
  a.stop_lat,
  a.stop_lon,
  a.alert_type,
  a.severity,
  a.headway_seconds,
  a.prev_trip,
  a.curr_trip,
  a.arrival_time,
  CASE
    WHEN a.alert_type = 'BUNCHING' AND a.headway_seconds < 90   THEN 'HOLD TRAIN'
    WHEN a.alert_type = 'BUNCHING'                              THEN 'MONITOR'
    WHEN a.alert_type = 'GAP'      AND a.headway_seconds > 1200 THEN 'GAP FILL'
    WHEN a.alert_type = 'GAP'                                   THEN 'MONITOR'
    ELSE 'MONITOR'
  END AS action,
  TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1))          AS llm_action,
  TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Dispatcher Note:\*{0,2}\s*([^\n]+)', 1)) AS dispatcher_note,
  TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Rider Message:\*{0,2}\s*([^\n]+)', 1))   AS rider_message,
  TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Reasoning:\*{0,2}\s*([\s\S]+?)$', 1))    AS reasoning,
  CAST(response AS STRING) AS raw_response
FROM deduped_alerts AS a,
LATERAL TABLE(AI_RUN_AGENT(
  `dispatcher_agent`,
  CONCAT(
    'SERVICE ALERT\n',
    'Line: ', a.route_id, '  Direction: ', a.direction,
      ' (', CASE WHEN a.direction = 'N' THEN 'uptown/Bronx-bound' ELSE 'downtown/Brooklyn-bound' END, ')\n',
    'Station: ', COALESCE(NULLIF(a.stop_name, ''), a.stop_id), '\n',
    'Alert Type: ', a.alert_type, '\n',
    'Severity: ', a.severity, '\n',
    'Observed Headway: ', CAST(a.headway_seconds AS STRING), ' seconds ',
      '(normal is roughly 150-600s)\n',
    'Trailing Train: ', a.curr_trip, '   Leading Train: ', a.prev_trip
  ),
  MAP['debug', 'true']
));

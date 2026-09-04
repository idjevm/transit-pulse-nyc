-- 09: Source table over the HTTP Source Connector topic — mta.service_alerts
--
-- This is the CONNECTOR ingest path (vs. the Python producer for the live GTFS-RT
-- feeds): a Confluent fully-managed HTTP Source Connector polls a real JSON feed
-- (default: MTA service alerts) and lands records in mta.service_alerts with a
-- JSON Schema registered in Schema Registry (Stream Governance). See
-- deploy/connectors/http_source_service_alerts.json and deploy/README.md.
--
-- NOTE: the columns below must match the JSON keys of the feed you pointed the
-- connector at (set ALERTS_HTTP_URL in deploy.env). provision.sh creates the
-- connector but does NOT auto-run this file, precisely because the shape depends
-- on your feed. Once data is flowing, adjust the columns, run this by hand in the
-- Flink workspace, then (optionally) build a view like mta_active_alerts.
--
-- value.format = 'json-registry' reads the JSON Schema the connector registered,
-- so this stream is governed the same way as the Avro topics.

CREATE TABLE IF NOT EXISTS `mta_service_alerts` (
  `alert_id`      STRING COMMENT 'Feed alert id',
  `event_id`      STRING COMMENT 'Feed event id',
  `update_number` STRING COMMENT 'Alert update sequence number',
  `agency`        STRING COMMENT 'e.g. NYCT Subway',
  `affected`      STRING COMMENT 'Affected subway lines (e.g. A | H)',
  `status_label`  STRING COMMENT 'Alert status (e.g. delays, planned work)',
  `header`        STRING COMMENT 'Short alert headline',
  `description`   STRING COMMENT 'Full alert details',
  `date`          BIGINT COMMENT 'Alert timestamp in epoch milliseconds'
)
DISTRIBUTED INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'json-registry',
  'scan.startup.mode' = 'earliest-offset'
);

-- Optional: a governed view of currently-active alerts the dashboard could read.
-- CREATE TABLE IF NOT EXISTS `mta_active_alerts`
-- WITH ('changelog.mode' = 'append') AS
-- SELECT `route_id`, `header`, `description`, `updated_at`
-- FROM `mta_service_alerts`
-- WHERE LOWER(`status`) = 'active';

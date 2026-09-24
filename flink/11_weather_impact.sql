-- 11: Weather-Impact Enriched Headway Alerts
-- Enriches subway train headway alerts with live NYC weather observations from Open-Meteo.

CREATE TABLE IF NOT EXISTS `mta_weather_impact_alerts` (
  `route_id`            STRING COMMENT 'Subway line, e.g. 2, A, L',
  `direction`           STRING COMMENT 'N (uptown) or S (downtown)',
  `stop_name`           STRING COMMENT 'Station name',
  `alert_type`          STRING COMMENT 'BUNCHING or GAP',
  `severity`            STRING COMMENT 'LOW, MEDIUM, HIGH',
  `headway_seconds`     BIGINT COMMENT 'Observed headway in seconds',
  `temperature_c`       DOUBLE COMMENT 'Current NYC temperature (Celsius)',
  `precipitation_mm`    DOUBLE COMMENT 'Current NYC precipitation (mm)',
  `wind_speed_kmh`      DOUBLE COMMENT 'Current NYC wind speed (km/h)',
  `weather_condition`   STRING COMMENT 'CLEAR | OVERCAST | DRIZZLE | RAIN',
  `weather_risk_level`  STRING COMMENT 'NORMAL_TRACK_CONDITIONS | MODERATE_WEATHER_RISK | HIGH_WEATHER_DISRUPTION',
  `arrival_time`        TIMESTAMP(3) COMMENT 'Alert event time'
)
DISTRIBUTED INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

INSERT INTO `mta_weather_impact_alerts`
SELECT
  a.route_id,
  a.direction,
  a.stop_name,
  a.alert_type,
  a.severity,
  a.headway_seconds,
  COALESCE(w.`current`.temperature_2m, 18.0) AS temperature_c,
  COALESCE(w.`current`.precipitation, 0.0) AS precipitation_mm,
  COALESCE(w.`current`.wind_speed_10m, 5.0) AS wind_speed_kmh,
  CASE
    WHEN w.`current`.weather_code >= 80 THEN 'RAIN_SHOWERS'
    WHEN w.`current`.weather_code >= 51 THEN 'DRIZZLE'
    WHEN w.`current`.weather_code >= 1 THEN 'OVERCAST'
    ELSE 'CLEAR'
  END AS weather_condition,
  CASE
    WHEN COALESCE(w.`current`.precipitation, 0.0) > 1.0 OR COALESCE(w.`current`.wind_speed_10m, 0.0) > 30.0 THEN 'HIGH_WEATHER_DISRUPTION'
    WHEN COALESCE(w.`current`.precipitation, 0.0) > 0.0 OR COALESCE(w.`current`.wind_speed_10m, 0.0) > 15.0 THEN 'MODERATE_WEATHER_RISK'
    ELSE 'NORMAL_TRACK_CONDITIONS'
  END AS weather_risk_level,
  a.arrival_time
FROM (
  SELECT 'NYC' AS city, * FROM `mta_headway_alerts`
) AS a
JOIN (
  SELECT 'NYC' AS city, * FROM `nyc_weather_events`
) AS w
ON a.city = w.city;


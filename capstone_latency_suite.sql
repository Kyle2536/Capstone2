-- capstone_latency_suite_rush.sql
-- Analyze rush-hour pipeline runs written to:
--   - Business: kafka_pipeline_rush
--   - Latency : kafka_latencies_rush
--   - View    : kafka_latency_ms_rush

-- 0) (Optional) use the target database
-- USE capstone;

-- 1) Pick the most recent run_id automatically (by highest record_id)
SET @rid := (
  SELECT run_id
  FROM kafka_latencies_rush
  ORDER BY record_id DESC
  LIMIT 1
);

SELECT CONCAT('Analyzing run_id = ', COALESCE(@rid,'<none found>')) AS active_run;

-- Safety: bail out early if no runs exist
SELECT COUNT(*) AS rows_in_rush_lat
FROM kafka_latencies_rush;

-- 2) Has the dashboard rendered all rows for this run?
SELECT
  SUM(dashboard_rendered_at IS NOT NULL) AS rendered,
  COUNT(*)                               AS total
FROM kafka_latencies_rush
WHERE run_id = @rid;

-- 3) Wall-to-wall time: first sensor stamp → last dashboard render (seconds)
SELECT
  ROUND(TIMESTAMPDIFF(MICROSECOND,
      MIN(sensor_created_at),
      MAX(dashboard_rendered_at)
  )/1000000, 3) AS wall_sec_sensor_to_dashboard
FROM kafka_latencies_rush
WHERE run_id = @rid
  AND dashboard_rendered_at IS NOT NULL;

-- 4) Per-record end-to-end stats from the ms view (seconds)
SELECT
  COUNT(*)                                               AS row_count,
  ROUND(SUM(ms_sensor_to_render)/1000, 3)               AS total_sec_sensor_to_dashboard,
  ROUND(AVG(ms_sensor_to_render)/1000, 3)               AS avg_sec_sensor_to_dashboard,
  ROUND(MIN(ms_sensor_to_render)/1000, 3)               AS min_sec_sensor_to_dashboard,
  ROUND(MAX(ms_sensor_to_render)/1000, 3)               AS max_sec_sensor_to_dashboard
FROM kafka_latency_ms_rush
WHERE run_id = @rid;

-- 5) DB-only: ingest → SQL (seconds)
SELECT
  COUNT(*)                                                       AS row_count,
  ROUND(SUM((ms_sensor_to_sql - ms_sensor_to_ingest))/1000, 3)  AS total_sec_ingest_to_sql,
  ROUND(AVG((ms_sensor_to_sql - ms_sensor_to_ingest))/1000, 3)  AS avg_sec_ingest_to_sql,
  ROUND(MIN((ms_sensor_to_sql - ms_sensor_to_ingest))/1000, 3)  AS min_sec_ingest_to_sql,
  ROUND(MAX((ms_sensor_to_sql - ms_sensor_to_ingest))/1000, 3)  AS max_sec_ingest_to_sql
FROM kafka_latency_ms_rush
WHERE run_id = @rid;

-- 6) SQL → Dashboard: compute directly from latency table (seconds)
--    This isolates time after commit to when the point is drawn.
SELECT
  COUNT(*) AS row_count,
  ROUND(SUM(TIMESTAMPDIFF(MICROSECOND, sql_written_at, dashboard_rendered_at))/1000000, 3) AS total_sec_sql_to_dashboard,
  ROUND(AVG(TIMESTAMPDIFF(MICROSECOND, sql_written_at, dashboard_rendered_at))/1000000, 3) AS avg_sec_sql_to_dashboard,
  ROUND(MIN(TIMESTAMPDIFF(MICROSECOND, sql_written_at, dashboard_rendered_at))/1000000, 3) AS min_sec_sql_to_dashboard,
  ROUND(MAX(TIMESTAMPDIFF(MICROSECOND, sql_written_at, dashboard_rendered_at))/1000000, 3) AS max_sec_sql_to_dashboard
FROM kafka_latencies_rush
WHERE run_id = @rid
  AND dashboard_rendered_at IS NOT NULL;

-- 7) (Optional) Percentiles for end-to-end (seconds) - MySQL 8+ only
-- SELECT DISTINCT
--   ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ms_sensor_to_render)/1000, 3) AS p50_sec_to_d,
--   ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ms_sensor_to_render)/1000, 3) AS p90_sec_to_d,
--   ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ms_sensor_to_render)/1000, 3) AS p99_sec_to_d
-- FROM kafka_latency_ms_rush
-- WHERE run_id = @rid;

-- 8) Compare all runs at a glance
SELECT
  run_id,
  COUNT(*) AS n,
  ROUND(AVG(ms_sensor_to_render)/1000, 3) AS avg_sec_to_dashboard,
  ROUND(AVG(ms_sensor_to_sql)/1000, 3)    AS avg_sec_to_sql
FROM kafka_latency_ms_rush
GROUP BY run_id
ORDER BY MIN(record_id) DESC;

-- 9) (Sanity) Speed by hour for the active run (UTC hour)
--    If you want local America/Chicago hours, adjust at ingest time or store a local-hour column.
SELECT
  HOUR(ts)                          AS utc_hour,
  COUNT(*)                          AS n,
  ROUND(AVG(p.peakspeed), 2)        AS avg_speed_mph,
  ROUND(STD(p.peakspeed), 2)        AS std_speed_mph
FROM kafka_pipeline_rush p
JOIN kafka_latencies_rush l USING (record_id)
WHERE l.run_id = @rid
GROUP BY utc_hour
ORDER BY utc_hour;

-- 10) (Sanity) Arrival rate per minute for the active run (UTC)
SELECT
  DATE_FORMAT(CONCAT(p.created_at, ' ', p.ts), '%Y-%m-%d %H:%i') AS utc_minute,
  COUNT(*) AS rows_per_min
FROM kafka_pipeline_rush p
JOIN kafka_latencies_rush l USING (record_id)
WHERE l.run_id = @rid
GROUP BY utc_minute
ORDER BY utc_minute;

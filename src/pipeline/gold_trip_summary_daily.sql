CREATE OR REFRESH MATERIALIZED VIEW gold_trip_summary_daily
COMMENT "Daily trip counts and duration stats"
AS
SELECT
  DATE(start_time) AS trip_date,
  COUNT(*) AS trip_count,
  SUM(duration_minutes) AS total_duration_minutes,
  AVG(duration_minutes) AS avg_duration_minutes,
  MIN(duration_minutes) AS min_duration_minutes,
  MAX(duration_minutes) AS max_duration_minutes
FROM silver_trips
GROUP BY DATE(start_time)

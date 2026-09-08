-- @param start_date DATE = '2026-01-01'
-- @param end_date DATE = '2026-12-31'
-- Headline numbers for the selected date range.
SELECT
  COUNT(*) AS trip_count,
  AVG(duration_minutes) AS avg_duration_minutes,
  AVG(distance_km) AS avg_distance_km,
  SUM(distance_km) AS total_distance_km,
  SUM(duration_minutes) AS total_duration_minutes
FROM car_usage.dev.silver_trips
WHERE to_date(start_time) BETWEEN :start_date AND :end_date;

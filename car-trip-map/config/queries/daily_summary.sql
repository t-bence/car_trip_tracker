-- @param start_date DATE = '2026-01-01'
-- @param end_date DATE = '2026-12-31'
-- The gold daily aggregation, restricted to the selected date range.
SELECT
  trip_date,
  trip_count,
  round(avg_duration_minutes, 1) AS avg_duration_minutes,
  round(avg_distance_km, 2) AS avg_distance_km,
  round(total_distance_km, 2) AS total_distance_km
FROM car_usage.dev.gold_trip_summary_daily
WHERE trip_date BETWEEN :start_date AND :end_date
ORDER BY trip_date;

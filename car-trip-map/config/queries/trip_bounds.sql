-- The full date range of the logged trips, used to seed the date filter.
SELECT
  MIN(to_date(start_time)) AS first_trip_date,
  MAX(to_date(start_time)) AS last_trip_date,
  MAX(end_time) AS last_event_time,
  COUNT(*) AS total_trip_count
FROM car_usage.dev.silver_trips;

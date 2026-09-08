-- @param start_date DATE = '2026-01-01'
-- @param end_date DATE = '2026-12-31'
-- One row per trip, with the start and end point drawn on the map.
SELECT
  start_event_id,
  end_event_id,
  start_time,
  end_time,
  duration_minutes,
  distance_km,
  start_latitude,
  start_longitude,
  end_latitude,
  end_longitude
FROM car_usage.dev.silver_trips
WHERE to_date(start_time) BETWEEN :start_date AND :end_date
ORDER BY start_time;

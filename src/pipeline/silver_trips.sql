-- Reconstructs trips by pairing each 'start' event with the next chronological
-- 'end' event. Assumes a single vehicle logging one trip at a time (events
-- strictly alternate start/end) - not built to handle overlapping trips.
CREATE OR REFRESH MATERIALIZED VIEW silver_trips (
  CONSTRAINT valid_pairing EXPECT (end_time > start_time) ON VIOLATION DROP ROW
)
COMMENT "Trips reconstructed by pairing consecutive start/end events"
AS
WITH ordered_events AS (
  SELECT
    id,
    event_type,
    event_time,
    LEAD(event_type) OVER (ORDER BY event_time, id) AS next_event_type,
    LEAD(id) OVER (ORDER BY event_time, id) AS next_id,
    LEAD(event_time) OVER (ORDER BY event_time, id) AS next_event_time
  FROM silver_trip_events
)
SELECT
  id AS start_event_id,
  next_id AS end_event_id,
  event_time AS start_time,
  next_event_time AS end_time,
  (unix_timestamp(next_event_time) - unix_timestamp(event_time)) / 60.0 AS duration_minutes
FROM ordered_events
WHERE event_type = 'start'
  AND next_event_type = 'end'

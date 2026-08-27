-- Assumes Lakehouse Sync (configured manually in the UI - see README) targets
-- this pipeline's catalog/schema, so the CDC history table can be read by its
-- bare name.
CREATE OR REFRESH MATERIALIZED VIEW silver_trip_events (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_type EXPECT (event_type IN ('start', 'end')) ON VIOLATION DROP ROW
)
COMMENT "Current state of trip start/end events, deduplicated from the Lakebase CDC history table"
AS
SELECT id, event_type, event_time, note
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _pg_lsn DESC) AS rn
  FROM lb_trip_events_history
  WHERE _pg_change_type IN ('insert', 'update_postimage', 'delete')
)
WHERE rn = 1
  AND _pg_change_type != 'delete'

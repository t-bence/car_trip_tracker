-- Assumes Lakehouse Sync (configured manually in the UI - see README) targets
-- this pipeline's catalog/schema, so the CDC history table can be read by its
-- bare name. Lakebase only stores a raw JSON `payload` TEXT cell plus the
-- insert timestamp, so this layer is also where that JSON gets parsed.
CREATE OR REFRESH MATERIALIZED VIEW silver_trip_events (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_type EXPECT (event_type IN ('start', 'end')) ON VIOLATION DROP ROW
)
COMMENT "Current state of trip start/end events, deduplicated from the Lakebase CDC history table and parsed from their JSON payload"
AS
SELECT
  id,
  get_json_object(payload, '$.event_type') AS event_type,
  get_json_object(payload, '$.note') AS note,
  received_at AS event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _pg_lsn DESC) AS rn
  FROM lb_trip_events_history
  WHERE _pg_change_type IN ('insert', 'update_postimage', 'delete')
)
WHERE rn = 1
  AND _pg_change_type != 'delete'

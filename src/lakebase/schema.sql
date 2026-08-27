-- One-time manual setup: run against the project's default "databricks_postgres"
-- database (Lakehouse Sync requires the source table to live there - see
-- README). Not deployed by the bundle.
--
--   databricks postgres generate-database-credential \
--     projects/lakebase/branches/production/endpoints/primary --profile <PROFILE>
--   PGPASSWORD='<token>' psql "host=<host> user=<your-email> dbname=databricks_postgres sslmode=require" \
--     -f src/lakebase/schema.sql

CREATE TABLE IF NOT EXISTS trip_events (
    id SERIAL PRIMARY KEY,
    payload TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Lakehouse Sync requires full row images for updates/deletes.
ALTER TABLE trip_events REPLICA IDENTITY FULL;

-- Lets the Data API caller (identified by this Databricks user's bearer token)
-- read/write the table. Replace with the identity the iOS Shortcut authenticates as.
CREATE ROLE "toth.bence.mihaly@gmail.com" LOGIN;
GRANT USAGE ON SCHEMA public TO "toth.bence.mihaly@gmail.com";
GRANT SELECT, INSERT ON trip_events TO "toth.bence.mihaly@gmail.com";
GRANT USAGE ON SEQUENCE trip_events_id_seq TO "toth.bence.mihaly@gmail.com";

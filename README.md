# car_trip_tracker

A personal car-trip logging system built on Databricks Free Edition.

An iOS Shortcut posts trip start/stop events straight to Lakebase's Data API
(no app in between) into a Postgres table. Lakehouse Sync then streams those
changes into a Unity Catalog Delta table (bronze), and a Lakeflow Declarative
Pipeline turns that into a silver `trips` table (pairing start/stop events)
and two gold tables with the trip metrics.

```
iOS Shortcut --HTTP(Data API)--> Lakebase Postgres (logs)
                                        |  Lakehouse Sync (CDC, UI-only)
                                        v
                car_usage.lakebase_cdc.lb_logs_history (bronze, Delta)
                                        |  Lakeflow Declarative Pipeline
                                        v
                       car_usage.dev.silver_trip_events
                                        |
                             car_usage.dev.silver_trips
                                   /            \
              gold_trip_summary_daily        gold_trip_stats
```

## Tables

Everything the pipeline writes lands in `${var.catalog}.${var.schema}`, which
is `car_usage.dev` for the dev target and `car_usage.prod` for prod.

| Table | Type | Content |
| --- | --- | --- |
| `silver_trip_events` | Streaming Table | one row per logged event: `event_id`, `event_type` (`start`/`stop`), `event_time`, `latitude`, `longitude` |
| `silver_trips` | Materialized View | one row per trip: start/end event ids, start/end time and coordinates, `duration_minutes`, `distance_km` |
| `gold_trip_summary_daily` | Materialized View | per day: `trip_count`, average/total/min/max duration and distance |
| `gold_trip_stats` | Materialized View | a single row with the same metrics over all trips - the average trip length in time and in distance |

`distance_km` is the great-circle (straight-line) distance between the trip's
start and end point, computed with the haversine formula. The Shortcut only
logs the two endpoints, so the distance actually driven on the road cannot be
computed from this data.

## Project layout

- `src/lakebase/schema.sql`: one-time DDL for the `logs` table (run manually -
  see Setup below, not deployed by the bundle).
- `resources/pipeline.yml`, `src/pipeline/`: the Lakeflow Declarative Pipeline
  (Python; bronze is the Lakehouse Sync CDC table, not part of this pipeline):
  - `transformations.py` - pure DataFrame-in/DataFrame-out logic, unit tested
    in `tests/test_pipeline_transformations.py` (no Databricks needed)
  - `silver_trip_events.py` - Streaming Table: parses each event's JSON
    payload, reading `lb_logs_history` incrementally. `logs` is insert-only,
    so the CDC history table only ever contains `insert` rows for it - no
    "latest state" dedup needed (see below for why)
  - `silver_trips.py` - Materialized View: pairs consecutive start/stop events
    into trips (needs a full ordering across history, so it's a batch read)
  - `gold_trip_summary_daily.py`, `gold_trip_stats.py` - Materialized Views
    with the trip metrics
- `resources/job.yml`: a job that refreshes the pipeline every hour (the
  trigger is paused automatically in the dev target).

### The payload is double-encoded JSON

The Shortcut sends the event object already serialized, so Postgres stores a
JSON *string* in `payload`, not a JSON object:
`"{\"latitude\":\"47.5\",\"event\":\"start\",...}"`.
`parse_trip_event` therefore unwraps it with `get_json_object(payload, "$")`
before reading the individual fields. `logged_at` arrives as
`Thu, 03 Sep 2026 08:18:57 +0200`; Spark cannot parse the weekday name, so
those first five characters are cut off before `to_timestamp`.

### Why a Streaming Table for silver_trip_events, not the CDC-dedup pattern?

Lakehouse Sync's `lb_<table>_history` tables are built for the general case:
Postgres rows that get updated or deleted, where you need "current state"
reconstructed via `ROW_NUMBER() ... ORDER BY _pg_lsn DESC`. That's the
pattern Databricks' own CDC-to-medallion guidance defaults to, and it means a
Materialized View that rescans the whole history table on every run.

`logs` never gets updated or deleted - the Shortcut only INSERTs - so
every CDC record for it is an `insert`. The history table itself is still
plain append-only Delta underneath (each Postgres change, including
updates/deletes when they happen, is one more appended audit row, never an
in-place mutation), so nothing stops a Streaming Table from reading it via
`spark.readStream.table(...)`: Databricks tracks the last-processed Delta
version and each new Lakehouse Sync commit becomes one incremental
micro-batch, instead of a full recompute. That's the "ingest incrementally
off the Delta versions" the CDC dedup pattern is normally there to avoid
needing - it's just already available for free once you drop the
update/delete-handling logic your source doesn't produce. The
`silver_trips` pairing step still has to look across the *entire* event
history at once (each trip needs to see both its start and its end), so it
stays a Materialized View - that kind of full-dataset join/aggregation can't
be expressed incrementally.

## Setup (one-time, manual)

Two of these steps have no CLI/API - Lakebase Data API and Lakehouse Sync are
UI-only features.

1. Run `src/lakebase/schema.sql` against the project's default
   `databricks_postgres` database (Lakehouse Sync only syncs tables from that
   database, not a custom one) - see the comment at the top of the file for
   the exact commands.
2. In the workspace: **Catalog → lakebase project → Data API → Enable**, then
   grant the calling identity access (`src/lakebase/schema.sql` already grants
   the role; add more `CREATE ROLE ... GRANT ...` lines for other identities).
3. In the workspace: **Catalog → lakebase project → production branch →
   Lakehouse Sync → Start Sync**, source database `databricks_postgres` /
   schema `public`, destination `car_usage.lakebase_cdc` (the
   `source_table` variable in `databricks.yml`). This creates
   `lb_logs_history` and keeps it updated automatically.
4. Create the output schema once per target (the bundle does not manage it):
   ```
   $ databricks experimental aitools tools query \
       'CREATE SCHEMA IF NOT EXISTS car_usage.dev' --profile <PROFILE>
   ```
5. Deploy and run the pipeline (see below).

The iOS Shortcut then POSTs to the Data API's `/public/logs` endpoint with a
Databricks OAuth/PAT bearer token and a single `payload` field holding JSON as
text, e.g.
`{"payload": "{\"event\":\"start\",\"latitude\":\"47.5\",\"longitude\":\"19.0\",\"logged_at\":\"Thu, 03 Sep 2026 08:18:57 +0200\"}"}`.
`id` is filled in automatically. The event time comes from `logged_at`, not
from the insert time, so events logged offline and posted later still land on
the right day.

The 'car_trip_tracker' project was generated by using the default-python template.

* `tests/`: Unit tests for the shared Python code.
* `fixtures/`: Fixtures for data sets (primarily used for testing).


## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

*  Make sure you have the UV package manager installed.
   It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
*  Run `uv sync --dev` to install the project's dependencies.


# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

5. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```
   Tests run against a local PySpark session (`spark` fixture in
   `tests/conftest.py`, `pyspark` is a dev dependency) - no Databricks
   auth/compute needed.

# car_trip_tracker

A personal car-trip logging system, split across AWS and Databricks.

An iOS Shortcut posts trip start/stop events to an AWS Lambda, which stores
each one in S3 as a JSON object. A Databricks Lakeflow Declarative Pipeline
reads that prefix with Auto Loader and turns it into a silver `trips` table
(pairing start/stop events) and two gold tables with the trip metrics. A
Databricks App draws the result on a map.

```
iOS Shortcut --HTTPS--> AWS Lambda (car-trip-ingest)
                                |  PUT, one JSON object per event
                                v
              s3://car-trip-events-<account>/events/<event_id>.json
                                |  Auto Loader, Unity Catalog external location
                                v
                    car_usage.<schema>.bronze_trip_events
                                |  Lakeflow Declarative Pipeline
                                v
                    car_usage.<schema>.silver_trip_events
                                |
                       car_usage.<schema>.silver_trips
                             /            \
        gold_trip_summary_daily        gold_trip_stats
```

Ingest runs on AWS because logging a trip has to work at any hour, whether or
not any Databricks compute happens to be awake. The pipeline only has to run
once an hour.

## Repository layout

| Path | Side | What it is |
| --- | --- | --- |
| `aws/` | AWS | The ingest Lambda, its SAM template and their unit tests. See [aws/README.md](aws/README.md) |
| `databricks.yml`, `resources/`, `src/pipeline/` | Databricks | The bundle, the job and the Lakeflow pipeline |
| `car-trip-map/` | Databricks | The AppKit app that shows the trips |
| `scripts/` | Databricks | The Unity Catalog setup script for the S3 bucket |
| `tests/` | Databricks | Unit tests for the pipeline transformations |

## Tables

Everything the pipeline writes lands in `${var.catalog}.${var.schema}`, which
is `car_usage.dev` for the dev target and `car_usage.prod` for prod.

| Table | Type | Content |
| --- | --- | --- |
| `bronze_trip_events` | Streaming Table | one row per JSON object landed in S3, read with Auto Loader, timestamps still as ISO 8601 strings |
| `silver_trip_events` | Streaming Table | one row per logged event, typed: `event_id`, `event_type` (`start`/`stop`), `event_time`, `received_at`, `latitude`, `longitude` |
| `silver_trips` | Materialized View | one row per trip: start/end event ids, start/end time and coordinates, `duration_minutes`, `distance_km` |
| `gold_trip_summary_daily` | Materialized View | per day: `trip_count`, average/total/min/max duration and distance |
| `gold_trip_stats` | Materialized View | a single row with the same metrics over all trips - the average trip length in time and in distance |

`distance_km` is the great-circle (straight-line) distance between the trip's
start and end point, computed with the haversine formula. The Shortcut only
logs the two endpoints, so the distance actually driven on the road cannot be
computed from this data.

## Pipeline layout

- `resources/pipeline.yml`, `src/pipeline/`: the Lakeflow Declarative Pipeline
  (Python):
  - `transformations.py` - pure DataFrame-in/DataFrame-out logic, unit tested
    in `tests/test_pipeline_transformations.py` (no Databricks needed)
  - `bronze_trip_events.py` - Streaming Table: Auto Loader over the S3 events
    prefix, one row per JSON object the Lambda wrote. The schema is declared,
    and anything that does not fit lands in `_rescued_data`
  - `silver_trip_events.py` - Streaming Table: casts the landed fields and
    drops rows with an unknown event type, no timestamp or no coordinates
  - `silver_trips.py` - Materialized View: pairs consecutive start/stop events
    into trips (needs a full ordering across history, so it's a batch read)
  - `gold_trip_summary_daily.py`, `gold_trip_stats.py` - Materialized Views
    with the trip metrics
- `resources/job.yml`: a job that refreshes the pipeline every hour (the
  trigger is paused automatically in the dev target).
- `car-trip-map/`: the Databricks App that shows the trips (an AppKit project,
  deployed by the `prod` target of this bundle).

### Why the ingest normalises and the pipeline does not

The Lambda unwraps whatever the Shortcut posted, parses `logged_at` into a UTC
`event_time` and writes a flat, typed JSON object. All of that logic is in
`aws/src/trip_event.py`, where it gets plain Python unit tests.
`parse_trip_event` in the pipeline is then only a cast.

### Why a Streaming Table for the event tables

New events only ever arrive as new S3 objects, so Auto Loader tracks which
files it has already read and each run picks up only the new ones. The
`silver_trips` pairing step has to look across the *entire* event history at
once, because each trip needs to see both its start and its end, so it stays a
Materialized View.

Duplicates are handled at the door rather than in Spark: the S3 key is a hash
of the event, so a retry from the Shortcut overwrites the same object.

## The car-trip-map app

`car-trip-map/` is a Databricks App (AppKit: TypeScript, React) that reads
`car_usage.prod` through a SQL warehouse and shows:

- headline numbers for the selected date range: trip count, average trip
  length in minutes, average trip distance, total distance
- a map with every trip drawn as a line from its start point (green) to its
  end point (red), with a histogram of trip starts and a slider under it that
  narrow the map to the trips that started inside a chosen window
- trips per day, and a histogram of trip durations with the share of trips
  under 20 minutes
- every drive of 20+ minutes on a time axis, with the longest gap between them

The last two answer the question the tracker exists for: whether the driving
profile suits a diesel. A particulate filter needs a sustained hot run to burn
its soot off, so many short trips and long gaps between long drives are the
warning signs.

Everything is filtered by one date range, seeded with the full range of the
data. The map uses Leaflet with OpenStreetMap tiles.

The app is part of this bundle, defined under the `prod` target, so it deploys
with everything else:

```
$ databricks bundle deploy -t prod --profile <PROFILE>
```

The app runs its queries as its own service principal, which needs read
access to the data (once, after the app is first created):

```
$ SP=$(databricks apps get car-trip-map --profile <PROFILE> -o json \
    | jq -r .service_principal_client_id)
$ databricks experimental aitools tools query \
    "GRANT USE CATALOG ON CATALOG car_usage TO \`$SP\`" --profile <PROFILE>
$ databricks experimental aitools tools query \
    "GRANT USE SCHEMA, SELECT ON SCHEMA car_usage.prod TO \`$SP\`" --profile <PROFILE>
```

For local development, `cd car-trip-map && npm run dev`.

## Setup

### 1. AWS

See [aws/README.md](aws/README.md). In short: put the shared secret in SSM,
`sam deploy`, and note the Function URL and the `EventsPath` output.

### 2. Unity Catalog access to the bucket

```
$ scripts/setup_uc_external_location.sh <PROFILE>
```

This creates the storage credential and the external location over
`s3://car-trip-events-<account>/`, then validates that the IAM role works.
It runs twice, with a `sam deploy` in between that makes the IAM role
self-assuming. See [aws/README.md](aws/README.md#giving-databricks-read-access).

### 3. Databricks

Set `events_path` in `databricks.yml` to the `EventsPath` output of the stack,
create the output schemas (the bundle does not manage them), then deploy:

```
$ databricks experimental aitools tools query \
    'CREATE SCHEMA IF NOT EXISTS car_usage.dev' --profile <PROFILE>
$ databricks bundle deploy -t dev --profile <PROFILE>
$ databricks bundle run trip_pipeline -t dev --profile <PROFILE>
```

### 4. The iOS Shortcut

POST to the Function URL with an `X-Api-Key` header and the event as the body:

```json
{"payload": "{\"event\":\"start\",\"latitude\":\"47.5\",\"longitude\":\"19.0\",\"logged_at\":\"Thu, 03 Sep 2026 08:18:57 +0200\"}"}
```

A flat object without the `payload` wrapper works too. The event time comes
from `logged_at`, not from the arrival time, so events logged offline and
posted later still land on the right day.

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
   This runs both sides: `tests/` against a local PySpark session (`spark`
   fixture in `tests/conftest.py`) and `aws/tests/` as plain Python. Neither
   needs Databricks or AWS credentials.

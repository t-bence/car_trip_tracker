"""Trip start/stop events, parsed from the Lakebase CDC history table.

The source table is Lakehouse Sync's CDC history table for the Lakebase
`logs` table. It lives in a different schema than this pipeline's output, so
it is read by its fully-qualified name, taken from the pipeline configuration.

`logs` in Lakebase is insert-only - the iOS Shortcut only ever INSERTs, never
UPDATEs or DELETEs a row - so the history table only ever emits 'insert'
records for it. No "latest state per id" dedup is needed, and the history
table is append-only, so a Streaming Table can read it incrementally off
Delta's own version tracking instead of rescanning the whole table.
"""

from pyspark import pipelines as dp
from transformations import parse_trip_event

SOURCE_TABLE = spark.conf.get("source_table")  # noqa: F821


@dp.table(
    comment="Trip start/stop events, parsed from their JSON payload, streamed "
    "incrementally off the Lakehouse Sync CDC history table",
)
@dp.expect_or_drop("valid_event_type", "event_type IN ('start', 'stop')")
@dp.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dp.expect_or_drop("valid_coordinates", "latitude IS NOT NULL AND longitude IS NOT NULL")
def silver_trip_events():
    history = spark.readStream.table(SOURCE_TABLE).where("_pg_change_type = 'insert'")  # noqa: F821
    return parse_trip_event(history)

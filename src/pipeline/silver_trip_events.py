"""Trip start/end events, parsed from the Lakebase CDC history table.

Assumes Lakehouse Sync (configured manually in the UI - see README) targets
this pipeline's catalog/schema, so the source table can be read by its bare
name.

trip_events in Lakebase is insert-only - the Shortcut only ever INSERTs, never
UPDATEs or DELETEs a row - so Lakehouse Sync's CDC history table only ever
emits 'insert' records for it. That means no "latest state per id" dedup is
needed here (see the medallion-from-cdc pattern the databricks-lakebase skill
recommends for entities that DO get updated/deleted): the history table
itself is an append-only Delta table, so a Streaming Table can read it
incrementally off Delta's own version tracking instead of an MV rescanning
the whole table on every run.
"""

from pyspark import pipelines as dp
from transformations import parse_trip_event


@dp.table(
    comment="Trip start/end events, parsed from their JSON payload, streamed "
    "incrementally off the Lakehouse Sync CDC history table",
)
@dp.expect_or_drop("valid_event_type", "event_type IN ('start', 'end')")
def silver_trip_events():
    history = spark.readStream.table("lb_trip_events_history").where(  # noqa: F821
        "_pg_change_type = 'insert'"
    )
    return parse_trip_event(history)

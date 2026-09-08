"""Trip start/stop events, parsed out of the Lakebase CDC history table.

The history table is append-only, so it is read incrementally as a stream.
Its `insert` records are the logged events; the table name comes from the
pipeline configuration because it lives in another schema.
"""

from pyspark import pipelines as dp
from transformations import parse_trip_event

SOURCE_TABLE = spark.conf.get("source_table")  # noqa: F821


@dp.table(
    comment="Trip start/stop events, parsed from their JSON payload",
)
@dp.expect_or_drop("valid_event_type", "event_type IN ('start', 'stop')")
@dp.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dp.expect_or_drop("valid_coordinates", "latitude IS NOT NULL AND longitude IS NOT NULL")
def silver_trip_events():
    history = spark.readStream.table(SOURCE_TABLE).where("_pg_change_type = 'insert'")  # noqa: F821
    return parse_trip_event(history)

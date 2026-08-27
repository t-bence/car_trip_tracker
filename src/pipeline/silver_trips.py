"""Trips reconstructed by pairing consecutive start/end events.

Pairing needs a global ordering across the full event history, so this stays
a Materialized View (batch read) on top of the silver_trip_events Streaming
Table, rather than an incremental append.
"""

from pyspark import pipelines as dp
from transformations import pair_trip_events


@dp.materialized_view(comment="Trips reconstructed by pairing consecutive start/end events")
@dp.expect_or_drop("valid_pairing", "end_time > start_time")
def silver_trips():
    events = spark.read.table("silver_trip_events")  # noqa: F821
    return pair_trip_events(events)

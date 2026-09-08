"""Trips reconstructed by pairing consecutive start/stop events.

Pairing needs a global ordering across the full event history, so this is a
Materialized View over a batch read.
"""

from pyspark import pipelines as dp
from transformations import pair_trip_events


@dp.materialized_view(comment="Trips reconstructed by pairing consecutive start/stop events")
@dp.expect_or_drop("valid_pairing", "end_time > start_time")
def silver_trips():
    events = spark.read.table("silver_trip_events")  # noqa: F821
    return pair_trip_events(events)

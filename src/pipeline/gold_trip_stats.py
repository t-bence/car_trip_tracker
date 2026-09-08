"""One row with the average trip length in time and in distance."""

from pyspark import pipelines as dp
from transformations import summarize_trips_overall


@dp.materialized_view(comment="Average trip length in time (minutes) and in distance (km), over all trips")
def gold_trip_stats():
    trips = spark.read.table("silver_trips")  # noqa: F821
    return summarize_trips_overall(trips)

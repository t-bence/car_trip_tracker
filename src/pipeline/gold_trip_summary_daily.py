"""Daily trip counts and duration stats."""

from pyspark import pipelines as dp
from transformations import summarize_trips_daily


@dp.materialized_view(comment="Daily trip counts and duration stats", cluster_by=["trip_date"])
def gold_trip_summary_daily():
    trips = spark.read.table("silver_trips")  # noqa: F821
    return summarize_trips_daily(trips)

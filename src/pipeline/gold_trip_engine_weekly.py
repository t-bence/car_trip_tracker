"""One row per week with the engine condition counts, weeks without trips included."""

from pyspark import pipelines as dp
from transformations import summarize_engine_use_weekly


@dp.materialized_view(
    comment="One row per week from the first trip to today: trips, cold starts, cold short trips, "
    "possibly interrupted regenerations, long drives, and driving and warm minutes",
    cluster_by=["week_start"],
)
def gold_trip_engine_weekly():
    profile = spark.read.table("gold_trip_engine_profile")  # noqa: F821
    return summarize_engine_use_weekly(profile)

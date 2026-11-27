"""One row per trip with the engine conditions a diesel cares about."""

from pyspark import pipelines as dp
from transformations import EngineThresholds, profile_engine_use

THRESHOLDS = EngineThresholds(
    cold_start_parked_hours=float(spark.conf.get("cold_start_parked_hours")),  # noqa: F821
    warm_up_minutes=float(spark.conf.get("warm_up_minutes")),  # noqa: F821
    long_drive_minutes=float(spark.conf.get("long_drive_minutes")),  # noqa: F821
)


@dp.materialized_view(
    comment="One row per trip: the parking time before it, cold start, warm minutes, long drive and "
    "possibly interrupted particulate filter regeneration"
)
def gold_trip_engine_profile():
    trips = spark.read.table("silver_trips")  # noqa: F821
    return profile_engine_use(trips, THRESHOLDS)

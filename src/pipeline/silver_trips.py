"""Trips reconstructed by pairing consecutive start/end events.

Assumes a single vehicle logging one trip at a time (events strictly
alternate start/end) - not built to handle overlapping trips.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(comment="Trips reconstructed by pairing consecutive start/end events")
@dp.expect_or_drop("valid_pairing", "end_time > start_time")
def silver_trips():
    events = spark.read.table("silver_trip_events")
    order = Window.orderBy("event_time", "id")
    paired = (
        events.withColumn("next_event_type", F.lead("event_type").over(order))
        .withColumn("next_id", F.lead("id").over(order))
        .withColumn("next_event_time", F.lead("event_time").over(order))
        .where((F.col("event_type") == "start") & (F.col("next_event_type") == "end"))
    )
    return paired.select(
        F.col("id").alias("start_event_id"),
        F.col("next_id").alias("end_event_id"),
        F.col("event_time").alias("start_time"),
        F.col("next_event_time").alias("end_time"),
        (
            (F.unix_timestamp("next_event_time") - F.unix_timestamp("event_time")) / 60.0
        ).alias("duration_minutes"),
    )

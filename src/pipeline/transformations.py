"""Pure data-processing logic for the trip pipeline.

Kept free of `spark.read`/`readStream`/`dp.*` calls so it can be unit tested
with a plain local SparkSession - the pipeline files (silver_trip_events.py,
silver_trips.py, gold_trip_summary_daily.py) just wire these functions to
their actual sources.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_trip_event(history: DataFrame) -> DataFrame:
    """Extracts event_type/note out of each row's JSON payload."""
    return history.select(
        "id",
        F.get_json_object("payload", "$.event_type").alias("event_type"),
        F.get_json_object("payload", "$.note").alias("note"),
        F.col("received_at").alias("event_time"),
    )


def pair_trip_events(events: DataFrame) -> DataFrame:
    """Pairs each 'start' event with the next chronological 'end' event.

    Assumes a single vehicle logging one trip at a time (events strictly
    alternate start/end) - not built to handle overlapping trips.
    """
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


def summarize_trips_daily(trips: DataFrame) -> DataFrame:
    """Daily trip counts and duration stats."""
    return (
        trips.withColumn("trip_date", F.to_date("start_time"))
        .groupBy("trip_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("duration_minutes").alias("total_duration_minutes"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.min("duration_minutes").alias("min_duration_minutes"),
            F.max("duration_minutes").alias("max_duration_minutes"),
        )
    )

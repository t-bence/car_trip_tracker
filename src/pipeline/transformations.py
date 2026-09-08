"""Pure data-processing logic for the trip pipeline.

Every function takes a DataFrame and returns one, so the whole thing runs on a
plain local SparkSession in tests/test_pipeline_transformations.py. The
pipeline files wire these functions to their actual sources.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0

# The Shortcut sends "Thu, 03 Sep 2026 08:18:57 +0200". Spark cannot parse the
# weekday name, so the first 5 characters ("Thu, ") are cut off before parsing.
LOGGED_AT_FORMAT = "dd MMM yyyy HH:mm:ss Z"
WEEKDAY_PREFIX_LENGTH = 5


def parse_trip_event(history: DataFrame) -> DataFrame:
    """Extracts event type, coordinates and timestamp out of each payload.

    The payload column holds a double-encoded JSON object, so the outer string
    is unwrapped with `get_json_object(payload, "$")` first.
    """
    unwrapped = F.get_json_object("payload", "$")
    logged_at = F.get_json_object(unwrapped, "$.logged_at")

    return history.select(
        F.col("id").cast("int").alias("event_id"),
        F.get_json_object(unwrapped, "$.event").alias("event_type"),
        F.to_timestamp(F.substring(logged_at, WEEKDAY_PREFIX_LENGTH + 1, 64), LOGGED_AT_FORMAT).alias("event_time"),
        F.get_json_object(unwrapped, "$.latitude").cast("double").alias("latitude"),
        F.get_json_object(unwrapped, "$.longitude").cast("double").alias("longitude"),
    )


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres between two coordinate columns."""
    lat1_rad, lat2_rad = F.radians(lat1), F.radians(lat2)
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = F.sin(d_lat / 2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(d_lon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def pair_trip_events(events: DataFrame) -> DataFrame:
    """Pairs each 'start' event with the next chronological 'stop' event.

    Assumes one car logging one trip at a time, so events alternate start/stop.
    The distance is the straight line between the two endpoints, which are the
    only positions logged.
    """
    order = Window.orderBy("event_time", "event_id")
    paired = (
        events.withColumn("next_event_type", F.lead("event_type").over(order))
        .withColumn("next_event_id", F.lead("event_id").over(order))
        .withColumn("next_event_time", F.lead("event_time").over(order))
        .withColumn("next_latitude", F.lead("latitude").over(order))
        .withColumn("next_longitude", F.lead("longitude").over(order))
        .where((F.col("event_type") == "start") & (F.col("next_event_type") == "stop"))
    )

    return paired.select(
        F.col("event_id").alias("start_event_id"),
        F.col("next_event_id").alias("end_event_id"),
        F.col("event_time").alias("start_time"),
        F.col("next_event_time").alias("end_time"),
        ((F.unix_timestamp("next_event_time") - F.unix_timestamp("event_time")) / 60.0).alias("duration_minutes"),
        F.col("latitude").alias("start_latitude"),
        F.col("longitude").alias("start_longitude"),
        F.col("next_latitude").alias("end_latitude"),
        F.col("next_longitude").alias("end_longitude"),
        haversine_km(
            F.col("latitude"),
            F.col("longitude"),
            F.col("next_latitude"),
            F.col("next_longitude"),
        ).alias("distance_km"),
    )


def _trip_metrics() -> list:
    """The trip count plus the duration and distance aggregates."""
    return [
        F.count("*").alias("trip_count"),
        F.avg("duration_minutes").alias("avg_duration_minutes"),
        F.avg("distance_km").alias("avg_distance_km"),
        F.sum("duration_minutes").alias("total_duration_minutes"),
        F.sum("distance_km").alias("total_distance_km"),
        F.min("duration_minutes").alias("min_duration_minutes"),
        F.max("duration_minutes").alias("max_duration_minutes"),
        F.min("distance_km").alias("min_distance_km"),
        F.max("distance_km").alias("max_distance_km"),
    ]


def summarize_trips_daily(trips: DataFrame) -> DataFrame:
    """Daily trip counts with duration and distance stats."""
    return trips.withColumn("trip_date", F.to_date("start_time")).groupBy("trip_date").agg(*_trip_metrics())


def summarize_trips_overall(trips: DataFrame) -> DataFrame:
    """One row with the average trip length in time and in distance."""
    return trips.agg(*_trip_metrics()).select(
        "trip_count",
        "avg_duration_minutes",
        "avg_distance_km",
        "total_duration_minutes",
        "total_distance_km",
        "min_duration_minutes",
        "max_duration_minutes",
        "min_distance_km",
        "max_distance_km",
    )

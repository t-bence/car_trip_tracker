"""Pure data-processing logic for the trip pipeline.

Every function takes a DataFrame and returns one, so the whole thing runs on a
plain local SparkSession in tests/test_pipeline_transformations.py. The
pipeline files wire these functions to their actual sources.
"""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0


def parse_trip_event(events: DataFrame) -> DataFrame:
    """Casts the landed event fields to their real types.

    The ingest Lambda already unwrapped the payload and converted `logged_at`
    into `event_time`, an ISO 8601 UTC string, so there is no JSON left to pull
    apart here.
    """
    return events.select(
        F.col("event_id").cast("string").alias("event_id"),
        F.col("event").alias("event_type"),
        F.to_timestamp("event_time").alias("event_time"),
        F.to_timestamp("received_at").alias("received_at"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
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
    order = Window.orderBy("event_time", "received_at", "event_id")
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


@dataclass(frozen=True)
class EngineThresholds:
    """The rules of thumb that turn trip times into engine conditions.

    The pipeline reads them from its configuration in resources/pipeline.yml.
    """

    # A trip is a cold start when the car was parked at least this long before it.
    cold_start_parked_hours: float = 3.0
    # The driving time the engine needs after a cold start to reach its working temperature.
    warm_up_minutes: float = 10.0
    # A trip of at least this length gives the particulate filter time to regenerate.
    long_drive_minutes: float = 25.0


def profile_engine_use(trips: DataFrame, thresholds: EngineThresholds) -> DataFrame:
    """Adds the engine conditions of each trip: cold start, warm minutes, long drive.

    A warm-up cycle starts at each cold start and lasts until the next one. The
    engine is warm once the driving time in the cycle reaches the warm-up time.
    So a 3-minute trip after a cold start and a 4-minute trip after a short
    stop are both driven with a cold engine.
    """
    order = Window.orderBy("start_time", "start_event_id")
    up_to_this_trip = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    previous_end = F.lag("end_time").over(order)
    parked_minutes = (F.unix_timestamp("start_time") - F.unix_timestamp(previous_end)) / 60.0

    with_cold_starts = trips.withColumn("parked_minutes_before", parked_minutes).withColumn(
        "is_cold_start",
        F.col("parked_minutes_before").isNull()
        | (F.col("parked_minutes_before") >= thresholds.cold_start_parked_hours * 60),
    )

    with_cycles = with_cold_starts.withColumn(
        "warm_up_cycle", F.sum(F.col("is_cold_start").cast("int")).over(up_to_this_trip)
    )
    earlier_in_cycle = (
        Window.partitionBy("warm_up_cycle")
        .orderBy("start_time", "start_event_id")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    driven_before = F.coalesce(F.sum("duration_minutes").over(earlier_in_cycle), F.lit(0.0))
    cold_minutes = F.least(
        F.col("duration_minutes"),
        F.greatest(F.lit(0.0), F.lit(thresholds.warm_up_minutes) - driven_before),
    )

    return with_cycles.select(
        "start_event_id",
        "end_event_id",
        "start_time",
        "end_time",
        "duration_minutes",
        "start_latitude",
        "start_longitude",
        "end_latitude",
        "end_longitude",
        "parked_minutes_before",
        "is_cold_start",
        (F.col("duration_minutes") - cold_minutes).alias("warm_minutes"),
        # The trip ends before the engine reaches its working temperature.
        (driven_before + F.col("duration_minutes") < thresholds.warm_up_minutes).alias("is_cold_short_trip"),
        (F.col("duration_minutes") >= thresholds.long_drive_minutes).alias("is_long_drive"),
        # Long enough to warm up from cold, too short for a regeneration to finish.
        (
            (F.col("duration_minutes") >= thresholds.warm_up_minutes)
            & (F.col("duration_minutes") < thresholds.long_drive_minutes)
        ).alias("is_possibly_interrupted_regen"),
    )


def summarize_engine_use_weekly(profile: DataFrame, until_date: Column | None = None) -> DataFrame:
    """One row per week, from the week of the first trip to the week of `until_date`.

    Weeks without trips are included with zero counts, because a week without
    a long drive is exactly what the diesel question is about. `days_covered`
    is the number of days of the week inside the tracked period, so the first
    and the current week count as partial weeks. `until_date` is today by
    default.
    """
    until = until_date if until_date is not None else F.current_date()
    first_trip_date = profile.agg(F.min(F.to_date("start_time")).alias("first_trip_date"))

    weeks = first_trip_date.select(
        "first_trip_date",
        F.explode(
            F.sequence(
                F.to_date(F.date_trunc("week", "first_trip_date")),
                F.to_date(F.date_trunc("week", until)),
                F.expr("INTERVAL 1 WEEK"),
            )
        ).alias("week_start"),
    ).where(F.col("first_trip_date").isNotNull())

    per_week = (
        profile.withColumn("week_start", F.to_date(F.date_trunc("week", "start_time")))
        .groupBy("week_start")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("duration_minutes").alias("driving_minutes"),
            F.sum("warm_minutes").alias("warm_minutes"),
            F.sum(F.col("is_cold_start").cast("int")).alias("cold_start_count"),
            F.sum(F.col("is_cold_short_trip").cast("int")).alias("cold_short_trip_count"),
            F.sum(F.col("is_possibly_interrupted_regen").cast("int")).alias("possibly_interrupted_regen_count"),
            F.sum(F.col("is_long_drive").cast("int")).alias("long_drive_count"),
        )
    )

    week_end = F.date_add("week_start", 6)
    counts = [
        "trip_count",
        "cold_start_count",
        "cold_short_trip_count",
        "possibly_interrupted_regen_count",
        "long_drive_count",
    ]
    minutes = ["driving_minutes", "warm_minutes"]

    return weeks.join(per_week, "week_start", "left").select(
        "week_start",
        (F.datediff(F.least(week_end, until), F.greatest("week_start", "first_trip_date")) + 1).alias("days_covered"),
        ((F.col("week_start") >= F.col("first_trip_date")) & (week_end < until)).alias("is_complete_week"),
        *[F.coalesce(F.col(name), F.lit(0)).cast("long").alias(name) for name in counts],
        *[F.coalesce(F.col(name), F.lit(0.0)).alias(name) for name in minutes],
    )

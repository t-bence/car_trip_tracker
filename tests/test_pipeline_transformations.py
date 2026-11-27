"""Unit tests for src/pipeline/transformations.py."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "pipeline"))

from pyspark.sql import functions as F
from transformations import (
    EngineThresholds,
    haversine_km,
    pair_trip_events,
    parse_trip_event,
    profile_engine_use,
    summarize_engine_use_weekly,
    summarize_trips_daily,
    summarize_trips_overall,
)

LANDED_SCHEMA = (
    "event_id STRING, event STRING, event_time STRING, latitude DOUBLE, "
    "longitude DOUBLE, logged_at STRING, received_at STRING"
)
EVENTS_SCHEMA = (
    "event_id STRING, event_type STRING, event_time TIMESTAMP, received_at TIMESTAMP, latitude DOUBLE, longitude DOUBLE"
)
TRIPS_SCHEMA = (
    "start_event_id STRING, end_event_id STRING, start_time TIMESTAMP, end_time TIMESTAMP, "
    "duration_minutes DOUBLE, distance_km DOUBLE"
)


def _ts(iso: str) -> datetime:
    """A naive datetime, matching how Spark collects TIMESTAMP columns."""
    return datetime.fromisoformat(iso)


def _utc(iso: str) -> datetime:
    """An aware UTC datetime, for parsed timestamps whose instant must not
    depend on where the tests run."""
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def _landed(
    event_id: str = "a1b2c3d4e5f60718",
    event: str = "start",
    event_time: str = "2026-09-03T06:18:57Z",
    latitude: float = 47.51136133402844,
    longitude: float = 19.00823675836947,
    received_at: str = "2026-09-03T06:20:00Z",
) -> tuple:
    """One row the way Auto Loader reads it out of an event object in S3."""
    return (
        event_id,
        event,
        event_time,
        latitude,
        longitude,
        "Thu, 03 Sep 2026 08:18:57 +0200",
        received_at,
    )


def test_parse_trip_event_casts_the_landed_fields(spark):
    landed = spark.createDataFrame([_landed()], LANDED_SCHEMA)
    row = parse_trip_event(landed).collect()[0]
    assert row.event_id == "a1b2c3d4e5f60718"
    assert row.event_type == "start"
    assert row.event_time.astimezone(timezone.utc) == _utc("2026-09-03T06:18:57")
    assert row.received_at.astimezone(timezone.utc) == _utc("2026-09-03T06:20:00")
    assert row.latitude == 47.51136133402844
    assert row.longitude == 19.00823675836947


def test_parse_trip_event_handles_a_stop_event(spark):
    landed = spark.createDataFrame(
        [_landed(event="stop", event_time="2026-09-03T06:39:07Z")],
        LANDED_SCHEMA,
    )
    row = parse_trip_event(landed).collect()[0]
    assert row.event_type == "stop"
    assert row.event_time.astimezone(timezone.utc) == _utc("2026-09-03T06:39:07")


def test_parse_trip_event_returns_nulls_for_unusable_values(spark):
    """The expectations on silver_trip_events drop these rows."""
    landed = spark.createDataFrame(
        [("id", None, "not a timestamp", None, None, None, None)],
        LANDED_SCHEMA,
    )
    row = parse_trip_event(landed).collect()[0]
    assert row.event_type is None
    assert row.event_time is None
    assert row.latitude is None
    assert row.longitude is None


def test_haversine_km_matches_a_known_distance(spark):
    # Budapest (47.4979, 19.0402) to Vienna (48.2082, 16.3738) is ~214 km.
    df = spark.createDataFrame([(47.4979, 19.0402, 48.2082, 16.3738)], "a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE")
    distance = df.select(haversine_km(df.a, df.b, df.c, df.d).alias("km")).collect()[0].km
    assert 212.0 < distance < 216.0


def test_haversine_km_is_zero_for_the_same_point(spark):
    df = spark.createDataFrame([(47.4979, 19.0402, 47.4979, 19.0402)], "a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE")
    assert df.select(haversine_km(df.a, df.b, df.c, df.d).alias("km")).collect()[0].km == 0.0


def test_pair_trip_events_pairs_start_with_next_stop(spark):
    events = spark.createDataFrame(
        [
            ("e1", "start", _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:00:05"), 47.4979, 19.0402),
            ("e2", "stop", _ts("2026-01-01T08:30:00"), _ts("2026-01-01T08:30:05"), 47.5079, 19.0402),
        ],
        EVENTS_SCHEMA,
    )
    rows = pair_trip_events(events).collect()
    assert len(rows) == 1
    assert rows[0].start_event_id == "e1"
    assert rows[0].end_event_id == "e2"
    assert rows[0].duration_minutes == 30.0
    assert 1.0 < rows[0].distance_km < 1.2


def test_pair_trip_events_ignores_unmatched_start(spark):
    events = spark.createDataFrame(
        [("e1", "start", _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:00:05"), 47.4979, 19.0402)],
        EVENTS_SCHEMA,
    )
    assert pair_trip_events(events).count() == 0


def test_pair_trip_events_ignores_stop_without_preceding_start(spark):
    events = spark.createDataFrame(
        [
            ("e1", "stop", _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:00:05"), 47.4979, 19.0402),
            ("e2", "start", _ts("2026-01-01T09:00:00"), _ts("2026-01-01T09:00:05"), 47.4979, 19.0402),
        ],
        EVENTS_SCHEMA,
    )
    assert pair_trip_events(events).count() == 0


def test_pair_trip_events_pairs_several_consecutive_trips(spark):
    events = spark.createDataFrame(
        [
            ("e1", "start", _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:00:05"), 47.4979, 19.0402),
            ("e2", "stop", _ts("2026-01-01T08:30:00"), _ts("2026-01-01T08:30:05"), 47.5079, 19.0402),
            ("e3", "start", _ts("2026-01-01T18:00:00"), _ts("2026-01-01T18:00:05"), 47.5079, 19.0402),
            ("e4", "stop", _ts("2026-01-01T18:20:00"), _ts("2026-01-01T18:20:05"), 47.4979, 19.0402),
        ],
        EVENTS_SCHEMA,
    )
    rows = sorted(pair_trip_events(events).collect(), key=lambda r: r.start_event_id)
    assert [r.start_event_id for r in rows] == ["e1", "e3"]
    assert [r.duration_minutes for r in rows] == [30.0, 20.0]


def test_summarize_trips_daily_aggregates_by_date(spark):
    trips = spark.createDataFrame(
        [
            ("e1", "e2", _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:30:00"), 30.0, 10.0),
            ("e3", "e4", _ts("2026-01-01T18:00:00"), _ts("2026-01-01T18:10:00"), 10.0, 4.0),
            ("e5", "e6", _ts("2026-01-02T08:00:00"), _ts("2026-01-02T08:05:00"), 5.0, 1.0),
        ],
        TRIPS_SCHEMA,
    )
    rows = {row.trip_date: row for row in summarize_trips_daily(trips).collect()}
    day_one = rows[_ts("2026-01-01T00:00:00").date()]
    assert day_one.trip_count == 2
    assert day_one.avg_duration_minutes == 20.0
    assert day_one.avg_distance_km == 7.0
    assert day_one.total_duration_minutes == 40.0
    assert day_one.total_distance_km == 14.0
    assert day_one.min_duration_minutes == 10.0
    assert day_one.max_duration_minutes == 30.0
    assert day_one.min_distance_km == 4.0
    assert day_one.max_distance_km == 10.0


def test_summarize_trips_overall_averages_every_trip(spark):
    trips = spark.createDataFrame(
        [
            ("e1", "e2", _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:30:00"), 30.0, 10.0),
            ("e3", "e4", _ts("2026-01-02T18:00:00"), _ts("2026-01-02T18:10:00"), 10.0, 4.0),
        ],
        TRIPS_SCHEMA,
    )
    rows = summarize_trips_overall(trips).collect()
    assert len(rows) == 1
    assert rows[0].trip_count == 2
    assert rows[0].avg_duration_minutes == 20.0
    assert rows[0].avg_distance_km == 7.0


PROFILE_TRIPS_SCHEMA = (
    "start_event_id STRING, end_event_id STRING, start_time TIMESTAMP, end_time TIMESTAMP, "
    "duration_minutes DOUBLE, start_latitude DOUBLE, start_longitude DOUBLE, "
    "end_latitude DOUBLE, end_longitude DOUBLE, distance_km DOUBLE"
)
THRESHOLDS = EngineThresholds(cold_start_parked_hours=3.0, warm_up_minutes=10.0, long_drive_minutes=25.0)


def _trip(trip_id: str, start: str, minutes: float) -> tuple:
    """A silver_trips row that starts at `start` and lasts `minutes`."""
    start_time = _ts(start)
    end_time = start_time + timedelta(minutes=minutes)
    return (trip_id, f"{trip_id}-end", start_time, end_time, minutes, 47.5, 19.0, 47.5, 19.0, 0.0)


def _profile(spark, *trips: tuple) -> dict:
    """The engine profile of the trips, keyed by start_event_id."""
    profile = profile_engine_use(spark.createDataFrame(list(trips), PROFILE_TRIPS_SCHEMA), THRESHOLDS)
    return {row.start_event_id: row for row in profile.collect()}


def test_profile_engine_use_treats_the_first_trip_as_a_cold_start(spark):
    rows = _profile(spark, _trip("t1", "2026-01-05T08:00:00", 30.0))
    assert rows["t1"].parked_minutes_before is None
    assert rows["t1"].is_cold_start


def test_profile_engine_use_finds_cold_starts_by_parking_time(spark):
    rows = _profile(
        spark,
        _trip("t1", "2026-01-05T08:00:00", 30.0),
        # Parked 2 hours after t1 ended: the engine is still considered warm.
        _trip("t2", "2026-01-05T10:30:00", 30.0),
        # Parked exactly 3 hours after t2 ended: a cold start.
        _trip("t3", "2026-01-05T14:00:00", 30.0),
    )
    assert rows["t2"].parked_minutes_before == 120.0
    assert not rows["t2"].is_cold_start
    assert rows["t3"].parked_minutes_before == 180.0
    assert rows["t3"].is_cold_start


def test_profile_engine_use_counts_the_warm_up_across_short_stops(spark):
    rows = _profile(
        spark,
        # A cold start, 3 minutes of driving: the engine stays cold.
        _trip("t1", "2026-01-05T08:00:00", 3.0),
        # 5 minutes parked, 4 more minutes: 7 minutes in total, still cold.
        _trip("t2", "2026-01-05T08:08:00", 4.0),
        # 5 minutes parked, 8 more minutes: warm after 3 of them.
        _trip("t3", "2026-01-05T08:17:00", 8.0),
    )
    assert [rows[t].warm_minutes for t in ("t1", "t2", "t3")] == [0.0, 0.0, 5.0]
    assert [rows[t].is_cold_short_trip for t in ("t1", "t2", "t3")] == [True, True, False]


def test_profile_engine_use_restarts_the_warm_up_after_a_cold_start(spark):
    rows = _profile(
        spark,
        _trip("t1", "2026-01-05T08:00:00", 30.0),
        _trip("t2", "2026-01-05T18:00:00", 6.0),
    )
    assert rows["t1"].warm_minutes == 20.0
    assert rows["t2"].is_cold_start
    assert rows["t2"].warm_minutes == 0.0
    assert rows["t2"].is_cold_short_trip


def test_profile_engine_use_classifies_long_drives_and_interrupted_regens(spark):
    rows = _profile(
        spark,
        _trip("short", "2026-01-05T08:00:00", 9.0),
        _trip("medium", "2026-01-06T08:00:00", 15.0),
        _trip("long", "2026-01-07T08:00:00", 25.0),
    )
    assert [rows[t].is_long_drive for t in ("short", "medium", "long")] == [False, False, True]
    assert [rows[t].is_possibly_interrupted_regen for t in ("short", "medium", "long")] == [False, True, False]


def test_summarize_engine_use_weekly_fills_weeks_without_trips(spark):
    profile = profile_engine_use(
        spark.createDataFrame(
            [
                # Wednesday 2026-01-07: a cold start and a cold short trip.
                _trip("t1", "2026-01-07T08:00:00", 5.0),
                # Monday 2026-01-19: a cold start and a long drive.
                _trip("t2", "2026-01-19T08:00:00", 40.0),
            ],
            PROFILE_TRIPS_SCHEMA,
        ),
        THRESHOLDS,
    )
    # Thursday 2026-01-22 is "today".
    weekly = summarize_engine_use_weekly(profile, F.lit("2026-01-22").cast("date"))
    rows = {row.week_start.isoformat(): row for row in weekly.collect()}

    assert sorted(rows) == ["2026-01-05", "2026-01-12", "2026-01-19"]

    first = rows["2026-01-05"]
    assert (first.days_covered, first.is_complete_week) == (5, False)
    assert (first.trip_count, first.cold_start_count, first.cold_short_trip_count) == (1, 1, 1)
    assert first.long_drive_count == 0

    empty = rows["2026-01-12"]
    assert (empty.days_covered, empty.is_complete_week) == (7, True)
    assert (empty.trip_count, empty.long_drive_count, empty.driving_minutes) == (0, 0, 0.0)

    current = rows["2026-01-19"]
    assert (current.days_covered, current.is_complete_week) == (4, False)
    assert (current.long_drive_count, current.warm_minutes) == (1, 30.0)

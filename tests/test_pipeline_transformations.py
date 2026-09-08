"""Unit tests for src/pipeline/transformations.py, using the local
SparkSession fixture from tests/conftest.py.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "pipeline"))

from transformations import (
    haversine_km,
    pair_trip_events,
    parse_trip_event,
    summarize_trips_daily,
    summarize_trips_overall,
)

EVENTS_SCHEMA = "event_id INT, event_type STRING, event_time TIMESTAMP, latitude DOUBLE, longitude DOUBLE"
TRIPS_SCHEMA = (
    "start_event_id INT, end_event_id INT, start_time TIMESTAMP, end_time TIMESTAMP, "
    "duration_minutes DOUBLE, distance_km DOUBLE"
)


def _ts(iso: str) -> datetime:
    # Spark collects TIMESTAMP columns as naive datetimes in the driver's local
    # timezone, so timestamps that are only written and read back stay
    # consistent as naive values.
    return datetime.fromisoformat(iso)


def _utc(iso: str) -> datetime:
    """The same instant as an aware UTC datetime, for values whose absolute
    time matters (a parsed '+0200' timestamp must not depend on where the
    tests run)."""
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def _payload(event: str, logged_at: str, latitude: float, longitude: float) -> str:
    """Builds a payload exactly the way Lakebase stores it: a JSON object that
    is itself JSON-encoded as a string."""
    inner = json.dumps(
        {
            "latitude": str(latitude),
            "event": event,
            "longitude": str(longitude),
            "logged_at": logged_at,
        }
    )
    return json.dumps(inner)


def test_parse_trip_event_reads_the_double_encoded_payload(spark):
    history = spark.createDataFrame(
        [(37, _payload("start", "Thu, 03 Sep 2026 08:18:57 +0200", 47.51136133402844, 19.00823675836947))],
        "id INT, payload STRING",
    )
    row = parse_trip_event(history).collect()[0]
    assert row.event_id == 37
    assert row.event_type == "start"
    # 08:18:57 +0200 is 06:18:57 UTC.
    assert row.event_time.astimezone(timezone.utc) == _utc("2026-09-03T06:18:57")
    assert row.latitude == 47.51136133402844
    assert row.longitude == 19.00823675836947


def test_parse_trip_event_handles_a_stop_event(spark):
    history = spark.createDataFrame(
        [(38, _payload("stop", "Thu, 03 Sep 2026 08:39:07 +0200", 47.49550683875737, 19.03457501193951))],
        "id INT, payload STRING",
    )
    row = parse_trip_event(history).collect()[0]
    assert row.event_type == "stop"
    assert row.event_time.astimezone(timezone.utc) == _utc("2026-09-03T06:39:07")


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
            (1, "start", _ts("2026-01-01T08:00:00"), 47.4979, 19.0402),
            (2, "stop", _ts("2026-01-01T08:30:00"), 47.5079, 19.0402),
        ],
        EVENTS_SCHEMA,
    )
    rows = pair_trip_events(events).collect()
    assert len(rows) == 1
    assert rows[0].start_event_id == 1
    assert rows[0].end_event_id == 2
    assert rows[0].duration_minutes == 30.0
    assert 1.0 < rows[0].distance_km < 1.2


def test_pair_trip_events_ignores_unmatched_start(spark):
    events = spark.createDataFrame(
        [(1, "start", _ts("2026-01-01T08:00:00"), 47.4979, 19.0402)],
        EVENTS_SCHEMA,
    )
    assert pair_trip_events(events).count() == 0


def test_pair_trip_events_ignores_stop_without_preceding_start(spark):
    events = spark.createDataFrame(
        [
            (1, "stop", _ts("2026-01-01T08:00:00"), 47.4979, 19.0402),
            (2, "start", _ts("2026-01-01T09:00:00"), 47.4979, 19.0402),
        ],
        EVENTS_SCHEMA,
    )
    assert pair_trip_events(events).count() == 0


def test_pair_trip_events_pairs_several_consecutive_trips(spark):
    events = spark.createDataFrame(
        [
            (1, "start", _ts("2026-01-01T08:00:00"), 47.4979, 19.0402),
            (2, "stop", _ts("2026-01-01T08:30:00"), 47.5079, 19.0402),
            (3, "start", _ts("2026-01-01T18:00:00"), 47.5079, 19.0402),
            (4, "stop", _ts("2026-01-01T18:20:00"), 47.4979, 19.0402),
        ],
        EVENTS_SCHEMA,
    )
    rows = sorted(pair_trip_events(events).collect(), key=lambda r: r.start_event_id)
    assert [r.start_event_id for r in rows] == [1, 3]
    assert [r.duration_minutes for r in rows] == [30.0, 20.0]


def test_summarize_trips_daily_aggregates_by_date(spark):
    trips = spark.createDataFrame(
        [
            (1, 2, _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:30:00"), 30.0, 10.0),
            (3, 4, _ts("2026-01-01T18:00:00"), _ts("2026-01-01T18:10:00"), 10.0, 4.0),
            (5, 6, _ts("2026-01-02T08:00:00"), _ts("2026-01-02T08:05:00"), 5.0, 1.0),
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
            (1, 2, _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:30:00"), 30.0, 10.0),
            (3, 4, _ts("2026-01-02T18:00:00"), _ts("2026-01-02T18:10:00"), 10.0, 4.0),
        ],
        TRIPS_SCHEMA,
    )
    rows = summarize_trips_overall(trips).collect()
    assert len(rows) == 1
    assert rows[0].trip_count == 2
    assert rows[0].avg_duration_minutes == 20.0
    assert rows[0].avg_distance_km == 7.0

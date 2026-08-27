"""Unit tests for src/pipeline/transformations.py.

Uses a local SparkSession instead of conftest.py's Databricks Connect fixture
(no Databricks auth/compute needed) - a local `spark` fixture defined in this
file overrides the session-wide one from conftest.py for tests in here only.
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "pipeline"))

from transformations import (
    pair_trip_events,
    parse_trip_event,
    summarize_trips_daily,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _ts(iso: str) -> datetime:
    # Spark collects TIMESTAMP columns as naive datetimes in the session
    # timezone (pinned to UTC above), so the expected value must be naive too.
    return datetime.fromisoformat(iso)


def test_parse_trip_event_extracts_type_and_note(spark):
    history = spark.createDataFrame(
        [(1, '{"event_type": "start", "note": "airport run"}', _ts("2026-01-01T08:00:00"))],
        "id INT, payload STRING, received_at TIMESTAMP",
    )
    row = parse_trip_event(history).collect()[0]
    assert row.event_type == "start"
    assert row.note == "airport run"
    assert row.event_time == _ts("2026-01-01T08:00:00")


def test_parse_trip_event_missing_note_is_null(spark):
    history = spark.createDataFrame(
        [(1, '{"event_type": "end"}', _ts("2026-01-01T08:30:00"))],
        "id INT, payload STRING, received_at TIMESTAMP",
    )
    assert parse_trip_event(history).collect()[0].note is None


def test_pair_trip_events_pairs_start_with_next_end(spark):
    events = spark.createDataFrame(
        [
            (1, "start", _ts("2026-01-01T08:00:00"), None),
            (2, "end", _ts("2026-01-01T08:30:00"), None),
        ],
        "id INT, event_type STRING, event_time TIMESTAMP, note STRING",
    )
    rows = pair_trip_events(events).collect()
    assert len(rows) == 1
    assert rows[0].start_event_id == 1
    assert rows[0].end_event_id == 2
    assert rows[0].duration_minutes == 30.0


def test_pair_trip_events_ignores_unmatched_start(spark):
    events = spark.createDataFrame(
        [(1, "start", _ts("2026-01-01T08:00:00"), None)],
        "id INT, event_type STRING, event_time TIMESTAMP, note STRING",
    )
    assert pair_trip_events(events).count() == 0


def test_pair_trip_events_ignores_end_without_preceding_start(spark):
    events = spark.createDataFrame(
        [
            (1, "end", _ts("2026-01-01T08:00:00"), None),
            (2, "start", _ts("2026-01-01T09:00:00"), None),
        ],
        "id INT, event_type STRING, event_time TIMESTAMP, note STRING",
    )
    assert pair_trip_events(events).count() == 0


def test_summarize_trips_daily_aggregates_by_date(spark):
    trips = spark.createDataFrame(
        [
            (1, 2, _ts("2026-01-01T08:00:00"), _ts("2026-01-01T08:30:00"), 30.0),
            (3, 4, _ts("2026-01-01T18:00:00"), _ts("2026-01-01T18:10:00"), 10.0),
            (5, 6, _ts("2026-01-02T08:00:00"), _ts("2026-01-02T08:05:00"), 5.0),
        ],
        "start_event_id INT, end_event_id INT, start_time TIMESTAMP, "
        "end_time TIMESTAMP, duration_minutes DOUBLE",
    )
    rows = {row.trip_date: row for row in summarize_trips_daily(trips).collect()}
    day_one = rows[_ts("2026-01-01T00:00:00").date()]
    assert day_one.trip_count == 2
    assert day_one.total_duration_minutes == 40.0
    assert day_one.avg_duration_minutes == 20.0
    assert day_one.min_duration_minutes == 10.0
    assert day_one.max_duration_minutes == 30.0

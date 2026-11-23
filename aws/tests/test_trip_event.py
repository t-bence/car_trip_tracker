"""Unit tests for aws/src/trip_event.py."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from trip_event import (
    InvalidEvent,
    build_event,
    object_key,
    parse_logged_at,
    unwrap_payload,
)

LOGGED_AT = "Thu, 03 Sep 2026 08:18:57 +0200"
EVENT = {
    "latitude": "47.51136133402844",
    "event": "start",
    "longitude": "19.00823675836947",
    "logged_at": LOGGED_AT,
}
RECEIVED_AT = datetime(2026, 9, 3, 6, 20, 0, tzinfo=timezone.utc)


def test_unwrap_payload_takes_a_flat_event():
    assert unwrap_payload(EVENT) == EVENT


def test_unwrap_payload_takes_a_json_string_body():
    assert unwrap_payload(json.dumps(EVENT)) == EVENT


def test_unwrap_payload_takes_a_nested_payload_object():
    assert unwrap_payload({"payload": EVENT}) == EVENT


def test_unwrap_payload_takes_the_shortcuts_encoded_payload():
    """The shape the Shortcut posts today: the object serialized into a string."""
    assert unwrap_payload({"payload": json.dumps(EVENT)}) == EVENT


def test_unwrap_payload_takes_a_double_encoded_payload():
    """The object serialized twice, which older Shortcut versions sent."""
    assert unwrap_payload({"payload": json.dumps(json.dumps(EVENT))}) == EVENT


def test_unwrap_payload_rejects_a_non_object():
    with pytest.raises(InvalidEvent):
        unwrap_payload("[1, 2, 3]")


def test_unwrap_payload_rejects_broken_json():
    with pytest.raises(InvalidEvent):
        unwrap_payload("{not json")


def test_parse_logged_at_converts_to_utc():
    # 08:18:57 +0200 is 06:18:57 UTC.
    assert parse_logged_at(LOGGED_AT) == datetime(2026, 9, 3, 6, 18, 57, tzinfo=timezone.utc)


def test_parse_logged_at_rejects_a_missing_value():
    with pytest.raises(InvalidEvent):
        parse_logged_at(None)


def test_parse_logged_at_rejects_nonsense():
    with pytest.raises(InvalidEvent):
        parse_logged_at("yesterday afternoon")


def test_build_event_stores_typed_fields():
    stored = build_event(EVENT, received_at=RECEIVED_AT)
    assert stored["event"] == "start"
    assert stored["event_time"] == "2026-09-03T06:18:57Z"
    assert stored["latitude"] == 47.51136133402844
    assert stored["longitude"] == 19.00823675836947
    assert stored["logged_at"] == LOGGED_AT
    assert stored["received_at"] == "2026-09-03T06:20:00Z"


def test_build_event_rejects_an_unknown_event_type():
    with pytest.raises(InvalidEvent):
        build_event({**EVENT, "event": "pause"})


def test_build_event_rejects_a_missing_coordinate():
    with pytest.raises(InvalidEvent):
        build_event({key: value for key, value in EVENT.items() if key != "latitude"})


def test_build_event_rejects_a_non_numeric_coordinate():
    with pytest.raises(InvalidEvent):
        build_event({**EVENT, "longitude": "east a bit"})


def test_event_id_is_stable_across_wrappings_and_receive_times():
    """The same event posted twice has to land on the same S3 key."""
    flat = build_event(EVENT, received_at=RECEIVED_AT)
    wrapped = build_event({"payload": json.dumps(EVENT)}, received_at=datetime.now(timezone.utc))
    assert flat["event_id"] == wrapped["event_id"]
    assert object_key(flat) == object_key(wrapped)


def test_event_id_differs_for_a_different_event():
    start = build_event(EVENT, received_at=RECEIVED_AT)
    stop = build_event({**EVENT, "event": "stop"}, received_at=RECEIVED_AT)
    assert start["event_id"] != stop["event_id"]


def test_object_key_is_the_event_id_under_the_prefix():
    stored = build_event(EVENT, received_at=RECEIVED_AT)
    assert object_key(stored) == f"events/{stored['event_id']}.json"

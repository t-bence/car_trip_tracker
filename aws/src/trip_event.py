"""Turning a posted body into the trip event object that gets stored in S3.

Every function here is pure, so it can be unit tested without AWS. Nothing in
this module imports boto3.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

EVENT_TYPES = ("start", "stop")

# The iOS Shortcut sends "Thu, 03 Sep 2026 08:18:57 +0200", which is RFC 2822.
# It is parsed here instead of in Spark, so the stored event_time is plain
# ISO 8601 in UTC.


class InvalidEvent(ValueError):
    """The posted body is not a usable trip event."""


def unwrap_payload(body: object) -> dict:
    """The event object, whichever way it was wrapped.

    Three shapes are accepted, because the Shortcut has sent all of them over
    time: a flat event object, `{"payload": {...}}`, and `{"payload": "<json
    string>"}` where the object was serialized before being posted. A payload
    string may itself be encoded more than once, so it is decoded until an
    object comes out.
    """
    if isinstance(body, (str, bytes)):
        body = _loads(body)
    if not isinstance(body, dict):
        raise InvalidEvent(f"expected a JSON object, got {type(body).__name__}")

    event = body.get("payload", body)
    for _ in range(5):
        if isinstance(event, (str, bytes)):
            event = _loads(event)
            continue
        break
    if not isinstance(event, dict):
        raise InvalidEvent("payload did not decode to a JSON object")
    return event


def _loads(raw: object) -> object:
    try:
        return json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidEvent(f"body is not valid JSON: {exc}") from exc


def parse_logged_at(value: object) -> datetime:
    """The logged_at string as an aware UTC datetime."""
    if not isinstance(value, str) or not value.strip():
        raise InvalidEvent("logged_at is missing")
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError) as exc:
        raise InvalidEvent(f"logged_at is not a valid date: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coordinate(event: dict, field: str) -> float:
    """A coordinate as a float. The Shortcut sends them as strings."""
    if field not in event or event[field] is None:
        raise InvalidEvent(f"{field} is missing")
    try:
        return float(event[field])
    except (TypeError, ValueError) as exc:
        raise InvalidEvent(f"{field} is not a number: {event[field]!r}") from exc


def build_event(body: object, received_at: datetime | None = None) -> dict:
    """The object to store, built out of a posted body.

    `event_id` is a hash of the event itself, so posting the same event twice
    produces the same id and the same S3 key. A retry from the Shortcut then
    overwrites one object instead of creating a duplicate.
    """
    event = unwrap_payload(body)

    event_type = event.get("event")
    if event_type not in EVENT_TYPES:
        raise InvalidEvent(f"event must be one of {EVENT_TYPES}, got {event_type!r}")

    event_time = parse_logged_at(event.get("logged_at"))
    latitude = _coordinate(event, "latitude")
    longitude = _coordinate(event, "longitude")
    received = (received_at or datetime.now(timezone.utc)).astimezone(timezone.utc)

    stored = {
        "event": event_type,
        "event_time": _iso(event_time),
        "latitude": latitude,
        "longitude": longitude,
        "logged_at": event["logged_at"],
        "received_at": _iso(received),
    }
    return {"event_id": event_id(stored), **stored}


def event_id(stored: dict) -> str:
    """A stable id for an event, derived from the event itself.

    Only the four fields that identify the event are hashed. `received_at` and
    the raw `logged_at` string are left out, so re-posting the same event gives
    the same id.
    """
    identity = {
        "event": stored["event"],
        "event_time": stored["event_time"],
        "latitude": stored["latitude"],
        "longitude": stored["longitude"],
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def object_key(stored: dict, prefix: str = "events") -> str:
    """The S3 key for an event: one flat prefix, one object per event id."""
    return f"{prefix}/{stored['event_id']}.json"


def _iso(moment: datetime) -> str:
    """ISO 8601 in UTC, with a trailing Z and whole seconds."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

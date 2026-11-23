"""Unit tests for aws/src/handler.py.

The handler reads its configuration from the environment at import time and
talks to S3 and SSM, so both clients are replaced with fakes.
"""

import base64
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

API_KEY = "a-shared-secret"
EVENT = {
    "latitude": "47.51136133402844",
    "event": "start",
    "longitude": "19.00823675836947",
    "logged_at": "Thu, 03 Sep 2026 08:18:57 +0200",
}


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = json.loads(Body)


@pytest.fixture()
def handler_module(monkeypatch):
    """The handler, with its bucket, key and S3 client replaced."""
    monkeypatch.setenv("EVENTS_BUCKET", "car-trip-events-test")
    monkeypatch.setenv("EVENTS_PREFIX", "events")
    monkeypatch.setenv("API_KEY", API_KEY)

    sys.modules.pop("handler", None)
    import handler

    handler.s3 = FakeS3()
    return handler


def _request(body, api_key=API_KEY, base64_encoded=False):
    raw = body if isinstance(body, str) else json.dumps(body)
    if base64_encoded:
        raw = base64.b64encode(raw.encode()).decode()
    headers = {"x-api-key": api_key} if api_key is not None else {}
    return {"headers": headers, "body": raw, "isBase64Encoded": base64_encoded}


def test_a_valid_event_is_stored(handler_module):
    response = handler_module.handler(_request({"payload": json.dumps(EVENT)}))
    assert response["statusCode"] == 200

    body = json.loads(response["body"])
    stored = handler_module.s3.objects[("car-trip-events-test", body["key"])]
    assert body["key"] == f"events/{body['event_id']}.json"
    assert stored["event"] == "start"
    assert stored["event_time"] == "2026-09-03T06:18:57Z"
    assert stored["latitude"] == 47.51136133402844


def test_a_flat_body_is_stored(handler_module):
    assert handler_module.handler(_request(EVENT))["statusCode"] == 200


def test_a_base64_encoded_body_is_stored(handler_module):
    assert handler_module.handler(_request(EVENT, base64_encoded=True))["statusCode"] == 200


def test_posting_the_same_event_twice_writes_one_object(handler_module):
    handler_module.handler(_request(EVENT))
    handler_module.handler(_request({"payload": json.dumps(EVENT)}))
    assert len(handler_module.s3.objects) == 1


def test_a_missing_api_key_is_rejected(handler_module):
    response = handler_module.handler(_request(EVENT, api_key=None))
    assert response["statusCode"] == 401
    assert handler_module.s3.objects == {}


def test_a_wrong_api_key_is_rejected(handler_module):
    response = handler_module.handler(_request(EVENT, api_key="guess"))
    assert response["statusCode"] == 401
    assert handler_module.s3.objects == {}


def test_the_api_key_header_is_case_insensitive(handler_module):
    request = _request(EVENT)
    request["headers"] = {"X-Api-Key": API_KEY}
    assert handler_module.handler(request)["statusCode"] == 200


def test_an_invalid_event_is_rejected(handler_module):
    response = handler_module.handler(_request({**EVENT, "event": "pause"}))
    assert response["statusCode"] == 400
    assert handler_module.s3.objects == {}


def test_a_broken_body_is_rejected(handler_module):
    response = handler_module.handler(_request("{not json"))
    assert response["statusCode"] == 400
    assert handler_module.s3.objects == {}

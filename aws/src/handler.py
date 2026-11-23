"""The ingest Lambda behind the Function URL the iOS Shortcut posts to.

It checks the shared secret, turns the body into a trip event and writes it to
S3 as one JSON object. All of the parsing lives in trip_event.py; this module
only does the AWS wiring.
"""

from __future__ import annotations

import base64
import json
import logging
import os

import boto3
from trip_event import InvalidEvent, build_event, object_key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ["EVENTS_BUCKET"]
PREFIX = os.environ.get("EVENTS_PREFIX", "events")
API_KEY_PARAMETER = os.environ.get("API_KEY_PARAMETER", "/car-trip-tracker/api-key")
API_KEY_HEADER = "x-api-key"

s3 = boto3.client("s3")
_api_key: str | None = None


def api_key() -> str:
    """The shared secret, read once per cold start.

    It comes from a SecureString in SSM Parameter Store so it is not in the
    template, the stack parameters or this repo. API_KEY overrides it for
    local runs.
    """
    global _api_key
    if _api_key is None:
        override = os.environ.get("API_KEY")
        if override:
            _api_key = override
        else:
            ssm = boto3.client("ssm")
            response = ssm.get_parameter(Name=API_KEY_PARAMETER, WithDecryption=True)
            _api_key = response["Parameter"]["Value"]
    return _api_key


def request_body(event: dict) -> str:
    """The raw request body, decoded if the Function URL base64-encoded it."""
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        return base64.b64decode(body).decode()
    return body


def authorized(event: dict) -> bool:
    """Whether the request carries the shared secret.

    Function URLs lowercase header names, but the lookup is case-insensitive
    anyway so the handler can be called directly in tests.
    """
    headers = {key.lower(): value for key, value in (event.get("headers") or {}).items()}
    presented = headers.get(API_KEY_HEADER)
    if not presented:
        return False
    return _constant_time_equals(presented, api_key())


def _constant_time_equals(left: str, right: str) -> bool:
    import hmac

    return hmac.compare_digest(left, right)


def handler(event: dict, context: object = None) -> dict:
    if not authorized(event):
        logger.warning("rejected a request without a valid api key")
        return _response(401, {"error": "unauthorized"})

    try:
        stored = build_event(request_body(event))
    except InvalidEvent as exc:
        logger.warning("rejected an invalid event: %s", exc)
        return _response(400, {"error": str(exc)})

    key = object_key(stored, PREFIX)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(stored).encode(),
        ContentType="application/json",
    )
    logger.info("stored %s", key)
    return _response(200, {"event_id": stored["event_id"], "key": key})


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }

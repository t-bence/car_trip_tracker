"""Car trip tracker app.

Receives trip start/end events (e.g. from an iOS Shortcut) and stores them
in a Lakebase (Postgres) table. Auth is handled by the Databricks Apps
platform itself (Bearer token = PAT or OAuth token of an authorized
identity) - this app does not implement its own auth.
"""

import os
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from typing import Optional

import psycopg2
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Car Trip Tracker")


class EventType(str, Enum):
    START = "start"
    END = "end"


class TripEventIn(BaseModel):
    event_type: EventType
    event_time: Optional[datetime] = None
    note: Optional[str] = None


class TripEventOut(BaseModel):
    id: int
    event_type: EventType
    event_time: datetime
    note: Optional[str] = None


@contextmanager
def get_connection():
    # Lakebase auth is a short-lived OAuth token, not a static password - generate
    # a fresh one per connection. Fine at this app's trip volume (a few events/day);
    # a long-running pool would need a token-refresh loop instead.
    w = WorkspaceClient()
    token = w.postgres.generate_database_credential(endpoint=os.environ["LAKEBASE_ENDPOINT"]).token
    conn = psycopg2.connect(
        host=os.environ["PGHOST"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=token,
        port=os.environ.get("PGPORT", "5432"),
        sslmode=os.environ.get("PGSSLMODE", "require"),
    )
    try:
        yield conn
    finally:
        conn.close()


@app.on_event("startup")
def create_schema():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trip_events (
                id SERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
                note TEXT
            )
            """
        )
        conn.commit()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/trip", response_model=TripEventOut)
def log_trip_event(event: TripEventIn):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trip_events (event_type, event_time, note)
            VALUES (%s, COALESCE(%s, now()), %s)
            RETURNING id, event_type, event_time, note
            """,
            (event.event_type.value, event.event_time, event.note),
        )
        row = cur.fetchone()
        conn.commit()
    return TripEventOut(id=row[0], event_type=row[1], event_time=row[2], note=row[3])


@app.get("/api/trips", response_model=list[TripEventOut])
def list_trip_events(limit: int = 50):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, event_type, event_time, note FROM trip_events "
            "ORDER BY event_time DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    return [TripEventOut(id=r[0], event_type=r[1], event_time=r[2], note=r[3]) for r in rows]

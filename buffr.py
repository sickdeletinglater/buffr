import asyncio
import json
import logging
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Any, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DB_PATH = "buffr.db"
QUEUE_MAX_SIZE = 10000
WORKER_DELAY_SECONDS = 0.005
SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 10
MAX_EVENT_NAME_LENGTH = 100
MAX_PROPERTIES_BYTES = 4096

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("buffr")


class Event(BaseModel):
    event: str = Field(min_length=1, max_length=MAX_EVENT_NAME_LENGTH)
    userId: Union[int, str]
    properties: dict[str, Any] = Field(default_factory=dict)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT NOT NULL,
            user_id TEXT NOT NULL,
            properties TEXT NOT NULL,
            received_at REAL NOT NULL,
            saved_at REAL NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_event ON events(event)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id)")
    conn.commit()
    return conn


def save_event(conn: sqlite3.Connection, item: dict) -> None:
    conn.execute(
        "INSERT INTO events (event, user_id, properties, received_at, saved_at) VALUES (?, ?, ?, ?, ?)",
        (item["event"], str(item["userId"]), json.dumps(item["properties"]), item["received_at"], time.time()),
    )
    conn.commit()


async def worker(app: FastAPI) -> None:
    queue: asyncio.Queue = app.state.queue
    conn: sqlite3.Connection = app.state.conn
    stats: dict = app.state.stats
    while True:
        item = await queue.get()
        try:
            await asyncio.to_thread(save_event, conn, item)
            stats["saved"] += 1
        except Exception:
            stats["failed"] += 1
            log.exception("failed to save event")
        finally:
            queue.task_done()
        await asyncio.sleep(WORKER_DELAY_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.conn = init_db()
    app.state.queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    app.state.stats = {"accepted": 0, "rejected": 0, "saved": 0, "failed": 0}
    task = asyncio.create_task(worker(app))
    log.info("buffr started")
    yield
    try:
        await asyncio.wait_for(app.state.queue.join(), timeout=SHUTDOWN_DRAIN_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        log.warning("shutdown with %d events still queued", app.state.queue.qsize())
    task.cancel()
    app.state.conn.close()


app = FastAPI(title="buffr", lifespan=lifespan)


@app.post("/events", status_code=202)
async def ingest(event: Event):
    if len(json.dumps(event.properties)) > MAX_PROPERTIES_BYTES:
        app.state.stats["rejected"] += 1
        raise HTTPException(status_code=413, detail="properties too large")
    item = {
        "event": event.event,
        "userId": event.userId,
        "properties": event.properties,
        "received_at": time.time(),
    }
    try:
        app.state.queue.put_nowait(item)
    except asyncio.QueueFull:
        app.state.stats["rejected"] += 1
        raise HTTPException(status_code=503, detail="queue full", headers={"Retry-After": "1"})
    app.state.stats["accepted"] += 1
    return {"status": "queued"}


@app.get("/stats")
async def stats():
    return {**app.state.stats, "queued": app.state.queue.qsize(), "capacity": QUEUE_MAX_SIZE}


@app.get("/health")
async def health():
    return {"status": "ok"}

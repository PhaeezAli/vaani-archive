"""The HTTP API and the process entry point that the kill scenario and the demo start.

Usage:
    python -m archive.server --root /tmp/archive/store --node-id a --port 0 --port-file /tmp/port-a

main() waits for the lease (open_when_free), recovers, binds the port, writes the port file only
after recovery and listen(), then serves until uvicorn stops, and closes the archive on the way
out. Every endpoint is a plain def and maps errors itself: an off-grid value is 422, Backpressure
429, Fenced and OSError 503, CorruptObject and UnsupportedFormat 500 with the key, and a KeyError
only in /similar becomes 404.
"""
import argparse
import os
import resource
import socket
import sys
import time

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, Field

from archive.archive import MEMTABLE_MAX_ROWS, Archive
from archive.codec import CHANNELS
from archive.manifest import LeaseHeld
from archive.store import LocalStore, ObjectStore

MAX_READINGS_PER_REQUEST = 10_000   # bounds one request's memory
MAX_HISTORY_DAYS = 7                # one history response is at most 7 days of one station
LEASE_WAIT_S = 30.0                 # open_when_free gives up after this long

class ReadingIn(BaseModel):
    station: int = Field(ge=0, lt=2**32)
    time: int = Field(ge=0, lt=2**40)               # event time, whole seconds since the epoch
    channel: int = Field(ge=0, lt=len(CHANNELS))
    value: float = Field(gt=-1e6, lt=1e6)           # gt and lt also reject nan and inf
    revision: int = Field(default=0, ge=0, lt=2**32)

class IngestBody(BaseModel):
    readings: list[ReadingIn] = Field(min_length=1, max_length=MAX_READINGS_PER_REQUEST)

def create_app(archive: Archive) -> FastAPI:
    """POST /ingest, GET /stations/{station}/history, GET /stations/{station}/similar, GET /stats.
    Every endpoint is a plain def, so FastAPI runs it in its thread pool and a blocking ingest
    waits there."""
    raise NotImplementedError

def open_when_free(store: ObjectStore, node_id: str, wait_s: float = LEASE_WAIT_S,
                   **options) -> Archive:
    """Retry Archive(store, node_id, **options) every 0.25 s while LeaseHeld, printing who holds
    it, for up to wait_s. Then re-raise."""
    raise NotImplementedError

def main(argv: list[str] | None = None) -> None:
    """python -m archive.server --root DIR --node-id NAME [--port 0] [--port-file PATH]
    [--memtable-rows N]"""
    raise NotImplementedError

if __name__ == "__main__":
    main()

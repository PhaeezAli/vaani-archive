"""The HTTP API and the process entry point that the kill scenario and the demo start.

Usage:
    python -m archive.server --root /tmp/archive/store --node-id a --port 0 --port-file /tmp/port-a

main() waits for the lease (open_when_free), recovers, binds the port, writes the port file only
after recovery and listen(), then serves until uvicorn stops, and closes the archive on the way
out. Every endpoint is a plain def and maps errors itself: an off-grid value is 422, Backpressure
429, Fenced and OSError 503, CorruptObject and UnsupportedFormat 500 with the key, and a KeyError
only in /similar becomes 404. If recovery or the bind fails, main() raises and the process exits.
"""
import argparse
import os
import resource
import socket
import sys
import time
from typing import Annotated, Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, Field

from archive.archive import MEMTABLE_MAX_ROWS, Archive, Backpressure
from archive.codec import (CHANNEL, CHANNELS, INGEST_TIME, REVISION, SEQ, TIME, VALUE,
                           CorruptObject, UnsupportedFormat, quantise, unquantise)
from archive.manifest import Fenced, LeaseHeld
from archive.store import LocalStore, ObjectStore

MAX_READINGS_PER_REQUEST = 10_000   # bounds one request's memory
MAX_HISTORY_DAYS = 7                # one history response is at most 7 days of one station
LEASE_WAIT_S = 30.0                 # open_when_free gives up after this long
LEASE_RETRY_S = 0.25                # how often open_when_free tries again while the lease is held
DAY_S = 86_400                      # seconds in a UTC day
# ru_maxrss is in bytes on macOS and in KiB on Linux
RSS_UNIT = 2**20 if sys.platform == "darwin" else 2**10
StationId = Annotated[int, Path(ge=0, lt=2**32)]          # a station id fits the WAL's u32 field

class ReadingIn(BaseModel):
    station: int = Field(ge=0, lt=2**32)
    time: int = Field(ge=0, lt=2**40)               # event time, whole seconds since the epoch
    channel: int = Field(ge=0, lt=len(CHANNELS))
    value: float = Field(gt=-1e6, lt=1e6)           # gt and lt also reject nan and inf
    revision: int = Field(default=0, ge=0, lt=2**32)

class IngestBody(BaseModel):
    readings: list[ReadingIn] = Field(min_length=1, max_length=MAX_READINGS_PER_REQUEST)

def _error(status: int, error: Exception) -> HTTPException:
    # a TimeoutError carries no message, so its repr is the detail
    return HTTPException(status, str(error) or repr(error))

def _reading_out(row: tuple) -> dict:
    return {"time": row[TIME], "channel": row[CHANNEL],
            "value": unquantise(row[CHANNEL], row[VALUE]), "revision": row[REVISION],
            "ingest_seq": row[SEQ], "ingest_time": row[INGEST_TIME]}

def create_app(archive: Archive) -> FastAPI:
    """POST /ingest, GET /stations/{station}/history, GET /stations/{station}/similar, GET /stats.
    Every endpoint is a plain def, so FastAPI runs it in its thread pool and a blocking ingest
    waits there."""
    app = FastAPI()

    @app.post("/ingest")
    def ingest(body: IngestBody) -> dict:
        try:
            readings = [(r.station, r.channel, r.time, r.revision, quantise(r.channel, r.value))
                        for r in body.readings]
        except ValueError as error:
            raise _error(422, error) from error
        try:
            wal_seq, first_seq = archive.ingest(readings)
        except Backpressure as error:
            raise HTTPException(429, str(error), headers={"Retry-After": "1"}) from error
        except (Fenced, OSError) as error:
            raise _error(503, error) from error
        return {"acked": len(readings), "wal_seq": wal_seq, "first_ingest_seq": first_seq}

    @app.get("/stations/{station}/history")
    def history(station: StationId, start: int, end: int,
                versions: Literal["latest", "all"] = "latest") -> dict:
        if not 0 < end - start <= MAX_HISTORY_DAYS * DAY_S:
            raise HTTPException(422, f"need 0 < end - start <= {MAX_HISTORY_DAYS} days")
        try:
            rows = archive.history(station, start, end, versions == "all")
        except (CorruptObject, UnsupportedFormat) as error:
            raise _error(500, error) from error
        except (Fenced, OSError) as error:
            raise _error(503, error) from error
        rows.sort(key=lambda row: (row[TIME], row[CHANNEL], row[REVISION], row[SEQ]))
        return {"station": station, "versions": versions,
                "readings": [_reading_out(row) for row in rows]}

    @app.get("/stations/{station}/similar")
    def similar(station: StationId, k: Annotated[int, Query(ge=1, le=100)] = 10) -> dict:
        try:
            ranked = archive.similar(station, k)
        except KeyError as error:
            raise HTTPException(404, f"station {station} has no flushed data") from error
        except Fenced as error:
            raise _error(503, error) from error
        return {"station": station,
                "similar": [{"station": other, "score": float(score)} for other, score in ranked]}

    @app.get("/stats")
    def stats() -> dict:
        peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / RSS_UNIT
        return {**archive.stats(), "peak_rss_mb": round(peak_rss_mb, 1)}

    return app

def open_when_free(store: ObjectStore, node_id: str, wait_s: float = LEASE_WAIT_S,
                   **options) -> Archive:
    """Retry Archive(store, node_id, **options) every 0.25 s while LeaseHeld, printing who holds
    it, for up to wait_s. Then re-raise."""
    deadline = time.monotonic() + wait_s
    while True:
        try:
            return Archive(store, node_id, **options)
        except LeaseHeld as error:
            if time.monotonic() >= deadline:
                raise
            print(f"{node_id}: waiting for the lease: {error}", flush=True)
            time.sleep(LEASE_RETRY_S)

def _listen(port: int) -> socket.socket:
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
    sock.listen(128)
    return sock

def _write_port_file(path: str, port: int) -> None:
    # written whole and moved into place, so a reader never sees half a number
    staging = f"{path}.tmp"
    with open(staging, "w") as file:
        file.write(f"{port}\n")
    os.replace(staging, path)

def main(argv: list[str] | None = None) -> None:
    """python -m archive.server --root DIR --node-id NAME [--port 0] [--port-file PATH]
    [--memtable-rows N]"""
    parser = argparse.ArgumentParser(description="Serve the archive over HTTP.")
    parser.add_argument("--root", required=True, help="the store directory")
    parser.add_argument("--node-id", required=True, help="this writer's name in the lease")
    parser.add_argument("--port", type=int, default=0, help="0 lets the kernel pick a free port")
    parser.add_argument("--port-file", help="gets the port once recovery and listen() are done")
    parser.add_argument("--memtable-rows", type=int, default=MEMTABLE_MAX_ROWS)
    args = parser.parse_args(argv)
    archive = open_when_free(LocalStore(args.root), args.node_id,
                             memtable_max_rows=args.memtable_rows)
    stats = archive.stats()
    print(f"{args.node_id}: writer token {stats['writer_token']}, "
          f"replayed {stats['replayed_wal_objects']} WAL objects", flush=True)
    try:
        sock = _listen(args.port)
        port = sock.getsockname()[1]
        if args.port_file:
            _write_port_file(args.port_file, port)
        print(f"{args.node_id}: serving on 127.0.0.1:{port}", flush=True)
        config = uvicorn.Config(create_app(archive), log_level="warning")
        uvicorn.Server(config).run(sockets=[sock])
    finally:
        archive.close()

if __name__ == "__main__":
    main()

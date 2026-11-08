"""The one mutable object: live segments, the WAL watermark, the next ingest_seq and the writer
lease with its fencing token, all in manifest.json.

Usage:
    m = acquire(store, "a", now=1000)                     # raises the writer token by one
    m = commit(store, m["writer_token"], lambda m: m["segments"].append(entry))
    m = renew(store, m["writer_token"], now=1003)

Every change goes through a conditional put in a CAS loop that re-reads, re-checks the token and
re-applies the change on every attempt. LeaseHeld if another node's lease is live, Fenced if a
newer writer holds the archive, UnsupportedFormat for an unknown manifest format. An OSError from
a put propagates and may hide a put that landed, so the caller treats the commit as not done.
"""
import json
from collections.abc import Callable

from archive.codec import UnsupportedFormat
from archive.store import ObjectStore, PreconditionFailed

MANIFEST_KEY = "manifest.json"
MANIFEST_FORMAT = 1
LEASE_SECONDS = 10   # takeover waits at most 10 s after the last renewal

class Fenced(Exception):
    """A newer writer holds the archive. This writer must stop writing."""

class LeaseHeld(Exception):
    """Another node's lease is live (now < lease_until)."""

def empty() -> dict:
    """A fresh copy of the manifest before the first writer (section 4.4)."""
    return {"commit": 0, "format": MANIFEST_FORMAT, "lease_holder": None, "lease_until": 0,
            "next_ingest_seq": 1, "segments": [], "wal_watermark": 0, "writer_token": 0}

def read(store: ObjectStore) -> tuple[dict, str | None]:
    """(manifest, etag), or (empty(), None) before the first writer. UnsupportedFormat if format
    is not MANIFEST_FORMAT."""
    try:
        data, etag = store.get(MANIFEST_KEY)
    except KeyError:
        return empty(), None
    m = json.loads(data)
    if m.get("format") != MANIFEST_FORMAT:
        raise UnsupportedFormat(f"{MANIFEST_KEY}: unknown format {m.get('format')!r}")
    return m, etag

def _put(store: ObjectStore, m: dict, etag: str | None) -> None:
    data = json.dumps(m, sort_keys=True, indent=1).encode()
    if etag is None:
        # only the very first writer creates the manifest, and two first writers cannot both win
        store.put(MANIFEST_KEY, data, if_none_match=True)
    else:
        store.put(MANIFEST_KEY, data, if_match=etag)

def acquire(store: ObjectStore, holder: str, now: int, lease_seconds: int = LEASE_SECONDS) -> dict:
    """Take the lease with writer_token + 1 if it is free, expired (now >= lease_until) or already
    ours. Otherwise LeaseHeld."""
    while True:
        m, etag = read(store)
        if m["lease_holder"] not in (None, holder) and now < m["lease_until"]:
            raise LeaseHeld(f"{m['lease_holder']} holds the lease until {m['lease_until']}")
        # the same holder still gets a new token, so an older process with this id is fenced
        m["writer_token"] += 1
        m["lease_holder"] = holder
        m["lease_until"] = now + lease_seconds
        m["commit"] += 1
        try:
            _put(store, m, etag)
            return m
        except PreconditionFailed:
            continue

def commit(store: ObjectStore, token: int, change: Callable[[dict], None]) -> dict:
    """CAS loop: read, Fenced if the stored token is not ours, apply change to the fresh copy,
    commit + 1, put with If-Match, and on PreconditionFailed start again. Return what was
    stored."""
    while True:
        m, etag = read(store)
        # re-checked and re-applied to a fresh copy on every try, so a retry never loses a renewal
        if m["writer_token"] != token:
            raise Fenced(f"writer token {m['writer_token']} is newer than {token}")
        change(m)
        m["commit"] += 1
        try:
            _put(store, m, etag)
            return m
        except PreconditionFailed:
            continue

def renew(store: ObjectStore, token: int, now: int, lease_seconds: int = LEASE_SECONDS) -> dict:
    """commit() whose change sets lease_until = now + lease_seconds. Fenced if another writer took
    over."""
    return commit(store, token, lambda m: m.update(lease_until=now + lease_seconds))

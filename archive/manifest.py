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
    raise NotImplementedError

def read(store: ObjectStore) -> tuple[dict, str | None]:
    """(manifest, etag), or (empty(), None) before the first writer. UnsupportedFormat if format
    is not MANIFEST_FORMAT."""
    raise NotImplementedError

def _put(store: ObjectStore, m: dict, etag: str | None) -> None:
    raise NotImplementedError

def acquire(store: ObjectStore, holder: str, now: int, lease_seconds: int = LEASE_SECONDS) -> dict:
    """Take the lease with writer_token + 1 if it is free, expired (now >= lease_until) or already
    ours. Otherwise LeaseHeld."""
    raise NotImplementedError

def commit(store: ObjectStore, token: int, change: Callable[[dict], None]) -> dict:
    """CAS loop: read, Fenced if the stored token is not ours, apply change to the fresh copy,
    commit + 1, put with If-Match, and on PreconditionFailed start again. Return what was
    stored."""
    raise NotImplementedError

def renew(store: ObjectStore, token: int, now: int, lease_seconds: int = LEASE_SECONDS) -> dict:
    """commit() whose change sets lease_until = now + lease_seconds. Fenced if another writer took
    over."""
    raise NotImplementedError

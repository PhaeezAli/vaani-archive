"""Object storage with S3 semantics on one local directory. It is the only durable state (doc
section 2).

Usage:
    store = LocalStore("/tmp/archive/store")
    etag = store.put("wal/000000000001", data, if_none_match=True)
    data, etag = store.get("wal/000000000001")

Every put is on stable storage before it returns: the bytes go to a staging file that is synced,
then linked or renamed into place, then the directory is synced. A failed condition raises
PreconditionFailed and writes nothing. Any OSError propagates, and after one the object may or may
not exist, exactly as after a timed-out S3 PUT, so callers treat it as unknown.
"""
import fcntl
import hashlib
import os
import pathlib
import typing
import urllib.parse
import uuid

LOCK_NAME = ".lock"   # flock target. Names that start with "." are never keys.
# staging files live next to the objects so rename and link stay on one filesystem
TMP_PREFIX = ".tmp-"

class PreconditionFailed(Exception):
    """A conditional put found the object in another state (S3 answers 412). Nothing was
    written."""

class ObjectStore(typing.Protocol):
    """What the archive needs from object storage: PutObject with If-Match or If-None-Match,
    GetObject, ranged GetObject, ListObjectsV2, DeleteObject."""

    def put(self, key: str, data: bytes, *, if_match: str | None = None,
            if_none_match: bool = False) -> str: ...

    def get(self, key: str) -> tuple[bytes, str]: ...

    def get_range(self, key: str, start: int, length: int) -> bytes: ...

    def list(self, prefix: str) -> list[str]: ...

    def delete(self, key: str) -> None: ...

def fsync_fully(fd: int) -> None:
    """F_FULLFSYNC on macOS, where plain fsync stops at the drive's cache. os.fsync elsewhere."""
    raise NotImplementedError

class LocalStore:
    """One flat directory used as a bucket, with the S3 contract in the module docstring."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        """Use directory root as the bucket and create it if missing."""
        raise NotImplementedError

    def path(self, key: str) -> pathlib.Path:
        """The file that holds key: root / urllib.parse.quote(key, safe="")."""
        raise NotImplementedError

    def put(self, key: str, data: bytes, *, if_match: str | None = None,
            if_none_match: bool = False) -> str:
        """Store data under key on stable storage before returning and return its etag.
        PreconditionFailed if if_none_match and the key exists, or if if_match is not the current
        etag or the key is missing."""
        raise NotImplementedError

    def get(self, key: str) -> tuple[bytes, str]:
        """Return (data, etag) from one read. KeyError if the key does not exist."""
        raise NotImplementedError

    def get_range(self, key: str, start: int, length: int) -> bytes:
        """Up to length bytes from offset start. A short read at the end is returned as is and the
        caller checks. KeyError if missing."""
        raise NotImplementedError

    def list(self, prefix: str) -> list[str]:
        """Keys that start with prefix, in lexicographic order. Never a staging file or the lock
        file."""
        raise NotImplementedError

    def delete(self, key: str) -> None:
        """Remove key. A missing key is not an error, as in S3."""
        raise NotImplementedError

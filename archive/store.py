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
    if hasattr(fcntl, "F_FULLFSYNC"):
        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)

class LocalStore:
    """One flat directory used as a bucket, with the S3 contract in the module docstring."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        """Use directory root as the bucket and create it if missing."""
        self.root = pathlib.Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, key: str) -> pathlib.Path:
        """The file that holds key: root / urllib.parse.quote(key, safe="")."""
        return self.root / urllib.parse.quote(key, safe="")

    def put(self, key: str, data: bytes, *, if_match: str | None = None,
            if_none_match: bool = False) -> str:
        """Store data under key on stable storage before returning and return its etag.
        PreconditionFailed if if_none_match and the key exists, or if if_match is not the current
        etag or the key is missing."""
        if if_match is not None and if_none_match:
            raise ValueError("pass if_match or if_none_match, not both")
        final = self.path(key)
        staging = self.root / (TMP_PREFIX + uuid.uuid4().hex)
        try:
            with open(staging, "xb") as out:   # "x" is O_WRONLY | O_CREAT | O_EXCL
                out.write(data)
                out.flush()
                fsync_fully(out.fileno())
            if if_none_match:
                try:
                    os.link(staging, final)   # an atomic create-if-absent, so no lock is needed
                except FileExistsError:
                    raise PreconditionFailed(f"{key} already exists") from None
            elif if_match is not None:
                self._replace_if_match(staging, final, if_match)
            else:
                os.replace(staging, final)
        finally:
            staging.unlink(missing_ok=True)   # after a link the staging name still exists
        # sync the one flat directory, so the new name is as durable as the bytes
        fd = os.open(self.root, os.O_RDONLY)
        try:
            fsync_fully(fd)
        finally:
            os.close(fd)
        return hashlib.sha256(data).hexdigest()

    def _replace_if_match(self, staging: pathlib.Path, final: pathlib.Path, etag: str) -> None:
        # flock belongs to the open file description, so it also excludes other LocalStore
        # objects in this process. Closing releases it, and the kernel drops it if we die.
        lock_fd = os.open(self.root / LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                current = final.read_bytes()
            except FileNotFoundError:
                raise PreconditionFailed(f"{final.name} is missing, so no etag matches") from None
            if hashlib.sha256(current).hexdigest() != etag:
                raise PreconditionFailed(f"{final.name} was replaced after etag {etag} was read")
            os.replace(staging, final)
        finally:
            os.close(lock_fd)

    def get(self, key: str) -> tuple[bytes, str]:
        """Return (data, etag) from one read. KeyError if the key does not exist."""
        try:
            data = self.path(key).read_bytes()
        except FileNotFoundError:
            raise KeyError(key) from None
        return data, hashlib.sha256(data).hexdigest()

    def get_range(self, key: str, start: int, length: int) -> bytes:
        """Up to length bytes from offset start. A short read at the end is returned as is and the
        caller checks. KeyError if missing."""
        try:
            with open(self.path(key), "rb") as source:
                source.seek(start)
                return source.read(length)
        except FileNotFoundError:
            raise KeyError(key) from None

    def list(self, prefix: str) -> list[str]:
        """Keys that start with prefix, in lexicographic order. Never a staging file or the lock
        file."""
        names = [name for name in os.listdir(self.root) if not name.startswith(".")]
        keys = [urllib.parse.unquote(name) for name in names]
        return sorted(key for key in keys if key.startswith(prefix))

    def delete(self, key: str) -> None:
        """Remove key. A missing key is not an error, as in S3."""
        # no directory sync: a deleted WAL object that comes back is at or below the watermark
        self.path(key).unlink(missing_ok=True)

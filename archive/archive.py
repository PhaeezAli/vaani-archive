"""The engine. Constructing an Archive makes this process the writer for all 16 shards.

After recovery, only the commit thread writes WAL objects and segments. Two threads put the
manifest: the commit thread (segments and watermark) and the renewer (lease fields), and the CAS
loop in manifest.commit orders them. Recovery runs in the constructor before either thread starts.

Usage:
    archive = Archive(LocalStore("/tmp/archive/store"), "a")
    wal_seq, first_seq = archive.ingest([(7, 0, 1762560000, 0, 213)])
    rows = archive.history(7, 1762560000, 1762563600)
    archive.close()

An ingest returns only after its WAL object is durable and a manifest read after the put still
shows this writer's token. A full memtable raises Backpressure. A newer writer raises Fenced. An
ambiguous WAL put or any exception the commit thread does not expect stops the writer: every
waiting and later request gets OSError with the reason, the renewer exits and the lease lapses,
so a fresh writer can recover from the store.
"""
import heapq
import threading
import time
import zlib
from collections.abc import Callable
from concurrent.futures import Future

from archive import manifest, rules
from archive.codec import (SEGMENT_VERSION, CorruptObject, Reading, Row, Segment, decode_wal,
                           encode_segment, encode_wal)
from archive.manifest import LEASE_SECONDS, Fenced
from archive.similarity import SimilarityIndex, day_summary
from archive.store import ObjectStore, PreconditionFailed

NUM_SHARDS = 16                 # the doc
# the doc says 100 to 200 ms and about 5 PUTs a second, which is the 200 ms end
COMMIT_WINDOW_S = 0.2
# PLACEHOLDER: wave 3 writes the value printed by demo.memtable_row_bytes() and the date
MEMTABLE_ROW_BYTES = 240
MEMTABLE_MAX_ROWS = 64 * 2**20 // MEMTABLE_ROW_BYTES   # the doc's 64 MB cap, counted in rows
COMPACT_AT_SEGMENTS = 4         # a history read touches at most 4 segments per day
ACK_TIMEOUT_S = 30.0            # an ingest waits this long for its group, then the API answers 503
IDLE_WAIT_S = 1.0               # the commit thread wakes this often, so a failed flush is retried
JOIN_TIMEOUT_S = 30.0           # close() waits this long for each thread, then raises
DAY_S = 86_400                  # seconds in a UTC day

class Backpressure(Exception):
    """Memtable rows plus queued rows plus this batch would pass the cap. The API answers 429."""

def shard_of(station: int) -> int:
    """zlib.crc32 of the 4-byte big-endian id, mod NUM_SHARDS. Stable across processes, unlike
    hash()."""
    return zlib.crc32(station.to_bytes(4, "big")) % NUM_SHARDS

def day_of(event_time: int) -> int:
    """UTC day number: event_time // 86400."""
    return event_time // DAY_S

def wal_key(seq: int) -> str:
    """f"wal/{seq:012d}". Zero padding makes lexicographic order numeric."""
    return f"wal/{seq:012d}"

def segment_key(shard: int, day: int, token: int, n: int) -> str:
    """f"seg/{shard:02d}/{day:05d}/{token:06d}-{n:09d}". The token keeps two writers from ever
    making the same key."""
    return f"seg/{shard:02d}/{day:05d}/{token:06d}-{n:09d}"

def compact_rows(segments: list[Segment], shard: int, day: int, version: int) -> tuple[bytes, int]:
    """heapq.merge of the segments' iter_rows, then rules.distinct_versions, then encode_segment.
    One decoded block per input in memory."""
    raise NotImplementedError

class Archive:
    """The single writer: recovery, group commit, memtable, flush, GC, compaction and reads."""
    index: SimilarityIndex

    def __init__(self, store: ObjectStore, node_id: str, *, now: Callable[[], float] = time.time,
                 lease_seconds: int = LEASE_SECONDS, renew: bool = True,
                 memtable_max_rows: int = MEMTABLE_MAX_ROWS,
                 commit_window_s: float = COMMIT_WINDOW_S,
                 compact_at_segments: int = COMPACT_AT_SEGMENTS,
                 segment_version: int = SEGMENT_VERSION) -> None:
        """Become the writer: acquire the lease, rebuild similarity totals, replay the WAL above
        the watermark, write the fence, flush if over the threshold, then start the commit thread
        and (if renew) the renewer. LeaseHeld, Fenced, CorruptObject, UnsupportedFormat,
        OSError."""
        raise NotImplementedError

    def ingest(self, readings: list[Reading], timeout: float = ACK_TIMEOUT_S) -> tuple[int, int]:
        """Queue one batch and wait until it is durable and re-checked. Return (wal_seq, first
        ingest_seq of the batch). Backpressure, Fenced, OSError (a storage error or a stopped
        writer), TimeoutError (fate unknown, resending is safe)."""
        raise NotImplementedError

    def history(self, station: int, start: int, end: int, all_versions: bool = False) -> list[Row]:
        """Rows with start <= event_time < end from one snapshot, in storage order: the latest
        version per key, or every distinct version."""
        raise NotImplementedError

    def similar(self, station: int, k: int = 10) -> list[tuple[int, float]]:
        """The k most similar stations over flushed data. KeyError if the station has none.
        Fenced once this writer knows it was replaced."""
        raise NotImplementedError

    def flush(self) -> None:
        """Ask the commit thread to flush now, even below the threshold, and wait up to
        ACK_TIMEOUT_S. Fenced or OSError from the flush, TimeoutError (an OSError) if the commit
        thread never answers."""
        raise NotImplementedError

    def compact(self) -> int:
        """Ask the commit thread to compact every partition with 2 or more live segments, wait up
        to ACK_TIMEOUT_S, and return how many it compacted."""
        raise NotImplementedError

    def stats(self) -> dict:
        """node_id, writer_token, fenced, stopped, wal_puts, batches, flushes, compactions,
        segments, memtable_rows, pending_rows, wal_watermark, next_wal_seq, replayed_wal_objects,
        renewals."""
        raise NotImplementedError

    def close(self) -> None:
        """Commit what is queued and stop both threads, with no flush and no lease release.
        RuntimeError if a thread is still alive after JOIN_TIMEOUT_S. Idempotent."""
        raise NotImplementedError

    def _recover(self) -> None:
        raise NotImplementedError

    def _rebuild_summaries(self) -> None:
        raise NotImplementedError

    def _replay(self, seq: int, data: bytes) -> None:
        raise NotImplementedError

    def _apply(self, rows: list[Row]) -> None:
        raise NotImplementedError

    def _fence(self) -> None:
        raise NotImplementedError

    def _commit_loop(self) -> None:
        raise NotImplementedError

    def _commit(self, batches: list[tuple[list[Reading], Future]]) -> None:
        raise NotImplementedError

    def _housekeeping(self, requests: list[tuple[str, Future]]) -> None:
        raise NotImplementedError

    def _flush(self) -> tuple[dict, dict, int]:
        raise NotImplementedError

    def _refresh_summaries(self, parts: dict, old_entries: dict) -> None:
        raise NotImplementedError

    def _delete_wal_through(self, seq: int) -> None:
        raise NotImplementedError

    def _compact(self, shard: int, day: int) -> bool:
        raise NotImplementedError

    def _renew_loop(self) -> None:
        raise NotImplementedError

    def _stop(self, reason: str, batches: list[tuple[list[Reading], Future]],
              requests: list[tuple[str, Future]]) -> None:
        raise NotImplementedError

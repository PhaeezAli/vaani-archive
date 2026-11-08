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
import itertools
import threading
import time
import zlib
from collections.abc import Callable
from concurrent.futures import Future

from archive import manifest, rules
from archive.codec import (SEGMENT_VERSION, STATION, TIME, CorruptObject, Reading, Row, Segment,
                           decode_wal, encode_segment, encode_wal)
from archive.manifest import LEASE_SECONDS, Fenced
from archive.similarity import SimilarityIndex, day_summary
from archive.store import ObjectStore, PreconditionFailed

NUM_SHARDS = 16                 # the doc
# the doc says 100 to 200 ms and about 5 PUTs a second, which is the 200 ms end
COMMIT_WINDOW_S = 0.2
# CPython bytes per memtable row, measured by demo.memtable_row_bytes() on 9 Nov 2026 (285)
MEMTABLE_ROW_BYTES = 285
MEMTABLE_MAX_ROWS = 64 * 2**20 // MEMTABLE_ROW_BYTES   # the doc's 64 MB cap, counted in rows
COMPACT_AT_SEGMENTS = 4         # a history read touches at most 4 segments per day
ACK_TIMEOUT_S = 30.0            # an ingest waits this long for its group, then the API answers 503
IDLE_WAIT_S = 1.0               # the commit thread wakes this often, so a failed flush is retried
JOIN_TIMEOUT_S = 30.0           # close() waits this long for each thread, then raises
DAY_S = 86_400                  # seconds in a UTC day
# the counters stats() reports, each changed under _lock
COUNTERS = ("wal_puts", "batches", "flushes", "compactions", "replayed_wal_objects", "renewals")

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
    merged = heapq.merge(*(segment.iter_rows() for segment in segments))
    return encode_segment(rules.distinct_versions(merged), shard, day, version)

def _settle(future: Future, outcome: object) -> None:
    # only the commit thread resolves futures, so checking done first cannot race
    if future.done():
        return
    if isinstance(outcome, BaseException):
        future.set_exception(outcome)
    else:
        future.set_result(outcome)

def _fail(futures: list[Future], error: Exception) -> None:
    for future in futures:
        _settle(future, error)

def _by_partition(entries: list[dict]) -> dict[tuple[int, int], list[dict]]:
    parts = {}
    for entry in entries:
        parts.setdefault((entry["shard"], entry["day"]), []).append(entry)
    return parts

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
        self.store = store
        self.node_id = node_id
        self.now = now
        self.lease_seconds = lease_seconds
        self.memtable_max_rows = memtable_max_rows
        self.commit_window_s = commit_window_s
        self.compact_at_segments = compact_at_segments
        self.segment_version = segment_version
        self.index = SimilarityIndex()
        # shared between threads, guarded by _lock
        self._lock = threading.Lock()
        self._wake = threading.Condition(self._lock)
        self._queue = []            # (readings, Future) waiting for the next group
        self._pending_rows = 0
        self._oldest = 0.0          # time.monotonic() when the oldest queued batch arrived
        self._requests = []         # ("flush" or "compact", Future)
        self._memtable = {}         # station -> [Row]
        self._memtable_rows = 0
        self._fenced = False
        self._stopped = None        # the reason this writer stopped, once it has
        self._closing = False
        self._counts = dict.fromkeys(COUNTERS, 0)
        self._stop_renewer = threading.Event()
        # _manifest, token, _applied_wal, _next_wal_seq, _next_ingest_seq and _segment_counter
        # are set by _recover and changed only by the commit thread afterwards
        self._recover()
        # daemon threads, so an Archive a test forgot to close cannot hang the interpreter
        self._committer = threading.Thread(target=self._commit_loop, name=f"{node_id}-commit",
                                           daemon=True)
        self._committer.start()
        self._renewer = None
        if renew:
            self._renewer = threading.Thread(target=self._renew_loop, name=f"{node_id}-renew",
                                             daemon=True)
            self._renewer.start()

    def ingest(self, readings: list[Reading], timeout: float = ACK_TIMEOUT_S) -> tuple[int, int]:
        """Queue one batch and wait until it is durable and re-checked. Return (wal_seq, first
        ingest_seq of the batch). Backpressure, Fenced, OSError (a storage error or a stopped
        writer), TimeoutError (fate unknown, resending is safe)."""
        future = Future()
        with self._lock:
            self._refuse_new_work()
            queued = self._memtable_rows + self._pending_rows
            if queued + len(readings) > self.memtable_max_rows:
                raise Backpressure(f"memtable {self._memtable_rows} + queued {self._pending_rows}"
                                   f" + batch {len(readings)} > cap {self.memtable_max_rows}")
            if not self._queue:
                self._oldest = time.monotonic()
            self._queue.append((readings, future))
            self._pending_rows += len(readings)
            self._wake.notify()
        return future.result(timeout)

    def history(self, station: int, start: int, end: int, all_versions: bool = False) -> list[Row]:
        """Rows with start <= event_time < end from one snapshot, in storage order: the latest
        version per key, or every distinct version."""
        if end <= start:
            return []
        shard = shard_of(station)
        with self._lock:
            if self._fenced:
                raise self._fenced_error()
            entries = [entry for entry in self._manifest["segments"] if entry["shard"] == shard
                       and day_of(start) <= entry["day"] <= day_of(end - 1)]
            rows = [row for row in self._memtable.get(station, []) if start <= row[TIME] < end]
        for entry in entries:
            segment = Segment(self.store, entry["key"], entry["size"])
            rows.extend(row for row in segment.rows(station) if start <= row[TIME] < end)
        rows.sort()
        if all_versions:
            return list(rules.distinct_versions(rows))
        return list(rules.latest(rows))

    def similar(self, station: int, k: int = 10) -> list[tuple[int, float]]:
        """The k most similar stations over flushed data. KeyError if the station has none.
        Fenced once this writer knows it was replaced."""
        with self._lock:
            if self._fenced:
                raise self._fenced_error()
        return self.index.top_k(station, k)

    def flush(self) -> None:
        """Ask the commit thread to flush now, even below the threshold, and wait up to
        ACK_TIMEOUT_S. Fenced or OSError from the flush, TimeoutError (an OSError) if the commit
        thread never answers."""
        self._request("flush")

    def compact(self) -> int:
        """Ask the commit thread to compact every partition with 2 or more live segments, wait up
        to ACK_TIMEOUT_S, and return how many it compacted."""
        return self._request("compact")

    def stats(self) -> dict:
        """node_id, writer_token, fenced, stopped, wal_puts, batches, flushes, compactions,
        segments, memtable_rows, pending_rows, wal_watermark, next_wal_seq, replayed_wal_objects,
        renewals."""
        with self._lock:
            return {"node_id": self.node_id, "writer_token": self.token,
                    "fenced": self._fenced, "stopped": self._stopped, **self._counts,
                    "segments": len(self._manifest["segments"]),
                    "memtable_rows": self._memtable_rows, "pending_rows": self._pending_rows,
                    "wal_watermark": self._manifest["wal_watermark"],
                    "next_wal_seq": self._next_wal_seq}

    def close(self) -> None:
        """Commit what is queued and stop both threads, with no flush and no lease release.
        RuntimeError if a thread is still alive after JOIN_TIMEOUT_S. Idempotent."""
        with self._lock:
            if self._closing:
                return
            self._closing = True
            self._wake.notify_all()
        self._stop_renewer.set()
        for thread in (self._committer, self._renewer):
            if thread is None:
                continue
            thread.join(JOIN_TIMEOUT_S)
            if thread.is_alive():
                raise RuntimeError(f"{thread.name} is still alive after {JOIN_TIMEOUT_S} s")

    def _fenced_error(self) -> Fenced:
        return Fenced(f"{self.node_id} was replaced by a newer writer")

    def _refusal(self) -> Exception | None:
        # why this writer takes no more writes, or None. Call with _lock held.
        if self._stopped is not None:
            return OSError(self._stopped)
        if self._fenced:
            return self._fenced_error()
        return None

    def _refuse_new_work(self) -> None:
        # admission shared by ingest, flush and compact. Call with _lock held.
        refusal = self._refusal()
        if refusal is not None:
            raise refusal
        if self._closing:
            raise OSError("archive closed")

    def _request(self, kind: str) -> object:
        future = Future()
        with self._lock:
            self._refuse_new_work()
            self._requests.append((kind, future))
            self._wake.notify()
        return future.result(ACK_TIMEOUT_S)

    def _recover(self) -> None:
        # acquire first: from this moment every older writer's re-check after a WAL put fails
        m = manifest.acquire(self.store, self.node_id, int(self.now()), self.lease_seconds)
        self._manifest = m
        self.token = m["writer_token"]
        watermark = m["wal_watermark"]
        self._applied_wal = watermark
        self._next_wal_seq = watermark + 1
        self._next_ingest_seq = m["next_ingest_seq"]
        self._segment_counter = 0   # segment keys carry the token, so a new token starts afresh
        self._rebuild_summaries()
        # loop over what the store holds, never over a list the manifest claims
        listed = [int(key[4:]) for key in self.store.list("wal/")]
        tail = sorted(n for n in listed if n > watermark)
        for i, n in enumerate(tail):
            # compare with the expected number, not the neighbour, so a missing first one counts
            if n != watermark + 1 + i:
                raise CorruptObject(f"gap in the WAL: wal {watermark + 1 + i} is missing but"
                                    f" wal {n} exists")
        for n in tail:
            data, _ = self.store.get(wal_key(n))
            self._replay(n, data)
        self._fence()
        if self._memtable_rows >= self.memtable_max_rows // 2:
            self._finish_flush(*self._flush())

    def _rebuild_summaries(self) -> None:
        # a damaged segment raises here and the archive refuses to start (fail closed)
        for (shard, day), entries in sorted(_by_partition(self._manifest["segments"]).items()):
            segments = [Segment(self.store, entry["key"], entry["size"]) for entry in entries]
            merged = heapq.merge(*(segment.iter_rows() for segment in segments))
            for station, rows in itertools.groupby(merged, key=lambda row: row[STATION]):
                self.index.add(station, day_summary(rules.latest(rows)))

    def _replay(self, seq: int, data: bytes) -> None:
        token, first, _, rows = decode_wal(data, wal_key(seq))
        if token > self.token:
            raise Fenced(f"{wal_key(seq)} was written by token {token}, newer than {self.token}")
        # rows keep the ingest_seq and ingest_time they were made durable with
        with self._lock:
            self._apply(rows)
        self._applied_wal = seq
        self._next_wal_seq = seq + 1
        self._next_ingest_seq = max(self._next_ingest_seq, first + len(rows))
        self._counts["replayed_wal_objects"] += 1

    def _apply(self, rows: list[Row]) -> None:
        # call with _lock held
        for row in rows:
            self._memtable.setdefault(row[STATION], []).append(row)
        self._memtable_rows += len(rows)

    def _fence(self) -> None:
        while True:
            seq = self._next_wal_seq
            data = encode_wal(self.token, self._next_ingest_seq, int(self.now()), [])
            try:
                self.store.put(wal_key(seq), data, if_none_match=True)
            except PreconditionFailed:
                # a stale writer landed this after our listing and may have acknowledged it
                landed, _ = self.store.get(wal_key(seq))
                self._replay(seq, landed)
                continue
            self._applied_wal = seq
            self._next_wal_seq = seq + 1
            return

    def _commit_loop(self) -> None:
        while True:
            with self._lock:
                while not (self._queue or self._requests or self._closing):
                    if not self._wake.wait(IDLE_WAIT_S):
                        break   # periodic wake, so a failed flush is retried
                if self._closing and not self._queue:
                    leftover, self._requests = self._requests, []
                    break
                wait = 0.0
                if self._queue:
                    wait = self.commit_window_s - (time.monotonic() - self._oldest)
            if wait > 0:
                time.sleep(wait)   # batches that arrive now join this group
            with self._lock:
                batches, self._queue = self._queue, []
                requests, self._requests = self._requests, []
            try:
                if batches:
                    self._commit(batches)
                self._housekeeping(requests)
            except Exception as error:   # the only catch-all in the library
                self._stop(f"commit thread failed: {error!r}", batches, requests)
                return
        _fail([future for _, future in leftover], OSError("archive closed"))

    def _commit(self, batches: list[tuple[list[Reading], Future]]) -> None:
        with self._lock:
            refusal = self._refusal()
        if refusal is not None:
            self._refuse_group(batches, refusal)
            return
        readings = [reading for batch, _ in batches for reading in batch]
        seq, first, commit_time = self._next_wal_seq, self._next_ingest_seq, int(self.now())
        data = encode_wal(self.token, first, commit_time, readings)
        try:
            self.store.put(wal_key(seq), data, if_none_match=True)
        except PreconditionFailed:
            # only a fence or a newer writer's object can hold our next number
            self._refuse_group(batches, Fenced(f"{wal_key(seq)} is held by a newer writer"))
            return
        except OSError as error:
            # the object may exist: reusing the number reads as fencing, skipping leaves a gap
            self._stop(f"WAL put {wal_key(seq)} failed: {error!r}. This writer stopped, restart"
                       " it to recover", batches, [])
            return
        self._next_wal_seq = seq + 1
        self._next_ingest_seq = first + len(readings)
        # a durable put alone does not prove we are still the writer (6.5 case 4)
        try:
            stored, _ = manifest.read(self.store)
        except OSError as error:
            self._stop(f"manifest re-check after {wal_key(seq)} failed: {error!r}. This writer"
                       " stopped, restart it to recover", batches, [])
            return
        if stored["writer_token"] != self.token:
            self._refuse_group(batches, Fenced(f"writer token {stored['writer_token']} is newer"
                                               f" than {self.token}"))
            return
        rows = [(station, channel, t, revision, first + i, commit_time, value)
                for i, (station, channel, t, revision, value) in enumerate(readings)]
        with self._lock:
            self._apply(rows)
            self._pending_rows -= len(rows)
            self._applied_wal = seq
            self._counts["wal_puts"] += 1
            self._counts["batches"] += len(batches)
        offset = first
        for batch, future in batches:
            _settle(future, (seq, offset))
            offset += len(batch)

    def _refuse_group(self, batches: list[tuple[list[Reading], Future]], error: Exception) -> None:
        # this group never reaches the WAL: give its rows back to admission and fail it
        with self._lock:
            self._fenced = self._fenced or isinstance(error, Fenced)
            self._pending_rows -= sum(len(batch) for batch, _ in batches)
        _fail([future for _, future in batches], error)

    def _housekeeping(self, requests: list[tuple[str, Future]]) -> None:
        with self._lock:
            refusal = self._refusal()
        if refusal is not None:
            _fail([future for _, future in requests], refusal)
            return
        flushes = [future for kind, future in requests if kind == "flush"]
        flushed = None
        due = flushes or self._memtable_rows >= self.memtable_max_rows // 2
        if due and self._memtable_rows > 0:
            try:
                parts, old_entries, applied = self._flush()
            except Fenced as error:
                with self._lock:
                    self._fenced = True
                flushed = error
            except OSError as error:
                print(f"{self.node_id}: flush failed, will retry: {error!r}", flush=True)
                flushed = error
            else:
                self._finish_flush(parts, old_entries, applied)
        for future in flushes:
            _settle(future, flushed)
        compacts = [future for kind, future in requests if kind == "compact"]
        if compacts:
            outcome = self._compact_all()
            for future in compacts:
                _settle(future, outcome)

    def _finish_flush(self, parts: dict, old_entries: dict, applied: int) -> None:
        # runs only after the flush committed, on the commit thread or in recovery
        self._refresh_summaries(parts, old_entries)
        self._delete_wal_through(applied)
        for shard, day in sorted(parts):
            if len(self._live(shard, day)) < self.compact_at_segments:
                continue
            try:
                self._compact(shard, day)
            except OSError as error:
                print(f"{self.node_id}: compaction failed, will retry: {error!r}", flush=True)

    def _compact_all(self) -> int | Exception:
        with self._lock:
            refusal = self._refusal()   # a flush in this same turn may have just been fenced
        if refusal is not None:
            return refusal
        try:
            partitions = sorted(_by_partition(self._manifest["segments"]))
            return sum(self._compact(shard, day) for shard, day in partitions)
        except Fenced as error:
            with self._lock:
                self._fenced = True
            return error
        except OSError as error:
            return error

    def _live(self, shard: int, day: int) -> list[dict]:
        return _by_partition(self._manifest["segments"]).get((shard, day), [])

    def _flush(self) -> tuple[dict, dict, int]:
        # the commit thread is the only one that changes the memtable, so it reads it unlocked
        applied, next_seq = self._applied_wal, self._next_ingest_seq
        parts = {}
        for rows in self._memtable.values():
            for row in rows:
                parts.setdefault((shard_of(row[STATION]), day_of(row[TIME])), []).append(row)
        old_entries = {part: self._live(*part) for part in parts}
        entries = []
        for shard, day in sorted(parts):
            data, count = encode_segment(sorted(parts[shard, day]), shard, day,
                                         self.segment_version)
            entries.append(self._put_segment(shard, day, data, count))

        def change(m: dict) -> None:
            m["segments"].extend(entries)
            m["wal_watermark"] = applied
            m["next_ingest_seq"] = next_seq

        committed = manifest.commit(self.store, self.token, change)
        # one swap: a reader sees the old manifest and full memtable, or the new and empty one
        with self._lock:
            self._manifest = committed
            self._memtable = {}
            self._memtable_rows = 0
            self._counts["flushes"] += 1
        return parts, old_entries, applied

    def _put_segment(self, shard: int, day: int, data: bytes, count: int) -> dict:
        # If-None-Match turns "segment keys are unique" into a checked rule. PreconditionFailed
        # is not an OSError, so a collision reaches the catch-all and stops the writer.
        self._segment_counter += 1
        key = segment_key(shard, day, self.token, self._segment_counter)
        self.store.put(key, data, if_none_match=True)
        return {"key": key, "shard": shard, "day": day, "version": self.segment_version,
                "rows": count, "size": len(data)}

    def _refresh_summaries(self, parts: dict, old_entries: dict) -> None:
        # totals change by (new day summary - old day summary), so other days are never rescanned
        for (shard, day), rows in sorted(parts.items()):
            flushed = {}
            for row in rows:
                flushed.setdefault(row[STATION], []).append(row)
            segments = [Segment(self.store, entry["key"], entry["size"])
                        for entry in old_entries[shard, day]]
            for station, new in sorted(flushed.items()):
                old_rows = sorted(row for segment in segments for row in segment.rows(station))
                new_rows = sorted(old_rows + new)
                delta = day_summary(rules.latest(new_rows)) - day_summary(rules.latest(old_rows))
                self.index.add(station, delta)

    def _delete_wal_through(self, seq: int) -> None:
        # only ever called with the watermark of a commit that succeeded
        try:
            for key in self.store.list("wal/"):
                if int(key[4:]) > seq:
                    return
                self.store.delete(key)
        except OSError as error:
            print(f"{self.node_id}: WAL delete failed, the next flush retries: {error!r}",
                  flush=True)

    def _compact(self, shard: int, day: int) -> bool:
        entries = self._live(shard, day)
        if len(entries) < 2:
            return False
        segments = [Segment(self.store, entry["key"], entry["size"]) for entry in entries]
        # always the current version, which is the lazy upgrade of v1 segments
        data, count = compact_rows(segments, shard, day, self.segment_version)
        entry = self._put_segment(shard, day, data, count)
        replaced = {old["key"] for old in entries}

        def change(m: dict) -> None:
            m["segments"] = [old for old in m["segments"] if old["key"] not in replaced]
            m["segments"].append(entry)

        # the replaced segments stay in the store, because an older snapshot may still read them
        committed = manifest.commit(self.store, self.token, change)
        with self._lock:
            self._manifest = committed
            self._counts["compactions"] += 1
        return True

    def _renew_loop(self) -> None:
        # three tries per lease, so two missed renewals still keep it
        while not self._stop_renewer.wait(self.lease_seconds / 3):
            with self._lock:
                stopped = self._stopped
            if stopped is not None or not self._committer.is_alive():
                return   # let the lease lapse so any node can recover
            try:
                manifest.renew(self.store, self.token, int(self.now()), self.lease_seconds)
            except Fenced:
                with self._lock:
                    self._fenced = True
                print(f"{self.node_id}: lease lost to a newer writer", flush=True)
                return
            except OSError as error:
                print(f"{self.node_id}: lease renewal failed, will retry: {error!r}", flush=True)
                continue
            with self._lock:
                self._counts["renewals"] += 1

    def _stop(self, reason: str, batches: list[tuple[list[Reading], Future]],
              requests: list[tuple[str, Future]]) -> None:
        with self._lock:
            if self._stopped is None:
                self._stopped = reason
            error = OSError(self._stopped)
            batches = batches + self._queue
            requests = requests + self._requests
            self._queue, self._requests = [], []
            # a done future already had its rows moved to the memtable or given back
            self._pending_rows -= sum(len(batch) for batch, future in batches
                                      if not future.done())
        print(f"{self.node_id}: {reason}", flush=True)
        _fail([future for _, future in batches + requests], error)
        self._stop_renewer.set()

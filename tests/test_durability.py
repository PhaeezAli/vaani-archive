"""Tests 1 to 7: the store contract, the ack boundary, admission, fail-stop, flush order and
damaged objects. Every fault is injected on the nth matching store call, so each run is the
same, and every wait has a timeout that fails the test."""
import struct
import threading
import time
import zlib

import pytest
from fastapi.testclient import TestClient

from archive import manifest
from archive.archive import Backpressure, wal_key
from archive.codec import CorruptObject, Segment, UnsupportedFormat, decode_wal
from archive.server import create_app
from archive.store import LocalStore, PreconditionFailed
from tests.doubles import Clock, FailingStore, GatedStore
from tests.workload import DAY0, Oracle, check, history_fn

START, END = DAY0 * 86_400, (DAY0 + 2) * 86_400   # every batch below lies in these two days
WAIT_S = 5.0                                       # every wait fails the test after this long

def _batch(station: int, n: int, first: int = 0, day: int = 0) -> list[tuple]:
    return [(station, 0, START + day * 86_400 + 60 * (first + i), 0, 100 + i) for i in range(n)]

def _clean(counts: dict[str, int]) -> bool:
    return not any(counts[name] for name in ("missing", "extra", "wrong", "doubled"))

def _all_rows(archive, stations: list[int]) -> dict[int, list[tuple]]:
    return {station: archive.history(station, START, END, True) for station in stations}

def _tail_rows(store: LocalStore) -> int:
    watermark = manifest.read(store)[0]["wal_watermark"]
    keys = [key for key in store.list("wal/") if int(key[4:]) > watermark]
    return sum(len(decode_wal(store.get(key)[0], key)[3]) for key in keys)

def test_the_store_honours_conditions_and_lists_complete_objects_in_order(tmp_path):
    """The S3 contract the design leans on: If-None-Match, If-Match, and zero-padded keys listed
    in order without staging files (LESSONS item 6)."""
    store = LocalStore(tmp_path / "store")
    first = store.put("m", b"1", if_none_match=True)
    with pytest.raises(PreconditionFailed):
        store.put("m", b"2", if_none_match=True)
    assert store.get("m") == (b"1", first)
    second = store.put("m", b"3", if_match=first)
    with pytest.raises(PreconditionFailed):
        store.put("m", b"4", if_match=first)
    assert store.get("m") == (b"3", second)
    with pytest.raises(PreconditionFailed):
        store.put("missing", b"x", if_match=second)
    for key in (wal_key(100), wal_key(9), wal_key(10), "walrus"):
        store.put(key, b"w")
    (tmp_path / "store" / ".tmp-stray").write_bytes(b"half written")
    assert store.list("wal/") == [wal_key(9), wal_key(10), wal_key(100)]
    assert store.list("") == ["m", wal_key(9), wal_key(10), wal_key(100), "walrus"]

def test_ingest_returns_only_after_its_wal_object_is_durable(tmp_path, open_archive):
    """Doc section 2: a batch is acknowledged only after its write-ahead object has been written
    to object storage."""
    store = GatedStore(tmp_path / "store")
    archive = open_archive(store)
    store.entered.clear()
    store.gate.clear()
    before = store.list("wal/")
    result = []
    worker = threading.Thread(target=lambda: result.append(archive.ingest(_batch(7, 20))))
    worker.start()
    assert store.entered.wait(WAIT_S)
    time.sleep(0.3)
    assert worker.is_alive() and result == []
    assert store.list("wal/") == before
    store.gate.set()
    worker.join(WAIT_S)
    assert not worker.is_alive() and len(result) == 1
    assert wal_key(result[0][0]) in store.list("wal/")

def test_a_full_memtable_answers_429_at_the_cap_and_not_before(tmp_path, open_archive):
    """Doc section 4: when the memory table is full the node pushes back with HTTP 429 instead of
    growing, and an exact fit is still accepted."""
    store = FailingStore(tmp_path / "store", prefix="seg/")
    archive = open_archive(store, memtable_max_rows=200)
    for i in range(8):
        archive.ingest(_batch(7, 25, first=25 * i))
        assert archive.stats()["memtable_rows"] <= 200
    with pytest.raises(Backpressure):
        archive.ingest(_batch(7, 25, first=200))
    reading = {"station": 7, "time": START, "channel": 0, "value": 21.3}
    response = TestClient(create_app(archive)).post("/ingest", json={"readings": [reading]})
    assert response.status_code == 429 and response.headers["retry-after"] == "1"
    store.failing = False
    deadline = time.monotonic() + WAIT_S
    while archive.stats()["memtable_rows"] and time.monotonic() < deadline:
        time.sleep(0.05)
    assert archive.stats()["memtable_rows"] == 0
    archive.ingest(_batch(7, 25, first=200))

@pytest.mark.parametrize("landed", [True, False], ids=["landed", "not_landed"])
def test_a_failed_wal_put_stops_the_writer_and_recovery_keeps_every_ack(tmp_path, open_archive,
                                                                       landed):
    """Section 6.4: an ambiguous WAL put stops the writer, and it never costs an acknowledged
    batch."""
    store = FailingStore(tmp_path / "store", prefix="wal/", nth=3, landed=landed)
    archive = open_archive(store)   # put 1 is the fence, put 2 is batch 1, put 3 fails
    batches = [_batch(7, 20, first=20 * i) for i in range(4)]
    archive.ingest(batches[0])
    with pytest.raises(OSError):
        archive.ingest(batches[1])
    stopped = archive.stats()["stopped"]
    assert stopped is not None and wal_key(3) in stopped
    store.failing = False
    listed = store.list("wal/")
    with pytest.raises(OSError):
        archive.ingest(batches[2])
    assert store.list("wal/") == listed
    expected = Oracle()
    expected.add(batches[0])
    assert _clean(check(history_fn(archive, START, END), expected))
    archive.close()
    fresh = open_archive(LocalStore(tmp_path / "store"))
    if landed:
        expected.add(batches[1])   # unacknowledged, but durable, so it may appear
    assert _clean(check(history_fn(fresh, START, END), expected))
    fresh.ingest(batches[3])
    expected.add(batches[3])
    assert _clean(check(history_fn(fresh, START, END), expected))

FLUSH_FAULTS = {"segment_put": ("seg/", 2, "put"), "manifest_put": ("manifest.json", 1, "put"),
                "wal_delete": ("wal/", 1, "delete")}   # (prefix, nth, op)

@pytest.mark.parametrize("case", FLUSH_FAULTS)
def test_a_flush_interrupted_at_each_step_loses_nothing(tmp_path, open_archive, case):
    """Doc section 4 order (segments durable, then the manifest swap, then WAL deletes, orphans
    ignored) and doc section 2 recovery, with replay keeping each row's ingest_seq and time."""
    prefix, nth, op = FLUSH_FAULTS[case]
    store = FailingStore(tmp_path / "store", prefix, nth=nth, op=op, failing=False)
    archive = open_archive(store)
    oracle = Oracle()
    first = _batch(3, 20) + _batch(8, 20, day=1)
    archive.ingest(first)
    oracle.add(first)
    archive.flush()
    old_watermark = manifest.read(store)[0]["wal_watermark"]
    store.failing = True
    second = _batch(5, 20, first=30) + _batch(5, 20, first=30, day=1)   # two partitions
    seq, _ = archive.ingest(second)
    oracle.add(second)
    if case == "wal_delete":
        archive.flush()
    else:
        with pytest.raises(OSError):
            archive.flush()
    store.failing = False
    third = _batch(3, 20, first=60)
    archive.ingest(third)
    oracle.add(third)
    assert _clean(check(history_fn(archive, START, END), oracle))
    served = _all_rows(archive, oracle.stations())
    plain = LocalStore(tmp_path / "store")
    stored = manifest.read(plain)[0]
    if case == "wal_delete":
        assert stored["wal_watermark"] >= seq
        assert any(int(key[4:]) <= stored["wal_watermark"] for key in plain.list("wal/"))
    else:
        live = {entry["key"] for entry in stored["segments"]}
        assert set(plain.list("seg/")) - live
        assert stored["wal_watermark"] == old_watermark
    archive.close()
    for clock in (5000, 9000):
        reopened = open_archive(LocalStore(tmp_path / "store"), now=Clock(clock))
        assert reopened.stats()["memtable_rows"] == _tail_rows(plain)
        assert _all_rows(reopened, oracle.stations()) == served
        assert _clean(check(history_fn(reopened, START, END), oracle))
        reopened.close()

def _rewrite_meta_crc(data: bytearray) -> None:
    header_len, index_offset, n_stations, _, magic = struct.unpack("<IIII4s", data[-20:])
    meta = bytes(data[:header_len]) + bytes(data[index_offset:index_offset + 12 * n_stations])
    data[-20:] = struct.pack("<IIII4s", header_len, index_offset, n_stations, zlib.crc32(meta),
                             magic)

def _damage(case: str, store: LocalStore, tail: list[str], entry: dict) -> str:
    # damages persisted bytes by hand and returns what the error message must contain
    if case == "wal_object_missing":
        store.delete(tail[1])
        return "gap"
    if case == "wal_first_tail_object_missing":
        store.delete(tail[0])
        return "gap"
    key = tail[1] if case == "wal_byte_flipped" else entry["key"]
    data = bytearray(store.path(key).read_bytes())
    if case == "wal_byte_flipped":
        data[50] ^= 0xFF   # the low byte of the first row's value
    elif case == "segment_block_byte_flipped":
        offset, length = min(Segment(store, key, entry["size"]).index.values())
        data[offset + length // 2] ^= 0xFF
    elif case == "segment_truncated":
        data = data[:-10]
    elif case == "segment_version_3":
        data[4] = 3
        _rewrite_meta_crc(data)
    else:
        data[11:15] = struct.pack("<I", 50_000)   # the first channel's step in the v2 header
        _rewrite_meta_crc(data)
    store.path(key).write_bytes(data)
    return key

DAMAGE = {"wal_byte_flipped": CorruptObject, "wal_object_missing": CorruptObject,
          "wal_first_tail_object_missing": CorruptObject,
          "segment_block_byte_flipped": CorruptObject, "segment_truncated": CorruptObject,
          "segment_version_3": UnsupportedFormat, "segment_step_changed": UnsupportedFormat}

@pytest.mark.parametrize("case", DAMAGE)
def test_damaged_objects_are_refused_not_skipped(tmp_path, open_archive, case):
    """Recovery re-checks CRCs and refuses gaps instead of trusting a list (LESSONS items 3 and
    8), readers serve N and N-1 only, and a segment's step must match the table (doc 6 and 7)."""
    store = LocalStore(tmp_path / "store")
    archive = open_archive(store)
    for i in range(5):
        archive.ingest(_batch(7, 20, first=20 * i) + _batch(9, 20, first=20 * i))
        if i < 2:
            archive.flush()
    stored = manifest.read(store)[0]
    tail = [key for key in store.list("wal/") if int(key[4:]) > stored["wal_watermark"]]
    assert len(tail) == 3
    archive.close()
    needle = _damage(case, store, tail, stored["segments"][0])
    with pytest.raises(DAMAGE[case]) as refused:
        open_archive(LocalStore(tmp_path / "store"))
    assert needle in str(refused.value)

def test_an_unexpected_error_on_the_commit_thread_stops_the_writer_and_names_it(tmp_path,
                                                                                open_archive):
    """Section 6.4: an exception the commit thread does not handle stops the writer with a reason
    instead of killing the thread silently."""
    store = FailingStore(tmp_path / "store", prefix="seg/", op="get_range", error=CorruptObject,
                         failing=False)
    archive = open_archive(store)
    archive.ingest(_batch(7, 20))
    archive.flush()
    store.failing = True
    archive.ingest(_batch(7, 20, first=20))
    with pytest.raises(OSError):
        archive.flush()   # it commits, then the summary refresh reads the old segment
    stopped = archive.stats()["stopped"]
    assert stopped is not None and "CorruptObject" in stopped
    with pytest.raises(OSError, match="CorruptObject"):
        archive.ingest(_batch(7, 20, first=40))

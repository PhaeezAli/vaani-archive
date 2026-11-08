"""Tests 12 to 14: segments round trip and v2 integers beat v1 floats, a history read fetches only
one station's byte range, and v1 segments are served beside v2 until compaction rewrites them."""
import random

from archive import manifest
from archive.codec import STATION, Segment, encode_segment
from archive.store import LocalStore
from tests.doubles import CountingStore
from tests.workload import (DAY0, DAY_S, SEED, Oracle, base_readings, check, history_fn,
                            station_day_rows, value_steps)

# v2 bytes over v1 bytes, values and timestamps only. Measured 8 Nov 2026: 0.401 for the real
# codec and 0.727 under mutant 26 (values without deltas), so 0.54 leaves about 35% on each side.
BAND = 0.54
ABSENT = 999               # a station that no round-trip case holds
NEAR_2_40 = 2**40 - 1000   # event times close to the API's upper bound
CASES = {   # (station, channel, event_time, revision, ingest_seq, ingest_time, value)
    "negative values": [(1, 0, 60 * i, 0, i, 70 * i, v) for i, v in enumerate((-500, -1, 0, 3))],
    "single row": [(2, 3, 500, 0, 1, 501, 42)],
    "single channel": [(3, 4, 60 * i, 0, i, 60 * i + 1, i % 3) for i in range(30)],
    "50 revisions of one key": [(4, 1, 900, r, 10 + r, 1000 + r, 50 + 7 * r) for r in range(50)],
    "times near 2**40": [(5, c, NEAR_2_40 + 60 * i, 0, 2**40 + i, NEAR_2_40 + 60 * i + 2, 9 - i)
                         for c in range(5) for i in range(10)],
    "stations with gaps": [(s, 2, 3600 * i, 0, s + i, 3600 * i, 10130 + i)
                           for s in (0, 7, 300, 2**32 - 1) for i in range(5)],
    "ingest_time jumps back and forth": [(6, 0, 60 * i, 0, i, (9000, 10, 5000, 3)[i % 4], i)
                                         for i in range(12)]}

def test_segments_round_trip_and_v2_values_are_much_smaller_than_v1(tmp_path):
    """Doc section 7: integers at the sensor's step as deltas beat floats, and encoding loses
    nothing, ingest_time included (doc section 3)."""
    store = LocalStore(tmp_path / "store")
    for version in (1, 2):
        for name, case in CASES.items():
            rows = sorted(case)
            data, count = encode_segment(rows, 0, DAY0, version)
            store.put(f"seg/{version}/{name}", data, if_none_match=True)
            segment = Segment(store, f"seg/{version}/{name}", len(data))
            stations = sorted({row[STATION] for row in rows})
            per_station = [row for station in stations for row in segment.rows(station)]
            assert (count, list(segment.iter_rows()), per_station) == (len(rows), rows, rows), name
            assert segment.rows(ABSENT) == []
    values_only = [(s, c, t, 0, 0, 0, v) for s, c, t, _, _, _, v in station_day_rows(8)]
    v2 = len(encode_segment(values_only, 0, DAY0, 2)[0])
    v1 = len(encode_segment(values_only, 0, DAY0, 1)[0])
    assert v2 < BAND * v1, f"v2 {v2} bytes, v1 {v1} bytes"

def test_a_history_read_fetches_only_the_stations_byte_range(tmp_path):
    """Doc section 4: history reads only that station's byte range through a per-segment index."""
    store = CountingStore(tmp_path / "store")
    rows = station_day_rows(40, step_s=600)
    data, _ = encode_segment(rows, 0, DAY0)
    store.put("seg/00/20400/000001-000000001", data, if_none_match=True)
    store.bytes_read = 0
    segment = Segment(store, "seg/00/20400/000001-000000001", len(data))
    got = segment.rows(17)
    assert got and got == [row for row in rows if row[STATION] == 17]
    assert store.bytes_read == 20 + segment.header_len + 12 * 40 + segment.index[17][1]
    assert store.bytes_read < 0.15 * len(data)

def _versions(store: LocalStore) -> dict[tuple[int, int], set[int]]:
    parts = {}
    for entry in manifest.read(store)[0]["segments"]:
        version = store.get_range(entry["key"], 4, 1)[0]
        parts.setdefault((entry["shard"], entry["day"]), set()).add(version)
    return parts

def _assert_matches(archive, oracle: Oracle) -> None:
    counts = check(history_fn(archive, DAY0 * DAY_S, (DAY0 + 3) * DAY_S), oracle)
    assert counts["readings"] > 0 and counts["stations_ok"] == counts["stations"], counts

def test_v1_segments_are_served_beside_v2_and_compaction_rewrites_them_as_v2(open_archive,
                                                                             tmp_path):
    """Doc section 6: readers understand N and N-1, writers write N, and compaction upgrades old
    segments lazily while the mixed set is served correctly."""
    stations = [0, 1, 2, 3]
    x = base_readings(stations, DAY0, 2, 3600)
    # a noon correction for every (station, day) of X, so every v1 partition also gets a v2
    keys = [(s, 0, (DAY0 + day) * DAY_S + DAY_S // 2) for s in stations for day in (0, 1)]
    keys += random.Random(SEED).sample([r[:3] for r in x if r[:3] not in keys], 20 - len(keys))
    corrections = [(s, c, t, 1, value_steps(s, c, t, 1)) for s, c, t in keys]
    y = base_readings(stations, DAY0 + 2, 1, 3600)
    oracle = Oracle()
    for batch in (x, corrections, y):
        oracle.add(batch)
    old = open_archive(segment_version=1)
    old.ingest(x)
    old.flush()
    old.close()
    archive = open_archive()
    archive.ingest(corrections)
    archive.ingest(y)
    archive.flush()
    store = LocalStore(tmp_path / "store")
    before = _versions(store)
    assert set().union(*before.values()) == {1, 2}
    assert all(2 in versions for versions in before.values() if 1 in versions)
    _assert_matches(archive, oracle)
    archive.compact()
    assert set().union(*_versions(store).values()) == {2}
    _assert_matches(archive, oracle)
    archive.close()
    _assert_matches(open_archive(), oracle)

"""Test 11: doc section 3 against the deliberately slow dict oracle, through flush, compaction
and a reopen. A failed check prints its counts in the assertion message."""
from tests.workload import DAY0, SEED, Oracle, base_readings, check, history_fn, make_batches

START, END = DAY0 * 86_400, (DAY0 + 3) * 86_400   # the three days the workload covers

def _clean(counts: dict[str, int]) -> bool:
    return not any(counts[name] for name in ("missing", "extra", "wrong", "doubled"))

def test_history_matches_a_dict_oracle_through_flush_compaction_and_reopen(open_archive):
    """Doc section 3 (duplicates are no-ops, higher revisions win, ties go to ingest_seq, old
    revisions stay, the A, B, A case) and doc section 4 (segments merge with the memtable)."""
    batches, counts = make_batches(base_readings(list(range(12)), DAY0, 3, 3600), SEED, True)
    wanted = {"resends": 10, "corrections": 50, "conflicts": 20, "conflict_resends": 10,
              "out_of_order": 100}
    assert all(counts[name] >= least for name, least in wanted.items()), counts
    archive = open_archive(memtable_max_rows=1000, compact_at_segments=3)
    oracle = Oracle()
    checkpoints = {len(batches) * i // 5 for i in range(1, 5)}
    for i, batch in enumerate(batches):
        archive.ingest(batch)
        oracle.add(batch)
        if i in checkpoints:
            assert _clean(found := check(history_fn(archive, START, END), oracle)), found
    archive.flush()
    assert _clean(found := check(history_fn(archive, START, END), oracle)), found
    stats = archive.stats()
    assert stats["flushes"] >= 5 and stats["compactions"] >= 2, stats
    archive.close()
    assert _clean(found := check(history_fn(open_archive(), START, END), oracle)), found

"""Tests 8 to 10: the lease boundary, the CAS retry, and a writer replaced in the middle of a put.
Tests 8 and 9 call the manifest with integer clocks. Test 10 runs two Archives on one store."""
import threading

import pytest

from archive import manifest
from archive.archive import wal_key
from archive.codec import CHANNEL, VALUE, decode_wal
from archive.manifest import LEASE_SECONDS, Fenced, LeaseHeld, acquire, commit, renew
from archive.store import LocalStore
from tests.doubles import CallbackStore, Clock, GatedStore
from tests.workload import DAY0

WAIT_S = 5.0   # every wait on another thread fails the test after this long instead of hanging

def test_another_node_gets_the_lease_exactly_at_the_deadline_and_not_before(tmp_path):
    """Doc section 4: one writer at a time through a lease and a fencing token in the manifest."""
    x = LocalStore(tmp_path / "x")
    assert (acquire(x, "a", 100)["writer_token"], manifest.read(x)[0]["lease_until"]) == (1, 110)
    with pytest.raises(LeaseHeld):
        acquire(x, "b", 109)
    assert (acquire(x, "b", 110)["writer_token"], manifest.read(x)[0]["lease_until"]) == (2, 120)
    with pytest.raises(Fenced):
        renew(x, 1, 110)
    assert manifest.read(x)[0]["lease_holder"] == "b"
    y = LocalStore(tmp_path / "y")
    acquire(y, "a", 100)
    assert renew(y, 1, 110)["lease_until"] == 120
    with pytest.raises(LeaseHeld):
        acquire(y, "b", 119)
    assert (acquire(y, "a", 111)["writer_token"], manifest.read(y)[0]["lease_until"]) == (2, 121)
    with pytest.raises(Fenced):
        renew(y, 1, 112)

def test_a_renewal_between_read_and_put_forces_a_retry_that_keeps_both_changes(tmp_path):
    """Doc section 4: the manifest changes only by a conditional put, retried without loss."""
    store = LocalStore(tmp_path / "store")
    start = acquire(store, "a", 100)["commit"]
    entry = {"day": DAY0, "key": "seg/00/20400/000001-000000001", "rows": 1, "shard": 0,
             "size": 100, "version": 2}
    calls = []

    def change(m: dict) -> None:
        if not calls:
            renew(store, 1, 105)   # the renewer lands between this commit's read and its put
        calls.append(m["commit"])
        m["segments"].append(entry)

    commit(store, 1, change)
    stored, _ = manifest.read(store)
    assert len(calls) == 2
    assert (stored["lease_until"], stored["segments"]) == (115, [entry])
    assert stored["commit"] == start + 2

@pytest.mark.parametrize("case", ["after_list", "after_gc"])
def test_a_writer_replaced_mid_put_never_acknowledges_and_loses_nothing(open_archive, tmp_path,
                                                                         case):
    """Doc section 4: a node that was paused, lost its lease and then wakes up cannot commit."""
    root = tmp_path / "store"
    t = DAY0 * 86_400
    a1, p, x = [(7, 1, t, 0, 10)], [(7, 0, t, 0, 5)], [(7, 0, t, 0, 70)]
    outcome = []

    def stale_ingest() -> None:
        try:
            a.ingest(p)
            outcome.append("acked")
        except Fenced:
            outcome.append("fenced")
        except Exception as error:   # recorded and asserted later, never raised in a thread
            outcome.append(repr(error))

    gated = GatedStore(root)
    a = open_archive(gated if case == "after_gc" else LocalStore(root))
    a.ingest(a1)
    later = Clock(1000 + LEASE_SECONDS)   # a's lease has just expired
    if case == "after_list":
        # a's put lands after b listed the WAL and before b's fence, so b must replay it
        b = open_archive(CallbackStore(root, stale_ingest, on_list="wal/"), "b", now=later)
        b.ingest(x)
    else:
        f, token_a = a.stats()["next_wal_seq"], a.stats()["writer_token"]
        gated.gate.clear()
        gated.entered.clear()
        thread = threading.Thread(target=stale_ingest, daemon=True)
        thread.start()
        try:
            assert gated.entered.wait(WAIT_S)   # a now holds P at the gate, aimed at wal f
            b = open_archive(LocalStore(root), "b", now=later)
            b.flush()
            assert wal_key(f) not in LocalStore(root).list("wal/")
            assert manifest.read(LocalStore(root))[0]["wal_watermark"] >= f
        finally:
            gated.gate.set()
        thread.join(WAIT_S)
        assert not thread.is_alive()
        token, _, _, rows = decode_wal(LocalStore(root).get(wal_key(f))[0], wal_key(f))
        assert (token, [row[:4] + row[6:] for row in rows]) == (token_a, p)
    b.ingest(p)   # the client resends P after its 503
    latest = b.history(7, t, t + 1)
    every = b.history(7, t, t + 1, all_versions=True)
    assert outcome == ["fenced"]
    expected = [5, 70] if case == "after_list" else [5]
    assert [row[VALUE] for row in every if row[CHANNEL] == 0] == expected
    assert [row[VALUE] for row in every if row[CHANNEL] == 1] == [10]
    assert {row[CHANNEL]: row[VALUE] for row in latest} == {0: expected[-1], 1: 10}
    b.close()
    fresh = open_archive(LocalStore(root), "b", now=later)
    assert fresh.history(7, t, t + 1) == latest
    assert fresh.history(7, t, t + 1, all_versions=True) == every

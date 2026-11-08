"""The doc section 3 rules in one place, for every read path, compaction and the summaries.

Usage:
    rows = sorted(memtable_rows + segment_rows)
    answer = list(latest(rows))               # or list(distinct_versions(rows)) for versions=all

A duplicate keeps its first copy (lowest ingest_seq), the highest (revision, ingest_seq) wins and
every distinct version stays. Callers pass rows already sorted in storage order. These functions
never sort and raise nothing of their own.
"""
import itertools
from collections.abc import Iterable, Iterator

from archive.codec import REVISION, SEQ, VALUE, Row

def versions_by_key(rows: Iterable[Row]) -> Iterator[list[Row]]:
    """Group sorted rows by (station, channel, event_time). Per key keep the first row (lowest
    ingest_seq) of each distinct (revision, value)."""
    for _, same_key in itertools.groupby(rows, key=lambda row: row[:3]):
        # Rows of one key come sorted by (revision, ingest_seq), so the first copy is the oldest.
        # Keeping it is what makes a late resend a true no-op (the A, B, A case).
        firsts = {}
        for row in same_key:
            firsts.setdefault((row[REVISION], row[VALUE]), row)
        yield list(firsts.values())

def distinct_versions(rows: Iterable[Row]) -> Iterator[Row]:
    """Every distinct version of every key, in storage order (compaction and versions=all)."""
    return itertools.chain.from_iterable(versions_by_key(rows))

def latest(rows: Iterable[Row]) -> Iterator[Row]:
    """One row per key: the highest (revision, ingest_seq) after duplicates are dropped."""
    for kept in versions_by_key(rows):
        yield max(kept, key=lambda row: (row[REVISION], row[SEQ]))

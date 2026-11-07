"""The doc section 3 rules, in exactly one place: every read path, compaction and the similarity
summaries use them.

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
    raise NotImplementedError

def distinct_versions(rows: Iterable[Row]) -> Iterator[Row]:
    """Every distinct version of every key, in storage order (compaction and versions=all)."""
    raise NotImplementedError

def latest(rows: Iterable[Row]) -> Iterator[Row]:
    """One row per key: the highest (revision, ingest_seq) after duplicates are dropped."""
    raise NotImplementedError

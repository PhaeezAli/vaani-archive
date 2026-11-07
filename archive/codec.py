"""Every byte format in one place: the shared row layout, quantisation, WAL objects and segments.

Usage:
    steps = quantise(0, 21.3)          # 213 steps of 0.1 C
    data = encode_wal(token, first_seq, commit_time, readings)
    rows = Segment(store, key, size).rows(station)

Everything here is a pure function except Segment, which fetches byte ranges through an
ObjectStore. A damaged object raises CorruptObject and an intact object this reader cannot serve
raises UnsupportedFormat, both naming the key. No row is ever returned from bytes whose check
failed. A value that is not on its channel's grid raises ValueError, it is never rounded.
"""
import itertools
import struct
import zlib
from collections.abc import Iterable, Iterator

from archive.store import ObjectStore

CHANNELS: tuple[tuple[str, int], ...] = (
    ("temperature_c", 100_000), ("humidity_pct", 1_000_000), ("pressure_hpa", 100_000),
    ("wind_ms", 100_000), ("rain_mm", 200_000))
# (name, step in millionths of the unit): the sensor's resolution. Integers, so step arithmetic is
# exact.
STEPS: tuple[int, ...] = tuple(step for _, step in CHANNELS)
Row = tuple[int, int, int, int, int, int, int]
# (station, channel, event_time, revision, ingest_seq, ingest_time, value). Tuple order is storage
# order, so sorted() and heapq.merge need no key function. value is in whole steps.
STATION, CHANNEL, TIME, REVISION, SEQ, INGEST_TIME, VALUE = range(7)
# (station, channel, event_time, revision, value in steps), as a batch carries it
Reading = tuple[int, int, int, int, int]
WAL_MAGIC, SEGMENT_MAGIC, FOOTER_MAGIC = b"AWAL", b"ASEG", b"GESA"
WAL_VERSION = 1
WAL_READ_VERSIONS = (1,)            # only one WAL format exists so far
SEGMENT_VERSION = 2                 # writers write N
SEGMENT_READ_VERSIONS = (1, 2)      # readers read N and N-1 (doc section 6)
ZLIB_LEVEL = 6                      # the level the experiment used
GRID_TOLERANCE = 1e-6               # a value within a millionth of a step of the grid is on it
MICROS = 1_000_000                  # STEPS are in millionths of the unit

class CorruptObject(Exception):
    """Length, magic or CRC check failed, or a listed object is missing. These are not the bytes
    that were written. The message names the key."""

class UnsupportedFormat(Exception):
    """An intact object this reader cannot serve: an unknown version, or a step that differs from
    today's table. The message names the key."""

def quantise(channel: int, value: float) -> int:
    """Whole steps for value. ValueError if value is not on the channel's grid. It never rounds a
    real difference away."""
    steps = value * MICROS / STEPS[channel]
    whole = round(steps)
    if abs(steps - whole) > GRID_TOLERANCE:
        step = STEPS[channel] / MICROS
        raise ValueError(
            f"value {value} is not a whole number of channel {channel} steps of {step:g}")
    return whole

def unquantise(channel: int, steps: int) -> float:
    """Back to a float for the API, rounded to 6 decimals."""
    return round(steps * STEPS[channel] / MICROS, 6)

def _put_uvarint(out: bytearray, n: int) -> None:
    raise NotImplementedError

def _get_uvarint(data: bytes, pos: int) -> tuple[int, int]:
    raise NotImplementedError

def _zigzag(n: int) -> int:
    raise NotImplementedError

def _unzigzag(z: int) -> int:
    raise NotImplementedError

def _deltas(xs: list[int]) -> list[int]:
    raise NotImplementedError

def _undeltas(ds: list[int]) -> list[int]:
    raise NotImplementedError

def encode_wal(token: int, first_seq: int, commit_time: int, readings: list[Reading]) -> bytes:
    """One WAL object. Reading i gets ingest_seq first_seq + i. An empty list is a fence object."""
    raise NotImplementedError

def decode_wal(data: bytes, key: str) -> tuple[int, int, int, list[Row]]:
    """Return (token, first_seq, commit_time, rows) with ingest_seq and ingest_time filled in.
    CorruptObject or UnsupportedFormat."""
    raise NotImplementedError

def _encode_block(rows: list[Row], version: int) -> bytes:
    raise NotImplementedError

def _decode_block(raw: bytes, station: int, version: int, key: str) -> list[Row]:
    raise NotImplementedError

def encode_segment(rows: Iterable[Row], shard: int, day: int,
                   version: int = SEGMENT_VERSION) -> tuple[bytes, int]:
    """Sorted rows of one (shard, day) partition to (segment bytes, row count), decoding nothing
    and holding one station's rows at a time."""
    raise NotImplementedError

class Segment:
    """One segment read through ranged gets: footer, header and index at open, then one station
    block per request."""
    key: str
    version: int
    shard: int
    day: int
    header_len: int
    index: dict[int, tuple[int, int]]        # station -> (offset, length) of its zlib block

    def __init__(self, store: ObjectStore, key: str, size: int) -> None:
        """Fetch the footer, then header and index. Check footer magic, meta CRC, header magic,
        version, then steps."""
        raise NotImplementedError

    def _read_block(self, offset: int, length: int) -> bytes:
        raise NotImplementedError

    def rows(self, station: int) -> list[Row]:
        """Fetch and decode only this station's block. [] if the station is absent."""
        raise NotImplementedError

    def iter_rows(self) -> Iterator[Row]:
        """Every row in storage order, one decoded block in memory at a time."""
        raise NotImplementedError

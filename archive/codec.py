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
from operator import itemgetter

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
# Byte layouts from spec section 4, all little-endian. The WAL row is Reading in the same order.
WAL_HEADER = struct.Struct("<4sBQQQI")    # magic, version, token, first_seq, commit_time, count
WAL_ROW = struct.Struct("<IBqIq")         # station, channel, event_time, revision, value: 25 bytes
WAL_CRC = struct.Struct("<I")             # crc32 of every byte before it, at the very end
WAL_MIN_BYTES = WAL_HEADER.size + WAL_CRC.size   # 37, the size of a fence object
SEGMENT_HEADER = struct.Struct("<4sBBIB")  # magic, version, shard, day, n_channels: 11 bytes
INDEX_ENTRY = struct.Struct("<III")       # station, offset from the object start, block length
FOOTER = struct.Struct("<IIII4s")         # header_len, index_offset, n_stations, meta_crc, magic

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
    # LEB128: 7 bits per byte, low bits first, high bit set on every byte except the last
    while n >= 0x80:
        out.append(n & 0x7F | 0x80)
        n >>= 7
    out.append(n)

def _get_uvarint(data: bytes, pos: int) -> tuple[int, int]:
    n = 0
    shift = 0
    while data[pos] & 0x80:
        n |= (data[pos] & 0x7F) << shift
        shift += 7
        pos += 1
    return n | data[pos] << shift, pos + 1

def _zigzag(n: int) -> int:
    # an explicit branch, because Python ints are unbounded and the 64-bit xor trick needs a width
    if n >= 0:
        return 2 * n
    return -2 * n - 1

def _unzigzag(z: int) -> int:
    if z % 2 == 0:
        return z // 2
    return -(z + 1) // 2

def _deltas(xs: list[int]) -> list[int]:
    # the first delta is taken against 0, so the first value is stored as is
    return xs[:1] + [b - a for a, b in zip(xs, xs[1:])]

def _undeltas(ds: list[int]) -> list[int]:
    return list(itertools.accumulate(ds))

def encode_wal(token: int, first_seq: int, commit_time: int, readings: list[Reading]) -> bytes:
    """One WAL object. Reading i gets ingest_seq first_seq + i. An empty list is a fence object."""
    out = bytearray(WAL_HEADER.pack(WAL_MAGIC, WAL_VERSION, token, first_seq, commit_time,
                                    len(readings)))
    for reading in readings:
        out += WAL_ROW.pack(*reading)
    out += WAL_CRC.pack(zlib.crc32(out))
    return bytes(out)

def decode_wal(data: bytes, key: str) -> tuple[int, int, int, list[Row]]:
    """Return (token, first_seq, commit_time, rows) with ingest_seq and ingest_time filled in.
    CorruptObject or UnsupportedFormat."""
    if len(data) < WAL_MIN_BYTES:
        raise CorruptObject(f"{key}: {len(data)} bytes is shorter than a WAL header")
    magic, version, token, first_seq, commit_time, count = WAL_HEADER.unpack_from(data)
    if magic != WAL_MAGIC:
        raise CorruptObject(f"{key}: bad WAL magic {magic!r}")
    # the CRC comes from the last 4 bytes, so a damaged count is caught before anyone trusts it
    if zlib.crc32(data[:-WAL_CRC.size]) != WAL_CRC.unpack_from(data, len(data) - WAL_CRC.size)[0]:
        raise CorruptObject(f"{key}: WAL CRC mismatch")
    if version not in WAL_READ_VERSIONS:
        raise UnsupportedFormat(f"{key}: WAL version {version} is not in {WAL_READ_VERSIONS}")
    if len(data) != WAL_MIN_BYTES + WAL_ROW.size * count:
        raise CorruptObject(f"{key}: {len(data)} bytes does not fit a WAL of {count} rows")
    body = data[WAL_HEADER.size:-WAL_CRC.size]
    rows = [(station, channel, t, revision, first_seq + i, commit_time, value)
            for i, (station, channel, t, revision, value) in enumerate(WAL_ROW.iter_unpack(body))]
    return token, first_seq, commit_time, rows

def _encode_block(rows: list[Row], version: int) -> bytes:
    out = bytearray()
    for channel, run in itertools.groupby(rows, key=itemgetter(CHANNEL)):
        run = list(run)
        _put_uvarint(out, channel)
        _put_uvarint(out, len(run))
        d = _deltas([row[TIME] for row in run])
        # delta of delta: a clean cadence becomes t0, step, 0, 0, ... and each 0 is one byte
        columns = [d[:1] + _deltas(d[1:])]
        columns += [_deltas([row[field] for row in run]) for field in (REVISION, SEQ, INGEST_TIME)]
        if version >= 2:
            columns.append(_deltas([row[VALUE] for row in run]))
        for column in columns:
            for n in column:
                _put_uvarint(out, _zigzag(n))
        if version == 1:
            # v1 stores float64 physical values, the format before the experiment
            out += struct.pack(f"<{len(run)}d", *(unquantise(channel, row[VALUE]) for row in run))
    return bytes(out)

def _get_zigzags(raw: bytes, pos: int, n: int) -> tuple[list[int], int]:
    numbers = []
    for _ in range(n):
        z, pos = _get_uvarint(raw, pos)
        numbers.append(_unzigzag(z))
    return numbers, pos

def _decode_block(raw: bytes, station: int, version: int, key: str) -> list[Row]:
    rows = []
    pos = 0
    while pos < len(raw):
        channel, pos = _get_uvarint(raw, pos)
        n, pos = _get_uvarint(raw, pos)
        columns = []
        for _ in range(4 if version == 1 else 5):
            column, pos = _get_zigzags(raw, pos, n)
            columns.append(column)
        dod = columns[0]
        times = _undeltas(dod[:1] + _undeltas(dod[1:]))
        others = [_undeltas(column) for column in columns[1:]]
        if version == 1:
            floats = struct.unpack_from(f"<{n}d", raw, pos)
            pos += 8 * n
            try:
                others.append([quantise(channel, value) for value in floats])
            except ValueError as error:
                raise UnsupportedFormat(f"{key}: v1 {error}") from None
        rows += zip([station] * n, [channel] * n, times, *others)
    return rows

def encode_segment(rows: Iterable[Row], shard: int, day: int,
                   version: int = SEGMENT_VERSION) -> tuple[bytes, int]:
    """Sorted rows of one (shard, day) partition to (segment bytes, row count), decoding nothing
    and holding one station's rows at a time."""
    header = SEGMENT_HEADER.pack(SEGMENT_MAGIC, version, shard, day, len(CHANNELS))
    if version >= 2:
        header += struct.pack(f"<{len(STEPS)}I", *STEPS)
    out = bytearray(header)
    index = bytearray()
    count = 0
    for station, block_rows in itertools.groupby(rows, key=itemgetter(STATION)):
        block_rows = list(block_rows)
        block = zlib.compress(_encode_block(block_rows, version), ZLIB_LEVEL)
        index += INDEX_ENTRY.pack(station, len(out), len(block))
        out += block
        count += len(block_rows)
    n_stations = len(index) // INDEX_ENTRY.size
    footer = FOOTER.pack(len(header), len(out), n_stations, zlib.crc32(header + index),
                         FOOTER_MAGIC)
    return bytes(out + index + footer), count

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
        self.store = store
        self.key = key
        # size comes from the manifest, so a truncated object has no footer where we look
        footer = self._fetch(max(size - FOOTER.size, 0), FOOTER.size)
        if len(footer) != FOOTER.size or footer[-4:] != FOOTER_MAGIC:
            raise CorruptObject(f"{key}: no footer in the last {FOOTER.size} of {size} bytes, "
                                "the object is truncated or damaged")
        self.header_len, index_offset, n_stations, meta_crc, _ = FOOTER.unpack(footer)
        header = self._fetch(0, self.header_len)
        index = self._fetch(index_offset, INDEX_ENTRY.size * n_stations)
        if len(header) != self.header_len or len(index) != INDEX_ENTRY.size * n_stations:
            raise CorruptObject(f"{key}: header or index is shorter than the footer says")
        if zlib.crc32(header + index) != meta_crc:
            raise CorruptObject(f"{key}: header and index CRC mismatch")
        magic, self.version, self.shard, self.day, n_channels = SEGMENT_HEADER.unpack_from(header)
        if magic != SEGMENT_MAGIC:
            raise CorruptObject(f"{key}: bad segment magic {magic!r}")
        if self.version not in SEGMENT_READ_VERSIONS:
            raise UnsupportedFormat(f"{key}: segment version {self.version} is not readable")
        if n_channels > len(CHANNELS):
            raise UnsupportedFormat(f"{key}: {n_channels} channels, more than this reader knows")
        if self.version >= 2:
            self._check_steps(struct.unpack_from(f"<{n_channels}I", header, SEGMENT_HEADER.size))
        self.index = {station: (offset, length)
                      for station, offset, length in INDEX_ENTRY.iter_unpack(index)}

    def _check_steps(self, steps: tuple[int, ...]) -> None:
        # rescaling an older, coarser step exactly is not built, so a changed step is refused
        for channel, step in enumerate(steps):
            if step != STEPS[channel]:
                raise UnsupportedFormat(f"{self.key}: channel {channel} has step {step} in the "
                                        f"header but {STEPS[channel]} in today's table")

    def _fetch(self, start: int, length: int) -> bytes:
        try:
            return self.store.get_range(self.key, start, length)
        except KeyError:
            raise CorruptObject(f"{self.key}: listed in the manifest but missing") from None

    def _read_block(self, offset: int, length: int) -> bytes:
        raw = self._fetch(offset, length)
        if len(raw) != length:
            raise CorruptObject(f"{self.key}: block at {offset} is {len(raw)} of {length} bytes")
        try:
            # zlib checks the stream's own Adler-32, so there is no separate CRC per block
            return zlib.decompress(raw)
        except zlib.error as error:
            raise CorruptObject(f"{self.key}: block at {offset} failed zlib: {error}") from None

    def rows(self, station: int) -> list[Row]:
        """Fetch and decode only this station's block. [] if the station is absent."""
        if station not in self.index:
            return []
        offset, length = self.index[station]
        return _decode_block(self._read_block(offset, length), station, self.version, self.key)

    def iter_rows(self) -> Iterator[Row]:
        """Every row in storage order, one decoded block in memory at a time."""
        for station in self.index:
            yield from self.rows(station)

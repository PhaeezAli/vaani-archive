"""One command that runs the SIGKILL scenario of tests/test_process.py and measures three things.

Usage: .venv/bin/python demo.py   (no flags, about 20 s)

The work directory is kept and printed, so the real WAL objects, segments and manifest.json can be
opened under store/. Exit code 0 only if every check passed, else 1 with one FAILED line per
failed check. A harness failure raises RuntimeError with the server's log tail.
"""
import os
import shutil
import statistics
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

from archive.codec import encode_segment
from archive.store import LocalStore
from tests.scenarios import kill_scenario
from tests.workload import DAY0, station_day_rows

PUT_BYTES = 8 * 1024            # about one WAL object of 300 readings
MEMTABLE_BYTES = 64 * 2**20     # the doc's memtable cap

def _median_ms(step, n: int) -> float:
    folder = tempfile.mkdtemp(prefix="archive-sync-")
    times = []
    for i in range(n):
        started = time.perf_counter()
        step(folder, i)
        times.append(time.perf_counter() - started)
    shutil.rmtree(folder)
    return statistics.median(times) * 1000

def store_put_ms(n: int = 40) -> float:
    """Median of n LocalStore puts of 8 KB with If-None-Match, each to a new key: the real put,
    with F_FULLFSYNC on macOS."""
    data = os.urandom(PUT_BYTES)
    return _median_ms(lambda folder, i: LocalStore(folder).put(f"put-{i}", data,
                                                               if_none_match=True), n)

def plain_fsync_ms(n: int = 40) -> float:
    """Median of n cycles of writing 8 KB to a new file and calling plain os.fsync on the file
    and the directory. Not durable on macOS, so this is only the comparison."""
    data = os.urandom(PUT_BYTES)

    def step(folder: str, i: int) -> None:
        fd = os.open(os.path.join(folder, f"put-{i}"), os.O_WRONLY | os.O_CREAT, 0o644)
        os.write(fd, data)
        os.fsync(fd)
        os.close(fd)
        folder_fd = os.open(folder, os.O_RDONLY)
        os.fsync(folder_fd)
        os.close(folder_fd)

    return _median_ms(step, n)

def memtable_row_bytes(n: int = 200_000) -> float:
    """tracemalloc bytes per row for the memtable's real layout: dict station -> list of 7-int Row
    tuples with realistic magnitudes."""
    tracemalloc.start()
    memtable = {}
    for i in range(n):
        station = 1_000 + i % 20_000
        row = (station, i % 5, 1_762_560_000 + i, 0, 18_000_000 + i, 1_762_560_003 + i,
               100_000 + i % 997)
        memtable.setdefault(station, []).append(row)
    used = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    return used / n

def segment_sizes() -> dict[str, float]:
    """Bytes per reading for station_day_rows(8): v2 and v1 with every column, and v2 with values
    and timestamps only."""
    rows = station_day_rows(8)
    values_only = [(s, c, t, 0, 0, 0, v) for s, c, t, _, _, _, v in rows]
    sizes = {"v2": (rows, 2), "v1": (rows, 1), "v2 values only": (values_only, 2)}
    return {name: len(encode_segment(part, 0, DAY0, version)[0]) / len(rows)
            for name, (part, version) in sizes.items()}

def _table(header: tuple, rows: list[tuple]) -> None:
    widths = [max(len(str(cell)) for cell in column) for column in zip(header, *rows)]
    for row in [header, *rows]:
        print("  ".join(str(cell).ljust(width) for cell, width in zip(row, widths)).rstrip())
    print(flush=True)

def _measurements() -> list[tuple[str, str]]:
    row_bytes = memtable_row_bytes()
    sizes = segment_sizes()
    return [
        ("8 KB LocalStore put (F_FULLFSYNC on macOS), median of 40", f"{store_put_ms():.1f} ms"),
        ("8 KB write and plain fsync (not durable on macOS), median of 40",
         f"{plain_fsync_ms():.1f} ms"),
        (f"CPython bytes per memtable row (64 MB is {MEMTABLE_BYTES // round(row_bytes):,} rows)",
         f"{row_bytes:.0f}"),
        ("segment bytes per reading, v2, every column", f"{sizes['v2']:.2f}"),
        ("segment bytes per reading, v1 (float64 values)", f"{sizes['v1']:.2f}"),
        ("v2 with values and timestamps only (the doc's table)", f"{sizes['v2 values only']:.2f}")]

def main() -> int:
    started = time.monotonic()
    work = Path(tempfile.mkdtemp(prefix="archive-demo-"))
    print(f"work directory kept for inspection: {work}\n", flush=True)
    print(f"[{time.monotonic() - started:5.1f}s] kill scenario: 24 stations, 2 days of 30-minute "
          "readings, 5 channels, 8 clients", flush=True)
    result = kill_scenario(work)
    print(f"[{time.monotonic() - started:5.1f}s] measurements\n", flush=True)
    measurements = result["info"] + _measurements()
    _table(("check", "result", "ok"),
           [(name, text, "yes" if ok else "NO") for name, text, ok in result["checks"]])
    _table(("measurement", "value"), measurements)
    failed = [(name, text) for name, text, ok in result["checks"] if not ok]
    for name, text in failed:
        print(f"FAILED: {name}: {text}", flush=True)
    if not failed:
        print(f"all {len(result['checks'])} checks passed in {time.monotonic() - started:.1f} s. "
              "README.md lists the test behind each line.", flush=True)
    return 1 if failed else 0

if __name__ == "__main__":
    sys.exit(main())

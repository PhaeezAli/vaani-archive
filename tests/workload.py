"""The deliberately slow oracle and the seeded workloads that every test file and the demo share.

Usage: batches, counts = make_batches(base_readings(stations, DAY0, 3, 3600), SEED, True), then
oracle.add(batch) for each acknowledged batch, then check(history_fn(archive, start, end), oracle).
Everything is seeded, so every run is the same. make_batches raises ValueError if a part of the
workload has fewer than BATCH_MIN readings. The oracle does not use archive.rules, on purpose.
"""
import itertools
import math
import random
import zlib
from collections.abc import Callable

from archive.codec import CHANNEL, CHANNELS, REVISION, TIME, VALUE, Reading, Row

SEED = 20261108                   # any fixed seed, so every run is the same
DAY0 = 20400                      # an arbitrary fixed UTC day number in November 2025
GROUPS = ("coastal", "inland", "solar")   # three planted behaviours the similarity test recovers
# every batch, resends and corrections included. 8 workers x 60 bounds a group at 480 rows.
BATCH_MIN, BATCH_MAX = 20, 60
DAY_S, HOUR_S = 86_400, 3_600     # seconds in a UTC day and in an hour
SOLAR_HOURS = range(6, 20)        # solar stations send nothing from 20:00 to 06:00 UTC
PHASE_HOUR = 9                    # the daily wave rises through its mean at 09:00 UTC
PRESSURE = 2                      # the one channel whose profile every group shares
# (base, daily swing) in steps, per channel and group. Pressure gets a per-station offset on top.
PROFILES = (
    {"coastal": (150, 30), "inland": (120, 90), "solar": (200, 60)},
    {"coastal": (85, 0), "inland": (50, 15), "solar": (30, 0)},
    {"coastal": (10130, 0), "inland": (10130, 0), "solar": (10130, 0)},
    {"coastal": (60, 0), "inland": (25, 0), "solar": (15, 0)},
    {"coastal": (10, 0), "inland": (5, 0), "solar": (0, 0)})
SHUFFLE_WINDOW_S = 3 * HOUR_S     # arrivals are shuffled inside 3-hour windows
LATE_PERCENT = 5                  # readings moved to the very end, like a station after an outage
CORRECTION_PERCENT = 3            # readings sent again at revision 1
CONFLICT_PERCENT = 2              # keys overwritten at the same revision (the A, B, A case)
RESEND_CHANCE = 0.1               # about 1 batch in 10 is followed by a copy of an earlier batch
WALK_STEPS = 0.5                  # station_day_rows: random walk per reading, like the experiment
MAX_DELAY_S = 5                   # station_day_rows: ingest_time is the step plus 0 to 5 s

def group_of(station: int) -> str:
    """GROUPS[station % 3]."""
    return GROUPS[station % len(GROUPS)]

def _profile(station: int, channel: int, t: int) -> float:
    base, swing = PROFILES[channel][group_of(station)]
    wave = math.sin(2 * math.pi * ((t % DAY_S) / HOUR_S - PHASE_HOUR) / 24)
    offset = zlib.crc32(station.to_bytes(4, "big")) % 301 - 150 if channel == PRESSURE else 0
    return base + swing * wave + offset

def value_steps(station: int, channel: int, t: int, revision: int) -> int:
    """A reading's value in steps, a pure function of (key, revision): the group's daily profile, a
    per-station pressure offset, small hash noise, plus 7 per revision."""
    noise = zlib.crc32(f"{station}:{channel}:{t}".encode()) % 5 - 2
    return round(_profile(station, channel, t)) + noise + 7 * revision

def base_readings(stations: list[int], day0: int, days: int, step_s: int) -> list[Reading]:
    """Every (station, channel, time) at the cadence with revision 0. Solar stations send nothing
    from 20:00 to 06:00 UTC."""
    readings = []
    for t in range(day0 * DAY_S, (day0 + days) * DAY_S, step_s):
        for station in stations:
            if group_of(station) == "solar" and (t % DAY_S) // HOUR_S not in SOLAR_HOURS:
                continue
            for channel in range(len(CHANNELS)):
                readings.append((station, channel, t, 0, value_steps(station, channel, t, 0)))
    return readings

def _chunks(readings: list[Reading], rng: random.Random) -> list[list[Reading]]:
    if 0 < len(readings) < BATCH_MIN:
        raise ValueError(f"{len(readings)} readings cannot fill a batch of {BATCH_MIN}")
    batches = []
    start = 0
    while len(readings) - start > BATCH_MAX:
        # never cut so close to the end that fewer than BATCH_MIN readings are left
        size = rng.randint(BATCH_MIN, min(BATCH_MAX, len(readings) - start - BATCH_MIN))
        batches.append(readings[start:start + size])
        start += size
    if start < len(readings):
        batches.append(readings[start:])
    return batches

def _shuffled(readings: list[Reading], rng: random.Random) -> list[Reading]:
    windows = {}
    for reading in sorted(readings, key=lambda r: r[TIME]):
        windows.setdefault(reading[TIME] // SHUFFLE_WINDOW_S, []).append(reading)
    order = []
    for window in windows.values():
        rng.shuffle(window)
        order.extend(window)
    late = set(rng.sample(range(len(order)), len(order) * LATE_PERCENT // 100))
    return [r for i, r in enumerate(order) if i not in late] + [order[i] for i in sorted(late)]

def make_batches(readings: list[Reading], seed: int,
                 conflicts: bool) -> tuple[list[list[Reading]], dict[str, int]]:
    """Shuffled batches with resends, revision 1 corrections, late readings and, if conflicts,
    same-revision overwrites followed by a resend of the original. Returns the batches and the
    counts."""
    rng = random.Random(seed)
    base = _chunks(_shuffled(readings, rng), rng)
    plan = [(float(i), batch) for i, batch in enumerate(base)]   # (position, batch), sorted later
    fixed = rng.sample(readings, len(readings) * CORRECTION_PERCENT // 100)
    corrections = [(s, c, t, 1, value_steps(s, c, t, 1)) for s, c, t, _, _ in fixed]
    for batch in _chunks(corrections, rng):
        plan.append((rng.uniform(len(base) / 2, len(base)), batch))
    counts = {"corrections": len(corrections), "conflicts": 0, "conflict_resends": 0}
    if conflicts:
        home = {reading[:3]: i for i, batch in enumerate(base) for reading in batch}
        picked = rng.sample(readings, len(readings) * CONFLICT_PERCENT // 100)
        picked.sort(key=lambda r: home[r[:3]])
        # half of the overwrites go lower and half higher, so no tie-break by value can pass
        changed = [(s, c, t, revision, value + rng.randint(1, 3) * (-1 if i % 2 else 1))
                   for i, (s, c, t, revision, value) in enumerate(picked)]
        for batch in _chunks(changed, rng):
            after = max(home[r[:3]] for r in batch) + 0.5
            plan.append((after, batch))
            for i in sorted({home[r[:3]] for r in batch}):
                plan.append((rng.uniform(after, len(base)), list(base[i])))
                counts["conflict_resends"] += 1
        counts["conflicts"] = len(changed)
    batches = []
    counts["resends"] = 0
    for _, batch in sorted(plan, key=lambda item: item[0]):
        batches.append(batch)
        if rng.random() < RESEND_CHANCE:
            batches.append(list(rng.choice(batches)))
            counts["resends"] += 1
    sent = [reading[TIME] for batch in batches for reading in batch]
    newest = itertools.accumulate(sent, max, initial=-1)
    counts["out_of_order"] = sum(t < before for t, before in zip(sent, newest))
    counts["batches"] = len(batches)
    counts["readings"] = len(sent)
    return batches, counts

def station_day_rows(n_stations: int, step_s: int = 60, seed: int = SEED) -> list[Row]:
    """One UTC day as one writer stores it when each batch carries one time step of every station,
    sorted in storage order. Arrivals inside a step come in a seeded order, so ingest_seq
    interleaves, and ingest_time is the step plus 0 to 5 s. Values are a daily sine plus a random
    walk of 0.5 steps, like the doc's experiment."""
    rng = random.Random(seed)
    walks = {}
    rows = []
    seq = 0
    for t in range(DAY0 * DAY_S, (DAY0 + 1) * DAY_S, step_s):
        arrivals = [(s, c) for s in range(n_stations) for c in range(len(CHANNELS))]
        rng.shuffle(arrivals)
        for station, channel in arrivals:
            seq += 1
            walks[station, channel] = walks.get((station, channel), 0.0) + rng.gauss(0, WALK_STEPS)
            value = round(_profile(station, channel, t) + walks[station, channel])
            rows.append((station, channel, t, 0, seq, t + rng.randint(0, MAX_DELAY_S), value))
    return sorted(rows)

class Oracle:
    """Deliberately slow model of doc section 3, written without rules.py: per key a list of
    (revision, arrival, value). An already seen (revision, value) is ignored. The winner is the
    highest (revision, arrival)."""

    def __init__(self) -> None:
        self.keys = {}   # (station, time, channel) -> [(revision, arrival, value), ...]
        self.arrivals = 0

    def add(self, batch: list[Reading]) -> None:
        for station, channel, t, revision, value in batch:
            self.arrivals += 1
            versions = self.keys.setdefault((station, t, channel), [])
            if not any(r == revision and v == value for r, _, v in versions):
                versions.append((revision, self.arrivals, value))

    def latest(self, station: int) -> dict[tuple[int, int], tuple[int, int]]:
        """(time, channel) -> (revision, value) of the winning version."""
        answer = {}
        for (s, t, channel), versions in self.keys.items():
            if s == station:
                revision, _, value = max(versions)   # arrivals are unique, so values never compare
                answer[t, channel] = (revision, value)
        return answer

    def versions(self, station: int) -> set[tuple[int, int, int, int]]:
        """Every distinct (time, channel, revision, value) of the station."""
        return {(t, channel, revision, value) for (s, t, channel), versions in self.keys.items()
                if s == station for revision, _, value in versions}

    def stations(self) -> list[int]:
        """Every station that has at least one reading, in order."""
        return sorted({station for station, _, _ in self.keys})

def _wrong(got: tuple[int, int] | None, want: tuple[int, int], exact: bool) -> bool:
    # exact=False forgives only a newer revision than the oracle's: an unacked batch that landed
    return got != want and (exact or got is None or got[0] <= want[0])

def check(history: Callable[[int, bool], list[tuple[int, int, int, int]]], oracle: Oracle,
          exact: bool = True) -> dict[str, int]:
    """Compare every oracle station. Keys: readings, missing, extra, wrong, doubled, stations_ok,
    stations."""
    counts = dict.fromkeys(("readings", "missing", "extra", "wrong", "doubled", "stations_ok"), 0)
    for station in oracle.stations():
        expected = oracle.versions(station)
        every = history(station, True)
        latest = {(t, channel): (revision, value) for t, channel, revision, value
                  in history(station, False)}
        found = {
            "missing": len(expected - set(every)),
            "extra": len(set(every) - expected) if exact else 0,
            "wrong": sum(_wrong(latest.get(key), want, exact)
                         for key, want in oracle.latest(station).items()),
            "doubled": len(every) - len(set(every))}
        for name, number in found.items():
            counts[name] += number
        counts["readings"] += len(expected)
        counts["stations_ok"] += not any(found.values())
    counts["stations"] = len(oracle.stations())
    return counts

def history_fn(archive, start: int,
               end: int) -> Callable[[int, bool], list[tuple[int, int, int, int]]]:
    """Adapter for check over Archive.history: (station, all_versions) -> [(time, channel,
    revision, value)]."""
    def history(station: int, all_versions: bool) -> list[tuple[int, int, int, int]]:
        rows = archive.history(station, start, end, all_versions)
        return [(row[TIME], row[CHANNEL], row[REVISION], row[VALUE]) for row in rows]

    return history

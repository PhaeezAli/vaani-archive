"""Similar stations: a 64-number vector per station built only from hourly counts and sums, so
per-station totals can be updated by difference and only those totals stay in RAM.

Usage:
    index = SimilarityIndex()
    index.add(station, day_summary(latest(rows)))
    index.top_k(station, 10)          # [(other station, cosine score), ...] best first

Columns are z-scored across stations and rows L2-normalised before a brute-force cosine, with a
zero standard deviation or a zero row giving 0, so no NaN reaches a score. vector and top_k raise
KeyError for a station with no flushed data.
"""
import threading
from collections.abc import Iterable

import numpy as np

from archive.codec import CHANNEL, CHANNELS, TIME, VALUE, Row

HOURS = 24            # UTC hours in a day, the finest grain a summary keeps
DAY_S, HOUR_S = 86_400, 3_600   # seconds in a UTC day and in an hour
BUCKET_HOURS = 2      # 12 two-hour buckets a day per channel: 5 x 12 = 60 numbers
QUARTERS = 4          # share of the station's readings in each six-hour quarter of the day
DIM = len(CHANNELS) * HOURS // BUCKET_HOURS + QUARTERS   # 64
# A constant column's computed spread is rounding noise (about 1e-17 of its size, measured), not
# a real difference, so a spread this small next to the column's size counts as zero.
ZERO_SPREAD = 1e-9

def day_summary(rows: Iterable[Row]) -> np.ndarray:
    """(5, 24, 2) float64: count and sum of values per channel per UTC hour, from one
    station-day's latest rows. Summaries add."""
    summary = np.zeros((len(CHANNELS), HOURS, 2))
    for row in rows:
        hour = (row[TIME] % DAY_S) // HOUR_S
        # sums of whole steps in float64 are exact below 2**53, so add and subtract cancel exactly
        summary[row[CHANNEL], hour, 0] += 1
        summary[row[CHANNEL], hour, 1] += row[VALUE]
    return summary

def vector(total: np.ndarray) -> np.ndarray:
    """64 numbers from a station's summed summaries: 12 two-hour means per channel, then the share
    of its readings in each quarter of the day."""
    buckets = total.reshape(len(CHANNELS), HOURS // BUCKET_HOURS, BUCKET_HOURS, 2).sum(axis=2)
    counts = buckets[:, :, 0]
    means = np.divide(buckets[:, :, 1], counts, out=np.zeros_like(counts), where=counts > 0)
    # the quarter shares show where data goes missing, like a solar station that is dark at night
    per_hour = total[:, :, 0].sum(axis=0)
    quarters = per_hour.reshape(QUARTERS, HOURS // QUARTERS).sum(axis=1)
    shares = quarters / max(quarters.sum(), 1)
    return np.concatenate([means.ravel(), shares])

def _unit_rows(vectors: np.ndarray) -> np.ndarray:
    spread = vectors.std(axis=0)
    real = spread > ZERO_SPREAD * np.abs(vectors).max(axis=0)
    centred = vectors - vectors.mean(axis=0)
    z = np.divide(centred, spread, out=np.zeros_like(vectors), where=real)
    norms = np.linalg.norm(z, axis=1, keepdims=True)
    unit = np.divide(z, norms, out=np.zeros_like(z), where=norms > 0)
    return unit.astype(np.float32)

class SimilarityIndex:
    """Per-station totals of day summaries, updated by difference, ranked by brute-force cosine."""

    def __init__(self) -> None:
        # the commit thread calls add and HTTP threads call top_k, so one lock guards both fields
        self._lock = threading.Lock()
        self._totals = {}    # station -> (5, 24, 2) sum of its day summaries over flushed data
        self._cache = None   # (ids, unit rows), built by top_k and dropped by the next add

    def add(self, station: int, delta: np.ndarray) -> None:
        """total[station] += delta. A flush passes (new day summary - old day summary), so a
        correction never rescans."""
        with self._lock:
            if station not in self._totals:
                self._totals[station] = np.zeros((len(CHANNELS), HOURS, 2))
            self._totals[station] += delta
            self._cache = None

    def vector(self, station: int) -> np.ndarray:
        """The station's current vector. KeyError if it has no flushed data."""
        with self._lock:
            return vector(self._totals[station])

    def top_k(self, station: int, k: int) -> list[tuple[int, float]]:
        """The k most similar other stations, best first, k clamped to the number of other
        stations. KeyError if unknown."""
        with self._lock:
            if station not in self._totals:
                raise KeyError(station)
            if self._cache is None:
                ids = sorted(self._totals)
                stacked = np.stack([vector(self._totals[s]) for s in ids])
                self._cache = (ids, _unit_rows(stacked))
            ids, matrix = self._cache
        scores = matrix @ matrix[ids.index(station)]
        # ties go to the lower station id, written as a sort key so the order is easy to check
        order = sorted(range(len(ids)), key=lambda i: (-scores[i], ids[i]))
        others = [i for i in order if ids[i] != station]
        return [(ids[i], round(float(scores[i]), 6)) for i in others[:k]]

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

BUCKET_HOURS = 2      # 12 two-hour buckets a day per channel: 5 x 12 = 60 numbers
QUARTERS = 4          # share of the station's readings in each six-hour quarter of the day
DIM = len(CHANNELS) * 24 // BUCKET_HOURS + QUARTERS   # 64

def day_summary(rows: Iterable[Row]) -> np.ndarray:
    """(5, 24, 2) float64: count and sum of values per channel per UTC hour, from one
    station-day's latest rows. Summaries add."""
    raise NotImplementedError

def vector(total: np.ndarray) -> np.ndarray:
    """64 numbers from a station's summed summaries: 12 two-hour means per channel, then the share
    of its readings in each quarter of the day."""
    raise NotImplementedError

class SimilarityIndex:
    """Per-station totals of day summaries, updated by difference, ranked by brute-force cosine."""

    def __init__(self) -> None:
        raise NotImplementedError

    def add(self, station: int, delta: np.ndarray) -> None:
        """total[station] += delta. A flush passes (new day summary - old day summary), so a
        correction never rescans."""
        raise NotImplementedError

    def vector(self, station: int) -> np.ndarray:
        """The station's current vector. KeyError if it has no flushed data."""
        raise NotImplementedError

    def top_k(self, station: int, k: int) -> list[tuple[int, float]]:
        """The k most similar other stations, best first, k clamped to the number of other
        stations. KeyError if unknown."""
        raise NotImplementedError

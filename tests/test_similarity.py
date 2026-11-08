"""Tests 15 and 16: similar stations from day summaries, and a correction applied by difference."""
import random

import numpy as np
from fastapi.testclient import TestClient

from archive.server import create_app
from tests.workload import DAY0, SEED, base_readings, group_of, value_steps

CHUNK = 1_000   # readings per ingest call, like a client that sends in pieces

def test_similar_returns_stations_from_the_same_planted_group(open_archive):
    """Doc section 5: the stations that behave most like it, by cosine over z-scored vectors."""
    readings = base_readings(list(range(300, 324)), DAY0, days=3, step_s=1800)
    archive = open_archive()
    for start in range(0, len(readings), CHUNK):
        archive.ingest(readings[start:start + CHUNK])
    archive.flush()
    client = TestClient(create_app(archive))
    for station in (300, 303, 301, 304, 302, 305):   # two stations of each planted group
        response = client.get(f"/stations/{station}/similar", params={"k": 5})
        assert response.status_code == 200
        similar = response.json()["similar"]
        assert len(similar) == 5
        assert all(group_of(s["station"]) == group_of(station) for s in similar)
        assert station not in [s["station"] for s in similar]
        scores = [s["score"] for s in similar]
        assert scores == sorted(scores, reverse=True)
    wide = client.get("/stations/300/similar", params={"k": 50})
    assert (wide.status_code, len(wide.json()["similar"])) == (200, 23)
    assert client.get("/stations/999/similar", params={"k": 5}).status_code == 404

def test_a_correction_updates_the_vector_exactly_like_a_full_rebuild(open_archive):
    """Doc section 5: a correction replaces one day's summary by the difference, with no rescan."""
    readings = base_readings([400, 401, 402], DAY0, days=2, step_s=3600)
    archive = open_archive()
    archive.ingest(readings)
    archive.flush()
    v0 = archive.index.vector(400)
    first_day = [r for r in readings if r[0] == 400 and r[2] < (DAY0 + 1) * 86_400]
    fixed = random.Random(SEED).sample(first_day, 30)
    archive.ingest([(s, c, t, 1, value_steps(s, c, t, 1)) for s, c, t, _, _ in fixed])
    archive.flush()
    v1 = archive.index.vector(400)
    archive.close()
    v_full = open_archive().index.vector(400)   # a fresh writer rebuilds totals from every segment
    assert not np.array_equal(v1, v0)
    assert np.allclose(v1, v_full, rtol=0, atol=1e-9)

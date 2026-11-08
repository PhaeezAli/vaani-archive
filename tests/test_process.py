"""The SIGKILL scenario as a test. The scenario itself lives in tests/scenarios.py, and demo.py
prints the same checks. Usage: python -m pytest -q tests/test_process.py. A harness failure
raises RuntimeError with the server's log tail, so only a failed check fails the assert.
"""
from tests.scenarios import kill_scenario

def test_acknowledged_readings_survive_sigkill(tmp_path):
    """Doc section 2: killing any node at any moment loses nothing that was acknowledged."""
    checks = kill_scenario(tmp_path)["checks"]
    failed = [(name, result) for name, result, ok in checks if not ok]
    assert not failed

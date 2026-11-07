"""The real-process harness shared by tests/test_process.py and demo.py: the child-process wrapper,
the client ledger, and the SIGKILL scenario.

Usage: checks = kill_scenario(tmp_path)["checks"], then assert or print each (name, result, ok).
Neither caller adds proof logic. A harness failure (a child that never wrote its port file, a
worker that recorded an error, a wait that timed out) raises RuntimeError with the server's log
tail, never AssertionError, so a crash can never pass for a fired check. Every child process is
killed and waited for before any result is returned.
"""
import threading
from collections.abc import Callable, Iterable
from pathlib import Path

from archive.codec import Reading

REPO = Path(__file__).resolve().parents[1]
WORKERS = 8
KILL_MEMTABLE_ROWS = 3_000        # the kill server flushes at 1,500 rows (arithmetic below)
# above the first flush, which ends below 1,980 rows, and far below the second at 3,000
KILL_AFTER_ACKED_ROWS = 2_000
# no ack for this long after the freeze means every response already sent has arrived
QUIET_S = 0.1
# stays below REQUEST_TIMEOUT_S, so no outstanding request times out while frozen
FREEZE_DEADLINE_S = 5.0
PORT_FILE_TIMEOUT_S = 15.0
REQUEST_TIMEOUT_S = 10.0
PHASE_DEADLINE_S = 60.0

class ServerProcess:
    """One `python -m archive.server` child. The port comes from the child's own bind(0) through
    its port file. stop() always reaps it."""
    url: str

    def __init__(self, work: Path, node_id: str, *flags: str, log: Path) -> None:
        raise NotImplementedError

    def pause(self) -> None:
        """SIGSTOP."""
        raise NotImplementedError

    def kill(self) -> None:
        """SIGKILL and wait. It also ends a stopped child. returncode is -9."""
        raise NotImplementedError

    def stop(self) -> None:
        """SIGKILL if alive, and wait."""
        raise NotImplementedError

    def log_tail(self, lines: int = 30) -> str:
        raise NotImplementedError

class Sender:
    """Worker threads that post batches and record each batch's fate in one ledger: acked, unknown
    (sent, then a transport error) or never sent."""
    lock: threading.Condition
    acked: list[int]
    acked_seqs: list[int]
    unknown: list[int]
    errors: list[str]
    acked_rows: int
    outstanding: int
    paused: bool
    killed: bool

    def __init__(self, url: str, batches: list[list[Reading]], *, retry_transport: bool = False,
                 pause_at_rows: int | None = None,
                 pause: Callable[[], None] | None = None) -> None:
        raise NotImplementedError

    def run(self, indexes: Iterable[int]) -> None:
        """WORKERS threads over indexes[w::WORKERS]."""
        raise NotImplementedError

    def wait_paused(self, timeout: float) -> None:
        """RuntimeError on timeout."""
        raise NotImplementedError

    def wait_quiet(self, quiet_s: float, timeout: float) -> None:
        """RuntimeError on timeout."""
        raise NotImplementedError

    def join(self, timeout: float) -> None:
        """RuntimeError if a worker is alive or errors is not empty."""
        raise NotImplementedError

def http_history(url: str, start: int,
                 end: int) -> Callable[[int, bool], list[tuple[int, int, int, int]]]:
    """The check adapter over GET /stations/{id}/history, values quantised back to steps."""
    raise NotImplementedError

def get_stats(url: str) -> dict:
    raise NotImplementedError

def kill_scenario(work: Path) -> dict:
    raise NotImplementedError

"""The real-process harness shared by tests/test_process.py and demo.py: the child-process wrapper,
the client ledger, and the SIGKILL scenario.

Usage: checks = kill_scenario(tmp_path)["checks"], then assert or print each (name, result, ok).
Neither caller adds proof logic. A harness failure (a child that never wrote its port file, a
worker that recorded an error, a wait that timed out) raises RuntimeError with the server's log
tail, never AssertionError, so a crash can never pass for a fired check. Every child process is
killed and waited for before any result is returned.
"""
import contextlib
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path

import httpx

from archive import manifest
from archive.codec import Reading, decode_wal, quantise, unquantise
from archive.store import LocalStore
from tests.workload import DAY0, DAY_S, SEED, Oracle, base_readings, check, make_batches

REPO = Path(__file__).resolve().parents[1]
WORKERS = 8                       # clients, each with at most one request outstanding
KILL_MEMTABLE_ROWS = 3_000        # the kill server flushes at 1,500 rows (arithmetic below)
# above the first flush, which ends below 1,980 rows, and far below the second at 3,000
KILL_AFTER_ACKED_ROWS = 2_000
# no ack for this long after the freeze means every response already sent has arrived
QUIET_S = 0.1
# stays below REQUEST_TIMEOUT_S, so no outstanding request times out while frozen
FREEZE_DEADLINE_S = 5.0
PORT_FILE_TIMEOUT_S = 15.0        # recovery of a few WAL objects takes well under a second
REQUEST_TIMEOUT_S = 10.0          # 50 times the commit window, so only a dead server times out
PHASE_DEADLINE_S = 60.0           # one phase posts about 300 batches in a few seconds
REAP_TIMEOUT_S = 10.0             # SIGKILL ends a child at once, so a longer wait is a bug
RETRY_S = 0.05                    # pause before resending after 429, 503 or a retried error
POLL_S = 0.02                     # how often a wait looks again
STATIONS = list(range(100, 124))  # 24 stations, 8 per planted group
DAYS, STEP_S = 2, 1_800           # 2 days of 30-minute readings, about 300 batches
FORCED_DUPLICATES = 10            # acknowledged batches sent again after the restart
KILL_JOIN_S = REQUEST_TIMEOUT_S + 5.0   # after SIGKILL every outstanding post fails at once

def _client() -> httpx.Client:
    # a new connection per request, so no idle pooled connection races the keep-alive timer
    return httpx.Client(timeout=REQUEST_TIMEOUT_S,
                        limits=httpx.Limits(max_keepalive_connections=0))

class ServerProcess:
    """One `python -m archive.server` child. The port comes from the child's own bind(0) through
    its port file. stop() always reaps it."""

    def __init__(self, work: Path, node_id: str, *flags: str, log: Path) -> None:
        self.log = log
        port_file = work / f"port-{node_id}"
        port_file.unlink(missing_ok=True)
        command = [sys.executable, "-m", "archive.server", "--root", str(work / "store"),
                   "--node-id", node_id, "--port-file", str(port_file), *flags]
        with open(log, "ab") as out:
            self.process = subprocess.Popen(
                command, cwd=REPO, stdout=out, stderr=subprocess.STDOUT, start_new_session=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        deadline = time.monotonic() + PORT_FILE_TIMEOUT_S
        while not port_file.exists():
            if self.process.poll() is not None or time.monotonic() > deadline:
                self.stop()
                raise RuntimeError(f"server {node_id} wrote no port file\n{self.log_tail()}")
            time.sleep(POLL_S)
        self.url = f"http://127.0.0.1:{int(port_file.read_text())}"

    def pause(self) -> None:
        """SIGSTOP."""
        os.killpg(self.process.pid, signal.SIGSTOP)

    def kill(self) -> None:
        """SIGKILL and wait. It also ends a stopped child. returncode is -9."""
        self.stop()
        if self.process.returncode != -signal.SIGKILL:
            raise RuntimeError(f"server exited with {self.process.returncode} before the kill\n"
                               f"{self.log_tail()}")

    def stop(self) -> None:
        """SIGKILL if alive, and wait."""
        if self.process.poll() is None:
            # the child leads its own process group, so this also reaches anything it started
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
        try:
            self.process.wait(REAP_TIMEOUT_S)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"server {self.process.pid} survived SIGKILL") from error

    def log_tail(self, lines: int = 30) -> str:
        return "\n".join(self.log.read_text(errors="replace").splitlines()[-lines:])

class Sender:
    """Worker threads that post batches and record each batch's fate in one ledger: acked, unknown
    (sent, then a transport error) or never sent."""

    def __init__(self, url: str, batches: list[list[Reading]], *, retry_transport: bool = False,
                 pause_at_rows: int | None = None,
                 pause: Callable[[], None] | None = None) -> None:
        self.url = url
        self.batches = batches
        self.retry_transport = retry_transport
        self.pause_at_rows = pause_at_rows
        self.pause = pause
        self.lock = threading.Condition()   # re-entrant, so the kill can hold it across wait_quiet
        self.acked = []
        self.acked_seqs = []
        self.unknown = []
        self.errors = []
        self.acked_rows = 0
        self.sent_rows = 0                  # rows of every post made, resends included
        self.outstanding = 0
        self.last_ack = time.monotonic()
        self.paused = False
        self.killed = False
        self.running = 0
        self.threads = []

    def run(self, indexes: Iterable[int]) -> None:
        """WORKERS threads over indexes[w::WORKERS]."""
        indexes = list(indexes)
        for w in range(WORKERS):
            thread = threading.Thread(target=self._work, args=(indexes[w::WORKERS],), daemon=True)
            self.threads.append(thread)
            with self.lock:
                self.running += 1
            thread.start()

    def _work(self, mine: list[int]) -> None:
        try:
            with _client() as client:
                for index in mine:
                    if not self._send(client, index):
                        return
        except Exception as error:   # a crashed worker must fail join, never vanish
            with self.lock:
                self.errors.append(f"worker crashed: {error!r}")
        finally:
            with self.lock:
                self.running -= 1
                self.lock.notify_all()

    def _send(self, client: httpx.Client, index: int) -> bool:
        """Post one batch until it has a fate. True means go on to the next batch."""
        body = {"readings": [{"station": s, "time": t, "channel": c, "revision": r,
                              "value": unquantise(c, v)} for s, c, t, r, v in self.batches[index]]}
        deadline = time.monotonic() + PHASE_DEADLINE_S
        while True:
            with self.lock:
                if self.killed:
                    return False
                self.outstanding += 1
                self.sent_rows += len(self.batches[index])
            response, failure = None, None
            try:
                response = client.post(f"{self.url}/ingest", json=body)
            except httpx.TransportError as error:
                failure = error
            with self.lock:
                self.outstanding -= 1
                fate = self._record(index, response, failure)
            if fate != "retry":
                return fate == "acked"
            if time.monotonic() > deadline:
                with self.lock:
                    self.errors.append(f"batch {index} not acked after {PHASE_DEADLINE_S} s")
                return False
            time.sleep(RETRY_S)

    def _record(self, index: int, response: httpx.Response | None,
                failure: httpx.TransportError | None) -> str:
        """Called holding the lock. Returns "acked", "retry" or "stop"."""
        if failure is not None:
            if self.retry_transport:
                return "retry"
            # a refused connect never reached the server, so the batch stays never sent
            if not isinstance(failure, httpx.ConnectError):
                self.unknown.append(index)
            return "stop"
        if response.status_code in (429, 503):
            return "retry"
        if response.status_code != 200:
            self.errors.append(f"batch {index}: {response.status_code} {response.text[:200]}")
            return "stop"
        self.acked.append(index)
        self.acked_seqs.append(response.json()["wal_seq"])
        self.acked_rows += len(self.batches[index])
        self.last_ack = time.monotonic()
        if self.pause_at_rows is not None and not self.paused:
            if self.acked_rows >= self.pause_at_rows:
                self.pause()
                self.paused = True
        self.lock.notify_all()
        return "acked"

    def wait_paused(self, timeout: float) -> None:
        """RuntimeError on timeout."""
        with self.lock:
            self.lock.wait_for(lambda: self.paused or self.errors or not self.running, timeout)
            if not self.paused:
                raise RuntimeError(f"never paused: {self.acked_rows} rows acked, {self.errors}")

    def wait_quiet(self, quiet_s: float, timeout: float) -> None:
        """RuntimeError on timeout."""
        deadline = time.monotonic() + timeout
        with self.lock:
            while time.monotonic() - self.last_ack < quiet_s or self.outstanding < 1:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"no quiet moment with a post outstanding in {timeout} s")
                self.lock.wait(max(quiet_s - (time.monotonic() - self.last_ack), POLL_S))

    def join(self, timeout: float) -> None:
        """RuntimeError if a worker is alive or errors is not empty."""
        deadline = time.monotonic() + timeout
        for thread in self.threads:
            thread.join(max(deadline - time.monotonic(), 0))
        alive = sum(thread.is_alive() for thread in self.threads)
        if alive or self.errors:
            raise RuntimeError(f"{alive} workers still running, errors: {self.errors[:5]}")

def _get(url: str, params: dict | None = None) -> dict:
    with _client() as client:
        response = client.get(url, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"GET {url}: {response.status_code} {response.text[:200]}")
    return response.json()

def http_history(url: str, start: int,
                 end: int) -> Callable[[int, bool], list[tuple[int, int, int, int]]]:
    """The check adapter over GET /stations/{id}/history, values quantised back to steps."""
    def history(station: int, all_versions: bool) -> list[tuple[int, int, int, int]]:
        params = {"start": start, "end": end, "versions": "all" if all_versions else "latest"}
        readings = _get(f"{url}/stations/{station}/history", params)["readings"]
        return [(r["time"], r["channel"], r["revision"], quantise(r["channel"], r["value"]))
                for r in readings]

    return history

def get_stats(url: str) -> dict:
    """GET /stats."""
    return _get(f"{url}/stats")

def _wal_tail_rows(store: LocalStore, watermark: int) -> int:
    rows = 0
    for key in store.list("wal/"):
        if int(key[4:]) > watermark:
            rows += len(decode_wal(store.get(key)[0], key)[3])
    return rows

def _oracle_of(batches: list[list[Reading]], indexes: Iterable[int]) -> Oracle:
    oracle = Oracle()
    for index in indexes:
        oracle.add(batches[index])
    return oracle

def kill_scenario(work: Path) -> dict:
    """Ingest with 8 clients, freeze the server by progress, SIGKILL it, restart, check, resend,
    check again. Returns {"checks": [(name, result, ok), ...], "info": [(name, text), ...]}."""
    servers = []
    try:
        return _run_kill_scenario(work, servers)
    except RuntimeError as error:
        tail = servers[-1].log_tail() if servers else ""
        raise RuntimeError(f"{error}\nlast lines of the server log:\n{tail}") from error
    finally:
        for server in servers:
            server.stop()

def _kill_during_ingest(server: ServerProcess, batches: list[list[Reading]]) -> tuple:
    """Steps 2 and 3 of the scenario. Returns (sender, acked_at_kill, outstanding_at_kill)."""
    sender = Sender(server.url, batches, pause_at_rows=KILL_AFTER_ACKED_ROWS, pause=server.pause)
    sender.run(range(len(batches)))
    sender.wait_paused(PHASE_DEADLINE_S)
    with sender.lock:
        sender.wait_quiet(QUIET_S, FREEZE_DEADLINE_S)
        sender.killed = True
        acked_at_kill = len(sender.acked)
        outstanding_at_kill = sender.outstanding
        server.kill()
    sender.join(KILL_JOIN_S)
    return sender, acked_at_kill, outstanding_at_kill

def _resend(url: str, batches: list[list[Reading]], sender: Sender) -> Sender:
    """Step 8: unknown batches first, then forced duplicates, then every batch never acked."""
    seen = set(sender.acked) | set(sender.unknown)
    never = [index for index in range(len(batches)) if index not in seen]
    resent = sender.unknown + sender.acked[:FORCED_DUPLICATES] + never
    resender = Sender(url, batches, retry_transport=True)
    resender.run(resent)
    resender.join(PHASE_DEADLINE_S)
    if len(resender.acked) != len(resent):
        raise RuntimeError(f"{len(resender.acked)} of {len(resent)} resent batches acked")
    return resender

def _run_kill_scenario(work: Path, servers: list[ServerProcess]) -> dict:
    batches, _ = make_batches(base_readings(STATIONS, DAY0, DAYS, STEP_S), SEED, conflicts=False)
    flags = ("--memtable-rows", str(KILL_MEMTABLE_ROWS))
    servers.append(ServerProcess(work, "a", *flags, log=work / "server.log"))
    sender, acked_at_kill, outstanding_at_kill = _kill_during_ingest(servers[0], batches)
    store = LocalStore(work / "store")
    stored, _ = manifest.read(store)
    watermark = stored["wal_watermark"]
    tail_rows = _wal_tail_rows(store, watermark)
    segments = len(stored["segments"])
    unknown = len(sender.unknown)
    decisive = (acked_at_kill >= 1 and outstanding_at_kill >= 1 and unknown >= 1 and segments >= 1
                and max(sender.acked_seqs, default=0) > watermark and tail_rows >= 1)
    servers.append(ServerProcess(work, "a", *flags, log=work / "server.log"))
    url = servers[1].url
    history = http_history(url, DAY0 * DAY_S, (DAY0 + DAYS) * DAY_S)
    first = check(history, _oracle_of(batches, sender.acked), exact=False)
    stats1 = get_stats(url)
    resender = _resend(url, batches, sender)
    stats2 = get_stats(url)
    second = check(history, _oracle_of(batches, range(len(batches))), exact=True)
    group = (stats2["batches"] - stats1["batches"]) / (stats2["wal_puts"] - stats1["wal_puts"])
    doubled = first["doubled"] + second["doubled"]
    second_ok = not any(second[name] for name in ("missing", "extra", "wrong", "doubled"))
    checks = [
        ("kill run was decisive", f"{acked_at_kill} acked, {outstanding_at_kill} outstanding, "
         f"{unknown} unknown, {segments} segments, {tail_rows} rows in the WAL tail", decisive),
        ("acknowledged readings missing right after restart",
         f"{first['missing']:,} of {first['readings']:,}, {first['wrong']} keys behind",
         first["missing"] == 0 and first["wrong"] == 0),
        ("acknowledged readings returned twice", f"{doubled}", doubled == 0),
        ("stations matching the dict oracle after resends",
         f"{second['stations_ok']} of {second['stations']}", second_ok),
        ("batches per WAL put, 8 clients, 200 ms window", f"{group:.1f}", group >= 2)]
    info = [
        ("requests outstanding at the kill", f"{outstanding_at_kill}"),
        ("WAL objects replayed after the kill", f"{stats1['replayed_wal_objects']}"),
        ("readings sent including resends and corrections",
         f"{sender.sent_rows + resender.sent_rows:,}"),
        ("server peak RSS after the restart and resends", f"{stats2['peak_rss_mb']:.0f} MB")]
    return {"checks": checks, "info": info}

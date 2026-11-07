"""The only test doubles: a fake clock and LocalStore subclasses that each inject one fault.

They subclass the real store, so everything they do not override is the real behaviour (no mocks).
Usage: FailingStore(tmp_path / "store", prefix="wal/", nth=3, landed=True). A GatedStore whose
gate never opens raises OSError after GATE_TIMEOUT_S instead of hanging the test.
"""
import threading
from collections.abc import Callable
from pathlib import Path

from archive.store import LocalStore

GATE_TIMEOUT_S = 10.0   # a gated put that is never released fails instead of hanging the test

class Clock:
    """A fake wall clock in whole seconds. Clock(1000)() returns 1000. Tests move it with .t."""

    def __init__(self, t: int) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t

class GatedStore(LocalStore):
    """Each WAL put sets `entered`, then waits up to GATE_TIMEOUT_S for `gate` (an Event, open by
    default) before writing. OSError if it never opens."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.gate.set()

    def put(self, key: str, data: bytes, **conditions) -> str:
        if key.startswith("wal/"):
            self.entered.set()
            if not self.gate.wait(GATE_TIMEOUT_S):
                raise OSError(f"the gate for {key} never opened")
        return super().put(key, data, **conditions)

class FailingStore(LocalStore):
    """While `failing` is True, the nth matching operation and every later one raises error. With
    landed=True a put lands first, then raises."""

    def __init__(self, root: Path, prefix: str, nth: int = 1, op: str = "put",
                 landed: bool = False, failing: bool = True,
                 error: type[Exception] = OSError) -> None:
        super().__init__(root)
        self.prefix = prefix
        self.nth = nth
        self.op = op
        self.landed = landed
        self.failing = failing
        self.error = error
        self.seen = 0   # matching operations seen while failing was True

    def _trips(self, op: str, key: str) -> bool:
        if not self.failing or op != self.op or not key.startswith(self.prefix):
            return False
        self.seen += 1
        return self.seen >= self.nth

    def put(self, key: str, data: bytes, **conditions) -> str:
        if not self._trips("put", key):
            return super().put(key, data, **conditions)
        if self.landed:
            super().put(key, data, **conditions)
        raise self.error(f"injected put failure on {key}")

    def delete(self, key: str) -> None:
        if self._trips("delete", key):
            raise self.error(f"injected delete failure on {key}")
        super().delete(key)

    def get_range(self, key: str, start: int, length: int) -> bytes:
        if self._trips("get_range", key):
            raise self.error(f"injected get_range failure on {key}")
        return super().get_range(key, start, length)

class CallbackStore(LocalStore):
    """Runs action() once, right after the first list of prefix on_list has returned."""

    def __init__(self, root: Path, action: Callable[[], None], on_list: str) -> None:
        super().__init__(root)
        self.action = action
        self.on_list = on_list
        self.done = False

    def list(self, prefix: str) -> list[str]:
        keys = super().list(prefix)
        if prefix == self.on_list and not self.done:
            self.done = True
            self.action()
        return keys

class CountingStore(LocalStore):
    """Adds len(result) of every get and get_range to bytes_read."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.bytes_read = 0

    def get(self, key: str) -> tuple[bytes, str]:
        data, etag = super().get(key)
        self.bytes_read += len(data)
        return data, etag

    def get_range(self, key: str, start: int, length: int) -> bytes:
        data = super().get_range(key, start, length)
        self.bytes_read += len(data)
        return data

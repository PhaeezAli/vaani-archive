"""The open_archive fixture, and nothing else. Tests import doubles and workload as modules."""
from collections.abc import Callable

import pytest

from archive.archive import Archive
from archive.store import LocalStore
from tests.doubles import Clock

@pytest.fixture
def open_archive(tmp_path) -> Callable[..., Archive]:
    """open_archive(store=None, node_id="a", **options) opens an Archive on
    LocalStore(tmp_path / "store") unless store is given, with commit_window_s=0, renew=False and
    now=Clock(1000) unless given. It closes every Archive it opened at teardown."""
    opened = []

    def open_one(store=None, node_id="a", **options) -> Archive:
        options.setdefault("commit_window_s", 0)
        options.setdefault("renew", False)
        options.setdefault("now", Clock(1000))
        archive = Archive(store if store is not None else LocalStore(tmp_path / "store"), node_id,
                          **options)
        opened.append(archive)
        return archive

    yield open_one
    for archive in opened:
        archive.close()

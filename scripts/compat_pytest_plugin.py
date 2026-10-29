"""
pytest plugin for ``compat.yml``: record which tests failed, and how.

``compat.yml`` loads it with ``-p compat_pytest_plugin`` into whichever ref it
tests, releases older than this file included, and writes the record to
``$COMPAT_PYTEST_REPORT`` as JSON. A test that fails in its call with syrupy's
comparison (``== [- snapshot]``) shows a snapshot difference: the stored
expected output changed, as when Home Assistant adds a field to the entity
registry, not necessarily the integration's behaviour. Every other failure,
including an error in setup, teardown or collection, is a break.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPORT_ENV = "COMPAT_PYTEST_REPORT"
SNAPSHOT_DIFF = "== [- snapshot]"


@dataclass
class Recorder:
    """The failures of one test session, split into snapshot and other."""

    broken: list[str] = field(default_factory=list)
    snapshot: list[str] = field(default_factory=list)

    def add(self, report: pytest.TestReport | pytest.CollectReport) -> None:
        """Record a failed report under the kind of failure it shows."""
        if not report.failed:
            return
        node = report.nodeid or "<collection>"
        when = getattr(report, "when", "collect")
        kind = (
            self.snapshot
            if when == "call" and SNAPSHOT_DIFF in report.longreprtext
            else self.broken
        )
        if node not in kind:
            kind.append(node)

    def write(self, path: Path, exitstatus: int) -> None:
        """Write the record for ``compat_results.py``."""
        record = {
            "exitstatus": int(exitstatus),
            "broken": self.broken,
            "snapshot": self.snapshot,
        }
        path.write_text(json.dumps(record, indent=2) + "\n")


RECORDER = Recorder()


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Record a failed setup, call or teardown."""
    RECORDER.add(report)


def pytest_collectreport(report: pytest.CollectReport) -> None:
    """Record a module that failed to collect."""
    RECORDER.add(report)


def pytest_sessionfinish(exitstatus: int) -> None:
    """Write the record when compat asked for one."""
    if path := os.environ.get(REPORT_ENV):
        RECORDER.write(Path(path), exitstatus)

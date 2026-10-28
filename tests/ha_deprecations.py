"""Fail the test run when Home Assistant reports a deprecation by this integration.

Home Assistant warns months ahead when a custom integration uses something it
will remove: its frame helper logs "Detected that custom integration '<domain>'
... This will stop working in Home Assistant <version>", and its deprecation
helper logs "The deprecated <thing> was used from <domain>.". Python's own
DeprecationWarning, PendingDeprecationWarning and FutureWarning raised from the
integration's code count too. The run lists each one at the end and fails, so
no pull request, a test harness bump included, can bring one in unnoticed.

With ``HA_DEPRECATIONS_REPORT`` set to a path, the run writes them there as a
JSON list instead and leaves the exit status alone: the compatibility workflow
uses this to report a deprecation apart from a break.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import pytest

from custom_components.meridian_energy.const import DOMAIN

if TYPE_CHECKING:
    import warnings

REPORT_ENV = "HA_DEPRECATIONS_REPORT"

_INTEGRATION_PATH = f"custom_components/{DOMAIN}/"
_MESSAGES = (
    re.compile(rf"Detected that custom integration '{re.escape(DOMAIN)}' "),
    re.compile(rf"The deprecated .+ from {re.escape(DOMAIN)}\.(?: |$)"),
)
_WARNINGS = (DeprecationWarning, PendingDeprecationWarning, FutureWarning)


class _Session(Protocol):
    exitstatus: int | pytest.ExitCode


class DeprecationCollector(logging.Handler):
    """Collect the deprecations Home Assistant and Python report for the domain."""

    def __init__(self) -> None:
        """Listen from WARNING up; Home Assistant reports at WARNING."""
        super().__init__(level=logging.WARNING)
        self.reports: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        """Keep a Home Assistant report about this integration."""
        message = record.getMessage()
        if any(pattern.match(message) for pattern in _MESSAGES):
            self._add(message)

    def warning(self, warning_message: warnings.WarningMessage) -> None:
        """Keep a deprecation warning raised from the integration's own code."""
        filename = Path(warning_message.filename).as_posix()
        if not issubclass(warning_message.category, _WARNINGS):
            return
        if (start := filename.find(_INTEGRATION_PATH)) < 0:
            return
        self._add(
            f"{warning_message.category.__name__} at {filename[start:]}, "
            f"line {warning_message.lineno}: {warning_message.message}"
        )

    @contextmanager
    def expected(self) -> Iterator[list[str]]:
        """Hand the reports made inside the block to the caller, not the run."""
        start = len(self.reports)
        caught: list[str] = []
        try:
            yield caught
        finally:
            caught.extend(self.reports[start:])
            del self.reports[start:]

    def _add(self, text: str) -> None:
        if text not in self.reports:
            self.reports.append(text)


COLLECTOR = DeprecationCollector()


def finish(session: _Session, reports: list[str], report_path: str | None) -> None:
    """Write the reports for compat, or fail a run that would otherwise pass."""
    if report_path:
        Path(report_path).write_text(json.dumps(reports, indent=2) + "\n")
        return
    if reports and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    """Listen on the root logger, where every Home Assistant logger ends up."""
    logging.getLogger().addHandler(COLLECTOR)


def pytest_unconfigure(config: pytest.Config) -> None:  # noqa: ARG001
    """Stop listening."""
    logging.getLogger().removeHandler(COLLECTOR)


def pytest_warning_recorded(warning_message: warnings.WarningMessage) -> None:
    """Pass each warning pytest records to the collector."""
    COLLECTOR.warning(warning_message)


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    """List the reports that fail the run."""
    if not COLLECTOR.reports or os.environ.get(REPORT_ENV):
        return
    terminalreporter.section("Home Assistant deprecations", red=True)
    terminalreporter.line(
        f"{DOMAIN} uses what Home Assistant has deprecated; fix each before "
        "the version it names:"
    )
    for report in COLLECTOR.reports:
        terminalreporter.line(f"- {report}")


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Fail the run, or write the report compat asked for."""
    finish(session, COLLECTOR.reports, os.environ.get(REPORT_ENV))

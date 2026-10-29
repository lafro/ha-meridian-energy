"""Tests for the plugin that fails the run on a Home Assistant deprecation."""

import json
import logging
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import frame
from homeassistant.loader import async_get_integration

from custom_components.meridian_energy.const import DOMAIN
from tests import ha_deprecations
from tests.ha_deprecations import COLLECTOR, DeprecationCollector, finish


def _record(logger: str, message: str, *args: object) -> logging.LogRecord:
    return logging.LogRecord(logger, logging.WARNING, __file__, 1, message, args, None)


def _warning(category: type[Warning], filename: str) -> warnings.WarningMessage:
    return warnings.WarningMessage(category("old API"), category, filename, 12)


async def test_a_home_assistant_report_reaches_the_collector(
    hass: HomeAssistant,
) -> None:
    """Home Assistant's own report, as it words it today, is caught."""
    await async_get_integration(hass, DOMAIN)

    with COLLECTOR.expected() as caught:
        frame.report_usage(
            "calls a synthetic test API",
            breaks_in_ha_version="2099.1.0",
            integration_domain=DOMAIN,
        )

    assert len(caught) == 1
    assert caught[0].startswith(
        f"Detected that custom integration '{DOMAIN}' calls a synthetic test API. "
        "This will stop working in Home Assistant 2099.1.0"
    )
    assert caught[0] not in COLLECTOR.reports


def test_both_home_assistant_wordings_match_only_this_domain() -> None:
    collector = DeprecationCollector()
    collector.emit(
        _record(
            "homeassistant.helpers.frame",
            "Detected that custom integration '%s' %s at %s, line %s: %s. %s %s",
            DOMAIN,
            "calls X",
            f"custom_components/{DOMAIN}/sensor.py",
            7,
            "x()",
            "This will stop working in Home Assistant 2099.1.0, please",
            "report it",
        )
    )
    collector.emit(
        _record(
            "homeassistant.const",
            "The deprecated %s %s was %s from %s.%s Use %s instead, please %s",
            "constant",
            "OLD",
            "used",
            DOMAIN,
            " It will be removed in HA Core 2099.1.",
            "NEW",
            "report it",
        )
    )
    collector.emit(
        _record(
            "homeassistant.helpers.frame",
            "Detected that custom integration 'another_domain' calls X. Please",
        )
    )
    collector.emit(
        _record("homeassistant.const", "The deprecated constant OLD was used from x.")
    )
    collector.emit(_record("homeassistant", "Something else is deprecated"))

    assert len(collector.reports) == 2
    assert collector.reports[0].startswith(
        f"Detected that custom integration '{DOMAIN}'"
    )
    assert collector.reports[1].startswith(
        f"The deprecated constant OLD was used from {DOMAIN}."
    )


def test_a_report_seen_twice_is_listed_once() -> None:
    collector = DeprecationCollector()
    record = _record("x", f"Detected that custom integration '{DOMAIN}' calls X.")
    collector.emit(record)
    collector.emit(record)
    assert len(collector.reports) == 1


def test_only_deprecation_warnings_from_the_integration_count() -> None:
    collector = DeprecationCollector()
    inside = str(Path(f"custom_components/{DOMAIN}/sensor.py").resolve())

    collector.warning(_warning(DeprecationWarning, inside))
    collector.warning(_warning(PendingDeprecationWarning, inside))
    collector.warning(_warning(FutureWarning, inside))
    collector.warning(_warning(ResourceWarning, inside))
    collector.warning(_warning(DeprecationWarning, "/site-packages/aiohttp/web.py"))

    assert collector.reports == [
        f"{name} at custom_components/{DOMAIN}/sensor.py, line 12: old API"
        for name in ("DeprecationWarning", "PendingDeprecationWarning", "FutureWarning")
    ]


def test_reports_fail_a_run_that_would_pass() -> None:
    session = SimpleNamespace(exitstatus=pytest.ExitCode.OK)
    finish(session, ["a report"], None)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED


def test_no_reports_leave_the_run_alone() -> None:
    session = SimpleNamespace(exitstatus=pytest.ExitCode.OK)
    finish(session, [], None)
    assert session.exitstatus == pytest.ExitCode.OK


def test_reports_keep_another_failure_status() -> None:
    session = SimpleNamespace(exitstatus=pytest.ExitCode.INTERRUPTED)
    finish(session, ["a report"], None)
    assert session.exitstatus == pytest.ExitCode.INTERRUPTED


def test_report_mode_writes_the_reports_and_keeps_the_status(tmp_path: Path) -> None:
    session = SimpleNamespace(exitstatus=pytest.ExitCode.OK)
    path = tmp_path / "deprecations.json"

    finish(session, ["a report"], str(path))
    assert json.loads(path.read_text()) == ["a report"]
    assert session.exitstatus == pytest.ExitCode.OK

    finish(session, [], str(path))
    assert json.loads(path.read_text()) == []


def test_the_summary_lists_each_report(monkeypatch: pytest.MonkeyPatch) -> None:
    lines: list[str] = []
    reporter = SimpleNamespace(
        section=lambda title, **_: lines.append(f"== {title}"), line=lines.append
    )
    monkeypatch.delenv(ha_deprecations.REPORT_ENV, raising=False)
    monkeypatch.setattr(COLLECTOR, "reports", ["first", "second"])

    ha_deprecations.pytest_terminal_summary(reporter)

    assert lines[0] == "== Home Assistant deprecations"
    assert lines[-2:] == ["- first", "- second"]

    lines.clear()
    monkeypatch.setenv(ha_deprecations.REPORT_ENV, "report.json")
    ha_deprecations.pytest_terminal_summary(reporter)
    assert lines == []

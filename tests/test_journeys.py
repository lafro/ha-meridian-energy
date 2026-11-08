"""Contracts for the end-to-end journeys runner (``scripts/journeys.py``)."""

from __future__ import annotations

import argparse
import email.message
import io
import json
import re
import tempfile
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.meridian_energy.const import (
    DAILY_DATES,
    HISTORY_DAYS,
    WINDOW_DAYS,
)
from custom_components.meridian_energy.statistics import (
    property_key as integration_property_key,
)
from custom_components.meridian_energy.windows import (
    local_midnight,
    nz_date,
    windows_for,
)
from scripts import journeys, live_ha
from scripts.journeys import (
    HOUR,
    Expected,
    Journey,
    JourneyError,
    JourneyResult,
    LogMark,
    Row,
    Screen,
)

from . import meridian_standin
from .meridian_http import property_id

_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)
# New Zealand midnight at the start of 18 April 2026, standard time.
_FIRST = datetime(2026, 4, 17, 12, tzinfo=UTC)


def _history(first: datetime, hours: int, kind: str = "consumption") -> list[Row]:
    rows: list[Row] = []
    total = 0.0
    for n in range(hours):
        start = first + n * HOUR
        value = journeys.expected_value(kind, start)
        total += value
        rows.append(Row(start, value, total))
    return rows


def _expected(first: datetime, hours: int) -> Expected:
    """Return what an import 90 dates after ``first``'s date leaves."""
    imported = first + timedelta(days=90, hours=1, minutes=30)
    return Expected(imported, imported, first + hours * HOUR + timedelta(minutes=5))


# ---------------------------------------------------------------- statistics


def test_statistic_ids_are_the_integrations() -> None:
    for account in ("A-TEST-1", "A-JOURNEY-2B"):
        assert journeys.property_key(
            account, property_id(account)
        ) == integration_property_key(account, property_id(account))
    ids = journeys.statistic_ids("A-JOURNEY-1")
    assert set(ids) == {"consumption", "consumption_cost"}
    key = integration_property_key("A-JOURNEY-1", property_id("A-JOURNEY-1"))
    assert ids["consumption"] == f"meridian_energy:consumption_{key}"
    assert set(journeys.statistic_ids("A-JOURNEY-3", feed_in=True)) == {
        "consumption",
        "consumption_cost",
        "generation",
        "generation_credit",
    }


def test_expected_values_are_what_the_stand_in_sends() -> None:
    hour = _NOW.replace(minute=0)
    reading = float(meridian_standin.hour_reading(hour, 0))
    assert journeys.expected_value("consumption", hour) == reading
    assert journeys.expected_value("generation", hour) == reading
    cost = journeys.expected_value("consumption_cost", hour)
    assert cost == pytest.approx(reading * 0.285)
    assert journeys.expected_value("generation_credit", hour) == cost


def test_parse_rows_reads_the_recorders_milliseconds() -> None:
    start = _NOW.replace(minute=0)
    (row,) = journeys.parse_rows(
        [{"start": start.timestamp() * 1000, "state": 0.5, "sum": 1.5}]
    )
    assert row == Row(start, 0.5, 1.5)


def test_a_whole_history_has_no_problems() -> None:
    rows = _history(_FIRST, 2159)
    assert journeys.history_problems("consumption", rows, _expected(_FIRST, 2159)) == []
    cost = _history(_FIRST, 24, "consumption_cost")
    assert journeys.history_problems("consumption_cost", cost, None) == []


def test_every_kind_of_damage_is_found() -> None:
    first = datetime(2026, 4, 17, 13, tzinfo=UTC)
    rows = _history(first, 10)
    assert journeys.history_problems("consumption", [], None) == ["no rows"]
    gap = rows[:3] + rows[4:]
    (problem,) = [
        p for p in journeys.history_problems("consumption", gap, None) if "follows" in p
    ]
    assert "2:00:00" in problem
    repeated = [*rows[:3], rows[2], *rows[3:]]
    assert any(
        "follows" in p for p in journeys.history_problems("consumption", repeated, None)
    )
    zero = [*rows[:5], Row(rows[5].start, 0.0, rows[4].sum), *rows[6:]]
    zero_problems = journeys.history_problems("consumption", zero, None)
    assert any("not above zero" in p for p in zero_problems)
    assert any("the stand-in sent" in p for p in zero_problems)
    stepped = [*rows[:5], Row(rows[5].start, rows[5].state, rows[5].sum + 1)]
    assert any(
        "sum step" in p for p in journeys.history_problems("consumption", stepped, None)
    )
    offset = [Row(r.start, r.state, r.sum + 1) for r in rows]
    assert journeys.history_problems("consumption", offset, None)[0].startswith(
        "the first sum"
    )
    shifted = [Row(r.start + HOUR, r.state, r.sum) for r in rows]
    assert any(
        "the stand-in sent" in p
        for p in journeys.history_problems("consumption", shifted, None)
    )


def test_the_first_and_newest_hours_must_be_where_the_import_left_them() -> None:
    rows = _history(_FIRST, 48)
    expected = _expected(_FIRST, 48)
    assert journeys.history_problems("consumption", rows, expected) == []
    # The first hour is New Zealand midnight 90 dates before the import.
    assert expected.first_range == (_FIRST, _FIRST)
    day = timedelta(days=1)
    # An import that ran over midnight allows either date's.
    over_midnight = Expected(
        expected.imported_from - day, expected.imported_to, expected.newest_by
    )
    assert journeys.history_problems("consumption", rows, over_midnight) == []
    assert over_midnight.first_range == (_FIRST - day, _FIRST)
    later = Expected(
        expected.imported_from + day, expected.imported_to + day, expected.newest_by
    )
    assert journeys.history_problems("consumption", rows, later)[0].startswith(
        "first hour"
    )
    # A history that starts an hour late lacks that date's first hour.
    late = _history(_FIRST + HOUR, 47)
    assert journeys.history_problems("consumption", late, expected)[0].startswith(
        "first hour"
    )
    stale = Expected(
        expected.imported_from,
        expected.imported_to,
        expected.newest_by + timedelta(hours=3),
    )
    assert journeys.history_problems("consumption", rows, stale)[0].startswith(
        "newest hour"
    )


def test_the_target_and_the_windows_are_the_integrations() -> None:
    # 00:30 on 10 October in New Zealand, in summer time: 90 dates back is
    # in winter, so going back 90 days from the moment would miss a date.
    now = datetime(2026, 10, 9, 11, 30, tzinfo=UTC)
    today = nz_date(now)
    target = today - timedelta(days=HISTORY_DAYS)
    assert journeys.nz_date(now) == today
    assert journeys.backfill_start(now) == local_midnight(target)
    assert journeys.backfill_start(now) == datetime(2026, 7, 11, 12, tzinfo=UTC)
    assert journeys.WINDOW_DATES == WINDOW_DAYS
    assert journeys.RECENT_WINDOWS * WINDOW_DAYS == DAILY_DATES
    for count in (journeys.RECENT_WINDOWS, journeys.FIRST_SYNC_WINDOWS):
        dates = [today - timedelta(days=n) for n in range(count * WINDOW_DAYS)]
        assert journeys.newest_windows(today, count) == [
            (window.start_on.isoformat(), window.end_on.isoformat())
            for window in windows_for(dates)
        ]
    # A first sync's windows reach back to the target, and no further.
    assert journeys.newest_windows(today, journeys.FIRST_SYNC_WINDOWS)[-1][0] == (
        target.isoformat()
    )


def test_problems_are_cut_short() -> None:
    rows = [Row(r.start, 0.0, 0.0) for r in _history(_FIRST, 20)]
    problems = journeys.history_problems("consumption", rows, None)
    assert len(problems) == journeys.SHOWN_PROBLEMS + 1
    assert problems[-1].startswith("and ")


def test_changed_rows_names_lost_and_changed_hours() -> None:
    first = datetime(2026, 4, 17, 13, tzinfo=UTC)
    before = _history(first, 5)
    assert journeys.changed_rows(before, [*before, *_history(first, 7)[5:]]) == []
    after = [before[0], Row(before[1].start, 9.0, 9.0), *before[3:]]
    changed = journeys.changed_rows(before, after)
    assert len(changed) == 2
    assert "now 9.0/9.0" in changed[0]
    assert changed[1].endswith("is gone")
    # Nothing before proves nothing.
    assert journeys.changed_rows([], before) == ["no hours to compare"]


# -------------------------------------------------------------------- screens

_TRANSLATIONS = {
    "component.meridian_energy.title": "Meridian Energy",
    "component.meridian_energy.config.step.import_failed.title": "Import failed",
    "component.meridian_energy.config.step.import_failed.description": (
        "Select Submit.\n\nDetails: {detail}."
    ),
    "component.meridian_energy.config.step.import_failed.data."
    "continue_without_history": "Continue without history",
    "component.meridian_energy.config.error.retry_too_soon": "Wait {seconds} s.",
    "component.meridian_energy.config.progress.initial_import": "Importing.",
    "component.meridian_energy.config.create_entry.partial": "In the background.",
    "component.meridian_energy.config.abort.login_expired": "Expired.",
    "component.homeassistant.config.abort.reauth_successful": "Signed in again.",
    "component.meridian_energy.issues.sync_failing.title": "Not updating",
    "component.meridian_energy.issues.sync_failing.description": (
        "{reason}\n\nGoes away."
    ),
}


def test_a_form_is_rendered_with_its_placeholders_and_error() -> None:
    screen = journeys.render(
        {
            "type": "form",
            "handler": "meridian_energy",
            "step_id": "import_failed",
            "errors": {"base": "retry_too_soon"},
            "description_placeholders": {"detail": "HTTP 503", "seconds": "7"},
            "data_schema": [{"name": "continue_without_history"}],
        },
        _TRANSLATIONS,
    )
    assert screen == Screen(
        "form",
        "import_failed",
        "Import failed",
        "Select Submit.\n\nDetails: HTTP 503.",
        "Wait 7 s.",
        ("Continue without history",),
        (),
    )
    lines = screen.lines()
    assert lines[0] == "- **form `import_failed`**: Import failed"
    assert "  > Details: HTTP 503." in lines
    assert "  - Error line: Wait 7 s." in lines


def test_progress_abort_and_entry_screens_are_rendered() -> None:
    progress = journeys.render(
        {
            "type": "progress",
            "handler": "meridian_energy",
            "progress_action": "initial_import",
        },
        _TRANSLATIONS,
    )
    assert (progress.kind, progress.step, progress.text) == (
        "progress",
        "initial_import",
        "Importing.",
    )
    entry = journeys.render(
        {
            "type": "create_entry",
            "handler": "meridian_energy",
            "title": "Meridian Energy",
            "description": "partial",
        },
        _TRANSLATIONS,
    )
    assert (entry.step, entry.title, entry.text) == (
        "partial",
        "Meridian Energy",
        "In the background.",
    )
    abort = journeys.render(
        {"type": "abort", "handler": "meridian_energy", "reason": "login_expired"},
        _TRANSLATIONS,
    )
    assert abort.text == "Expired."
    untranslated = journeys.render(
        {"type": "abort", "handler": "meridian_energy", "reason": "reauth_successful"},
        _TRANSLATIONS,
    )
    assert untranslated.text == "reauth_successful"
    assert untranslated.missing == ("abort.reauth_successful",)
    assert "  - No translation for: abort.reauth_successful" in untranslated.lines()
    form = journeys.render(
        {
            "type": "form",
            "handler": "meridian_energy",
            "step_id": "otp",
            "data_schema": [{"name": "otp"}],
        },
        _TRANSLATIONS,
    )
    assert form.fields == ("otp",)
    assert form.missing == ("step.otp.title", "step.otp.data.otp")


def test_a_form_without_its_own_title_shows_the_integrations() -> None:
    words = {
        **_TRANSLATIONS,
        "component.meridian_energy.config.step.untitled.description": "Words.",
    }
    form = journeys.render(
        {"type": "form", "handler": "meridian_energy", "step_id": "untitled"}, words
    )
    assert (form.title, form.missing) == ("Meridian Energy", ())


def test_a_form_with_no_words_of_its_own_lacks_them() -> None:
    # A step renamed, or its texts dropped: only the dialog's own title shows.
    form = journeys.render(
        {"type": "form", "handler": "meridian_energy", "step_id": "renamed"},
        _TRANSLATIONS,
    )
    assert (form.title, form.missing) == ("Meridian Energy", ("step.renamed.title",))


def test_a_screen_with_an_unfilled_placeholder_lacks_its_words() -> None:
    screen = journeys.render(
        {
            "type": "form",
            "handler": "meridian_energy",
            "step_id": "import_failed",
            "description_placeholders": {},
            "data_schema": [],
        },
        _TRANSLATIONS,
    )
    assert screen.text == "Select Submit.\n\nDetails: {detail}."
    assert screen.missing == ("step.import_failed.description (no value for {detail})",)


def test_field_labels_come_from_the_handlers_words() -> None:
    screen = journeys.render(
        {
            "type": "form",
            "handler": "meridian_energy",
            "translation_domain": "homeassistant",
            "step_id": "import_failed",
            "errors": {"base": "retry_too_soon"},
            "description_placeholders": {"seconds": "7"},
            "data_schema": [{"name": "continue_without_history"}],
        },
        _TRANSLATIONS,
    )
    assert screen.fields == ("Continue without history",)
    assert screen.missing == (
        "step.import_failed.title",
        "error.retry_too_soon",
    )


# --------------------------------------------------------------------- report


def test_a_journey_passes_only_with_checks_all_passed_and_no_error() -> None:
    result = JourneyResult("first_setup", "First")
    assert not result.ok
    result.checks.append(journeys.Check("a", ok=True))
    assert result.ok
    result.checks.append(journeys.Check("b", ok=False, detail="x"))
    assert not result.ok
    stopped = JourneyResult("reauth", "Reauth", [journeys.Check("a", ok=True)])
    stopped.error = "JourneyError: timed out"
    assert not stopped.ok


def test_the_report_lists_every_check_and_screen() -> None:
    passed = JourneyResult(
        "first_setup",
        "First-time setup",
        [journeys.Check("the entry is loaded", ok=True, detail="loaded")],
        [Screen("form", "user", "Connect", "Enter it.", "", ("Email",), ())],
        seconds=12.4,
    )
    failed = JourneyResult(
        "reauth",
        "Reauth",
        [journeys.Check("sums", ok=False, detail="a|b")],
        error="JourneyError: timed out",
    )
    text = journeys.markdown([passed, failed], {"Home Assistant": "2026.10.4"})
    assert "- Home Assistant: 2026.10.4" in text
    assert "- Result: 1 of 2 journeys passed" in text
    assert "## PASS: First-time setup (`first_setup`, 12 s)" in text
    assert "| PASS | the entry is loaded | loaded |" in text
    assert "- **form `user`**: Connect" in text
    assert "## FAIL: Reauth (`reauth`, 0 s)" in text
    assert "Stopped: JourneyError: timed out" in text
    assert "| FAIL | sums | a/b |" in text


def test_the_report_goes_to_the_temporary_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    path = journeys.write_report([], {"Started": "now"}, 6)
    assert path.parent == tmp_path
    assert path.name.startswith("meridian-journeys-")
    assert "- Started: now" in path.read_text()
    assert "- Result: 0 of 6 journeys passed" in path.read_text()


# ------------------------------------------------------------------- journey


class _Client:
    """A client whose REST and websocket answers are canned and recorded."""

    def __init__(
        self, answers: dict[str, Any] | None = None, state: Path | None = None
    ) -> None:
        self.answers = answers or {}
        self.sent: list[str] = []
        self.data: list[dict[str, Any] | None] = []
        self.translated: list[tuple[str, str]] = []
        self.issues: list[dict[str, Any]] = []
        self.registry: list[dict[str, Any]] = []
        # The recorder's backlog and statistics, an answer for each request in
        # turn; the last answers every request after it.
        self.backlogs: list[int] = [0]
        self.periods: list[dict[str, Any]] = [{}]
        self.instance = SimpleNamespace(state=state or Path("/nonexistent"))

    def rest(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        self.sent.append(f"{method} {path}")
        self.data.append(data)
        answer = self.answers.get(path)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def ws(self, *messages: dict[str, Any]) -> list[Any]:
        if messages[0]["type"] == "repairs/list_issues":
            return [{"issues": self.issues}]
        if messages[0]["type"] == "config/entity_registry/list":
            return [self.registry]
        if messages[0]["type"] == "recorder/info":
            return [{"backlog": _next(self.backlogs)}]
        if messages[0]["type"] == "recorder/statistics_during_period":
            return [_next(self.periods)]
        assert messages[0]["type"] == "frontend/get_translations"
        (domain,) = messages[0]["integration"]
        category = messages[0]["category"]
        self.translated.append((domain, category))
        prefix = f"component.{domain}.{category}"
        return [
            {
                "resources": {
                    k: v
                    for k, v in _TRANSLATIONS.items()
                    if k == prefix or k.startswith(f"{prefix}.")
                }
            }
        ]


def _next(answers: list[Any]) -> Any:
    """Return the next canned answer; the last one answers every request after."""
    return answers.pop(0) if len(answers) > 1 else answers[0]


def _stop(stopped: list[str], state: Path) -> Any:
    """Return a fake ``live_ha.stop`` that records the name it was given."""

    def stop(name: str = "live-ha", before_delete: Any = None) -> None:
        stopped.append(name)
        if before_delete is not None:
            before_delete(state)

    return stop


def _journey(folder: Path, client: Any = None) -> Journey:
    chosen: Any = client or _Client()
    return Journey(chosen, folder, JourneyResult("x", "X"))


def test_checks_are_recorded_and_a_required_one_stops_the_journey(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    journey = _journey(tmp_path)
    assert journey.check("passes", ok=True)
    assert not journey.check("fails", ok=False, detail="why")
    with pytest.raises(JourneyError, match="needed: because"):
        journey.require("needed", ok=False, detail="because")
    assert [(c.name, c.ok) for c in journey.result.checks] == [
        ("passes", True),
        ("fails", False),
        ("needed", False),
    ]
    out = capsys.readouterr().out
    assert "  PASS  passes" in out
    assert "  FAIL  fails (why)" in out


def test_the_scenario_and_journal_are_the_stand_ins(tmp_path: Path) -> None:
    journey = _journey(tmp_path)
    assert journey.journal() == []
    scenario = meridian_standin.Scenario(accounts=("A-JOURNEY-9",), session_ended=True)
    journey.scenario(scenario)
    standin = meridian_standin.StandIn(tmp_path)
    assert standin.scenario() == scenario
    assert not list(tmp_path.glob("*.partial"))
    standin.journal({"at": "2026-07-16T12:00:00.250000+00:00", "operation": "a"})
    standin.journal({"at": "2026-07-16T12:00:00.750000+00:00", "operation": "b"})
    assert [r["operation"] for r in journey.journal()] == ["a", "b"]
    # To the microsecond: two requests in the same second are told apart.
    since = datetime(2026, 7, 16, 12, 0, 0, 500000, tzinfo=UTC)
    assert [r["operation"] for r in journey.journal(since)] == ["b"]


def test_a_syncs_windows_are_read_from_the_journal(tmp_path: Path) -> None:
    journey = _journey(tmp_path)
    standin = meridian_standin.StandIn(tmp_path)
    now = datetime.now(UTC)
    today = journeys.nz_date(now)
    since = now - timedelta(seconds=1)
    standin.journal(
        {
            "at": (since - timedelta(seconds=1)).isoformat(),
            "operation": "measurements",
            "direction": meridian_standin.CONSUMPTION,
            "start_on": "2020-01-01",
            "end_on": "2020-01-07",
        }
    )
    for start_on, end_on in journeys.newest_windows(today, 3):
        for direction in (meridian_standin.CONSUMPTION, meridian_standin.GENERATION):
            record = {
                "at": now.isoformat(),
                "operation": "measurements",
                "direction": direction,
                "start_on": start_on,
                "end_on": end_on,
            }
            standin.journal(record)
            # A page after the first is the same window.
            standin.journal({**record, "continuation": True})
    standin.journal({"at": now.isoformat(), "operation": "GetAccounts"})
    # The bill's day totals are not a window.
    standin.journal(
        {
            "at": now.isoformat(),
            "operation": "measurements",
            "direction": meridian_standin.CONSUMPTION,
            "start_on": "2026-07-01",
            "end_on": "2026-07-31",
            "frequency": "DAY_INTERVAL",
        }
    )
    assert journey.windows(since) == journeys.newest_windows(today, 3)
    journey.check_windows("the reload", since, 3)
    journey.check_windows("the reload", since, 3, meridian_standin.GENERATION)
    journey.check_windows("the first sync", since, 13)
    passed, export, wanting = journey.result.checks
    assert passed.ok
    assert passed.name == (
        "the reload: consumption asked for 3 windows of 7 dates back from today, "
        "newest first, and no other"
    )
    assert passed.detail == (f"3 windows, {today} back to {today - timedelta(days=20)}")
    assert export.ok
    assert not wanting.ok
    journey.check_windows("nothing", now + timedelta(seconds=1), 3)
    assert journey.result.checks[-1].detail == "0 windows"


def test_the_repair_notice_is_read_as_repairs_shows_it(tmp_path: Path) -> None:
    client = _Client()
    journey = _journey(tmp_path, client)
    assert journey.notice("E1") is None
    client.issues = [
        {"domain": "other", "issue_id": "sync_failing_E1", "translation_key": "x"},
        {
            "domain": "meridian_energy",
            "issue_id": "sync_failing_E2",
            "translation_key": "sync_failing",
            "translation_placeholders": {"reason": "Another entry"},
        },
        {
            "domain": "meridian_energy",
            "issue_id": "sync_failing_E1",
            "translation_key": "sync_failing",
            "translation_placeholders": {"reason": "No answer"},
        },
    ]
    assert journey.notice("E1") == ("Not updating", "No answer\n\nGoes away.")
    assert journey.notice("E1") == ("Not updating", "No answer\n\nGoes away.")
    assert client.translated == [("meridian_energy", "issues")]


def test_every_figure_must_have_a_sensor_with_a_value(tmp_path: Path) -> None:
    client = _Client()
    journey = _journey(tmp_path, client)
    client.registry = [
        {"entity_id": f"sensor.{key}", "config_entry_id": "E1", "translation_key": key}
        for key in ("bill_usage", "bill_cost", "total_due")
    ] + [
        {
            "entity_id": "sensor.billing_period_end",
            "config_entry_id": "E1",
            "translation_key": "billing_period_end",
            "disabled_by": "integration",
        },
        {"entity_id": "sensor.other", "config_entry_id": "E2", "translation_key": "x"},
    ]
    client.answers = {
        "/api/states/sensor.bill_usage": {"state": "12.5"},
        "/api/states/sensor.bill_cost": {"state": "unknown"},
        "/api/states/sensor.total_due": {"state": "40"},
    }

    assert journey.sensor_states("E1") == {
        "bill_usage": "12.5",
        "bill_cost": "unknown",
        "total_due": "40",
    }
    journey.check_figures("E1", ("bill_usage", "total_due"))
    journey.check_figures("E1", ("total_due", "bill_cost", "bill_export"))

    passed, failed = journey.result.checks
    assert passed.ok
    assert (
        passed.name == "every figure from Meridian has a value: bill usage, total due"
    )
    assert not failed.ok
    assert failed.detail == "missing: bill_export; unknown: bill_cost"
    assert set(journeys.FIGURES) == {
        "bill_usage",
        "bill_cost",
        "bill_export",
        "bill_credit",
        "bill_estimate",
        "total_due",
        "last_bill",
        "last_payment",
        "daily_charge",
        "anytime_price",
    }


def test_each_unit_must_have_its_history_complete(tmp_path: Path) -> None:
    journey = _journey(tmp_path)
    both = [
        {"direction": "consumption", "history_complete": True},
        {"direction": "generation", "history_complete": True},
    ]
    journey.check_complete({"units": both}, 2)
    journey.check_complete({"units": both[:1]}, 2)
    journey.check_complete(
        {"units": [{"direction": "consumption", "history_complete": False}]}, 1
    )
    journey.check_complete({}, 1)
    checks = journey.result.checks
    assert [check.ok for check in checks] == [True, False, False, False]
    assert checks[0].name == "each unit's history is complete, 2 in all"
    assert checks[0].detail == "consumption: True, generation: True"
    assert checks[-1].detail == "no units"


def test_an_update_waits_out_the_last_ones_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    slept: list[float] = []
    monkeypatch.setattr("scripts.journeys.time.sleep", slept.append)
    path = "/api/services/homeassistant/update_entity"
    client = _Client({path: []})
    client.registry = [
        {"entity_id": "sensor.b", "config_entry_id": "E1"},
        {"entity_id": "sensor.a", "config_entry_id": "E1"},
        {"entity_id": "sensor.c", "config_entry_id": "E1", "disabled_by": "user"},
        {"entity_id": "sensor.0", "config_entry_id": "E2"},
    ]
    journey = _journey(tmp_path, client)
    journey.update("E1")
    assert slept == []
    assert client.sent == [f"POST {path}"]
    assert client.data == [{"entity_id": "sensor.a"}]
    # Asked again at once: Home Assistant would run it only after 10 s.
    journey.update("E1")
    assert slept == [pytest.approx(journeys.UPDATE_COOLDOWN_S, abs=1)]
    client.answers[path] = JourneyError("refused")
    ended = journey.updated_at
    with pytest.raises(JourneyError):
        journey.update("E1")
    assert journey.updated_at > ended


def test_a_failed_first_sync_shows_in_the_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(journeys, "POLL_S", 0.01)
    monkeypatch.setattr(journeys, "SYNC_TIMEOUT_S", 0.05)
    entries = "/api/config/config_entries/entry?domain=meridian_energy"
    diagnostics = "/api/diagnostics/config_entry/E1"
    client = _Client(
        {
            entries: [{"entry_id": "E1", "state": "setup_in_progress"}],
            diagnostics: {"data": {"coordinator": {"last_failure": None}}},
        }
    )
    journey = _journey(tmp_path, client)
    with pytest.raises(JourneyError, match="first sync to fail"):
        journey.wait_failed("E1")
    client.answers[entries] = [{"entry_id": "E1", "state": "loaded"}]
    with pytest.raises(JourneyError, match="first sync to fail"):
        journey.wait_failed("E1")
    failure = {"key": "cannot_connect", "cause": "MeridianHttpError"}
    client.answers[diagnostics] = {"data": {"coordinator": {"last_failure": failure}}}
    assert journey.wait_failed("E1")["last_failure"] == failure


def test_statistics_are_read_once_the_recorder_has_saved_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(journeys, "POLL_S", 0.01)
    start = _FIRST.timestamp() * 1000
    hour = {"start": start, "state": 0.5, "sum": 0.5}
    later = {"start": start + 3_600_000, "state": 0.25, "sum": 0.75}
    client = _Client()
    # Writes still queued, then a read that changes under it, then two agree.
    client.backlogs = [2, 0]
    client.periods = [
        {"meridian_energy:a": [hour]},
        {"meridian_energy:a": [hour, later]},
        {"meridian_energy:a": [hour, later]},
    ]
    journey = _journey(tmp_path, client)
    rows = journey.statistics(["meridian_energy:b", "meridian_energy:a"])
    assert rows == {
        "meridian_energy:a": [
            Row(_FIRST, 0.5, 0.5),
            Row(_FIRST + HOUR, 0.25, 0.75),
        ],
        "meridian_energy:b": [],
    }
    assert client.periods == [{"meridian_energy:a": [hour, later]}]


def _log_line(level: str, name: str, message: str, offset: str = "") -> str:
    """Return a line as Home Assistant writes it to its log."""
    return f"2026-11-08 12:00:00.123{offset} {level} (MainThread) [{name}] {message}\n"


_OWN = "custom_components.meridian_energy"


def test_the_integrations_own_lines_are_read_from_the_mark(tmp_path: Path) -> None:
    journey = _journey(tmp_path, _Client(state=tmp_path))
    log = tmp_path / "config" / journeys.LOG_FILE
    assert journey.log_mark() == LogMark(0, 0)
    assert journey.own_log(LogMark(0, 0)) == []
    log.parent.mkdir()
    log.write_text(_log_line("WARNING", f"{_OWN}.coordinator", "before the mark"))
    mark = journey.log_mark()
    assert mark.size == log.stat().st_size
    with log.open("a") as out:
        out.write(_log_line("DEBUG", f"{_OWN}.transport", "a request", "+13:00"))
        out.write(_log_line("WARNING", "homeassistant.helpers.entity", "not its own"))
        out.write(_log_line("ERROR", f"{_OWN}_other", "another integration's"))
        out.write("Traceback (most recent call last):\n")
        out.write(_log_line("ERROR", _OWN, "its own"))
    assert journey.own_log(mark) == [("DEBUG", "a request"), ("ERROR", "its own")]


def test_a_restart_moves_the_marked_log_aside(tmp_path: Path) -> None:
    journey = _journey(tmp_path, _Client(state=tmp_path))
    log = tmp_path / "config" / journeys.LOG_FILE
    moved = log.with_name(f"{journeys.LOG_FILE}.1")
    log.parent.mkdir()
    log.write_text(_log_line("INFO", _OWN, "before the mark"))
    mark = journey.log_mark()
    with log.open("a") as out:
        out.write(_log_line("INFO", _OWN, "while stopping"))
    # Held open, so that no new file can take the marked one's inode.
    with log.open("rb"):
        log.replace(moved)
        log.write_text(_log_line("INFO", _OWN, "after the start"))
        assert journey.own_log(mark) == [
            ("INFO", "while stopping"),
            ("INFO", "after the start"),
        ]
        # A second start: the marked log is gone, and the new one is read whole.
        log.replace(moved)
        log.write_text(_log_line("INFO", _OWN, "after another start"))
        assert journey.own_log(mark) == [("INFO", "after another start")]


def test_the_lines_above_debug_must_be_those_expected(tmp_path: Path) -> None:
    journey = _journey(tmp_path, _Client(state=tmp_path))
    log = tmp_path / "config" / journeys.LOG_FILE
    mark = journey.log_mark()
    journey.check_log("unread", mark)
    log.parent.mkdir()
    with log.open("a") as out:
        out.write(_log_line("DEBUG", _OWN, "a request"))
        out.write(_log_line("ERROR", f"{_OWN}.coordinator", "Error fetching"))
        out.write(_log_line("INFO", f"{_OWN}.coordinator", "recovered"))
    lines = [("ERROR", "Error fetching"), ("INFO", "recovered")]
    journey.check_log("the outage", mark, lines)
    journey.check_log("the outage", mark, lines[::-1])
    journey.check_log("the outage", mark)
    quiet = journey.log_mark()
    with log.open("a") as out:
        out.write(_log_line("DEBUG", _OWN, "another request"))
    journey.check_log("the restart", quiet)
    unread, both, reordered, loud, calm = journey.result.checks
    assert not unread.ok
    assert unread.detail == "no line from it in home-assistant.log"
    assert both.ok
    assert both.name == (
        'the outage: the integration logged only ERROR "Error fetching"; '
        'INFO "recovered"'
    )
    assert not reordered.ok
    assert not loud.ok
    assert loud.name == "the outage: the integration logged nothing above debug level"
    assert loud.detail == 'ERROR "Error fetching"; INFO "recovered"'
    assert calm.ok
    assert calm.detail == "nothing above debug level"


def test_refused_requests_and_fake_errors_fail_the_check(tmp_path: Path) -> None:
    journey = _journey(tmp_path)
    journey.check_nothing_refused()
    assert all(check.ok for check in journey.result.checks)
    standin = meridian_standin.StandIn(tmp_path)
    standin.refuse("request", "updates.example.invalid")
    standin.journal({"at": "2026-07-16T12:00:00+00:00", "error": "unexpected URL"})
    journey.check_nothing_refused()
    refused, errors = journey.result.checks[2:]
    assert not refused.ok
    assert "request updates.example.invalid" in refused.detail
    assert not errors.ok
    assert errors.detail == "unexpected URL"


def test_screens_are_rendered_with_the_instances_translations(tmp_path: Path) -> None:
    journey = _journey(tmp_path)
    shown = journey.screen(
        {"type": "abort", "handler": "meridian_energy", "reason": "login_expired"}
    )
    assert shown.text == "Expired."
    assert journey.result.screens == [shown]


def test_a_screen_from_another_translation_domain_uses_its_words(
    tmp_path: Path,
) -> None:
    client = _Client()
    journey = _journey(tmp_path, client)
    journey.screen(
        {"type": "abort", "handler": "meridian_energy", "reason": "login_expired"}
    )
    shown = journey.screen(
        {
            "type": "abort",
            "handler": "meridian_energy",
            "reason": "reauth_successful",
            "translation_domain": "homeassistant",
        }
    )
    assert shown.text == "Signed in again."
    assert not shown.missing
    journey.screen(
        {
            "type": "abort",
            "handler": "meridian_energy",
            "reason": "reauth_successful",
            "translation_domain": "homeassistant",
        }
    )
    assert client.translated == [
        ("meridian_energy", "config"),
        ("meridian_energy", "title"),
        ("homeassistant", "config"),
    ]


def test_a_screen_without_its_words_fails_the_journey(tmp_path: Path) -> None:
    journey = _journey(tmp_path)
    journey.check_screens()
    assert journey.result.checks == []
    journey.screen(
        {"type": "abort", "handler": "meridian_energy", "reason": "login_expired"}
    )
    journey.check_screens()
    assert journey.result.checks[-1].ok
    journey.screen(
        {"type": "abort", "handler": "meridian_energy", "reason": "reauth_successful"}
    )
    journey.check_screens()
    shown = journey.result.checks[-1]
    assert not shown.ok
    assert shown.detail == (
        "no translation for abort reauth_successful: abort.reauth_successful"
    )


def test_a_flow_that_creates_an_entry_notes_it_for_removal(tmp_path: Path) -> None:
    path = "/api/config/config_entries/flow/F1"
    client = _Client(
        {
            path: {
                "type": "create_entry",
                "handler": "meridian_energy",
                "flow_id": "F1",
                "result": {"entry_id": "E1"},
            },
            "/api/config/config_entries/flow/F2": urllib.error.URLError("gone"),
        }
    )
    journey = _journey(tmp_path, client)
    journey.submit("F1", {"otp": "123456"})
    assert journey.entries == ["E1"]
    journey.flows.append("F2")
    journey.remove_entries()
    assert client.sent[-2:] == [
        "DELETE /api/config/config_entries/flow/F2",
        "DELETE /api/config/config_entries/entry/E1",
    ]


def test_wait_gives_up_after_its_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(journeys, "POLL_S", 0.01)
    calls: list[int] = []

    def third_time() -> bool | None:
        calls.append(1)
        return len(calls) > 2 or None

    assert journeys.wait("x", third_time, 5)
    assert len(calls) == 3
    with pytest.raises(JourneyError, match="timed out after 0 s waiting for y"):
        journeys.wait("y", lambda: None, 0.05)


def test_an_error_status_stops_the_journey_with_its_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(*_args: Any, **_kwargs: Any) -> Any:
        raise urllib.error.HTTPError(
            "http://127.0.0.1:8125/x",
            400,
            "Bad",
            email.message.Message(),
            io.BytesIO(b'{"message": "no"}'),
        )

    monkeypatch.setattr(live_ha, "call", refuse)
    client = journeys.Client(live_ha.Instance(Path("/x"), 8125, None, "1"))  # type: ignore[arg-type]
    client._token = "token"
    client._token_at = 1e12
    with pytest.raises(
        JourneyError, match=re.escape('POST /x returned HTTP 400: {"message": "no"}')
    ):
        client.rest("POST", "/x", {})


# --------------------------------------------------------------------- runner


def test_the_stand_in_goes_into_site_packages_with_the_fakes(tmp_path: Path) -> None:
    files = journeys.site_files(tmp_path / "standin")
    assert files["meridian_standin.pth"] == meridian_standin.PTH_LINE.encode()
    assert json.loads(files["meridian_standin/settings.json"]) == {
        "folder": str(tmp_path / "standin")
    }
    root = Path(journeys.__file__).resolve().parents[1] / "tests"
    assert (
        files["meridian_standin/__init__.py"]
        == (root / "meridian_standin.py").read_bytes()
    )
    for name in ("meridian_http.py", "kraken_fake.py"):
        assert files[f"meridian_standin/{name}"] == (root / name).read_bytes()


def test_the_journeys_never_take_the_live_tests_port_or_folder() -> None:
    assert journeys.journeys_port("8125") == 8125
    with pytest.raises(argparse.ArgumentTypeError, match="live test"):
        journeys.journeys_port(str(live_ha.DEFAULT_PORT))
    args = journeys.parse([])
    assert args.port == journeys.DEFAULT_PORT != live_ha.DEFAULT_PORT
    assert journeys.NAME != live_ha.DEFAULT_NAME
    assert journeys.parse(["--only", "restart", "reauth"]).only == [
        "restart",
        "reauth",
    ]
    with pytest.raises(SystemExit):
        journeys.parse(["--only", "nonsense"])


def test_the_stand_in_must_be_installed_in_home_assistant(tmp_path: Path) -> None:
    class _Process:
        pid = 4242

    instance = live_ha.Instance(tmp_path, 8125, _Process(), "1")  # type: ignore[arg-type]
    run = journeys.Run(instance, tmp_path)
    with pytest.raises(SystemExit, match="not installed"):
        run.check_standin()
    (tmp_path / "installed-4242").write_text("")
    run.check_standin()


def test_a_journey_that_stops_is_recorded_and_cleaned_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    removed: list[str] = []

    def stops(journey: Journey) -> None:
        journey.entries.append("E1")
        journey.check("got this far", ok=True)
        raise JourneyError("timed out")

    monkeypatch.setattr(journeys, "first_setup", stops)
    monkeypatch.setattr(
        Journey, "remove_entries", lambda self: removed.extend(self.entries)
    )
    run = journeys.Run(live_ha.Instance(tmp_path, 8125, None, "1"), tmp_path)  # type: ignore[arg-type]
    result = run.run("first_setup")
    assert not result.ok
    assert result.error == "JourneyError: timed out"
    assert removed == ["E1"]
    assert run.results == [result]


def test_a_journey_that_crashes_fails_and_the_next_one_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def crashes(journey: Journey) -> None:
        journey.check("got this far", ok=True)
        raise IndexError("list index out of range")

    monkeypatch.setattr(journeys, "first_setup", crashes)
    monkeypatch.setattr(journeys, "two_accounts", lambda j: j.check("ok", ok=True))
    monkeypatch.setattr(Journey, "remove_entries", lambda _self: None)
    run = journeys.Run(live_ha.Instance(tmp_path, 8125, None, "1"), tmp_path)  # type: ignore[arg-type]
    crashed = run.run("first_setup")
    assert not crashed.ok
    assert crashed.error == "IndexError: list index out of range"
    assert run.run("two_accounts").ok
    assert journeys.summary(run.results, 2) == "1 of 2 journeys passed"


def test_a_journey_whose_clean_up_fails_fails_and_the_next_one_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(_self: Journey) -> None:
        raise ValueError("Expecting value: line 1 column 1 (char 0)")

    monkeypatch.setattr(journeys, "first_setup", lambda j: j.check("ok", ok=True))
    monkeypatch.setattr(journeys, "two_accounts", lambda j: j.check("ok", ok=True))
    monkeypatch.setattr(Journey, "remove_entries", broken)
    run = journeys.Run(live_ha.Instance(tmp_path, 8125, None, "1"), tmp_path)  # type: ignore[arg-type]
    failed = run.run("first_setup")
    assert not failed.ok
    assert failed.error == (
        "the clean-up failed: ValueError: Expecting value: line 1 column 1 (char 0)"
    )
    monkeypatch.setattr(Journey, "remove_entries", lambda _self: None)
    assert run.run("two_accounts").ok
    assert journeys.summary(run.results, 2) == "1 of 2 journeys passed"


def test_a_failed_journey_whose_clean_up_fails_reports_both(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def stops(_journey: Journey) -> None:
        raise JourneyError("timed out")

    def broken(_self: Journey) -> None:
        raise ValueError("not JSON")

    monkeypatch.setattr(journeys, "first_setup", stops)
    monkeypatch.setattr(Journey, "remove_entries", broken)
    run = journeys.Run(live_ha.Instance(tmp_path, 8125, None, "1"), tmp_path)  # type: ignore[arg-type]
    failed = run.run("first_setup")
    assert failed.error == (
        "JourneyError: timed out; the clean-up failed: ValueError: not JSON"
    )


def test_an_interrupted_journey_fails_and_stops_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupted(journey: Journey) -> None:
        journey.check("got this far", ok=True)
        raise KeyboardInterrupt

    monkeypatch.setattr(journeys, "first_setup", interrupted)
    monkeypatch.setattr(Journey, "remove_entries", lambda _self: None)
    run = journeys.Run(live_ha.Instance(tmp_path, 8125, None, "1"), tmp_path)  # type: ignore[arg-type]
    with pytest.raises(KeyboardInterrupt):
        run.run("first_setup")
    (result,) = run.results
    assert not result.ok
    assert result.error == "the run stopped: KeyboardInterrupt"
    # Journeys that never ran did not pass.
    assert journeys.summary(run.results, 6) == "0 of 6 journeys passed"


def test_a_failed_restart_stops_the_run_but_keeps_its_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _Process:
        pid = 4242

    folder = tmp_path / "config" / journeys.STANDIN_FOLDER

    def launch(*_args: Any, **_kwargs: Any) -> live_ha.Instance:
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "installed-4242").write_text("")
        return live_ha.Instance(tmp_path, 8125, _Process(), "2026.10.4")  # type: ignore[arg-type]

    def restart(journey: Journey, relaunch: Any) -> None:
        journey.check("before the restart", ok=True)
        relaunch()

    def relaunch(*_args: Any, **_kwargs: Any) -> live_ha.Instance:
        raise SystemExit("Timed out waiting for the web server")

    stopped: list[str] = []
    monkeypatch.setattr(live_ha, "launch", launch)
    monkeypatch.setattr(live_ha, "relaunch", relaunch)
    monkeypatch.setattr(live_ha, "state_dir", lambda *_args: tmp_path / "state")
    monkeypatch.setattr(live_ha, "stop", _stop(stopped, tmp_path / "state"))
    monkeypatch.setattr(live_ha, "config_dir", lambda _state: tmp_path / "config")
    monkeypatch.setattr(journeys, "restart", restart)
    monkeypatch.setattr(Journey, "remove_entries", lambda _self: None)
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    with pytest.raises(SystemExit, match="web server"):
        journeys.main(["--only", "restart"])
    assert stopped == ["journeys"]
    out = capsys.readouterr().out
    assert (
        "FAIL  the run stopped: SystemExit: Timed out waiting for the web server" in out
    )
    assert "0 of 1 journeys passed" in out
    (report,) = tmp_path.glob("meridian-journeys-*.md")
    assert "Stopped: the run stopped: SystemExit: Timed out" in report.read_text()


def test_the_instance_is_always_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stopped: list[str] = []
    monkeypatch.setattr(live_ha, "state_dir", lambda *_args: tmp_path / "state")
    monkeypatch.setattr(live_ha, "stop", _stop(stopped, tmp_path / "state"))

    def broken(*_args: Any, **_kwargs: Any) -> live_ha.Instance:
        raise SystemExit("install failed")

    monkeypatch.setattr(live_ha, "launch", broken)
    with pytest.raises(SystemExit, match="install failed"):
        journeys.main([])
    assert stopped == ["journeys"]
    assert journeys.main(["--clean-up"]) == 0
    assert stopped == ["journeys", "journeys"]


def test_a_run_never_takes_over_an_existing_instance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stopped: list[str] = []
    (tmp_path / "state").mkdir()
    monkeypatch.setattr(live_ha, "state_dir", lambda *_args: tmp_path / "state")
    monkeypatch.setattr(live_ha, "stop", _stop(stopped, tmp_path / "state"))
    with pytest.raises(SystemExit, match="left by a run that was killed"):
        journeys.main([])
    assert stopped == []
    assert (tmp_path / "state").is_dir()


def test_nothing_starts_while_another_run_holds_the_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stopped: list[str] = []
    launched: list[bool] = []
    monkeypatch.setattr(live_ha, "state_dir", lambda *_args: tmp_path / "state")
    monkeypatch.setattr(live_ha, "stop", _stop(stopped, tmp_path / "state"))
    monkeypatch.setattr(live_ha, "launch", lambda *_a, **_k: launched.append(True))
    with journeys.only_run(tmp_path / "state"):
        for argv in ([], ["--clean-up"]):
            with pytest.raises(SystemExit, match="Another run is using"):
                journeys.main(argv)
    assert stopped == []
    assert launched == []
    # Released with the run: a clean-up can go ahead.
    assert journeys.main(["--clean-up"]) == 0
    assert stopped == ["journeys"]


def test_a_run_reports_and_exits_non_zero_when_a_journey_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _Process:
        pid = 4242

    folder = tmp_path / "config" / journeys.STANDIN_FOLDER
    launched: dict[str, Any] = {}

    def launch(*args: Any, **kwargs: Any) -> live_ha.Instance:
        launched.update(kwargs, args=args)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "installed-4242").write_text("")
        return live_ha.Instance(tmp_path, 8125, _Process(), "2026.10.4")  # type: ignore[arg-type]

    def run(self: journeys.Run, name: str) -> JourneyResult:
        result = JourneyResult(name, name, [journeys.Check("c", ok=name != "reauth")])
        self.results.append(result)
        return result

    stopped: list[str] = []
    monkeypatch.setattr(live_ha, "launch", launch)
    monkeypatch.setattr(live_ha, "state_dir", lambda *_args: tmp_path / "state")
    monkeypatch.setattr(live_ha, "stop", _stop(stopped, tmp_path / "state"))
    monkeypatch.setattr(live_ha, "config_dir", lambda _state: tmp_path / "config")
    monkeypatch.setattr(journeys.Run, "run", run)
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    assert journeys.main(["--only", "first_setup", "reauth"]) == 1
    assert launched["name"] == "journeys"
    assert launched["args"] == (None, None, 8125)
    assert json.loads(launched["site_files"]["meridian_standin/settings.json"]) == {
        "folder": str(folder)
    }
    assert "1 of 2 journeys passed" in capsys.readouterr().out
    assert journeys.main(["--only", "first_setup"]) == 0
    assert stopped == ["journeys", "journeys"]
    (report,) = sorted(tmp_path.glob("meridian-journeys-*.md"))[-1:]
    assert "Home Assistant: 2026.10.4" in report.read_text()
    assert "Refused by the stand-in: nothing" in report.read_text()


def test_a_refusal_after_the_last_check_fails_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _Process:
        pid = 4242

    folder = tmp_path / "config" / journeys.STANDIN_FOLDER

    def launch(*_args: Any, **_kwargs: Any) -> live_ha.Instance:
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "installed-4242").write_text("")
        return live_ha.Instance(tmp_path, 8125, _Process(), "2026.10.4")  # type: ignore[arg-type]

    def run(self: journeys.Run, name: str) -> JourneyResult:
        result = JourneyResult(name, name, [journeys.Check("c", ok=True)])
        self.results.append(result)
        return result

    def stop(name: str = "live-ha", before_delete: Any = None) -> None:
        del name
        # Home Assistant tried to reach the internet as it shut down.
        (folder / meridian_standin.REFUSED).write_text("t connection 192.0.2.1\n")
        before_delete(tmp_path / "state")

    monkeypatch.setattr(live_ha, "launch", launch)
    monkeypatch.setattr(live_ha, "state_dir", lambda *_args: tmp_path / "state")
    monkeypatch.setattr(live_ha, "stop", stop)
    monkeypatch.setattr(live_ha, "config_dir", lambda _state: tmp_path / "config")
    monkeypatch.setattr(journeys.Run, "run", run)
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    assert journeys.main(["--only", "first_setup"]) == 1
    out = capsys.readouterr().out
    assert "FAIL the stand-in refused 1: t connection 192.0.2.1" in out
    assert "1 of 1 journeys passed; FAIL: the stand-in refused 1" in out
    (report,) = tmp_path.glob("meridian-journeys-*.md")
    assert "Refused by the stand-in: 1: t connection 192.0.2.1" in report.read_text()
    assert "Result: 1 of 1 journeys passed; FAIL" in report.read_text()


def test_the_journeys_refuse_another_integration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(live_ha, "integration_domain", lambda: "watercare")
    with pytest.raises(SystemExit, match="not watercare"):
        journeys.main([])

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
from typing import Any

import pytest

from custom_components.meridian_energy.statistics import (
    property_key as integration_property_key,
)
from scripts import journeys, live_ha
from scripts.journeys import (
    HOUR,
    Expected,
    Journey,
    JourneyError,
    JourneyResult,
    Row,
    Screen,
)

from . import meridian_standin
from .meridian_http import property_id

_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)


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
    imported = first + timedelta(days=90) - timedelta(minutes=30)
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
    first = datetime(2026, 4, 17, 13, tzinfo=UTC)
    rows = _history(first, 2159)
    assert journeys.history_problems("consumption", rows, _expected(first, 2159)) == []
    cost = _history(first, 24, "consumption_cost")
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
    first = datetime(2026, 4, 17, 13, tzinfo=UTC)
    rows = _history(first, 48)
    expected = _expected(first, 48)
    assert journeys.history_problems("consumption", rows, expected) == []
    # Within the date, a lane may already have filled the hours before.
    later = Expected(
        expected.imported_from + timedelta(hours=5),
        expected.imported_to + timedelta(hours=5),
        expected.newest_by,
    )
    assert journeys.history_problems("consumption", rows, later) == []
    late = Expected(
        expected.imported_from + timedelta(days=2),
        expected.imported_to + timedelta(days=2),
        expected.newest_by,
    )
    assert journeys.history_problems("consumption", rows, late)[0].startswith(
        "first hour"
    )
    early = Expected(
        expected.imported_from - timedelta(hours=3),
        expected.imported_to - timedelta(hours=3),
        expected.newest_by,
    )
    assert journeys.history_problems("consumption", rows, early)[0].startswith(
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
    # A lane may complete the date: New Zealand midnight is the earliest first.
    earliest, latest = expected.first_range
    assert earliest == journeys.backfill_start(expected.imported_from)
    assert earliest.astimezone(journeys.NZ).hour == 0
    assert latest == first


def test_problems_are_cut_short() -> None:
    first = datetime(2026, 4, 17, 13, tzinfo=UTC)
    rows = [Row(r.start, 0.0, 0.0) for r in _history(first, 20)]
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


def test_preselected_reads_a_fields_default() -> None:
    result = {
        "data_schema": [
            {"name": "other", "default": ["X"]},
            {"name": "selected_accounts", "default": ["A-1"]},
            {"name": "empty"},
        ]
    }
    assert journeys.preselected(result, "selected_accounts") == ["A-1"]
    assert journeys.preselected(result, "empty") == []
    assert journeys.preselected(result, "missing") == []


def test_offered_reads_a_select_fields_options() -> None:
    result = {
        "data_schema": [
            {"name": "other"},
            {
                "name": "selected_accounts",
                "selector": {
                    "select": {
                        "options": [
                            {"value": "A-1", "label": "one"},
                            {"value": "A-2", "label": "two"},
                        ]
                    }
                },
            },
        ]
    }
    assert journeys.offered(result, "selected_accounts") == ["A-1", "A-2"]
    assert journeys.offered(result, "missing") == []
    assert journeys.offered({}, "selected_accounts") == []


# --------------------------------------------------------------------- report


def test_a_journey_passes_only_with_checks_all_passed_and_no_error() -> None:
    result = JourneyResult("first_setup", "First")
    assert not result.ok
    result.checks.append(journeys.Check("a", ok=True))
    assert result.ok
    result.checks.append(journeys.Check("b", ok=False, detail="x"))
    assert not result.ok
    stopped = JourneyResult("retry", "Retry", [journeys.Check("a", ok=True)])
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
        "retry",
        "Retry",
        [journeys.Check("sums", ok=False, detail="a|b")],
        error="JourneyError: timed out",
    )
    text = journeys.markdown([passed, failed], {"Home Assistant": "2026.10.4"})
    assert "- Home Assistant: 2026.10.4" in text
    assert "- Result: 1 of 2 journeys passed" in text
    assert "## PASS: First-time setup (`first_setup`, 12 s)" in text
    assert "| PASS | the entry is loaded | loaded |" in text
    assert "- **form `user`**: Connect" in text
    assert "## FAIL: Retry (`retry`, 0 s)" in text
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
    """A client whose REST answers are canned and recorded."""

    def __init__(self, answers: dict[str, Any] | None = None) -> None:
        self.answers = answers or {}
        self.sent: list[str] = []
        self.translated: list[tuple[str, str]] = []

    def rest(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        del data
        self.sent.append(f"{method} {path}")
        answer = self.answers.get(path)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def ws(self, *messages: dict[str, Any]) -> list[Any]:
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
    assert journeys.parse(["--only", "retry", "reauth"]).only == ["retry", "reauth"]
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
        result = JourneyResult(name, name, [journeys.Check("c", ok=name != "retry")])
        self.results.append(result)
        return result

    stopped: list[str] = []
    monkeypatch.setattr(live_ha, "launch", launch)
    monkeypatch.setattr(live_ha, "state_dir", lambda *_args: tmp_path / "state")
    monkeypatch.setattr(live_ha, "stop", _stop(stopped, tmp_path / "state"))
    monkeypatch.setattr(live_ha, "config_dir", lambda _state: tmp_path / "config")
    monkeypatch.setattr(journeys.Run, "run", run)
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    assert journeys.main(["--only", "first_setup", "retry"]) == 1
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

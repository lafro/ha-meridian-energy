"""Tests for the compatibility workflow's scripts (synthetic versions only)."""

import base64
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts import compat_plan, compat_pytest_plugin, compat_results
from scripts.compat_plan import Kit, Release

HARNESS = compat_plan.HARNESS


def _pypi(kits: dict[str, tuple[str, str]], **extra: Any) -> compat_plan.Fetch:
    """Fake PyPI: harness version -> (pinned Home Assistant, upload time)."""
    releases = {
        version: [{"upload_time_iso_8601": uploaded, "yanked": False}]
        for version, (_, uploaded) in kits.items()
    }
    releases.update(extra.get("releases", {}))
    calls: list[str] = []

    def fetch(path: str) -> Any:
        calls.append(path)
        if path == f"{HARNESS}/json":
            return {"releases": releases}
        if path.startswith("homeassistant/"):
            return {"info": {"requires_python": ">=3.14.2"}}
        version = path.split("/")[1]
        return {
            "info": {
                "requires_dist": [
                    "pytest==9.0.3",
                    f"homeassistant=={kits[version][0]}",
                ],
                "requires_python": ">=3.14",
            }
        }

    fetch.calls = calls  # type: ignore[attr-defined]
    return fetch


BETA_WEEK = {
    "0.13.380": ("2026.11.0b3", "2026-10-31T09:00:00Z"),
    "0.13.379": ("2026.11.0b0", "2026-10-29T09:00:00Z"),
    "0.13.378": ("2026.10.5", "2026-10-27T09:00:00Z"),
    "0.13.377": ("2026.10.4", "2026-10-25T09:00:00Z"),
}
QUIET_WEEK = {
    "0.13.378": ("2026.10.5", "2026-10-27T09:00:00Z"),
    "0.13.377": ("2026.10.4", "2026-10-25T09:00:00Z"),
}
STABLE = Kit("0.13.378", "2026.10.5", "3.14")
BETA = Kit("0.13.380", "2026.11.0b3", "3.14")
MINIMUM = Kit("", "2026.10.0", "3.14")
RELEASE = Release(tag="v9.9.9b1", sha="r" * 40, minimum="2026.10.0")


def test_versions_order_betas_before_their_release() -> None:
    ordered = [
        "2026.10.4",
        "2026.11.0.dev1",
        "2026.11.0b0",
        "2026.11.0b3",
        "2026.11.0",
        "2026.11.1",
    ]
    assert sorted(ordered, key=compat_plan.version_key) == ordered
    assert compat_plan.is_pre_release("2026.11.0b3")
    assert not compat_plan.is_pre_release("2026.11.0")
    with pytest.raises(ValueError, match="Not a Home Assistant version"):
        compat_plan.version_key("latest")


def test_python_is_the_floor_of_the_requirement() -> None:
    assert compat_plan.python_for(">=3.14") == "3.14"
    assert compat_plan.python_for(">=3.15.1,<4") == "3.15"
    with pytest.raises(ValueError, match="No Python floor"):
        compat_plan.python_for(None)


def test_a_beta_harness_gives_a_beta_kit_and_an_older_stable_one() -> None:
    fetch = _pypi(BETA_WEEK)
    assert compat_plan.kits(fetch) == (STABLE, BETA)


def test_without_a_beta_only_the_newest_harness_is_read() -> None:
    fetch = _pypi(QUIET_WEEK)
    assert compat_plan.kits(fetch) == (STABLE, None)
    assert fetch.calls == [f"{HARNESS}/json", f"{HARNESS}/0.13.378/json"]  # type: ignore[attr-defined]


def test_yanked_and_empty_harness_releases_are_passed_over() -> None:
    fetch = _pypi(
        QUIET_WEEK,
        releases={
            "0.13.381": [
                {"upload_time_iso_8601": "2026-11-01T09:00:00Z", "yanked": True}
            ],
            "0.13.382": [],
        },
    )
    assert compat_plan.kits(fetch) == (STABLE, None)


def test_no_harness_for_a_final_release_stops_the_plan() -> None:
    only_betas = {k: v for k, v in BETA_WEEK.items() if "b" in v[0]}
    with pytest.raises(SystemExit, match="final Home Assistant"):
        compat_plan.kits(_pypi(only_betas))


def test_a_harness_that_pins_no_home_assistant_is_an_error() -> None:
    with pytest.raises(ValueError, match="does not pin"):
        compat_plan.pinned_home_assistant(["pytest==9.0.3", "homeassistant>=2026.10"])


def test_targets_in_a_beta_week() -> None:
    entries = compat_plan.targets(
        main_sha="m" * 40,
        main_minimum="2026.10.0",
        stable=STABLE,
        beta=BETA,
        minimum=MINIMUM,
        release=RELEASE,
    )
    assert [(e["target"], e["channel"]) for e in entries] == [
        ("main", "stable"),
        ("main", "beta"),
        ("main", "minimum"),
        ("release", "stable"),
        ("release", "beta"),
    ]
    assert entries[0]["install"] == f"{HARNESS}==0.13.378"
    assert entries[2]["install"] == f"{HARNESS} homeassistant==2026.10.0"
    assert entries[3] == {
        "target": "release",
        "ref": "r" * 40,
        "tag": "v9.9.9b1",
        "channel": "stable",
        "harness": "0.13.378",
        "homeassistant": "2026.10.5",
        "python": "3.14",
        "install": f"{HARNESS}==0.13.378",
    }


def test_targets_without_a_beta_or_a_release() -> None:
    entries = compat_plan.targets(
        main_sha="m" * 40,
        main_minimum="2026.10.0",
        stable=STABLE,
        beta=None,
        minimum=MINIMUM,
        release=None,
    )
    assert [(e["target"], e["channel"]) for e in entries] == [
        ("main", "stable"),
        ("main", "minimum"),
    ]


def test_a_ref_is_not_tested_below_its_own_minimum() -> None:
    entries = compat_plan.targets(
        main_sha="m" * 40,
        main_minimum="2026.11.0",
        stable=STABLE,
        beta=BETA,
        minimum=Kit("", "2026.11.0", "3.14"),
        release=RELEASE,
    )
    assert [(e["target"], e["channel"]) for e in entries] == [
        ("main", "minimum"),
        ("release", "stable"),
        ("release", "beta"),
    ]


def test_the_fingerprint_follows_the_targets() -> None:
    def plan(sha: str) -> list[dict[str, str]]:
        return compat_plan.targets(
            main_sha=sha,
            main_minimum="2026.10.0",
            stable=STABLE,
            beta=None,
            minimum=MINIMUM,
            release=RELEASE,
        )

    assert compat_plan.fingerprint(plan("a" * 40)) == compat_plan.fingerprint(
        plan("a" * 40)
    )
    assert compat_plan.fingerprint(plan("a" * 40)) != compat_plan.fingerprint(
        plan("b" * 40)
    )
    assert len(compat_plan.fingerprint(plan("a" * 40))) == 16


def _github(
    *, releases: list[dict[str, Any]], artifacts: list[dict[str, Any]]
) -> compat_plan.Fetch:
    hacs = base64.b64encode(
        json.dumps({"homeassistant": "2026.10.0"}).encode()
    ).decode()

    def fetch(path: str) -> Any:
        if "/releases?" in path:
            return releases
        if "/commits/" in path:
            assert path.endswith("/commits/v9.9.9b1")
            return {"sha": "r" * 40}
        if "/contents/hacs.json?ref=" in path:
            return {"content": hacs}
        if "/actions/artifacts?name=compat-done-" in path:
            return {"artifacts": artifacts}
        raise AssertionError(path)

    return fetch


def test_the_newest_release_passes_over_drafts() -> None:
    fetch = _github(
        releases=[
            {"draft": True, "tag_name": "v10.0.0"},
            {"draft": False, "tag_name": "v9.9.9b1"},
        ],
        artifacts=[],
    )
    assert compat_plan.newest_release(fetch, "owner/repo") == RELEASE
    assert (
        compat_plan.newest_release(_github(releases=[], artifacts=[]), "owner/repo")
        is None
    )


MAIN_RUN = {"head_branch": "main", "repository_id": 7, "head_repository_id": 7}
FORK_RUN = {"head_branch": "main", "repository_id": 7, "head_repository_id": 8}
BRANCH_RUN = {"head_branch": "fix/x", "repository_id": 7, "head_repository_id": 7}


@pytest.mark.parametrize(
    ("event", "artifacts", "run"),
    [
        ("schedule", [], "true"),
        ("schedule", [{"expired": True, "workflow_run": MAIN_RUN}], "true"),
        ("schedule", [{"expired": False, "workflow_run": MAIN_RUN}], "false"),
        # Only a run on this repository's own main can mark targets tested:
        # a fork's pull request runs its own copy of the workflow here.
        ("schedule", [{"expired": False, "workflow_run": FORK_RUN}], "true"),
        ("schedule", [{"expired": False, "workflow_run": BRANCH_RUN}], "true"),
        ("schedule", [{"expired": False}], "true"),
        (
            "schedule",
            [
                {"expired": False, "workflow_run": FORK_RUN},
                {"expired": False, "workflow_run": MAIN_RUN},
            ],
            "false",
        ),
        ("workflow_dispatch", [{"expired": False, "workflow_run": MAIN_RUN}], "true"),
        ("pull_request", [{"expired": False, "workflow_run": MAIN_RUN}], "true"),
    ],
)
def test_main_writes_the_plan_and_skips_what_was_tested(
    tmp_path: Path, event: str, artifacts: list[dict[str, Any]], run: str
) -> None:
    (tmp_path / "hacs.json").write_text('{"homeassistant": "2026.10.0"}')
    output = tmp_path / "output"
    summary = tmp_path / "summary"
    env = {
        "GITHUB_REPOSITORY": "owner/repo",
        "GITHUB_SHA": "m" * 40,
        "GITHUB_EVENT_NAME": event,
        "GITHUB_OUTPUT": str(output),
        "GITHUB_STEP_SUMMARY": str(summary),
    }
    github = _github(
        releases=[{"draft": False, "tag_name": "v9.9.9b1"}], artifacts=artifacts
    )

    compat_plan.main(env=env, root=tmp_path, pypi=_pypi(BETA_WEEK), github=github)

    lines = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert lines["run"] == run
    assert len(json.loads(lines["matrix"])) == 5
    assert len(lines["fingerprint"]) == 16
    assert (
        "| v9.9.9b1 (`rrrrrrr`) | beta | 2026.11.0b3 | 0.13.380 |"
        in summary.read_text()
    )


def _report(
    when: str, text: str, *, failed: bool = True, nodeid: str = "t.py::a"
) -> Any:
    return SimpleNamespace(failed=failed, nodeid=nodeid, when=when, longreprtext=text)


def test_the_plugin_sorts_failures_into_snapshot_and_broken(tmp_path: Path) -> None:
    recorder = compat_pytest_plugin.Recorder()
    diff = "AssertionError: assert [+ received] == [- snapshot]"
    recorder.add(_report("call", diff, nodeid="t.py::snapshot"))
    recorder.add(_report("call", diff, nodeid="t.py::snapshot"))
    recorder.add(_report("teardown", diff, nodeid="t.py::snapshot"))
    recorder.add(_report("call", "assert 1 == 2", nodeid="t.py::plain"))
    recorder.add(_report("call", "assert 1 == 2", failed=False, nodeid="t.py::ok"))
    recorder.add(SimpleNamespace(failed=True, nodeid="", longreprtext="ImportError"))

    assert recorder.snapshot == ["t.py::snapshot"]
    assert recorder.broken == ["t.py::snapshot", "t.py::plain", "<collection>"]

    path = tmp_path / "record.json"
    recorder.write(path, pytest.ExitCode.TESTS_FAILED)
    assert json.loads(path.read_text()) == {
        "exitstatus": 1,
        "broken": ["t.py::snapshot", "t.py::plain", "<collection>"],
        "snapshot": ["t.py::snapshot"],
    }


def test_the_plugin_writes_only_when_asked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "record.json"
    monkeypatch.setattr(
        compat_pytest_plugin, "RECORDER", compat_pytest_plugin.Recorder()
    )
    monkeypatch.delenv(compat_pytest_plugin.REPORT_ENV, raising=False)
    compat_pytest_plugin.pytest_sessionfinish(0)
    assert not path.exists()

    monkeypatch.setenv(compat_pytest_plugin.REPORT_ENV, str(path))
    compat_pytest_plugin.pytest_runtest_logreport(_report("call", "assert 0"))
    compat_pytest_plugin.pytest_collectreport(
        _report("collect", "ImportError", nodeid="t.py")
    )
    compat_pytest_plugin.pytest_sessionfinish(1)
    assert json.loads(path.read_text())["broken"] == ["t.py::a", "t.py"]


def test_the_plugin_classifies_a_real_syrupy_failure(tmp_path: Path) -> None:
    """Loaded by name as compat.yml loads it, it reads syrupy's real report."""
    module = tmp_path / "test_sample.py"
    record = tmp_path / "record.json"
    env = {
        **os.environ,
        "PYTHONPATH": str(Path("scripts").resolve()),
        compat_pytest_plugin.REPORT_ENV: str(record),
    }

    def run(value: int, *, passes: bool, update: bool = False) -> None:
        module.write_text(
            textwrap.dedent(
                f"""
                def test_snapshot(snapshot):
                    assert {{"value": {value}}} == snapshot

                def test_plain():
                    assert {passes}
                """
            )
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                str(module),
                "-p",
                "compat_pytest_plugin",
                "-p",
                "no:cacheprovider",
                "-o",
                "asyncio_mode=auto",
                "--rootdir",
                str(tmp_path),
                *(["--snapshot-update"] if update else []),
            ],
            cwd=tmp_path,
            env=env,
            check=False,
            capture_output=True,
        )

    run(1, passes=True, update=True)
    assert json.loads(record.read_text()) == {
        "exitstatus": 0,
        "broken": [],
        "snapshot": [],
    }

    run(2, passes=False)
    assert json.loads(record.read_text()) == {
        "exitstatus": 1,
        "broken": ["test_sample.py::test_plain"],
        "snapshot": ["test_sample.py::test_snapshot"],
    }


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (None, "broken"),
        ({"exitstatus": 0, "broken": [], "snapshot": []}, "passed"),
        ({"exitstatus": 1, "broken": [], "snapshot": ["t::s"]}, "snapshot-differences"),
        ({"exitstatus": 1, "broken": ["t::b"], "snapshot": ["t::s"]}, "broken"),
        ({"exitstatus": 1, "broken": [], "snapshot": []}, "broken"),
        ({"exitstatus": 2, "broken": [], "snapshot": []}, "broken"),
        ({"exitstatus": 5, "broken": [], "snapshot": []}, "broken"),
    ],
)
def test_outcomes(record: dict[str, Any] | None, expected: str) -> None:
    assert compat_results.outcome(record) == expected


MATRIX = {
    "target": "main",
    "ref": "m" * 40,
    "tag": "",
    "channel": "beta",
    "harness": "0.13.380",
    "homeassistant": "2026.11.0b3",
    "python": "3.14",
    "install": f"{HARNESS}==0.13.380",
}
MANIFEST = {"domain": "synthetic_domain", "version": "9.9.9b1"}


def _result(**changes: Any) -> dict[str, Any]:
    item = compat_results.result(
        matrix=MATRIX,
        installed="2026.11.0b3 0.13.380",
        manifest=MANIFEST,
        record={"exitstatus": 0, "broken": [], "snapshot": []},
        retry_record=None,
        deprecations=[],
        run_url="https://example.com/run/1",
    )
    return {**item, **changes}


def test_a_result_records_what_was_tested() -> None:
    assert _result() == {
        "schema": 1,
        "integration": "synthetic_domain",
        "integration_version": "9.9.9b1",
        "target": "main",
        "tag": "",
        "ref": "m" * 40,
        "channel": "beta",
        "homeassistant": "2026.11.0b3",
        "harness": "0.13.380",
        "python": "3.14",
        "outcome": "passed",
        "retried": False,
        "broken_tests": [],
        "snapshot_tests": [],
        "deprecations": [],
        "run_url": "https://example.com/run/1",
    }


def test_a_retry_replaces_the_first_record() -> None:
    item = compat_results.result(
        matrix=MATRIX,
        installed="",
        manifest=None,
        record={"exitstatus": 1, "broken": ["t::flaky"], "snapshot": []},
        retry_record={"exitstatus": 0, "broken": [], "snapshot": []},
        deprecations=None,
        run_url="u",
    )
    assert item["outcome"] == "passed"
    assert item["retried"] is True
    assert item["homeassistant"] == "2026.11.0b3"
    assert item["integration_version"] == ""


def test_a_ref_without_the_deprecation_plugin_is_not_checked(tmp_path: Path) -> None:
    out = tmp_path / "result" / "result.json"
    compat_results.main(
        [
            "result",
            "--matrix",
            json.dumps(MATRIX),
            "--installed",
            "2026.11.0b3 0.13.380",
            "--record",
            str(tmp_path / "missing.json"),
            "--deprecations",
            str(tmp_path / "missing.json"),
            "--run-url",
            "u",
            "--out",
            str(out),
        ]
    )
    item = json.loads(out.read_text())
    assert item["deprecations"] is None
    assert item["outcome"] == "broken"
    assert item["broken_tests"] == []


def test_the_outcome_command_reads_a_record(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    record = tmp_path / "record.json"
    record.write_text('{"exitstatus": 1, "broken": [], "snapshot": ["t::s"]}')
    compat_results.main(["outcome", str(record)])
    assert capsys.readouterr().out == "snapshot-differences\n"


def _write(directory: Path, *items: dict[str, Any]) -> Path:
    for index, item in enumerate(items):
        path = directory / f"compat-result-{index}" / "result.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(item))
    return directory


def test_check_needs_a_result_for_every_target(tmp_path: Path) -> None:
    results = _write(tmp_path, _result())
    compat_results.main(["check", str(results), json.dumps([MATRIX])])
    with pytest.raises(SystemExit, match=r"No result for \[\('main', 'stable'\)\]"):
        compat_results.main(
            [
                "check",
                str(results),
                json.dumps([MATRIX, {**MATRIX, "channel": "stable"}]),
            ]
        )


def test_nothing_broken_or_deprecated_reports_nothing() -> None:
    results = [_result(), _result(outcome="snapshot-differences", deprecations=None)]
    assert compat_results.report_broken(results) == ""
    assert compat_results.report_deprecated(results) == ""


def test_the_broken_report_changes_only_with_what_is_broken() -> None:
    broken = _result(outcome="broken", broken_tests=[f"t::{n}" for n in range(7)])
    text = compat_results.report_broken([broken, _result(channel="stable")])
    marker = text.splitlines()[0]

    assert marker.startswith("<!-- compat-state: ")
    assert "| main (`mmmmmmm`) | beta | 2026.11.0b3 | 0.13.380 |" in text
    assert "`t::4` and 2 more" in text
    assert "| stable |" not in text
    same = compat_results.report_broken(
        [{**broken, "ref": "n" * 40, "broken_tests": ["t::other"]}]
    )
    assert same.splitlines()[0] == marker
    newer = compat_results.report_broken([{**broken, "homeassistant": "2026.11.0b4"}])
    assert newer.splitlines()[0] != marker
    crashed = compat_results.report_broken([{**broken, "broken_tests": []}])
    assert "| the run |" in crashed


def test_the_deprecation_report_lists_each_message_once() -> None:
    message = "Detected that custom integration 'synthetic_domain' calls X.\nPlease fix"
    text = compat_results.report_deprecated(
        [
            _result(deprecations=[message]),
            _result(
                target="release",
                tag="v9.9.9b1",
                deprecations=[message],
                homeassistant="2026.10.5",
            ),
            _result(deprecations=None),
        ]
    )
    assert (
        text.count(
            "- Detected that custom integration 'synthetic_domain' calls X. Please fix"
        )
        == 1
    )
    assert "Seen on: main on 2026.11.0b3, v9.9.9b1 on 2026.10.5." in text
    assert "Run: https://example.com/run/1" in text


def test_the_summary_lists_every_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    results = _write(
        tmp_path,
        _result(),
        _result(channel="minimum", deprecations=None, retried=True, harness=""),
    )
    compat_results.main(["report", "--kind", "summary", str(results)])
    text = capsys.readouterr().out
    assert "| main (`mmmmmmm`) | beta | 2026.11.0b3 | 0.13.380 | passed | 0 |" in text
    assert "| minimum | 2026.11.0b3 | - | passed (retried) | not checked |" in text

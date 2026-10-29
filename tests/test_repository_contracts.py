"""Tests for security-critical repository and release contracts."""

import json
import re
import tomllib
from importlib.metadata import requires
from pathlib import Path
from typing import Any

import pytest
import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

from scripts import check_versions


def test_release_workflow_publishes_only_the_verified_main_commit() -> None:
    """Prevent a pre-existing release tag from selecting another commit."""
    workflow = yaml.safe_load(Path(".github/workflows/release.yml").read_text())
    release_steps = workflow["jobs"]["release"]["steps"]
    release_script = "\n".join(
        str(step.get("run", "")) for step in release_steps if isinstance(step, dict)
    )

    assert "git/ref/tags/$RELEASE_TAG" in release_script
    assert "git/refs" in release_script
    assert '"$GITHUB_SHA"' in release_script
    assert "--verify-tag" in release_script
    assert "--target" not in release_script


def test_release_validators_use_isolated_workspaces() -> None:
    """Keep Hassfest and HACS away from the Python job's populated virtualenv."""
    workflow = yaml.safe_load(Path(".github/workflows/release.yml").read_text())
    jobs = workflow["jobs"]

    assert {"python", "hassfest", "hacs", "release"} <= jobs.keys()
    assert jobs["release"]["needs"] == ["python", "hassfest", "hacs"]
    assert jobs["release"]["environment"] == "release"
    for job_name in ("hassfest", "hacs"):
        uses = [
            step.get("uses", "")
            for step in jobs[job_name]["steps"]
            if isinstance(step, dict)
        ]
        assert any("actions/checkout@" in action for action in uses)
        assert not any("setup-uv@" in action for action in uses)


def test_dependency_updates_keep_the_lock_file_authoritative() -> None:
    """Dependabot must update uv.lock, and CI must refuse a stale lock."""
    config = yaml.safe_load(Path(".github/dependabot.yml").read_text())
    assert {update["package-ecosystem"] for update in config["updates"]} == {
        "github-actions",
        "uv",
    }
    for name in ("validate.yml", "release.yml"):
        workflow = Path(".github/workflows", name).read_text()
        assert "uv sync --all-groups --locked" in workflow
        assert "--frozen" not in workflow

    project = tomllib.loads(Path("pyproject.toml").read_text())
    assert "<" not in project["tool"]["uv"]["required-version"]


def test_pull_requests_are_tested_on_a_supported_release() -> None:
    """The locked Home Assistant is a final release, no older than the minimum.

    The harness pin selects the Home Assistant that every pull request is
    tested on. Betas are compat.yml's job, and a release older than the
    minimum in hacs.json is one this integration does not support: the
    harness 0.13.370 locked 2026.10.0b4, below the minimum 2026.10.0, and
    nothing said so. The locked version is read rather than the installed one,
    which compat.yml replaces.
    """
    lock = tomllib.loads(Path("uv.lock").read_text())
    versions = [
        Version(package["version"])
        for package in lock["package"]
        if package["name"] == "homeassistant"
    ]
    assert len(versions) == 1, f"uv.lock has {len(versions)} homeassistant entries"
    (locked,) = versions
    minimum = Version(json.loads(Path("hacs.json").read_text())["homeassistant"])

    assert not locked.is_prerelease, f"{locked} is a pre-release"
    assert locked >= minimum, f"{locked} is older than hacs.json's {minimum}"


def test_dependabot_leaves_the_harness_pins_to_the_harness() -> None:
    """Dependabot ignores exactly the dev dependencies the harness pins with ==.

    An update to one of them on its own cannot resolve, so Dependabot skips it
    and records an error that fails the uv job (pytest 9.1.1 did, against the
    harness's ``pytest==9.0.3``). Those packages move when the harness pin is
    bumped instead. The list must not cover anything else, because ``ignore``
    also stops Dependabot's security pull requests for a package. Each entry
    must name the package only: one narrowed with ``versions`` or
    ``update-types`` lets Dependabot try the package's other updates, which
    cannot resolve either.
    """
    _check_harness_ignore_list(_dependabot_uv_ignore())


@pytest.mark.parametrize("name", [None, "", 123, True, ["pytest"]])
def test_dependabot_contract_rejects_a_dependency_name_that_is_not_a_name(
    name: object,
) -> None:
    """An empty or non-string dependency-name fails the assertion, not an error."""
    entry = {"dependency-name": name}
    with pytest.raises(
        AssertionError, match="not just a package name: .*" + re.escape(repr(entry))
    ):
        _check_harness_ignore_list([*_dependabot_uv_ignore(), entry])


def _dependabot_uv_ignore() -> list[dict[str, Any]]:
    config = yaml.safe_load(Path(".github/dependabot.yml").read_text())
    uv_updates = next(
        update for update in config["updates"] if update["package-ecosystem"] == "uv"
    )
    return uv_updates.get("ignore", [])


def _check_harness_ignore_list(ignore: list[dict[str, Any]]) -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text())
    direct = {
        canonicalize_name(Requirement(requirement).name)
        for requirement in project["dependency-groups"]["dev"]
    }
    harness_pins = {
        canonicalize_name(requirement.name)
        for requirement in map(
            Requirement, requires("pytest-homeassistant-custom-component") or []
        )
        if any(spec.operator == "==" for spec in requirement.specifier)
    }
    # An entry whose dependency-name is missing, empty or not a string shows
    # as its repr, so this assertion reports it rather than a KeyError, a
    # TypeError from sorting or an error from canonicalize_name.
    narrowed = sorted(
        _ignore_entry_label(item)
        for item in ignore
        if item.keys() != {"dependency-name"} or not _is_package_name(item)
    )
    assert not narrowed, f"not just a package name: {narrowed}"
    ignored = {canonicalize_name(item["dependency-name"]) for item in ignore}

    # Equality catches a missing entry, a misspelt one, one the harness does
    # not pin and one the harness has stopped pinning exactly.
    expected = direct & harness_pins
    assert ignored == expected, (
        f"missing: {sorted(expected - ignored)}; "
        f"not a direct dev dependency pinned with == by the harness: "
        f"{sorted(ignored - expected)}"
    )


def _is_package_name(item: dict[str, Any]) -> bool:
    name = item.get("dependency-name")
    return isinstance(name, str) and bool(name)


def _ignore_entry_label(item: dict[str, Any]) -> str:
    return item["dependency-name"] if _is_package_name(item) else repr(item)


def test_dependabot_labels_are_explicit() -> None:
    """Without labels, Dependabot creates and applies github_actions."""
    config = yaml.safe_load(Path(".github/dependabot.yml").read_text())
    labels = {
        update["package-ecosystem"]: update["labels"] for update in config["updates"]
    }
    assert labels == {
        "github-actions": ["dependencies", "github-actions"],
        "uv": ["dependencies", "python"],
    }


def _compat_workflow() -> dict[Any, Any]:
    return yaml.safe_load(Path(".github/workflows/compat.yml").read_text())


def _commands(script: str) -> str:
    """Join continuation lines and whitespace so each command matches whole."""
    return " ".join(script.replace("\\\n", " ").split())


def test_compat_runs_when_home_assistant_or_the_integration_changes() -> None:
    """The plan polls; the tests run only for targets not already tested.

    A scheduled run whose targets (each ref with its harness and Home
    Assistant) carry an earlier run's done marker stops after the plan, so
    polling every 6 hours costs a few seconds until a beta, a release, a patch,
    a harness or a commit to main changes them.
    """
    workflow = _compat_workflow()
    # PyYAML reads the key "on" as True.
    triggers = workflow[True]
    assert triggers["schedule"] == [{"cron": "17 */6 * * *"}]
    assert "workflow_dispatch" in triggers
    assert set(triggers["pull_request"]["paths"]) == {
        ".github/workflows/compat.yml",
        "scripts/compat_*.py",
    }
    assert workflow["permissions"] == {"contents": "read"}

    plan = workflow["jobs"]["plan"]
    assert plan["permissions"] == {"contents": "read", "actions": "read"}
    assert "scripts/compat_plan.py" in plan["steps"][-1]["run"]

    tests = workflow["jobs"]["tests"]
    assert tests["if"] == "needs.plan.outputs.run == 'true'"
    assert tests["strategy"]["fail-fast"] is False
    assert "permissions" not in tests
    steps = {step.get("name"): step for step in tests["steps"]}
    install = steps["Install the harness, unpinned from uv.lock"]
    assert install["env"]["INSTALL"] == "${{ matrix.install }}"
    assert "uv venv --no-config" in install["run"]
    assert "uv pip install --no-config" in install["run"]
    run = _commands(steps["Run the test suite"]["run"])
    # The contract tests check the pinned harness on purpose, so they are left
    # out; coverage gates belong to validate.yml.
    assert (
        "args=(-p compat_pytest_plugin --no-cov --snapshot-warn-unused "
        "--ignore=tests/test_repository_contracts.py)"
    ) in run
    assert "--last-failed" in run
    record = steps["Record the result"]["run"]
    assert "--manifest custom_components/meridian_energy/manifest.json" in record


def test_compat_failures_open_a_labelled_tracking_issue() -> None:
    workflow = _compat_workflow()
    report = workflow["jobs"]["report"]
    assert report["if"] == "always() && needs.plan.outputs.run == 'true'"
    assert report["permissions"] == {"actions": "read", "issues": "write"}

    steps = {step.get("name"): step for step in report["steps"]}
    sync = steps["Open, update or close the tracking issues"]
    # Pull requests that change the workflow run it, but never touch issues
    # or mark targets tested.
    assert sync["if"] == "github.event_name != 'pull_request'"
    assert steps["Mark these targets tested"]["if"] == sync["if"]
    upload = next(step for step in report["steps"] if "compat-done-" in str(step))
    assert upload["if"] == sync["if"]

    script = sync["run"]
    commands = _commands(script)
    # --force, not "|| true": only "already exists" is harmless.
    assert (
        'gh label create "$label" --force --color "$color" '
        '--description "$description" '
        'gh issue create --title "$title" --body-file "$text" --label "$label"'
    ) in commands
    assert "|| true" not in script
    # Anyone can open an issue with the title, so the tracking issue is found
    # by its label and by the bot that opened it, never by title alone.
    assert (
        "issues?state=open&labels=${label}&creator=github-actions%5Bbot%5D"
    ) in script
    assert "--search" not in script
    # The colour and description of compat are the live label's.
    assert (
        'sync compat e99695 "Compatibility check failing against the newest '
        'Home Assistant" "Compatibility check failing (opened by compat.yml)"'
    ) in commands
    assert 'sync deprecation fbca04 "Home Assistant reports a deprecation' in commands


def test_dependabot_moves_the_harness_after_the_month_s_release() -> None:
    """The uv updates run once a month, while the harness pins a release.

    The harness pins the newest Home Assistant, betas included. From about the
    23rd until the day after the release, by the 8th, that is a beta, which
    test_pull_requests_are_tested_on_a_supported_release refuses (the harness
    locked 2026.10.0b4 on 1 October). Any day from the 9th to the 21st avoids
    that window; compat.yml tests the patches between.
    """
    config = yaml.safe_load(Path(".github/dependabot.yml").read_text())
    uv_updates = next(
        update for update in config["updates"] if update["package-ecosystem"] == "uv"
    )
    schedule = uv_updates["schedule"]
    assert schedule.keys() == {"interval", "cronjob"}
    assert schedule["interval"] == "cron"
    minute, hour, day, month, weekday = schedule["cronjob"].split()
    assert (month, weekday) == ("*", "*")
    assert day.isdigit(), f"one day of the month, not {day!r}"
    assert 9 <= int(day) <= 21
    assert minute.isdigit()
    assert hour.isdigit()


def test_workflow_actions_are_pinned_to_commit_shas() -> None:
    pinned = re.compile(r"^[\w.-]+/[\w./-]+@[0-9a-f]{40}$")
    for path in Path(".github/workflows").glob("*.yml"):
        workflow = yaml.safe_load(path.read_text())
        for job in workflow["jobs"].values():
            for step in job.get("steps", []):
                if "uses" in step:
                    assert pinned.match(step["uses"]), (path.name, step["uses"])


def test_every_checkout_drops_the_job_token() -> None:
    """No later step, including unpinned compat installs, can reuse the token."""
    for path in Path(".github/workflows").glob("*.yml"):
        workflow = yaml.safe_load(path.read_text())
        for job in workflow["jobs"].values():
            for step in job.get("steps", []):
                if "actions/checkout@" in step.get("uses", ""):
                    assert step.get("with", {}).get("persist-credentials") is False, (
                        path.name
                    )


def test_release_gates_skip_the_cache_and_publish_changelog_notes() -> None:
    workflow = yaml.safe_load(Path(".github/workflows/release.yml").read_text())
    python_job = workflow["jobs"]["python"]
    setup_uv = next(
        step for step in python_job["steps"] if "setup-uv@" in step.get("uses", "")
    )
    assert setup_uv["with"]["enable-cache"] is False

    versions = next(
        step for step in python_job["steps"] if step.get("id") == "versions"
    )
    assert "check_versions.py" in versions["run"]
    assert "--release-notes" in versions["run"]
    assert python_job["outputs"]["notes"] == "${{ steps.versions.outputs.notes }}"

    publish = workflow["jobs"]["release"]["steps"][-1]
    assert publish["env"]["RELEASE_NOTES"] == "${{ needs.python.outputs.notes }}"
    assert "--notes-file" in publish["run"]


def test_check_versions_requires_a_changelog_section(tmp_path: Path) -> None:
    root = tmp_path
    (root / "custom_components/meridian_energy").mkdir(parents=True)
    (root / "custom_components/meridian_energy/manifest.json").write_text(
        '{"version": "9.8.7"}'
    )
    (root / "pyproject.toml").write_text('[project]\nversion = "9.8.7"\n')
    changelog = root / "CHANGELOG.md"

    changelog.write_text("# Changelog\n\n## 9.8.6\n\n- Older.\n")
    with pytest.raises(SystemExit, match=r"no '## 9\.8\.7' section"):
        check_versions.main([], root)

    changelog.write_text("# Changelog\n\n## 9.8.7\n\n## 9.8.6\n\n- Older.\n")
    with pytest.raises(SystemExit, match="is empty"):
        check_versions.main([], root)

    changelog.write_text(
        "# Changelog\n\n## 9.8.7\n\n### Fixed\n\n- New.\n\n## 9.8.6\n\n- Older.\n"
    )
    notes = tmp_path / "notes.md"
    check_versions.main(["v9.8.7", "--release-notes", str(notes)], root)
    assert notes.read_text() == "### Fixed\n\n- New.\n"

    with pytest.raises(SystemExit, match="Tag mismatch"):
        check_versions.main(["v9.8.8"], root)
    with pytest.raises(SystemExit, match="needs a path"):
        check_versions.main(["--release-notes"], root)


def test_check_versions_reads_a_beta_from_its_release_section(tmp_path: Path) -> None:
    root = tmp_path
    (root / "custom_components/meridian_energy").mkdir(parents=True)
    (root / "custom_components/meridian_energy/manifest.json").write_text(
        '{"version": "9.8.7b2"}'
    )
    (root / "pyproject.toml").write_text('[project]\nversion = "9.8.7b2"\n')
    (root / "CHANGELOG.md").write_text(
        "# Changelog\n\n## 9.8.7\n\n### Fixed\n\n- New.\n\n## 9.8.6\n\n- Older.\n"
    )
    notes = tmp_path / "notes.md"

    check_versions.main(["v9.8.7b2", "--release-notes", str(notes)], root)

    text = notes.read_text()
    assert text.startswith("Beta of 9.8.7 for testing.")
    assert text.endswith("### Fixed\n\n- New.\n")
    with pytest.raises(SystemExit, match="Tag mismatch"):
        check_versions.main(["v9.8.7"], root)
    with pytest.raises(SystemExit, match="Tag mismatch"):
        check_versions.main(["v9.8.7b1"], root)
    assert check_versions.changelog_version("9.8.7rc1") == "9.8.7rc1"
    # Digits as release.yml reads them, [0-9]: Python's \d takes other scripts' too.
    assert check_versions.changelog_version("9.8.7b٢") == "9.8.7b٢"


def test_release_workflow_marks_only_beta_tags_as_pre_releases() -> None:
    workflow = yaml.safe_load(Path(".github/workflows/release.yml").read_text())
    tag_pattern = r"^v[0-9]+\.[0-9]+\.[0-9]+(b[0-9]+)?$"
    versions = next(
        step
        for step in workflow["jobs"]["python"]["steps"]
        if step.get("id") == "versions"
    )
    publish = workflow["jobs"]["release"]["steps"]
    create_tag, release = publish[0], publish[-1]

    assert tag_pattern in versions["run"]
    assert tag_pattern in create_tag["run"]
    assert "=~ b[0-9]+$ ]]" in release["run"]
    assert "--prerelease --latest=false" in release["run"]
    for tag in ("v0.3.0", "v0.3.0b1", "v10.20.30b4"):
        assert re.fullmatch(tag_pattern.strip("^$"), tag)
    for tag in ("v0.3.0rc1", "v0.3.0-b1", "0.3.0", "v0.3"):
        assert not re.fullmatch(tag_pattern.strip("^$"), tag)


def test_repository_versions_and_changelog_agree(tmp_path: Path) -> None:
    notes = tmp_path / "notes.md"
    check_versions.main(["--release-notes=" + str(notes)])
    project = tomllib.loads(Path("pyproject.toml").read_text())
    version = project["project"]["version"]
    section = check_versions.changelog_section(
        check_versions.changelog_version(version), Path("CHANGELOG.md").read_text()
    )
    assert notes.read_text().endswith(section)
    assert section.strip()

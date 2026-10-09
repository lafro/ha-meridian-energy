"""Tests for security-critical repository and release contracts."""

import re
import tomllib
from importlib.metadata import requires
from pathlib import Path

import pytest
import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

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
    config = yaml.safe_load(Path(".github/dependabot.yml").read_text())
    uv_updates = next(
        update for update in config["updates"] if update["package-ecosystem"] == "uv"
    )
    ignore = uv_updates.get("ignore", [])
    # An entry with no dependency-name shows as its repr, so this assertion
    # reports it rather than a KeyError.
    narrowed = sorted(
        item.get("dependency-name", repr(item))
        for item in ignore
        if item.keys() != {"dependency-name"}
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


def test_compat_failures_open_a_labelled_tracking_issue() -> None:
    workflow = yaml.safe_load(Path(".github/workflows/compat.yml").read_text())
    report = workflow["jobs"]["report"]
    assert report["if"] == "failure() && github.event_name == 'schedule'"
    assert report["permissions"] == {"issues": "write"}
    assert workflow["permissions"] == {"contents": "read"}
    script = report["steps"][0]["run"]
    # Join continuation lines and whitespace so each command matches whole.
    commands = " ".join(script.replace("\\\n", " ").split())
    # --force, not "|| true": only "already exists" is harmless. The colour and
    # description are the live label's.
    assert (
        "gh label create compat --force --color e99695 "
        '--description "Weekly HA compatibility check failed (opened by compat.yml)" '
        "gh issue create"
    ) in commands
    assert "|| true" not in script
    assert 'gh issue create --title "$title" --body "$body" --label compat' in script


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


def test_repository_versions_and_changelog_agree(tmp_path: Path) -> None:
    notes = tmp_path / "notes.md"
    check_versions.main(["--release-notes=" + str(notes)])
    project = tomllib.loads(Path("pyproject.toml").read_text())
    section = check_versions.changelog_section(
        project["project"]["version"], Path("CHANGELOG.md").read_text()
    )
    assert notes.read_text() == section
    assert section.strip()

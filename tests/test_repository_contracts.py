"""Tests for security-critical repository and release contracts."""

import re
import tomllib
from pathlib import Path

import pytest
import yaml

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

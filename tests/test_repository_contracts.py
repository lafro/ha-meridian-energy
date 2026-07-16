"""Tests for security-critical repository and release contracts."""

from pathlib import Path

import yaml


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

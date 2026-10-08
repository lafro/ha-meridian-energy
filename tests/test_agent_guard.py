"""Tests for the agent guard registered in .claude/settings.json."""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
GUARD_SH = ROOT / ".claude/hooks/guard.sh"
GUARD_PY = ROOT / ".claude/hooks/guard.py"
SETTINGS = ROOT / ".claude/settings.json"
# Synthetic connector IDs: real connector IDs never belong in this public repo.
HA_TOOLS = (
    "mcp__claude_ai_Home_Assistant__ha_restart",
    "mcp__00000000-0000-0000-0000-000000000000__ha_call_service",
    "mcp__ha_mcp__ha_get_state",
)


def _git_env() -> dict[str, str]:
    """Ignore the machine's own git config so results do not depend on it."""
    return {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}


def _make_repo(path: Path, spec: dict[str, Any]) -> None:
    """Create a throwaway repository shaped by a test case's ``repo`` block."""
    branch = spec.get("branch", "main")

    def run(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(path), *args],
            check=True,
            capture_output=True,
            env=_git_env(),
        )

    run("init", "-q", "-b", branch)
    run(
        "-c",
        "user.name=guard-test",
        "-c",
        "user.email=guard-test@example.com",
        "commit",
        "-q",
        "--allow-empty",
        "-m",
        "init",
    )
    run("remote", "add", "origin", "https://example.invalid/repo.git")
    if upstream := spec.get("upstream"):
        run("update-ref", f"refs/remotes/origin/{upstream}", "HEAD")
        run("config", f"branch.{branch}.remote", "origin")
        run("config", f"branch.{branch}.merge", f"refs/heads/{upstream}")
    if tag := spec.get("tag"):
        run("tag", tag)
    for key, value in spec.get("config", {}).items():
        run("config", key, value)


def _run_guard(
    payload: str, cwd: Path, command: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command or ["sh", str(GUARD_SH)],
        input=payload,
        capture_output=True,
        text=True,
        cwd=cwd,
        env=_git_env(),
        timeout=30,
        check=False,
    )


def _bash_event(command: str, cwd: Path) -> str:
    return json.dumps(
        {
            "session_id": "test",
            "hook_event_name": "PreToolUse",
            "cwd": str(cwd),
            "tool_name": "Bash",
            "tool_input": {"command": command},
        }
    )


ALLOWED = [
    ("git status", None),
    ("git push -u origin fix/v0-2-6", {"branch": "fix/v0-2-6"}),
    ("git push origin chore/tidy", {"branch": "chore/tidy"}),
    ("git push", {"branch": "fix/thing", "upstream": "fix/thing"}),
    ("git tag", None),
    ("git tag -l 'v0.*'", None),
    ("git tag --contains HEAD", None),
    ("git tag --sort=-v:refname", None),
    ("gh pr create --fill", None),
    ("gh pr checks 38 --watch", None),
    ("gh release view v0.2.5", None),
    ("gh release list", None),
    ("gh workflow run compat.yml", None),
    ("gh workflow view release.yml", None),
    ("gh api repos/owner/repo/pulls/38", None),
    ("gh api repos/owner/repo/git/ref/tags/v0.2.5", None),
    ("gh api -X PATCH repos/owner/repo/issues/5 -f state=closed", None),
    ("curl -s https://api.github.com/repos/owner/repo/releases", None),
    ("uv run pytest && git push origin fix/thing", {"branch": "fix/thing"}),
]

BLOCKED = [
    # Merges, in any spelling.
    ("gh pr merge 38 --squash", None),
    ("gh -R owner/repo pr merge 38", None),
    ("timeout 60 gh pr merge 38", None),
    ("echo $(gh pr merge 38)", None),
    ("gh api -X PUT repos/owner/repo/pulls/38/merge", None),
    ("gh api repos/owner/repo/pulls/38/merge -X PUT -f merge_method=squash", None),
    (
        (
            "curl -X PUT -H 'Accept: application/json' "
            "https://api.github.com/repos/owner/repo/pulls/1/merge"
        ),
        None,
    ),
    (
        (
            "gh api graphql -f query='mutation { mergePullRequest(input: "
            '{pullRequestId: "x"}) { clientMutationId } }\''
        ),
        None,
    ),
    # Refs, tags and releases through the API.
    ("gh api repos/owner/repo/git/refs -f ref=refs/tags/v0.2.6 -f sha=abc", None),
    ("gh api --method PATCH repos/owner/repo/git/refs/heads/x -F force=true", None),
    ("gh api repos/owner/repo/git/tags -f tag=v1", None),
    ("gh api repos/owner/repo/releases -f tag_name=v0.2.6", None),
    ("gh api -X POST repos/owner/repo/actions/workflows/release.yml/dispatches", None),
    ("gh api -X PUT repos/owner/repo/rulesets/1 --input ruleset.json", None),
    ("gh api -X PUT repos/owner/repo/contents/README.md -f message=x", None),
    ("gh api -X DELETE repos/owner/repo", None),
    # Releases and the Release workflow through gh.
    ("gh release create v0.2.6 --generate-notes", None),
    ("gh release upload v0.2.6 notes.txt", None),
    ("gh release delete v0.2.5 --yes", None),
    ("gh workflow run release.yml -f tag=v0.2.6", None),
    ("gh workflow run Release --ref main", None),
    ("gh repo delete owner/repo --yes", None),
    # Pushes to main.
    ("git push origin main", None),
    ("git push origin HEAD:main", {"branch": "fix/thing"}),
    ("git -C . push origin HEAD:refs/heads/main", {"branch": "fix/thing"}),
    ("git push", {"branch": "main", "upstream": "main"}),
    (
        "git push",
        {
            "branch": "fix/thing",
            "upstream": "main",
            "config": {"push.default": "upstream"},
        },
    ),
    ("git push", {"branch": "fix/thing", "config": {"push.default": "matching"}}),
    ("git push --all origin", {"branch": "fix/thing"}),
    ("cd . && git push origin main", None),
    ("bash -c 'git push origin main'", None),
    ("env GIT_TRACE=1 git push origin main", None),
    # Force pushes and remote-ref deletion.
    ("git push --force origin fix/thing", {"branch": "fix/thing"}),
    ("git push -uf origin fix/thing", {"branch": "fix/thing"}),
    ("git push --force-with-lease origin fix/thing", {"branch": "fix/thing"}),
    ("git push origin --delete fix/thing", None),
    ("git push origin :fix/thing", None),
    # Tags.
    ("git push origin v0.2.6", None),
    ("git push origin refs/tags/v0.2.6", None),
    ("git push origin HEAD:refs/tags/v9.9.9", {"branch": "fix/thing"}),
    ("git push --tags", {"branch": "fix/thing"}),
    ("git push --follow-tags origin fix/thing", {"branch": "fix/thing"}),
    ("git push origin candidate", {"branch": "fix/thing", "tag": "candidate"}),
    (
        "git push",
        {
            "branch": "fix/thing",
            "upstream": "fix/thing",
            "config": {"push.followTags": "true"},
        },
    ),
    ("git tag v0.2.6", None),
    ("git tag -a v0.2.6 -m release", None),
    ("git tag -d v0.2.5", None),
    ("git tag -f v0.2.5 HEAD", None),
]


@pytest.mark.parametrize(("command", "repo"), ALLOWED)
def test_guard_allows_ordinary_commands(
    tmp_path: Path, command: str, repo: dict[str, Any] | None
) -> None:
    if repo is not None:
        _make_repo(tmp_path, repo)
    result = _run_guard(_bash_event(command, tmp_path), tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


@pytest.mark.parametrize(("command", "repo"), BLOCKED)
def test_guard_blocks_merges_tags_releases_and_main(
    tmp_path: Path, command: str, repo: dict[str, Any] | None
) -> None:
    if repo is not None:
        _make_repo(tmp_path, repo)
    result = _run_guard(_bash_event(command, tmp_path), tmp_path)
    assert result.returncode == 2, command
    assert "Blocked by this repository's agent guard" in result.stderr


@pytest.mark.parametrize("tool", HA_TOOLS)
def test_guard_blocks_home_assistant_tools(tmp_path: Path, tool: str) -> None:
    payload = json.dumps({"tool_name": tool, "tool_input": {}, "cwd": str(tmp_path)})
    result = _run_guard(payload, tmp_path)
    assert result.returncode == 2
    assert "Home Assistant tools are not used" in result.stderr


def test_guard_fails_closed_on_unreadable_input(tmp_path: Path) -> None:
    risky = _run_guard('{"tool_name": "Bash", git push origin main', tmp_path)
    harmless = _run_guard("not json at all", tmp_path)
    assert risky.returncode == 2
    assert harmless.returncode == 0


def test_guard_falls_back_to_a_text_check_without_python(tmp_path: Path) -> None:
    """Without python3 on PATH the shell entry point still blocks risky calls."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("cat", "dirname", "grep", "printf"):
        found = next(
            (
                Path(entry, tool)
                for entry in os.environ["PATH"].split(os.pathsep)
                if Path(entry, tool).is_file()
            ),
            None,
        )
        if found is not None:
            (bin_dir / tool).symlink_to(found)
    env = {**_git_env(), "PATH": str(bin_dir)}

    def run(command: str) -> int:
        return subprocess.run(
            ["/bin/sh", str(GUARD_SH)],
            input=_bash_event(command, tmp_path),
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        ).returncode

    assert run("gh pr merge 38") == 2
    assert run("ls -la") == 0


def test_guard_runs_on_the_current_interpreter_directly(tmp_path: Path) -> None:
    result = _run_guard(
        _bash_event("gh pr merge 38", tmp_path),
        tmp_path,
        [sys.executable, "-I", str(GUARD_PY)],
    )
    assert result.returncode == 2


def test_guard_stays_compatible_with_older_python() -> None:
    """Agent sessions may have an older python3 than the project's 3.14."""
    ast.parse(GUARD_PY.read_text(), feature_version=(3, 9))


def test_settings_register_both_hooks_and_keep_connector_ids_out() -> None:
    text = SETTINGS.read_text()
    settings = json.loads(text)
    entries = settings["hooks"]["PreToolUse"]
    by_matcher = {entry["matcher"]: entry["hooks"] for entry in entries}

    ha_matcher = next(matcher for matcher in by_matcher if "__ha_" in matcher)
    for tool in HA_TOOLS:
        assert re.search(ha_matcher, tool), tool
    for tool in ("Bash", "Read", "mcp__github__create_pull_request"):
        assert not re.search(ha_matcher, tool), tool
    ha_hook = by_matcher[ha_matcher][0]
    result = subprocess.run(
        ["sh", "-c", ha_hook["command"]],
        input="{}",
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 2
    assert "Home Assistant" in result.stderr

    assert by_matcher["Bash"][0]["command"] == (
        'sh "${CLAUDE_PROJECT_DIR}/.claude/hooks/guard.sh"'
    )
    assert re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-", text) is None
    deny = settings["permissions"]["deny"]
    assert "Bash(gh pr merge *)" in deny
    assert "Bash(gh api *pulls/*/merge*)" in deny
    assert "Bash(git push * refs/tags/*)" in deny

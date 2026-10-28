"""
Plan a compatibility run: what to test, and whether it has been tested already.

Usage (in ``compat.yml``): ``compat_plan.py``, with ``GITHUB_REPOSITORY``,
``GITHUB_SHA``, ``GITHUB_EVENT_NAME``, ``GITHUB_OUTPUT`` and ``GH_TOKEN`` set.

- **Kits.** The test harness, ``pytest-homeassistant-custom-component``, pins
  one Home Assistant version per release and follows every Home Assistant
  release and most betas, about a day later. *stable* is the newest harness
  for a final release; *beta* is the newest harness when that one pins a beta.
- **Targets.** ``main`` and the newest published release (pre-releases
  included), each on *stable* and, in a beta week, *beta*; and ``main`` on the
  *minimum*: the Home Assistant version in ``hacs.json``, whose harness the
  install resolves. A ref is not tested on a kit older than its own minimum.
- **Fingerprint.** A hash of the targets. A scheduled run that finds the
  artifact ``compat-done-<fingerprint>`` stops: those exact targets were
  tested, so nothing is run until Home Assistant, the harness or the
  integration changes.

Writes ``matrix`` (JSON), ``fingerprint`` and ``run`` to ``GITHUB_OUTPUT``.
Only the standard library is used, so any Python 3.14 runs it.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

HARNESS = "pytest-homeassistant-custom-component"
PYPI = "https://pypi.org/pypi/"
GITHUB_API = "https://api.github.com/"
DONE_PREFIX = "compat-done-"
# Harness releases looked at for one that pins a final release: a beta week
# brings at most about five.
LOOKBACK = 20
_PRE_RELEASE = re.compile(r"(?:a|b|rc|\.dev)\d+$")
_VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)(?:(a|b|rc)(\d+))?(?:\.dev(\d+))?$")
_PYTHON_FLOOR = re.compile(r">=\s*(\d+)\.(\d+)")
_PHASES = {"a": 0, "b": 1, "rc": 2, None: 3}

Fetch = Callable[[str], Any]


@dataclass(frozen=True)
class Kit:
    """A harness release, the Home Assistant it pins and the Python it needs."""

    harness: str
    homeassistant: str
    python: str


@dataclass(frozen=True)
class Release:
    """The newest published release of the integration."""

    tag: str
    sha: str
    minimum: str


def is_pre_release(version: str) -> bool:
    """Return whether a Home Assistant version is an alpha, beta, rc or dev."""
    return bool(_PRE_RELEASE.search(version))


def version_key(version: str) -> tuple[int, ...]:
    """Order Home Assistant versions; a beta sorts before its release."""
    match = _VERSION.match(version)
    if not match:
        msg = f"Not a Home Assistant version: {version!r}"
        raise ValueError(msg)
    year, month, patch, phase, number, dev = match.groups()
    # A bare .devN comes before every alpha, beta and rc of its release.
    phase_key = _PHASES[phase] if phase or not dev else -1
    dev_key = (0, int(dev)) if dev else (1, 0)
    return (int(year), int(month), int(patch), phase_key, int(number or 0), *dev_key)


def python_for(requires_python: str | None) -> str:
    """Return the oldest Python ``major.minor`` a requirement allows."""
    match = _PYTHON_FLOOR.search(requires_python or "")
    if not match:
        msg = f"No Python floor in {requires_python!r}"
        raise ValueError(msg)
    return f"{match.group(1)}.{match.group(2)}"


def pinned_home_assistant(requires_dist: list[str]) -> str:
    """Return the Home Assistant version a harness release pins with ``==``."""
    for requirement in requires_dist:
        match = re.fullmatch(r"homeassistant\s*==\s*(\S+)", requirement.strip())
        if match:
            return match.group(1)
    msg = "The harness does not pin homeassistant"
    raise ValueError(msg)


def kits(fetch_pypi: Fetch) -> tuple[Kit, Kit | None]:
    """Return the *stable* kit, and the *beta* kit while a beta is newest."""
    releases = fetch_pypi(f"{HARNESS}/json")["releases"]
    published = sorted(
        (
            (files[0]["upload_time_iso_8601"], version)
            for version, files in releases.items()
            if files and not all(file.get("yanked") for file in files)
        ),
        reverse=True,
    )
    beta = None
    for index, (_, version) in enumerate(published[:LOOKBACK]):
        info = fetch_pypi(f"{HARNESS}/{version}/json")["info"]
        kit = Kit(
            harness=version,
            homeassistant=pinned_home_assistant(info["requires_dist"]),
            python=python_for(info["requires_python"]),
        )
        if not is_pre_release(kit.homeassistant):
            return kit, beta
        if index == 0:
            beta = kit
    msg = f"No {HARNESS} release for a final Home Assistant in the newest {LOOKBACK}"
    raise SystemExit(msg)


def minimum_kit(fetch_pypi: Fetch, minimum: str) -> Kit:
    """Return the kit for the minimum; the install resolves its harness."""
    info = fetch_pypi(f"homeassistant/{minimum}/json")["info"]
    return Kit(
        harness="", homeassistant=minimum, python=python_for(info["requires_python"])
    )


def newest_release(fetch_github: Fetch, repository: str) -> Release | None:
    """Return the newest published release, pre-releases included."""
    releases = fetch_github(f"repos/{repository}/releases?per_page=10")
    published = [release for release in releases if not release["draft"]]
    if not published:
        return None
    tag = published[0]["tag_name"]
    quoted = urllib.parse.quote(tag, safe="")
    sha = fetch_github(f"repos/{repository}/commits/{quoted}")["sha"]
    hacs = fetch_github(f"repos/{repository}/contents/hacs.json?ref={quoted}")
    minimum = json.loads(base64.b64decode(hacs["content"]))["homeassistant"]
    return Release(tag=tag, sha=sha, minimum=minimum)


def targets(
    *,
    main_sha: str,
    main_minimum: str,
    stable: Kit,
    beta: Kit | None,
    minimum: Kit,
    release: Release | None,
) -> list[dict[str, str]]:
    """Return one matrix entry for each ref and kit worth testing."""
    refs = [("main", main_sha, "", main_minimum)]
    if release:
        refs.append(("release", release.sha, release.tag, release.minimum))
    entries = []
    for target, ref, tag, ref_minimum in refs:
        channels = [("stable", stable), ("beta", beta)]
        if target == "main":
            channels.append(("minimum", minimum))
        for channel, kit in channels:
            if kit is None:
                continue
            if version_key(kit.homeassistant) < version_key(ref_minimum):
                continue
            requirement = (
                f"{HARNESS}=={kit.harness}"
                if kit.harness
                else f"{HARNESS} homeassistant=={kit.homeassistant}"
            )
            entries.append(
                {
                    "target": target,
                    "ref": ref,
                    "tag": tag,
                    "channel": channel,
                    "harness": kit.harness,
                    "homeassistant": kit.homeassistant,
                    "python": kit.python,
                    "install": requirement,
                }
            )
    return entries


def fingerprint(entries: list[dict[str, str]]) -> str:
    """Return a short hash that changes whenever any target changes."""
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def already_tested(fetch_github: Fetch, repository: str, digest: str) -> bool:
    """Return whether a run left the done marker for this fingerprint."""
    name = urllib.parse.quote(DONE_PREFIX + digest, safe="")
    found = fetch_github(f"repos/{repository}/actions/artifacts?name={name}&per_page=5")
    return any(not artifact["expired"] for artifact in found["artifacts"])


def summary(entries: list[dict[str, str]], *, run: bool, digest: str) -> str:
    """Return the step summary: the targets, and whether they run."""
    lines = [
        "### Compatibility plan",
        "",
        f"Fingerprint `{digest}`: "
        + ("testing." if run else "already tested; nothing new to test."),
        "",
        "| Target | Channel | Home Assistant | Harness |",
        "|---|---|---|---|",
    ]
    lines.extend(
        f"| {entry['tag'] or entry['target']} (`{entry['ref'][:7]}`) "
        f"| {entry['channel']} | {entry['homeassistant']} "
        f"| {entry['harness'] or 'resolved at install'} |"
        for entry in entries
    )
    return "\n".join(lines) + "\n"


def _fetch(url: str, headers: Mapping[str, str]) -> Any:
    request = urllib.request.Request(url, headers=dict(headers))  # noqa: S310
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        return json.load(response)


def fetch_pypi(path: str) -> Any:
    """Read PyPI's JSON API."""
    return _fetch(PYPI + path, {"Accept": "application/json"})


def fetch_github(path: str) -> Any:
    """Read GitHub's REST API with the job's token."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.environ.get("GH_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    return _fetch(GITHUB_API + path, headers)


def main(
    env: Mapping[str, str] = os.environ,
    root: Path = Path(),
    pypi: Fetch = fetch_pypi,
    github: Fetch = fetch_github,
) -> None:
    """Write the plan to ``GITHUB_OUTPUT`` and the step summary."""
    repository = env["GITHUB_REPOSITORY"]
    main_minimum = json.loads((root / "hacs.json").read_text())["homeassistant"]
    stable, beta = kits(pypi)
    entries = targets(
        main_sha=env["GITHUB_SHA"],
        main_minimum=main_minimum,
        stable=stable,
        beta=beta,
        minimum=minimum_kit(pypi, main_minimum),
        release=newest_release(github, repository),
    )
    digest = fingerprint(entries)
    run = env["GITHUB_EVENT_NAME"] != "schedule" or not already_tested(
        github, repository, digest
    )
    with Path(env["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"matrix={json.dumps(entries)}\n")
        output.write(f"fingerprint={digest}\n")
        output.write(f"run={str(run).lower()}\n")
    text = summary(entries, run=run, digest=digest)
    if summary_path := env.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a") as handle:
            handle.write(text)
    sys.stdout.write(text)


if __name__ == "__main__":
    main()

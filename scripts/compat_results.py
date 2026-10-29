"""
Turn compatibility test runs into results, and results into issue texts.

Usage (in ``compat.yml``):

- ``compat_results.py outcome RECORD``: print the outcome of a record that
  ``compat_pytest_plugin.py`` wrote.
- ``compat_results.py result --matrix JSON --installed "HA HARNESS"
  --manifest PATH --record PATH --retry-record PATH --deprecations PATH
  --run-url URL --out PATH``: write one target's result and print its outcome.
- ``compat_results.py check RESULTS MATRIX``: fail unless every target in the
  plan's matrix has a result under the directory ``RESULTS``.
- ``compat_results.py report --kind broken|deprecated|summary RESULTS``: print
  the tracking issue's text, or nothing when there is nothing to track.

Outcomes: ``passed``; ``snapshot-differences``, when only syrupy snapshots
differ (the expected output stored for the pinned harness, not a break);
``broken``, for any other failure, a crash, or no record at all.

A result (``schema`` 1) has ``integration``, ``integration_version``,
``target`` (``main`` or ``release``), ``tag``, ``ref``, ``channel``
(``stable``, ``beta`` or ``minimum``), ``homeassistant``, ``harness``,
``python``, ``outcome``, ``retried``, ``broken_tests``, ``snapshot_tests``,
``deprecations`` (a list, or ``null`` when the ref cannot report them) and
``run_url``. Only the standard library is used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

PASSED = "passed"
SNAPSHOT = "snapshot-differences"
BROKEN = "broken"
SCHEMA = 1
LISTED = 20

Result = dict[str, Any]


def _load(path: Path | None) -> Any:
    if path is None or not path.is_file():
        return None
    return json.loads(path.read_text())


def outcome(record: dict[str, Any] | None) -> str:
    """Return the outcome a test session's record shows."""
    if record is None or record["broken"] or record["exitstatus"] not in {0, 1}:
        return BROKEN
    if record["snapshot"]:
        return SNAPSHOT
    # Exit status 1 with no failing test: something else failed the run.
    return PASSED if record["exitstatus"] == 0 else BROKEN


def result(
    *,
    matrix: dict[str, str],
    installed: str,
    manifest: dict[str, str] | None,
    record: dict[str, Any] | None,
    retry_record: dict[str, Any] | None,
    deprecations: list[str] | None,
    run_url: str,
) -> Result:
    """Return one target's result; a retry's record replaces the first one."""
    final = retry_record if retry_record is not None else record
    homeassistant, _, harness = installed.partition(" ")
    return {
        "schema": SCHEMA,
        "integration": (manifest or {}).get("domain", ""),
        "integration_version": (manifest or {}).get("version", ""),
        "target": matrix["target"],
        "tag": matrix["tag"],
        "ref": matrix["ref"],
        "channel": matrix["channel"],
        "homeassistant": homeassistant or matrix["homeassistant"],
        "harness": harness or matrix["harness"],
        "python": matrix["python"],
        "outcome": outcome(final),
        "retried": retry_record is not None,
        "broken_tests": (final or {}).get("broken", [])[:LISTED],
        "snapshot_tests": (final or {}).get("snapshot", [])[:LISTED],
        "deprecations": deprecations,
        "run_url": run_url,
    }


def load_results(directory: Path) -> list[Result]:
    """Read every ``result.json`` under ``directory``, in a stable order."""
    results = [json.loads(path.read_text()) for path in directory.rglob("result.json")]
    return sorted(results, key=lambda item: (item["target"], item["channel"]))


def check(results: list[Result], matrix: list[dict[str, str]]) -> None:
    """Fail unless every planned target has a result."""
    planned = {(entry["target"], entry["channel"]) for entry in matrix}
    present = {(item["target"], item["channel"]) for item in results}
    if missing := sorted(planned - present):
        msg = f"No result for {missing}; the next scheduled run tries again"
        raise SystemExit(msg)


def _code(item: Result) -> str:
    return f"{item['tag'] or item['target']} (`{item['ref'][:7]}`)"


def _state(parts: list[Any]) -> str:
    canonical = json.dumps(sorted(parts), separators=(",", ":"))
    return (
        f"<!-- compat-state: {hashlib.sha256(canonical.encode()).hexdigest()[:16]} -->"
    )


def _tests(names: list[str]) -> str:
    shown = ", ".join(f"`{name}`" for name in names[:5])
    more = len(names) - 5
    return shown + (f" and {more} more" if more > 0 else "") if names else "the run"


def report_broken(results: list[Result]) -> str:
    """Return the compat issue's text, or "" when nothing is broken."""
    broken = [item for item in results if item["outcome"] == BROKEN]
    if not broken:
        return ""
    state = _state(
        [
            [item["tag"] or item["target"], item["channel"], item["homeassistant"]]
            for item in broken
        ]
    )
    lines = [
        state,
        "The compatibility run found the integration broken on:",
        "",
        "| Code | Channel | Home Assistant | Harness | What failed |",
        "|---|---|---|---|---|",
    ]
    lines.extend(
        f"| {_code(item)} | {item['channel']} | {item['homeassistant']} "
        f"| {item['harness'] or '-'} | {_tests(item['broken_tests'])} |"
        for item in broken
    )
    lines += [
        "",
        f"Run: {broken[0]['run_url']}",
        "",
        (
            "A break on a beta needs a fix released before that Home Assistant version "
            "ships. This issue is updated when what fails changes, and closes itself "
            "when a run finds nothing broken."
        ),
    ]
    return "\n".join(lines) + "\n"


def report_deprecated(results: list[Result]) -> str:
    """Return the deprecation issue's text, or "" when nothing is reported."""
    seen: dict[str, list[str]] = {}
    for item in results:
        for message in item["deprecations"] or []:
            where = f"{item['tag'] or item['target']} on {item['homeassistant']}"
            seen.setdefault(message, []).append(where)
    if not seen:
        return ""
    lines = [
        _state(list(seen)),
        (
            "Home Assistant reports that the integration uses something it has "
            "deprecated. Each message names the version that removes it; fix it in "
            "a release before then."
        ),
        "",
    ]
    for message, where in sorted(seen.items()):
        lines.append(f"- {' '.join(message.split())}")
        lines.append(f"  Seen on: {', '.join(where)}.")
    run_url = next(item["run_url"] for item in results if item["deprecations"])
    lines += [
        "",
        f"Run: {run_url}",
        "",
        (
            "This issue is updated when the list changes, and closes itself when a "
            "run reports none."
        ),
    ]
    return "\n".join(lines) + "\n"


def report_summary(results: list[Result]) -> str:
    """Return a table of every result for the run's summary."""
    lines = [
        "### Compatibility results",
        "",
        "| Code | Channel | Home Assistant | Harness | Outcome | Deprecations |",
        "|---|---|---|---|---|---|",
    ]
    for item in results:
        deprecations = item["deprecations"]
        count = "not checked" if deprecations is None else str(len(deprecations))
        retried = " (retried)" if item["retried"] else ""
        lines.append(
            f"| {_code(item)} | {item['channel']} | {item['homeassistant']} "
            f"| {item['harness'] or '-'} | {item['outcome']}{retried} | {count} |"
        )
    return "\n".join(lines) + "\n"


REPORTS = {
    "broken": report_broken,
    "deprecated": report_deprecated,
    "summary": report_summary,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("outcome").add_argument("record", type=Path)
    make = commands.add_parser("result")
    make.add_argument("--matrix", required=True)
    make.add_argument("--installed", default="")
    for name in ("manifest", "record", "retry-record", "deprecations", "out"):
        make.add_argument(f"--{name}", type=Path, required=name == "out")
    make.add_argument("--run-url", required=True)
    verify = commands.add_parser("check")
    verify.add_argument("results", type=Path)
    verify.add_argument("matrix")
    render = commands.add_parser("report")
    render.add_argument("--kind", choices=sorted(REPORTS), required=True)
    render.add_argument("results", type=Path)
    return parser


def main(argv: list[str] | None = None) -> None:
    """Run one command; see the module docstring."""
    args = _parser().parse_args(argv)
    if args.command == "outcome":
        sys.stdout.write(outcome(_load(args.record)) + "\n")
    elif args.command == "result":
        item = result(
            matrix=json.loads(args.matrix),
            installed=args.installed.strip(),
            manifest=_load(args.manifest),
            record=_load(args.record),
            retry_record=_load(args.retry_record),
            deprecations=_load(args.deprecations),
            run_url=args.run_url,
        )
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(item, indent=2) + "\n")
        sys.stdout.write(item["outcome"] + "\n")
    elif args.command == "check":
        check(load_results(args.results), json.loads(args.matrix))
    else:
        sys.stdout.write(REPORTS[args.kind](load_results(args.results)))


if __name__ == "__main__":
    main()

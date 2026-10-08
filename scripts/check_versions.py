"""
Verify project, integration, release-tag and CHANGELOG versions agree.

Usage: ``check_versions.py [REF] [--release-notes PATH]``

- ``manifest.json`` and ``pyproject.toml`` must carry the same version.
- When ``REF`` is a tag (``vX.Y.Z``), it must name that version.
- ``CHANGELOG.md`` must have a non-empty ``## X.Y.Z`` section for it.
- ``--release-notes PATH`` writes that section to ``PATH`` for the release.
"""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTES_OPTION = "--release-notes"


def changelog_section(version: str, text: str) -> str:
    """Return the body of the ``## <version>`` CHANGELOG section."""
    heading = f"## {version}"
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if line.strip() == heading), None)
    if start is None:
        raise SystemExit(f"CHANGELOG.md has no '{heading}' section")
    end = next(
        (i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")),
        len(lines),
    )
    body = "\n".join(lines[start + 1 : end]).strip()
    if not body:
        raise SystemExit(f"CHANGELOG.md section '{heading}' is empty")
    return body + "\n"


def _parse_args(argv: list[str]) -> tuple[str, Path | None]:
    """Return the ref name and the optional release-notes path."""
    ref_name = ""
    notes_path: Path | None = None
    args = iter(argv)
    for arg in args:
        if arg == NOTES_OPTION:
            value = next(args, None)
            if not value:
                raise SystemExit(f"{NOTES_OPTION} needs a path")
            notes_path = Path(value)
        elif arg.startswith(f"{NOTES_OPTION}="):
            notes_path = Path(arg.split("=", 1)[1])
        elif not ref_name:
            ref_name = arg
        else:
            raise SystemExit(f"Unexpected argument: {arg}")
    return ref_name, notes_path


def main(argv: list[str] | None = None, root: Path = ROOT) -> None:
    """Validate all release version sources."""
    ref_name, notes_path = _parse_args(sys.argv[1:] if argv is None else argv)
    manifest = json.loads(
        (root / "custom_components/meridian_energy/manifest.json").read_text()
    )
    with (root / "pyproject.toml").open("rb") as project_file:
        project = tomllib.load(project_file)

    manifest_version = str(manifest["version"])
    project_version = str(project["project"]["version"])
    if manifest_version != project_version:
        raise SystemExit(
            f"Version mismatch: manifest={manifest_version}, project={project_version}"
        )

    if ref_name.startswith("v") and ref_name[1:] != manifest_version:
        raise SystemExit(
            f"Tag mismatch: tag={ref_name}, integration={manifest_version}"
        )

    notes = changelog_section(
        manifest_version, (root / "CHANGELOG.md").read_text(encoding="utf-8")
    )
    if notes_path is not None:
        notes_path.write_text(notes, encoding="utf-8")


if __name__ == "__main__":
    main()

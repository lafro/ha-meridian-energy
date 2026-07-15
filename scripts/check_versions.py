"""Verify project, integration and release-tag versions agree."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    """Validate all release version sources."""
    manifest = json.loads(
        (ROOT / "custom_components/meridian_energy/manifest.json").read_text()
    )
    with (ROOT / "pyproject.toml").open("rb") as project_file:
        project = tomllib.load(project_file)

    manifest_version = str(manifest["version"])
    project_version = str(project["project"]["version"])
    if manifest_version != project_version:
        raise SystemExit(
            f"Version mismatch: manifest={manifest_version}, project={project_version}"
        )

    ref_name = sys.argv[1] if len(sys.argv) > 1 else ""
    if ref_name.startswith("v") and ref_name[1:] != manifest_version:
        raise SystemExit(
            f"Tag mismatch: tag={ref_name}, integration={manifest_version}"
        )


if __name__ == "__main__":
    main()

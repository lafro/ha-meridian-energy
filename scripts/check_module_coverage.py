"""Enforce line and branch coverage for every integration module."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_INTEGRATION_PREFIX = "custom_components/meridian_energy/"
_MINIMUM_RATE = 0.95


def main() -> None:
    """Fail when any integration module falls below the required coverage."""
    report_path = Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.xml")
    root = ET.parse(report_path).getroot()  # noqa: S314
    failures: list[str] = []
    modules = 0
    for class_element in root.findall(".//class"):
        filename = class_element.get("filename", "")
        if not filename.startswith(_INTEGRATION_PREFIX):
            continue
        modules += 1
        line_rate = float(class_element.get("line-rate", "0"))
        branch_rate = float(class_element.get("branch-rate", "0"))
        if line_rate < _MINIMUM_RATE or branch_rate < _MINIMUM_RATE:
            failures.append(
                f"{filename}: line={line_rate:.2%}, branch={branch_rate:.2%}"
            )
    if modules == 0:
        raise SystemExit("No Meridian integration modules found in coverage report")
    if failures:
        raise SystemExit("Per-module coverage is below 95%:\n" + "\n".join(failures))


if __name__ == "__main__":
    main()

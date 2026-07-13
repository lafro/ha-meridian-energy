"""Tests for user-facing source strings."""

from __future__ import annotations

import json
from pathlib import Path


def test_english_translation_matches_source_strings() -> None:
    """Keep the source strings HA loads aligned with the English translation."""
    integration_dir = (
        Path(__file__).parents[1] / "custom_components" / "meridian_energy"
    )
    source = json.loads((integration_dir / "strings.json").read_text())
    english = json.loads((integration_dir / "translations" / "en.json").read_text())

    assert source == english
    assert source["entity"]["sensor"]["last_sync"]["name"] == "Last data update"

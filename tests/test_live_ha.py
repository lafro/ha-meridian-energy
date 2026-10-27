"""Contracts for the disposable live-test instance (``scripts/live_ha.py``)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from scripts import live_ha


def _configuration() -> dict[str, object]:
    return yaml.safe_load(live_ha.configuration_yaml("meridian_energy"))


def test_the_instance_is_reachable_from_this_computer_only(tmp_path: Path) -> None:
    (tmp_path / ".storage").mkdir()
    live_ha.write_http_store(tmp_path, 8124)
    data = json.loads((tmp_path / ".storage" / "http").read_text())["data"]
    assert data["stable"]["server_host"] == ["127.0.0.1"]
    assert data["stable"]["server_port"] == 8124
    assert data["pending"] is None
    assert data["yaml_migration_done"] is True
    assert live_ha.http_store_problems(tmp_path, 8124) == []
    # Web-server settings in YAML would be a trial that reverts to every address.
    config = _configuration()
    assert "http" not in config
    providers = config["homeassistant"]["auth_providers"]
    trusted = [p for p in providers if p["type"] == "trusted_networks"]
    assert trusted == [
        {
            "type": "trusted_networks",
            "trusted_networks": ["127.0.0.1/32"],
            "allow_bypass_login": True,
        }
    ]


def test_the_instance_loads_nothing_that_discovers_or_announces() -> None:
    config = _configuration()
    assert set(config) == {
        "homeassistant",
        "frontend",
        "config",
        "energy",
        "history",
        "recorder",
        "logger",
    }
    assert not set(config) & live_ha.NOT_ALLOWED


def test_the_integration_logs_at_debug_level() -> None:
    logs = _configuration()["logger"]["logs"]
    assert logs == {"custom_components.meridian_energy": "debug"}


def test_the_domain_is_the_repositorys_integration() -> None:
    assert live_ha.integration_domain() == "meridian_energy"


def test_stop_never_deletes_a_folder_it_did_not_make(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / "live-ha-meridian_energy"
    folder.mkdir()
    (folder / "keep.txt").write_text("not ours")
    monkeypatch.setattr(live_ha, "state_dir", lambda _domain: folder)
    with pytest.raises(SystemExit, match="not made by this script"):
        live_ha.stop()
    assert (folder / "keep.txt").is_file()


def test_stop_without_an_instance_does_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(live_ha, "state_dir", lambda _domain: tmp_path / "absent")
    live_ha.stop()
    assert not (tmp_path / "absent").exists()


def test_an_unconfirmed_or_wider_web_server_setting_is_a_problem(
    tmp_path: Path,
) -> None:
    (tmp_path / ".storage").mkdir()
    live_ha.write_http_store(tmp_path, 8124)
    store = tmp_path / ".storage" / "http"
    raw = json.loads(store.read_text())
    raw["data"]["pending"] = {"server_port": 8123}
    raw["data"]["stable"]["server_host"] = ["0.0.0.0"]
    store.write_text(json.dumps(raw))
    assert live_ha.http_store_problems(tmp_path, 8124) == [
        "an unconfirmed change is pending",
        "stored address ['0.0.0.0']:8124",
    ]
    assert live_ha.http_store_problems(tmp_path / "absent", 8124) == [
        "no stored web-server settings"
    ]


def test_the_install_ignores_the_repositorys_uv_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A test pin of Home Assistant in pyproject.toml must not decide what the
    # instance runs.
    calls: list[list[str]] = []

    def fake_run(*args: str | Path) -> str:
        calls.append([str(arg) for arg in args])
        return "1.0.0"

    monkeypatch.setattr(live_ha, "run", fake_run)
    assert live_ha.install_home_assistant(tmp_path, None) == "1.0.0"
    uv_calls = [call for call in calls if call[0] == "uv"]
    assert [call[1] for call in uv_calls] == ["venv", "pip"]
    assert all("--no-config" in call for call in uv_calls)

"""Contracts for the disposable live-test instance (``scripts/live_ha.py``)."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts import live_ha

DOMAIN = "meridian_energy"


def _configuration() -> dict[str, Any]:
    return yaml.safe_load(live_ha.configuration_yaml(DOMAIN))


def _lsof(name: str, protocol: str = "TCP") -> str:
    return f"Python 4242 user 10u IPv4 0x1 0t0 {protocol} {name}"


LISTENING = _lsof("127.0.0.1:8124 (LISTEN)")


def _child(seconds: float) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-c", f"import time; time.sleep({seconds})"],
        start_new_session=True,
    )


def test_the_instance_is_reachable_from_this_computer_only(tmp_path: Path) -> None:
    (tmp_path / ".storage").mkdir()
    live_ha.write_http_store(tmp_path, 8124)
    stored = json.loads((tmp_path / ".storage" / "http").read_text())
    assert (stored["version"], stored["minor_version"]) == live_ha.HTTP_STORE_VERSION
    data = stored["data"]
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


def test_the_store_form_matches_the_home_assistant_under_test() -> None:
    # The weekly compatibility workflow runs this against newer releases, so a
    # change to how Home Assistant stores web-server settings shows here
    # before a live test meets it.
    assert live_ha.http_store_version(Path(sys.executable)) == (
        live_ha.HTTP_STORE_VERSION
    )


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
    # What onboarding's core_config step would add, which is skipped.
    assert {"google_translate", "met", "radio_browser", "shopping_list"} <= (
        live_ha.NOT_ALLOWED
    )


def test_the_integration_logs_at_debug_level() -> None:
    logs = _configuration()["logger"]["logs"]
    assert logs == {f"custom_components.{DOMAIN}": "debug"}


def test_the_domain_is_the_repositorys_integration() -> None:
    assert live_ha.integration_domain() == DOMAIN


def test_a_ref_installs_that_commits_integration(tmp_path: Path) -> None:
    source = live_ha.install_integration(tmp_path, DOMAIN, "HEAD")
    assert source.startswith("HEAD (")
    manifest = tmp_path / "config" / "custom_components" / DOMAIN / "manifest.json"
    assert json.loads(manifest.read_text())["domain"] == DOMAIN


def test_an_unknown_ref_stops_with_gits_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="no-such-ref"):
        live_ha.install_integration(tmp_path, DOMAIN, "no-such-ref")


def test_only_the_instances_own_address_may_listen() -> None:
    lines = [
        LISTENING,
        _lsof("127.0.0.1:8124->127.0.0.1:50000 (ESTABLISHED)"),
        _lsof("*:5353", "UDP"),
        _lsof("*:8123 (LISTEN)"),
        _lsof("[::]:8124 (LISTEN)"),
        _lsof("*:1900", "UDP"),
    ]
    assert live_ha.listening_problems(lines, 8124) == [
        "*:8123 (LISTEN)",
        "[::]:8124 (LISTEN)",
        "UDP *:1900",
    ]
    assert live_ha.listens_on(lines, 8124)
    assert not live_ha.listens_on(lines[1:], 8124)
    assert not live_ha.listens_on([], 8124)


def _instance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    components: tuple[str, ...] = ("api", "frontend", DOMAIN),
    entries: tuple[str, ...] = ("analytics", "backup"),
    recovery_mode: bool = False,
    lines: tuple[str, ...] = (LISTENING,),
) -> None:
    config = tmp_path / "config"
    (config / ".storage").mkdir(parents=True)
    live_ha.write_http_store(config, 8124)
    (tmp_path / "port").write_text("8124")
    answers = {
        "/api/config": {
            "state": "RUNNING",
            "components": list(components),
            "recovery_mode": recovery_mode,
            "version": "1.0.0",
        },
        "/api/config/config_entries/entry": [{"domain": d} for d in entries],
        "/api/config/config_entries/flow_handlers": [DOMAIN],
    }
    monkeypatch.setattr(live_ha, "access_token", lambda _state, _port: "token")
    monkeypatch.setattr(
        live_ha, "call", lambda _port, _method, path, **_kwargs: answers[path]
    )
    monkeypatch.setattr(live_ha, "sockets", lambda _pid: list(lines))


def _check(tmp_path: Path) -> str:
    return live_ha.check_contained(tmp_path, lambda: True, 4242, 8124, DOMAIN)


def test_a_contained_instance_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _instance(tmp_path, monkeypatch)
    assert _check(tmp_path) == "1.0.0"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"components": ("api", "zeroconf.sensor")}, "must not load"),
        ({"entries": ("analytics", "met")}, "Unexpected config entries"),
        ({"recovery_mode": True}, "recovery mode"),
        ({"lines": (_lsof("*:8123 (LISTEN)"),)}, "listens beyond"),
        ({"lines": ()}, "cannot be shown to be contained"),
    ],
)
def test_an_uncontained_instance_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    _instance(tmp_path, monkeypatch, **kwargs)
    with pytest.raises(SystemExit, match=message):
        _check(tmp_path)


def test_a_crashed_home_assistant_is_noticed_at_once(tmp_path: Path) -> None:
    (tmp_path / "port").write_text("8124")
    child = _child(0)
    child.wait()
    with pytest.raises(SystemExit, match="stopped while waiting"):
        live_ha.wait_for(lambda: child.poll() is None, child.pid, tmp_path, dict, "x")


def test_terminate_stops_a_running_group_and_ignores_a_finished_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(live_ha, "STOP_TIMEOUT_S", 5)
    child = _child(60)
    started = time.monotonic()
    live_ha.terminate(child.pid, lambda: child.poll() is None)
    assert child.returncode == -signal.SIGTERM
    assert time.monotonic() - started < 5
    # An exited group leader not yet collected: macOS refuses the signal.
    finished = _child(0)
    time.sleep(0.5)
    live_ha.signal_group(finished.pid, signal.SIGKILL)
    live_ha.terminate(finished.pid, lambda: finished.poll() is None)
    assert finished.returncode == 0


def test_the_instance_is_recognised_by_its_command_line(tmp_path: Path) -> None:
    hass = tmp_path / "venv" / "bin" / "hass"
    hass.parent.mkdir(parents=True)
    hass.write_text("import time\ntime.sleep(60)\n")
    process = subprocess.Popen([sys.executable, hass], start_new_session=True)
    try:
        assert live_ha.is_instance(process.pid, tmp_path)
        assert not live_ha.is_instance(process.pid, tmp_path / "other")
        assert not live_ha.is_instance(os.getpid(), tmp_path)
    finally:
        process.kill()
        process.wait()


def test_stop_never_signals_a_process_that_is_not_the_instance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / f"live-ha-{DOMAIN}"
    folder.mkdir()
    (folder / live_ha.MARKER).write_text("")
    (folder / "port").write_text("8124")
    other = _child(60)
    (folder / "hass.pid").write_text(str(other.pid))
    monkeypatch.setattr(live_ha, "state_dir", lambda _domain: folder)
    monkeypatch.setattr(live_ha, "port_in_use", lambda _port: False)
    try:
        live_ha.stop()
        assert other.poll() is None
        assert not folder.exists()
    finally:
        os.killpg(other.pid, signal.SIGKILL)
        other.wait()


def test_stop_never_deletes_a_folder_it_did_not_make(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / f"live-ha-{DOMAIN}"
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
    monkeypatch.setattr(
        live_ha, "http_store_version", lambda _python: live_ha.HTTP_STORE_VERSION
    )
    assert live_ha.install_home_assistant(tmp_path, None) == "1.0.0"
    uv_calls = [call for call in calls if call[0] == "uv"]
    assert [call[1] for call in uv_calls] == ["venv", "pip"]
    assert all("--no-config" in call for call in uv_calls)


def test_a_release_with_another_store_form_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(live_ha, "run", lambda *_args: "2026.7.2")
    monkeypatch.setattr(live_ha, "http_store_version", lambda _python: None)
    with pytest.raises(SystemExit, match=r"cannot keep it to 127\.0\.0\.1"):
        live_ha.install_home_assistant(tmp_path, "2026.7.2")

"""
Run a disposable Home Assistant with this integration, for a live test.

Usage::

    uv run python scripts/live_ha.py start [--ref REF] [--ha-version VERSION]
                                           [--port PORT]
    uv run python scripts/live_ha.py stop

``start`` builds a new Home Assistant in its own folder in the system's
temporary directory and starts it with this integration, taken from the
working tree or from ``REF`` (a release tag or commit). The instance is
contained:

- It listens on 127.0.0.1 only, so nothing but this computer can reach it.
  The web server's settings are written to its store as already confirmed:
  in current releases, settings from YAML are a five-minute trial
  that falls back to the defaults, which listen on every address. A release
  that stores them in another form is refused. If a check fails at any point,
  ``start`` stops the instance at once.
- Its configuration is not ``default_config``: it loads only what a live test
  needs (the frontend, history, Energy and the integration) on top of the
  integrations Home Assistant always loads. Nothing that discovers or
  announces devices on the network loads, nor the cloud or mobile-app
  integrations, and analytics stays off.
- Onboarding creates the owner through the API, with a random password that
  is not kept, and skips the step that adds the weather, radio and shopping
  list integrations. A browser on this computer is signed in without a
  password (the ``trusted_networks`` provider, for 127.0.0.1 only). So can any
  other program on this computer, and in principle a web page open in its
  browser, act as the owner while the instance runs: stop it when done.
- ``start`` checks all of this, the config entries and what the process
  listens on, before it reports the instance ready.
- The integration logs at debug level to the instance's own log.

Add the integration from the printed link and sign in with a real account:
that is the live test. ``stop`` removes the integration's config entries,
stops Home Assistant and deletes the folder, with its database, log and stored
sign-in. Only uv's package cache outlives it, so the next ``start`` is quick.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"
DEFAULT_PORT = 8124
PYTHON = "3.14"
MARKER = ".live-ha"
# The first start installs the frontend and the recorder's requirements.
START_TIMEOUT_S = 900
STOP_TIMEOUT_S = 60
KILL_TIMEOUT_S = 10
# Unprivileged ports only: a port Home Assistant cannot bind sends it into
# recovery mode, whose defaults listen on every address.
PORTS = range(1024, 65536)
# The form of the web-server store this script writes. A release that keeps
# its settings any other way would ignore it and listen on every address.
HTTP_STORE_VERSION = (2, 2)
# Integrations that discover or announce devices, or reach past this
# computer. None of them may load. (Home Assistant always loads analytics,
# which sends nothing unless someone opts in, and network, which only lists
# this computer's adapters.)
NOT_ALLOWED = frozenset(
    {
        "bluetooth",
        "cloud",
        "default_config",
        "dhcp",
        "go2rtc",
        "hassio",
        "homekit",
        "homekit_controller",
        "google_translate",
        "matter",
        "met",
        "mobile_app",
        "otbr",
        "radio_browser",
        "shopping_list",
        "ssdp",
        "thread",
        "usb",
        "zeroconf",
    }
)
# The config entries Home Assistant creates on its own.
SYSTEM_ENTRIES = frozenset({"analytics", "backup"})
# SSDP and DHCP discovery ports. Not mDNS (5353): Home Assistant's web client
# listens there to look up ``.local`` names, and announcing Home Assistant is
# the zeroconf integration's job, which must not load.
DISCOVERY_PORTS = frozenset({"1900", "67", "68"})
# Onboarding steps marked done before the first start, so only the owner is
# created: the core_config step would add the weather, radio, text-to-speech
# and shopping list integrations.
SKIPPED_ONBOARDING = ("core_config", "analytics", "integration")


def say(message: str) -> None:
    """Print one line of progress."""
    print(message, flush=True)  # noqa: T201


def integration_domain() -> str:
    """Return the domain of the one integration under ``custom_components``."""
    domains = sorted(
        path.name
        for path in (ROOT / "custom_components").iterdir()
        if (path / "manifest.json").is_file()
    )
    if len(domains) != 1:
        raise SystemExit(f"Expected one integration in custom_components: {domains}")
    return domains[0]


def state_dir(domain: str) -> Path:
    """Return the folder that holds the whole instance."""
    return Path(tempfile.gettempdir()) / f"live-ha-{domain}"


def configuration_yaml(domain: str) -> str:
    """Return the instance's ``configuration.yaml``."""
    return f"""\
# Disposable live-test instance written by scripts/live_ha.py.
# Not default_config: only what a live test needs, so nothing discovers or
# announces devices, and only this computer can reach it.
homeassistant:
  name: Live test
  # Auckland city centre, a public landmark, never a real home.
  latitude: -36.8485
  longitude: 174.7633
  elevation: 0
  unit_system: metric
  currency: NZD
  country: NZ
  time_zone: Pacific/Auckland
  language: en
  auth_providers:
    - type: trusted_networks
      trusted_networks:
        - {HOST}/32
      allow_bypass_login: true
    - type: homeassistant

# No http: block. The web server's settings are in .storage/http, written
# before the first start (see scripts/live_ha.py): from YAML they would be a
# five-minute trial that falls back to listening on every address.

frontend:
config:
energy:
history:
recorder:

logger:
  default: warning
  logs:
    custom_components.{domain}: debug
"""


def run_bytes(*args: str | Path, cwd: Path | None = None) -> bytes:
    """Run a command and return its output; stop with its error if it fails."""
    try:
        result = subprocess.run(  # noqa: S603
            [str(arg) for arg in args], cwd=cwd, check=True, capture_output=True
        )
    except subprocess.CalledProcessError as err:
        command = " ".join(str(arg) for arg in args)
        error = err.stderr.decode(errors="replace").strip()
        raise SystemExit(f"{command} failed: {error}") from None
    return result.stdout


def run(*args: str | Path, cwd: Path | None = None) -> str:
    """Run a command and return its output as text."""
    return run_bytes(*args, cwd=cwd).decode().strip()


def port_in_use(port: int) -> bool:
    """Return whether something accepts connections on ``HOST:port``."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(1)
        return probe.connect_ex((HOST, port)) == 0


def is_instance(pid: int, state: Path) -> bool:
    """Return whether ``pid`` is still this instance's Home Assistant."""
    # A process ID is reused once its process ends, so it is checked against
    # the command line ``start`` used, never trusted on its own. -ww: the
    # whole command line, which Linux's ps otherwise cuts to 80 columns when
    # its output is not a terminal.
    result = subprocess.run(  # noqa: S603
        ["ps", "-ww", "-o", "command=", "-p", str(pid)],  # noqa: S607
        check=False,
        capture_output=True,
        text=True,
    )
    return str(state / "venv" / "bin" / "hass") in result.stdout


def signal_group(pid: int, signum: signal.Signals) -> None:
    """Signal the process group ``pid`` leads, unless it has already gone."""
    # macOS refuses to signal a group whose leader has exited.
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signum)


def call(
    port: int,
    method: str,
    path: str,
    *,
    token: str | None = None,
    data: dict[str, Any] | None = None,
    form: dict[str, str] | None = None,
) -> Any:
    """Send one request to the instance and return its decoded JSON."""
    headers: dict[str, str] = {}
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    if form is not None:
        body = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"http://{HOST}:{port}{path}", data=body, headers=headers, method=method
    )
    with opener().open(request, timeout=30) as response:
        raw = response.read()
    return json.loads(raw) if raw else None


def opener() -> urllib.request.OpenerDirector:
    """Return an opener that ignores proxy settings."""
    # Requests to the instance carry the owner's password and tokens, so they
    # never go through a proxy.
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def client_id(port: int) -> str:
    """Return the OAuth client id the instance's own frontend uses."""
    return f"http://{HOST}:{port}/"


def install_integration(state: Path, domain: str, ref: str | None) -> str:
    """Copy the integration into the instance and describe its source."""
    target = state / "config" / "custom_components"
    target.mkdir(parents=True)
    if ref is None:
        shutil.copytree(
            ROOT / "custom_components" / domain,
            target / domain,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        commit = run("git", "rev-parse", "--short", "HEAD", cwd=ROOT)
        dirty = run("git", "status", "--porcelain", "custom_components", cwd=ROOT)
        return f"the working tree at {commit}" + (", with changes" if dirty else "")
    commit = run(
        "git",
        "rev-parse",
        "--verify",
        "--end-of-options",
        f"{ref}^{{commit}}",
        cwd=ROOT,
    )
    archive = run_bytes(
        "git",
        "archive",
        "--format=tar",
        commit,
        f"custom_components/{domain}",
        cwd=ROOT,
    )
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(state / "config", filter="data")
    return f"{ref} ({commit[:7]})"


def http_store_version(python: Path) -> tuple[int, int] | None:
    """Return the web-server store version of the Home Assistant ``python`` runs."""
    result = subprocess.run(  # noqa: S603
        [
            python,
            "-c",
            (
                "from homeassistant.components.http import config; "
                "print(config.STORAGE_VERSION, config.STORAGE_MINOR_VERSION)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        major, minor = (int(part) for part in result.stdout.split())
    except ValueError:
        return None
    return major, minor


def install_home_assistant(state: Path, version: str | None) -> str:
    """Create the instance's own environment and return the installed version."""
    venv = state / "venv"
    # --no-config: the repository's own uv settings, such as a test pin of
    # Home Assistant, must not decide what the instance runs.
    run("uv", "venv", "--quiet", "--no-config", "--python", PYTHON, venv)
    spec = f"homeassistant=={version}" if version else "homeassistant"
    python = venv / "bin" / "python"
    run("uv", "pip", "install", "--quiet", "--no-config", "--python", python, spec)
    installed = run(
        python,
        "-c",
        "from homeassistant.const import __version__; print(__version__)",
    )
    if (stored := http_store_version(python)) != HTTP_STORE_VERSION:
        raise SystemExit(
            f"Home Assistant {installed} stores its web-server settings in another "
            f"form ({stored}), so this script cannot keep it to {HOST}"
        )
    return installed


def wait_for(
    running: Callable[[], bool], pid: int, state: Path, ready: Any, what: str
) -> Any:
    """Poll ``ready`` until it returns a value, while Home Assistant runs."""
    port = int((state / "port").read_text())
    deadline = time.monotonic() + START_TIMEOUT_S
    while time.monotonic() < deadline:
        if not running():
            raise SystemExit(
                f"Home Assistant stopped while waiting for {what}; "
                f"see {state / 'hass.out'}"
            )
        if problems := listening_problems(sockets(pid), port):
            raise SystemExit(f"The instance listens beyond {HOST}:{port}: {problems}")
        try:
            value = ready()
        except OSError, urllib.error.URLError, ValueError:
            value = None
        if value:
            return value
        time.sleep(1)
    raise SystemExit(f"Timed out waiting for {what}; see {state / 'hass.out'}")


def onboard(port: int) -> str:
    """Create the owner with a password that is not kept; return a token."""
    user = call(
        port,
        "POST",
        "/api/onboarding/users",
        data={
            "client_id": client_id(port),
            "name": "Live test",
            "username": "live-test",
            "password": secrets.token_urlsafe(32),
            "language": "en",
        },
    )
    tokens = call(
        port,
        "POST",
        "/auth/token",
        form={
            "grant_type": "authorization_code",
            "code": user["auth_code"],
            "client_id": client_id(port),
        },
    )
    return str(tokens["refresh_token"])


def write_onboarding_store(config: Path) -> None:
    """Mark every onboarding step but the owner's done before the first start."""
    storage = config / ".storage"
    storage.mkdir()
    (storage / "onboarding").write_text(
        json.dumps(
            {
                "version": 4,
                "minor_version": 1,
                "key": "onboarding",
                "data": {"done": list(SKIPPED_ONBOARDING)},
            }
        )
    )


def http_settings(port: int) -> dict[str, Any]:
    """Return the confirmed web-server settings: this computer only."""
    return {
        "server_host": [HOST],
        "server_port": port,
        "cors_allowed_origins": [],
        "login_attempts_threshold": -1,
        "ip_ban_enabled": True,
        "ssl_profile": "modern",
        "use_x_frame_options": True,
        "created_at": "2026-01-01T00:00:00+00:00",
        "error": None,
        "error_message": None,
    }


def write_http_store(config: Path, port: int) -> None:
    """Store the web-server settings as confirmed, so nothing can revert them."""
    major, minor = HTTP_STORE_VERSION
    (config / ".storage" / "http").write_text(
        json.dumps(
            {
                "version": major,
                "minor_version": minor,
                "key": "http",
                "data": {
                    "stable": http_settings(port),
                    "pending": None,
                    "yaml_migration_done": True,
                },
            }
        )
    )


def http_store_problems(config: Path, port: int) -> list[str]:
    """Return what in the stored web-server settings is not this computer only."""
    store = config / ".storage" / "http"
    if not store.is_file():
        return ["no stored web-server settings"]
    data = json.loads(store.read_text()).get("data", {})
    problems = []
    if data.get("pending") is not None:
        problems.append("an unconfirmed change is pending")
    stable = data.get("stable", {})
    if stable.get("server_host") != [HOST] or stable.get("server_port") != port:
        problems.append(
            f"stored address {stable.get('server_host')}:{stable.get('server_port')}"
        )
    return problems


def analytics_off(config: Path) -> bool:
    """Return whether analytics would send nothing."""
    store = config / ".storage" / "core.analytics"
    if not store.is_file():
        return True
    data = json.loads(store.read_text()).get("data", {})
    return not (data.get("onboarded") and data.get("preferences", {}).get("base"))


def access_token(state: Path, port: int) -> str:
    """Return a fresh access token from the stored refresh token."""
    refresh = json.loads((state / "auth.json").read_text())["refresh_token"]
    tokens = call(
        port,
        "POST",
        "/auth/token",
        form={
            "grant_type": "refresh_token",
            "refresh_token": refresh,
            "client_id": client_id(port),
        },
    )
    return str(tokens["access_token"])


def sockets(pid: int) -> list[str]:
    """Return ``lsof``'s lines for the internet sockets ``pid`` has open."""
    lsof = shutil.which("lsof")
    if lsof is None:
        raise SystemExit("lsof is needed to check what the instance listens on")
    result = subprocess.run(  # noqa: S603
        [lsof, "-nP", "-a", "-p", str(pid), "-i"],
        check=False,
        capture_output=True,
        text=True,
    )
    # lsof exits 1, with no output, when the process has no socket open yet.
    if result.returncode not in {0, 1}:
        raise SystemExit(f"lsof failed: {result.stderr.strip()}")
    return result.stdout.splitlines()[1:]


def _socket_fields(line: str) -> tuple[str, str, str]:
    """Return the protocol, the name and the local address of one lsof line."""
    fields = line.split()
    protocol = fields[7] if len(fields) > 7 else ""  # noqa: PLR2004
    name = " ".join(fields[8:])
    return protocol, name, name.split("->", 1)[0].split(" ", 1)[0]


def listening_problems(lines: list[str], port: int) -> list[str]:
    """Return every socket that listens beyond ``HOST:port`` or for discovery."""
    problems = []
    for line in lines:
        protocol, name, local = _socket_fields(line)
        if "(LISTEN)" in name and local != f"{HOST}:{port}":
            problems.append(name)
        elif protocol == "UDP" and local.rsplit(":", 1)[-1] in DISCOVERY_PORTS:
            problems.append(f"UDP {name}")
    return problems


def listens_on(lines: list[str], port: int) -> bool:
    """Return whether the lines show the web server listening on ``HOST:port``."""
    return any(
        protocol == "TCP" and name == f"{HOST}:{port} (LISTEN)"
        for protocol, name, _ in map(_socket_fields, lines)
    )


def check_contained(
    state: Path, running: Callable[[], bool], pid: int, port: int, domain: str
) -> str:
    """Fail unless the instance is running, contained and offers the domain."""
    token = access_token(state, port)
    config = wait_for(
        running,
        pid,
        state,
        lambda: (
            (c := call(port, "GET", "/api/config", token=token))["state"] == "RUNNING"
            and c
        ),
        "Home Assistant to finish starting",
    )
    if config.get("recovery_mode"):
        raise SystemExit("Home Assistant started in recovery mode")
    loaded = {component.split(".", 1)[0] for component in config["components"]}
    if forbidden := sorted(loaded & NOT_ALLOWED):
        raise SystemExit(f"Integrations that must not load are loaded: {forbidden}")
    entries = call(port, "GET", "/api/config/config_entries/entry", token=token)
    if extra := sorted({entry["domain"] for entry in entries} - SYSTEM_ENTRIES):
        raise SystemExit(f"Unexpected config entries: {extra}")
    if not analytics_off(state / "config"):
        raise SystemExit("Analytics is turned on")
    if problems := http_store_problems(state / "config", port):
        raise SystemExit(f"Web-server settings are not this computer only: {problems}")
    handlers = call(
        port,
        "GET",
        "/api/config/config_entries/flow_handlers",
        token=token,
    )
    if domain not in handlers:
        raise SystemExit(f"{domain} is not offered in Add integration")
    lines = sockets(pid)
    if problems := listening_problems(lines, port):
        raise SystemExit(f"The instance listens beyond {HOST}:{port}: {problems}")
    if not listens_on(lines, port):
        raise SystemExit(
            f"lsof does not show the instance listening on {HOST}:{port}, "
            "so it cannot be shown to be contained"
        )
    return str(config["version"])


def build(state: Path, domain: str, ref: str | None, version: str | None) -> str:
    """Write the whole instance into ``state``; return the version installed."""
    source = install_integration(state, domain, ref)
    say(f"Integration {domain}: {source}")
    say("Installing Home Assistant (the first run downloads it)...")
    installed = install_home_assistant(state, version)
    (state / "config" / "configuration.yaml").write_text(configuration_yaml(domain))
    write_onboarding_store(state / "config")
    write_http_store(state / "config", int((state / "port").read_text()))
    return installed


def start(ref: str | None, version: str | None, port: int) -> None:
    """Build, start and check a new instance."""
    domain = integration_domain()
    state = state_dir(domain)
    if state.exists():
        raise SystemExit(f"{state} exists: run 'stop' first")
    if port_in_use(port):
        raise SystemExit(f"Something already listens on {HOST}:{port}")
    if shutil.which("uv") is None:
        raise SystemExit("uv is needed: https://docs.astral.sh/uv/")
    state.mkdir(mode=0o700)
    (state / MARKER).write_text("Made by scripts/live_ha.py; 'stop' deletes it.\n")
    (state / "port").write_text(str(port))
    try:
        installed = build(state, domain, ref, version)
        # The install can take minutes: if the port was taken meanwhile, Home
        # Assistant would fall back to its defaults, every address included.
        if port_in_use(port):
            raise SystemExit(f"Something started listening on {HOST}:{port}")
    except BaseException:
        shutil.rmtree(state)
        raise
    with (state / "hass.out").open("wb") as out:
        process = subprocess.Popen(  # noqa: S603
            [state / "venv" / "bin" / "hass", "--config", state / "config"],
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    (state / "hass.pid").write_text(str(process.pid))
    say(f"Starting Home Assistant {installed} (pid {process.pid})...")

    def running() -> bool:
        # poll() also collects an exited Home Assistant, which a signal-0
        # probe would still find.
        return process.poll() is None

    try:
        wait_for(
            running,
            process.pid,
            state,
            lambda: call(port, "GET", "/api/onboarding") is not None,
            "the web server",
        )
        auth = state / "auth.json"
        auth.write_text(json.dumps({"refresh_token": onboard(port)}))
        auth.chmod(0o600)
        running_version = check_contained(state, running, process.pid, port, domain)
    except BaseException:
        terminate(process.pid, running)
        say(f"Stopped Home Assistant; its output is in {state / 'hass.out'}")
        say("'stop' deletes the folder")
        raise
    url = f"http://{HOST}:{port}"
    say(
        f"Home Assistant {running_version} is running at {url}, "
        "reachable from here only."
    )
    say(f"Add the integration: {url}/config/integrations/dashboard/add?domain={domain}")
    say(f"Log: {state / 'config' / 'home-assistant.log'}")
    say("Stop it and delete everything: uv run python scripts/live_ha.py stop")


def gone_within(running: Callable[[], bool], seconds: float) -> bool:
    """Wait up to ``seconds`` for ``running`` to turn false; return whether it did."""
    deadline = time.monotonic() + seconds
    while running():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)
    return True


def terminate(pid: int, running: Callable[[], bool]) -> None:
    """Stop Home Assistant's process group, forcibly if it does not stop."""
    if not running():
        return
    signal_group(pid, signal.SIGTERM)
    if not gone_within(running, STOP_TIMEOUT_S):
        signal_group(pid, signal.SIGKILL)
        # So that stop sees the port closed once Home Assistant has gone.
        gone_within(running, KILL_TIMEOUT_S)


def remove_entries(state: Path, port: int, domain: str) -> int:
    """Remove the domain's config entries and return how many there were."""
    token = access_token(state, port)
    entries = call(
        port, "GET", f"/api/config/config_entries/entry?domain={domain}", token=token
    )
    for entry in entries:
        call(
            port,
            "DELETE",
            f"/api/config/config_entries/entry/{entry['entry_id']}",
            token=token,
        )
    return len(entries)


def stop() -> None:
    """Remove the integration, stop the instance and delete its folder."""
    domain = integration_domain()
    state = state_dir(domain)
    if not state.exists():
        say(f"Nothing to stop: {state} does not exist")
        return
    if not (state / MARKER).is_file():
        raise SystemExit(f"{state} was not made by this script; not deleting it")
    port = int((state / "port").read_text())
    pid_file = state / "hass.pid"
    pid = int(pid_file.read_text()) if pid_file.is_file() else None
    if pid is not None and is_instance(pid, state):
        try:
            say(f"Removed {remove_entries(state, port, domain)} {domain} entries")
        except (OSError, urllib.error.URLError, KeyError, ValueError) as err:
            say(f"Could not remove the {domain} entries ({err}); deleting anyway")
        terminate(pid, lambda: is_instance(pid, state))
        say(f"Stopped Home Assistant (pid {pid})")
    elif pid is not None:
        say(f"Home Assistant is not running (pid {pid} is not this instance)")
    if port_in_use(port):
        raise SystemExit(f"Something still listens on {HOST}:{port}; not deleting")
    shutil.rmtree(state)
    say(f"Deleted {state}")


def port_number(value: str) -> int:
    """Return ``value`` as an unprivileged port, or reject it."""
    port = int(value)
    if port not in PORTS:
        message = f"use a port from {PORTS.start} to {PORTS.stop - 1}"
        raise argparse.ArgumentTypeError(message)
    return port


def main(argv: list[str] | None = None) -> None:
    """Parse the command line and run ``start`` or ``stop``."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0].strip())
    commands = parser.add_subparsers(dest="command", required=True)
    start_parser = commands.add_parser("start", help="build and start an instance")
    start_parser.add_argument("--ref", help="a release tag or commit to test")
    start_parser.add_argument("--ha-version", help="default: the newest release")
    start_parser.add_argument("--port", type=port_number, default=DEFAULT_PORT)
    commands.add_parser("stop", help="stop the instance and delete it")
    args = parser.parse_args(argv)
    if args.command == "start":
        start(args.ref, args.ha_version, args.port)
    else:
        stop()


if __name__ == "__main__":
    main(sys.argv[1:])

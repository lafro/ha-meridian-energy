"""
A stand-in Meridian inside a disposable Home Assistant, for the journeys.

Test-only, never shipped. ``scripts/journeys.py`` copies this module into the
instance's site-packages as the package ``meridian_standin``, with the tests'
fakes (``meridian_http.py``, ``kraken_fake.py``) beside it, and a ``.pth``
file that calls ``install_or_exit`` when the interpreter starts: before Home
Assistant or the integration imports anything, after a restart too. If the
stand-in cannot be imported or installed, the interpreter exits at once, so
Home Assistant never runs without it.

``install`` patches the process's ways out:

- ``aiohttp.ClientSession._request``, which every aiohttp request goes
  through, whatever its method. A request to one of the integration's hosts
  (the URLs in its ``const.py``) is answered by
  ``meridian_http.FakeMeridianSession``; a request to this computer
  (127.0.0.1, ::1 or localhost) goes ahead; any other is refused.
- TCP connections (``socket.socket.connect`` and ``connect_ex``) to any
  address but this computer's, and name lookups (``socket.getaddrinfo``,
  ``gethostbyname`` and ``gethostbyname_ex``) of any name but this
  computer's, so a library that does not use aiohttp cannot reach past
  this computer either.

It does not cover UDP, reverse lookups (``gethostbyaddr``, ``getnameinfo``),
resolvers written in C, or other programs Home Assistant runs, such as the
uv that installs its pinned requirements at its first start.

A refused request raises a connection error, and appends one line to
``refused.log`` in the stand-in's folder naming its kind and host only,
never its path or query. Every request the fake answers appends one JSON
line to ``journal.jsonl``: when, the operation, the synthetic account, the
direction and dates of a measurements window, and the status, or the fake's
error when it could not answer (such as a request the integration must never
send). ``installed-<pid>`` marks each process the stand-in is installed in.

The scenario is read from ``scenario.json`` in the folder on each request,
so the runner can change it between steps without any API (``Scenario``):
the login's accounts and which have a feed-in register, measurements that
fail with a status (a JSON error, or an HTML page as a gateway sends), and
a session Meridian has ended (every authenticated request refused with 401
and the session refresh rejected with 400).

The data is a function of the hour alone (``hour_reading``, ``hour_cost``),
from a fixed ``HISTORY_START`` to the end of today in New Zealand, whatever
the time of the request or the requests before it, so a restarted Home
Assistant sees the same history. Neighbouring hours differ, so an hour
shifted or counted twice shows in the statistics.
"""

from __future__ import annotations

import asyncio
import errno
import ipaddress
import json
import logging
import os
import socket
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from .meridian_http import FakeMeridianSession, Response

_LOGGER = logging.getLogger(__name__)

PACKAGE = "meridian_standin"
# Beside the installed package: {"folder": "<the stand-in's folder>"}.
SETTINGS = "settings.json"
SCENARIO = "scenario.json"
JOURNAL = "journal.jsonl"
REFUSED = "refused.log"
INSTALLED = "installed-{pid}"
# The exit status of an interpreter the stand-in could not be installed in.
EXIT_NOT_INSTALLED = 70
# The ``.pth`` line, run as the interpreter starts. If the stand-in cannot be
# imported or installed, the interpreter exits before Home Assistant can
# start: Python would only print a failed import in a ``.pth`` line, and go on.
PTH_LINE = (
    "import os; exec("
    + repr(
        f"try:\n import {PACKAGE}\n {PACKAGE}.install_or_exit()\n"
        f"except BaseException:\n os._exit({EXIT_NOT_INSTALLED})"
    )
    + ")\n"
)
# New Zealand midnight at the start of 1 January 2024, in UTC.
HISTORY_START = datetime(2023, 12, 31, 11, tzinfo=UTC)
CONSUMPTION = "CONSUMPTION"
GENERATION = "GENERATION"
_LOCAL_NAMES = frozenset({"localhost", "localhost.", "ip6-localhost"})
# The variables a journal record keeps, by its own name: synthetic values only.
_JOURNALLED = {
    "account": "accountNumber",
    "direction": "readingDirectionType",
    "start_on": "startOn",
    "end_on": "endOn",
}
_GATEWAY_PAGE = (
    b"<html><head><title>504 Gateway Time-out</title></head>"
    b"<body><h1>504 Gateway Time-out</h1><p>Synthetic gateway page.</p>"
    b"</body></html>"
)


def hour_reading(start: datetime, channel: int) -> str:
    """
    Return a synthetic reading, in kWh, that depends only on its hour.

    From 0.25 to 1.35 kWh: never zero, and each hour differs from the next.
    """
    hour = int(start.timestamp()) // 3600
    return f"{Decimal('0.25') + Decimal((hour * 7 + channel * 5) % 23) / 20:.2f}"


def hour_cost(start: datetime, channel: int) -> str:
    """Return the hour's synthetic cost, in cents: 28.5 cents a kWh."""
    return f"{Decimal(hour_reading(start, channel)) * Decimal('28.5'):.3f}"


@dataclass(frozen=True, slots=True)
class Scenario:
    """
    How the stand-in answers, read from ``scenario.json`` on each request.

    ``measurements_status``, when set, is the status measurements requests
    for ``failing_directions`` get: with a JSON error body, or with
    ``measurements_page`` "html", a gateway's HTML page. ``session_ended``
    refuses every authenticated request (401) and rejects the session
    refresh (400), as when Meridian ends the sign-in.
    """

    accounts: tuple[str, ...] = ("A-TEST-1",)
    feed_in: tuple[str, ...] = ()
    measurements_status: int | None = None
    measurements_page: str = "json"
    failing_directions: tuple[str, ...] = (CONSUMPTION, GENERATION)
    session_ended: bool = False

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Self:
        """Return the scenario a JSON object describes; missing keys keep defaults."""
        default = cls()
        status = data.get("measurements_status")
        page = str(data.get("measurements_page", default.measurements_page))
        if page not in {"json", "html"}:
            raise ValueError("measurements_page is json or html")
        return cls(
            accounts=tuple(
                str(item) for item in data.get("accounts", default.accounts)
            ),
            feed_in=tuple(str(item) for item in data.get("feed_in", default.feed_in)),
            measurements_status=None if status is None else int(status),
            measurements_page=page,
            failing_directions=tuple(
                str(item)
                for item in data.get("failing_directions", default.failing_directions)
            ),
            session_ended=bool(data.get("session_ended", default.session_ended)),
        )

    def to_json(self) -> dict[str, Any]:
        """Return the scenario as a JSON object."""
        return asdict(self)


def is_local(host: str | bytes | None) -> bool:
    """Return whether a host names this computer: loopback, or localhost."""
    if host is None:
        return False
    name = (host.decode() if isinstance(host, bytes) else host).strip("[]").lower()
    if name in _LOCAL_NAMES:
        return True
    try:
        return ipaddress.ip_address(name.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def needs_lookup(host: str | bytes | None) -> bool:
    """Return whether resolving a host would ask a name server."""
    if host is None or is_local(host):
        return False
    name = (host.decode() if isinstance(host, bytes) else host).strip("[]")
    try:
        ipaddress.ip_address(name.split("%", 1)[0])
    except ValueError:
        return name != ""
    return False


def _append(path: Path, line: str) -> None:
    """Append one line with a single write, as several processes may append."""
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.write(descriptor, line.encode())
    finally:
        os.close(descriptor)


@dataclass(slots=True)
class StandIn:
    """Answer the integration's requests from the fakes; refuse everything else."""

    folder: Path
    session: FakeMeridianSession | None = None
    _hosts: frozenset[str] = field(default_factory=frozenset)

    def scenario(self) -> Scenario:
        """Return the scenario in the folder, or the default one."""
        path = self.folder / SCENARIO
        if not path.is_file():
            return Scenario()
        return Scenario.from_json(json.loads(path.read_text()))

    def refuse(self, kind: str, host: str | bytes | None) -> None:
        """Record a refused request on one line, with its host only."""
        name = host.decode(errors="replace") if isinstance(host, bytes) else host
        at = datetime.now(UTC).isoformat(timespec="seconds")
        _append(self.folder / REFUSED, f"{at} {kind} {name}\n")
        _LOGGER.warning("The journeys stand-in refused %s to %s", kind, name)

    def journal(self, record: dict[str, Any]) -> None:
        """Record one answered request."""
        _append(self.folder / JOURNAL, json.dumps(record, sort_keys=True) + "\n")

    def mark_installed(self) -> None:
        """Mark this process as one the stand-in is installed in."""
        self.folder.mkdir(parents=True, exist_ok=True)
        (self.folder / INSTALLED.format(pid=os.getpid())).write_text("")

    def answers(self, host: str) -> bool:
        """Return whether the fake answers requests to this host."""
        if not self._hosts:
            try:
                self._hosts = _integration_hosts()
            except ImportError:
                # The integration cannot be imported yet, so no request can be
                # its own: nothing goes out meanwhile.
                return False
        return host.lower() in self._hosts

    def _load(self) -> tuple[Scenario, Any]:
        """Read the scenario and import the fakes; runs in a worker thread."""
        from . import meridian_http  # noqa: PLC0415 - needs the integration

        return self.scenario(), meridian_http

    async def answer(self, method: str, url: str, options: dict[str, Any]) -> Response:
        """Answer one request to the integration's hosts from the fake."""
        scenario, http = await asyncio.to_thread(self._load)
        session = self._prepare(scenario, http)
        record = _describe(http, url, options)
        try:
            if method.upper() != "POST":
                raise AssertionError(f"unexpected method {method.upper()}")
            response: Response = session.post(url, **options)
        except AssertionError as err:
            record["error"] = str(err)
            await asyncio.to_thread(self.journal, record)
            raise _refusal("the fake could not answer this request") from None
        record["status"] = response.status
        await asyncio.to_thread(self.journal, record)
        return response

    def _prepare(self, scenario: Scenario, http: Any) -> FakeMeridianSession:
        """Return the fake, set to the scenario and the time of the request."""
        now = datetime.now(UTC)
        if self.session is None:
            self.session = http.FakeMeridianSession(
                now,
                kraken=http.FakeKraken(
                    now,
                    history_start=HISTORY_START,
                    value=hour_reading,
                    cost=hour_cost,
                    repeat_cursors=True,
                ),
            )
        session = self.session
        assert session is not None
        session.kraken.now = now
        session.accounts = scenario.accounts
        session.feed_in = frozenset(scenario.feed_in)
        session.hook = partial(_graphql_answer, scenario, http)
        session.auth_hook = partial(_auth_answer, scenario, http)
        return session


def _integration_hosts() -> frozenset[str]:
    """Return the hosts of every URL the integration sends requests to."""
    from custom_components.meridian_energy import const  # noqa: PLC0415

    urls = (
        const.AUTH_EMAIL_URL,
        const.AUTH_OTP_URL,
        const.FIREBASE_CUSTOM_TOKEN_URL,
        const.FIREBASE_REFRESH_URL,
        const.GRAPHQL_URL,
    )
    return frozenset(str(urlsplit(url).hostname) for url in urls)


def _describe(http: Any, url: str, options: dict[str, Any]) -> dict[str, Any]:
    """Return a journal record of a request: synthetic values and dates only."""
    record: dict[str, Any] = {"at": datetime.now(UTC).isoformat()}
    if (operation := http.auth_operation(url)) is not None:
        record["operation"] = operation
        return record
    body = options.get("json") or {}
    name = str(body.get("operationName", "-"))
    record["operation"] = "measurements" if name == http.MEASUREMENTS_NAME else name
    variables = body.get("variables") or {}
    record.update(
        {
            key: variables[name]
            for key, name in _JOURNALLED.items()
            if variables.get(name) is not None
        }
    )
    if variables.get("before") is not None:
        record["continuation"] = True
    return record


def _graphql_answer(
    scenario: Scenario, http: Any, operation: str, variables: dict[str, Any]
) -> Response | None:
    """Return the scenario's answer to a GraphQL request, or None for the fake's."""
    if scenario.session_ended:
        response: Response = http.json_response(
            {"error": "synthetic session ended"}, status=401
        )
        return response
    if (
        operation == "measurements"
        and scenario.measurements_status is not None
        and variables.get("readingDirectionType") in scenario.failing_directions
    ):
        if scenario.measurements_page == "html":
            page: Response = http.Response(
                _GATEWAY_PAGE,
                status=scenario.measurements_status,
                headers={"Content-Type": "text/html"},
            )
            return page
        error: Response = http.json_response(
            {"errors": [{"message": "synthetic service error"}]},
            status=scenario.measurements_status,
        )
        return error
    return None


def _auth_answer(scenario: Scenario, http: Any, operation: str) -> Response | None:
    """Return the scenario's answer to a login or session request, or None."""
    if scenario.session_ended and operation == "session_refresh":
        response: Response = http.json_response(
            {"error": {"code": 400, "message": "TOKEN_EXPIRED"}}, status=400
        )
        return response
    return None


def _refusal(reason: str) -> Exception:
    """Return the connection error a refused aiohttp request raises."""
    import aiohttp  # noqa: PLC0415

    return aiohttp.ClientConnectionError(f"Refused by the journeys stand-in: {reason}")


def _request_patch(standin: StandIn, original: Any) -> Any:
    """Return ``ClientSession._request`` as the stand-in has it."""

    async def request(
        session: Any, method: str, str_or_url: Any, **options: Any
    ) -> Any:
        url = str(str_or_url)
        host = urlsplit(url).hostname or ""
        if is_local(host):
            return await original(session, method, str_or_url, **options)
        if standin.answers(host):
            return await standin.answer(method, url, options)
        standin.refuse("request", host)
        raise _refusal(host)

    return request


def _connect_patches(
    standin: StandIn, connect: Any, connect_ex: Any
) -> tuple[Any, Any]:
    """Return ``socket.connect`` and ``connect_ex`` as the stand-in has them."""

    def guard(sock: socket.socket, address: Any) -> None:
        if sock.family not in {socket.AF_INET, socket.AF_INET6}:
            return
        if sock.type != socket.SOCK_STREAM:
            return
        host = address[0] if isinstance(address, tuple) else address
        if not is_local(host):
            standin.refuse("connection", host)
            raise ConnectionRefusedError(
                errno.ECONNREFUSED, "Refused by the journeys stand-in"
            )

    def guarded_connect(sock: socket.socket, address: Any) -> None:
        guard(sock, address)
        connect(sock, address)

    def guarded_connect_ex(sock: socket.socket, address: Any) -> int:
        try:
            guard(sock, address)
        except ConnectionRefusedError:
            return errno.ECONNREFUSED
        result: int = connect_ex(sock, address)
        return result

    return guarded_connect, guarded_connect_ex


def _lookup_patch(standin: StandIn, original: Any) -> Any:
    """Return a name lookup that refuses any name but this computer's."""

    def lookup(host: Any, *args: Any, **kwargs: Any) -> Any:
        if needs_lookup(host):
            standin.refuse("lookup", host)
            raise socket.gaierror(socket.EAI_NONAME, "Refused by the journeys stand-in")
        return original(host, *args, **kwargs)

    return lookup


def patch(standin: StandIn) -> Callable[[], None]:
    """Patch aiohttp and the socket module; return a function that undoes it."""
    import aiohttp  # noqa: PLC0415

    session = aiohttp.ClientSession
    connect, connect_ex = _connect_patches(
        standin, socket.socket.connect, socket.socket.connect_ex
    )
    replacements: list[tuple[Any, str, Any, Any]] = [
        (
            session,
            "_request",
            session._request,
            _request_patch(standin, session._request),
        ),
        (socket.socket, "connect", socket.socket.connect, connect),
        (socket.socket, "connect_ex", socket.socket.connect_ex, connect_ex),
    ]
    replacements += [
        (
            socket,
            name,
            getattr(socket, name),
            _lookup_patch(standin, getattr(socket, name)),
        )
        for name in ("getaddrinfo", "gethostbyname", "gethostbyname_ex")
    ]
    for owner, name, _original, replacement in replacements:
        setattr(owner, name, replacement)

    def undo() -> None:
        for owner, name, original, _replacement in replacements:
            setattr(owner, name, original)

    return undo


def install(folder: Path | None = None) -> StandIn:
    """Install the stand-in in this process, with the folder from its settings."""
    if folder is None:
        settings = json.loads(Path(__file__).with_name(SETTINGS).read_text())
        folder = Path(settings["folder"])
    standin = StandIn(folder)
    patch(standin)
    standin.mark_installed()
    return standin


def install_or_exit(exit_now: Callable[[int], object] = os._exit) -> None:
    """Install the stand-in, or end the interpreter before anything else runs."""
    try:
        install()
    except BaseException:
        _LOGGER.exception("The journeys stand-in could not be installed")
        exit_now(EXIT_NOT_INSTALLED)

"""Contracts for the stand-in Meridian the journeys run Home Assistant against."""

from __future__ import annotations

import errno
import json
import os
import socket
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import pytest

from custom_components.meridian_energy.const import (
    AUTH_EMAIL_URL,
    FIREBASE_API_KEY,
    FIREBASE_REFRESH_URL,
    GRAPHQL_URL,
)

from . import meridian_standin
from .meridian_http import MEASUREMENTS_NAME, MEASUREMENTS_URL, property_id
from .meridian_standin import (
    HISTORY_START,
    Scenario,
    StandIn,
    hour_cost,
    hour_reading,
    is_local,
    needs_lookup,
)

_HOUR = timedelta(hours=1)


@pytest.fixture
def folder(tmp_path: Path) -> Path:
    path = tmp_path / "standin"
    path.mkdir()
    return path


@pytest.fixture
def local_requests(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stand in for aiohttp's own request, which a local request reaches."""
    sent: list[str] = []

    async def original(
        _session: aiohttp.ClientSession, method: str, url: Any, **_options: Any
    ) -> str:
        sent.append(f"{method} {url}")
        return "sent"

    monkeypatch.setattr(aiohttp.ClientSession, "_request", original)
    return sent


@pytest.fixture
def standin(folder: Path, local_requests: list[str]) -> Iterator[StandIn]:
    del local_requests
    installed = StandIn(folder)
    undo = meridian_standin.patch(installed)
    try:
        yield installed
    finally:
        undo()


def _scenario(folder: Path, scenario: Scenario) -> None:
    (folder / meridian_standin.SCENARIO).write_text(json.dumps(scenario.to_json()))


def _journal(folder: Path) -> list[dict[str, Any]]:
    path = folder / meridian_standin.JOURNAL
    return [json.loads(line) for line in path.read_text().splitlines()]


def _refused(folder: Path) -> list[str]:
    path = folder / meridian_standin.REFUSED
    if not path.is_file():
        return []
    return [line.split(" ", 1)[1] for line in path.read_text().splitlines()]


async def _post(url: str, **options: Any) -> tuple[int, bytes]:
    async with (
        aiohttp.ClientSession() as session,
        session.post(url, **options) as response,
    ):
        return response.status, await response.read()


def _graphql(name: str, variables: dict[str, Any]) -> dict[str, Any]:
    return {
        "operationName": name,
        "query": f"query {name}($x: Int) {{ x }}",
        "variables": variables,
    }


def _window(account: str, direction: str = "CONSUMPTION") -> dict[str, Any]:
    today = datetime.now(UTC).date()
    return {
        "accountNumber": account,
        "propertyId": property_id(account),
        "startOn": (today - timedelta(days=6)).isoformat(),
        "endOn": today.isoformat(),
        "last": 744,
        "readingFrequencyType": "HOUR_INTERVAL",
        "readingDirectionType": direction,
        "readingQualityType": "COMBINED",
    }


def test_the_data_depends_on_the_hour_alone() -> None:
    hour = datetime(2026, 7, 16, 12, tzinfo=UTC)
    readings = [Decimal(hour_reading(hour + n * _HOUR, 0)) for n in range(48)]
    assert readings == [Decimal(hour_reading(hour + n * _HOUR, 0)) for n in range(48)]
    assert min(readings) >= Decimal("0.25")
    assert max(readings) <= Decimal("1.35")
    assert all(a != b for a, b in pairwise(readings))
    assert Decimal(hour_cost(hour, 0)) == readings[0] * Decimal("28.5")
    # Midnight in New Zealand at the start of a date.
    assert HISTORY_START.astimezone(ZoneInfo("Pacific/Auckland")).hour == 0


def test_a_scenario_round_trips_and_keeps_defaults() -> None:
    scenario = Scenario(
        accounts=("A-TEST-1", "A-TEST-2"),
        feed_in=("A-TEST-2",),
        measurements_status=504,
        measurements_page="html",
        failing_directions=("GENERATION",),
        session_ended=True,
    )
    assert Scenario.from_json(json.loads(json.dumps(scenario.to_json()))) == scenario
    assert Scenario.from_json({}) == Scenario()
    with pytest.raises(ValueError, match="json or html"):
        Scenario.from_json({"measurements_page": "xml"})


@pytest.mark.parametrize(
    ("host", "local", "lookup"),
    [
        ("127.0.0.1", True, False),
        ("127.8.9.10", True, False),
        ("::1", True, False),
        ("[::1]", True, False),
        ("localhost", True, False),
        (b"localhost", True, False),
        ("192.0.2.1", False, False),
        ("2001:db8::1", False, False),
        ("api.example.invalid", False, True),
        (None, False, False),
        ("", False, False),
    ],
)
def test_only_this_computer_is_local(
    host: str | bytes | None, local: bool, lookup: bool
) -> None:
    assert is_local(host) is local
    assert needs_lookup(host) is lookup


async def test_a_request_to_another_host_is_refused_and_logged(
    standin: StandIn, folder: Path, local_requests: list[str]
) -> None:
    del standin
    with pytest.raises(aiohttp.ClientConnectionError, match="stand-in"):
        await _post("https://updates.example.invalid/path?secret=value")
    assert _refused(folder) == ["request updates.example.invalid"]
    assert "secret" not in (folder / meridian_standin.REFUSED).read_text()
    assert local_requests == []


async def test_a_request_to_this_computer_goes_ahead(
    standin: StandIn, folder: Path, local_requests: list[str]
) -> None:
    del standin
    async with aiohttp.ClientSession() as session:
        answer: Any = await session._request("GET", "http://127.0.0.1:8125/api/")
    assert answer == "sent"
    assert local_requests == ["GET http://127.0.0.1:8125/api/"]
    assert _refused(folder) == []


async def test_the_integrations_requests_are_answered_by_the_fake(
    standin: StandIn, folder: Path
) -> None:
    del standin
    _scenario(folder, Scenario(accounts=("A-TEST-1", "A-TEST-2")))
    status, body = await _post(GRAPHQL_URL, json=_graphql("accountsList", {}))
    assert status == 200
    numbers = [
        item["number"] for item in json.loads(body)["data"]["viewer"]["accounts"]
    ]
    assert numbers == ["A-TEST-1", "A-TEST-2"]
    status, body = await _post(
        MEASUREMENTS_URL, json=_graphql(MEASUREMENTS_NAME, _window("A-TEST-2"))
    )
    assert status == 200
    edges = json.loads(body)["data"]["account"]["property"]["measurements"]["edges"]
    for edge in edges[:5]:
        node = edge["node"]
        start = datetime.fromisoformat(node["startAt"])
        assert node["value"] == hour_reading(start, 0)
    status, _body = await _post(AUTH_EMAIL_URL, json={"email": "person@example.com"})
    assert status == 200
    journal = _journal(folder)
    assert [record["operation"] for record in journal] == [
        "accountsList",
        "measurements",
        "send_code",
    ]
    assert journal[1]["account"] == "A-TEST-2"
    assert journal[1]["direction"] == "CONSUMPTION"
    assert all(record["status"] == 200 for record in journal)
    assert "person@example.com" not in (folder / meridian_standin.JOURNAL).read_text()
    assert _refused(folder) == []


async def test_measurements_fail_as_the_scenario_says(
    standin: StandIn, folder: Path
) -> None:
    del standin
    _scenario(
        folder,
        Scenario(measurements_status=503, failing_directions=("GENERATION",)),
    )
    consumption = _graphql(MEASUREMENTS_NAME, _window("A-TEST-1"))
    generation = _graphql(MEASUREMENTS_NAME, _window("A-TEST-1", "GENERATION"))
    assert (await _post(MEASUREMENTS_URL, json=consumption))[0] == 200
    status, body = await _post(MEASUREMENTS_URL, json=generation)
    assert status == 503
    assert json.loads(body)["errors"]
    _scenario(folder, Scenario(measurements_status=504, measurements_page="html"))
    status, body = await _post(MEASUREMENTS_URL, json=consumption)
    assert status == 504
    assert body.startswith(b"<html>")
    _scenario(folder, Scenario())
    assert (await _post(MEASUREMENTS_URL, json=generation))[0] == 200
    assert [record["status"] for record in _journal(folder)] == [200, 503, 504, 200]


async def test_an_ended_session_refuses_queries_and_the_refresh(
    standin: StandIn, folder: Path
) -> None:
    del standin
    _scenario(folder, Scenario(session_ended=True))
    refresh = f"{FIREBASE_REFRESH_URL}?key={FIREBASE_API_KEY}"
    assert (await _post(GRAPHQL_URL, json=_graphql("accountsList", {})))[0] == 401
    assert (await _post(refresh, data={"grant_type": "refresh_token"}))[0] == 400
    assert (await _post(AUTH_EMAIL_URL, json={}))[0] == 200
    _scenario(folder, Scenario())
    assert (await _post(refresh, data={"grant_type": "refresh_token"}))[0] == 200


async def test_a_request_the_fake_cannot_answer_is_journalled_as_an_error(
    standin: StandIn, folder: Path
) -> None:
    del standin
    with pytest.raises(aiohttp.ClientConnectionError, match="could not answer"):
        await _post(GRAPHQL_URL, json=_graphql("account", {"accountNumber": "A-X"}))
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.ClientConnectionError):
            await session._request("GET", GRAPHQL_URL)
    errors = [record["error"] for record in _journal(folder)]
    assert errors == ["an account the login does not have", "unexpected method GET"]


def test_connections_and_lookups_beyond_this_computer_are_refused(
    socket_enabled: None, standin: StandIn, folder: Path
) -> None:
    # socket_enabled comes first, so the stand-in patches the real socket class. Its
    # setup hook may or may not already have done that, depending on plugin order.
    del standin, socket_enabled
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as outbound:
        with pytest.raises(ConnectionRefusedError):
            outbound.connect(("192.0.2.1", 443))
        assert outbound.connect_ex(("192.0.2.1", 443)) == errno.ECONNREFUSED
    with pytest.raises(socket.gaierror):
        socket.getaddrinfo("api.example.invalid", 443)
    with pytest.raises(socket.gaierror):
        socket.gethostbyname("api.example.invalid")
    with pytest.raises(socket.gaierror):
        socket.gethostbyname_ex("api.example.invalid")
    assert _refused(folder) == [
        "connection 192.0.2.1",
        "connection 192.0.2.1",
        "lookup api.example.invalid",
        "lookup api.example.invalid",
        "lookup api.example.invalid",
    ]
    # This computer stays reachable, by address or by name.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        with socket.create_connection(server.getsockname(), timeout=5):
            pass
    assert socket.getaddrinfo("127.0.0.1", 80)
    assert socket.gethostbyname("localhost")
    assert len(_refused(folder)) == 5


def test_undo_restores_the_originals(folder: Path) -> None:
    before = (
        aiohttp.ClientSession._request,
        socket.socket.connect,
        socket.socket.connect_ex,
        socket.getaddrinfo,
    )
    undo = meridian_standin.patch(StandIn(folder))
    assert socket.getaddrinfo is not before[3]
    undo()
    assert (
        aiohttp.ClientSession._request,
        socket.socket.connect,
        socket.socket.connect_ex,
        socket.getaddrinfo,
    ) == before


def test_install_marks_the_process(
    folder: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    undone: list[bool] = []
    monkeypatch.setattr(
        meridian_standin, "patch", lambda _standin: lambda: undone.append(True)
    )
    installed = meridian_standin.install(folder / "new")
    assert installed.folder == folder / "new"
    assert (folder / "new" / f"installed-{os.getpid()}").is_file()


def test_the_interpreter_exits_when_the_stand_in_cannot_be_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(folder: Path | None = None) -> StandIn:
        del folder
        raise FileNotFoundError("no settings")

    monkeypatch.setattr(meridian_standin, "install", broken)
    exits: list[int] = []
    meridian_standin.install_or_exit(exits.append)
    assert exits == [meridian_standin.EXIT_NOT_INSTALLED]
    assert meridian_standin.PTH_LINE.startswith("import meridian_standin;")

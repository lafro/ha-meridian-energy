"""
Run the setup and lifecycle journeys in a real Home Assistant, end to end.

Usage::

    uv run python scripts/journeys.py [--ref REF] [--ha-version VERSION]
                                      [--port PORT] [--only NAME ...]
    uv run python scripts/journeys.py --clean-up

The journeys run in a new Home Assistant (the newest release, or
``--ha-version``) with the integration from the working tree or ``REF``,
built and checked as ``scripts/live_ha.py`` builds a live-test instance, in
its own folder (``journeys-<domain>`` in the system's temporary directory)
and on its own port (8125 by default), so they never meet a live test.

Meridian is never contacted. A stand-in (``tests/meridian_standin.py``),
installed in the instance's environment before Home Assistant first starts,
answers the integration's requests from the tests' own fake, refuses every
other request that would leave this computer and records it, with its host
only. Every value is synthetic, and so is the sign-in: there is none.

Each journey drives Home Assistant through its REST and websocket APIs, as
the frontend would, with the owner's token, and checks what came of it: the
screens met, the config entry, its devices and sensors, and the integration's
statistics hour by hour against what the stand-in sent. ``--only`` runs some
of them, in their usual order. Each check and each journey is printed as
PASS or FAIL, and a report (markdown) with every check and every screen, as
Home Assistant shows it, is written to the system's temporary directory. The
instance is always stopped and deleted at the end, and the exit status is
non-zero if anything failed. ``--clean-up`` only deletes an instance a run
that was killed left behind.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import sys
import tempfile
import time
import urllib.error
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn
from zoneinfo import ZoneInfo

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # run as a script, not as scripts.journeys
    sys.path.insert(0, str(ROOT))

from scripts import live_ha  # noqa: E402
from tests import meridian_standin as standin  # noqa: E402
from tests.meridian_standin import Scenario  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

NAME = "journeys"
DEFAULT_PORT = 8125
DOMAIN = "meridian_energy"
NZ = ZoneInfo("Pacific/Auckland")
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
INITIAL_BACKFILL = timedelta(days=90)
# What the setup flow makes a retry wait after a failure, and a margin.
RETRY_DELAY = timedelta(seconds=120)
RETRY_MARGIN = timedelta(seconds=3)
# The first update after an import asks for the newest day or two: the tip.
TIP_DAYS = 2
TWO_DEVICES = 2
HTTP_OK = 200
OTP = "123456"
# Readings and costs are compared to the stand-in's within this.
TOLERANCE = 1e-6
POLL_S = 2.0
FLOW_TIMEOUT_S = 600
SYNC_TIMEOUT_S = 1200
REQUEST_TIMEOUT_S = 150
TOKEN_LIFETIME_S = 600
# Problems shown for one statistic before the rest are only counted.
SHOWN_PROBLEMS = 5
# The stand-in's folder, in the instance's configuration folder.
STANDIN_FOLDER = "journeys"
_FAKES = ("meridian_http.py", "kraken_fake.py")


class JourneyError(Exception):
    """A journey could not go on: what it waited for did not happen."""


def say(message: str) -> None:
    """Print one line of progress."""
    print(message, flush=True)  # noqa: T201


# ----------------------------------------------------------------- statistics


def property_key(account: str, property_id: str) -> str:
    """Return the integration's key for a property (``statistics.property_key``)."""
    return hashlib.sha256(f"{account}:{property_id}".encode()).hexdigest()[:12]


def statistic_ids(account: str, *, feed_in: bool = False) -> dict[str, str]:
    """Return the statistic IDs of an account's property, by kind."""
    from tests.meridian_http import property_id  # noqa: PLC0415

    key = property_key(account, property_id(account))
    kinds = ["consumption", "consumption_cost"]
    if feed_in:
        kinds += ["generation", "generation_credit"]
    return {kind: f"{DOMAIN}:{kind}_{key}" for kind in kinds}


def expected_value(kind: str, start: datetime) -> float:
    """Return what the stand-in sent for an hour: kWh, or NZD for money."""
    if kind.endswith(("_cost", "_credit")):
        return float(Decimal(standin.hour_cost(start, 0)) / 100)
    return float(standin.hour_reading(start, 0))


@dataclass(frozen=True, slots=True)
class Row:
    """One hour of a statistic."""

    start: datetime
    state: float
    sum: float


def parse_rows(raw: Iterable[dict[str, Any]]) -> list[Row]:
    """Return a statistic's rows from the recorder's websocket answer."""
    return [
        Row(
            datetime.fromtimestamp(item["start"] / 1000, UTC),
            float(item.get("state") or 0.0),
            float(item.get("sum") or 0.0),
        )
        for item in raw
    ]


def ceil_hour(value: datetime) -> datetime:
    """Return the start of the first whole hour at or after a time."""
    floored = value.replace(minute=0, second=0, microsecond=0)
    return floored if floored == value else floored + HOUR


def floor_hour(value: datetime) -> datetime:
    """Return the start of the hour a time is in."""
    return value.replace(minute=0, second=0, microsecond=0)


def backfill_start(now: datetime) -> datetime:
    """Return New Zealand midnight at the start of the date 90 days back."""
    day = (now - INITIAL_BACKFILL).astimezone(NZ).date()
    return datetime.combine(day, datetime.min.time(), tzinfo=NZ).astimezone(UTC)


@dataclass(frozen=True, slots=True)
class Expected:
    """
    What a statistic's history should hold.

    Its first hour is the first whole hour of the 90 days before an import
    that ran between ``imported_from`` and ``imported_to``, or, once a
    lane has completed that date, New Zealand midnight at its start; its
    newest is the last whole hour before ``newest_by``, or the one before.
    """

    imported_from: datetime
    imported_to: datetime
    newest_by: datetime

    @property
    def first_range(self) -> tuple[datetime, datetime]:
        """Return the earliest and latest first hour allowed."""
        return (
            backfill_start(self.imported_from),
            ceil_hour(self.imported_to - INITIAL_BACKFILL),
        )


def _hour_problems(kind: str, rows: Sequence[Row]) -> list[str]:
    """Return the hours that are not above zero, or not what the stand-in sent."""
    problems: list[str] = []
    for row in rows:
        when = f"{row.start:%Y-%m-%dT%H:%M}Z"
        if row.state <= 0:
            problems.append(f"{when} holds {row.state}, not above zero")
        sent = expected_value(kind, row.start)
        if abs(row.state - sent) > TOLERANCE:
            problems.append(f"{when} holds {row.state}, the stand-in sent {sent}")
    return problems


def _sequence_problems(rows: Sequence[Row]) -> list[str]:
    """Return the gaps, repeated hours and sum steps that are not their hour."""
    problems: list[str] = []
    if abs(rows[0].sum - rows[0].state) > TOLERANCE:
        problems.append(f"the first sum {rows[0].sum} is not its hour {rows[0].state}")
    for previous, row in pairwise(rows):
        when = f"{row.start:%Y-%m-%dT%H:%M}Z"
        step = row.start - previous.start
        if step != HOUR:
            problems.append(
                f"{when} follows {previous.start:%Y-%m-%dT%H:%M}Z by {step}"
            )
        elif abs(row.sum - previous.sum - row.state) > TOLERANCE:
            problems.append(
                f"{when}: sum step {row.sum - previous.sum:.6f}, hour {row.state}"
            )
    return problems


def _span_problems(rows: Sequence[Row], expected: Expected) -> list[str]:
    """Return whether the first and newest hours are not where they should be."""
    problems: list[str] = []
    earliest, latest = expected.first_range
    first = rows[0].start
    if not earliest <= first <= latest:
        problems.append(
            f"first hour {first:%Y-%m-%dT%H:%M}Z, expected from "
            f"{earliest:%Y-%m-%dT%H:%M}Z to {latest:%Y-%m-%dT%H:%M}Z"
        )
    newest = floor_hour(expected.newest_by) - HOUR
    if not newest - HOUR <= rows[-1].start <= newest:
        problems.append(
            f"newest hour {rows[-1].start:%Y-%m-%dT%H:%M}Z, expected "
            f"{newest:%Y-%m-%dT%H:%M}Z or the hour before"
        )
    return problems


def history_problems(
    kind: str, rows: Sequence[Row], expected: Expected | None
) -> list[str]:
    """
    Return what is wrong with a statistic's hourly history, or nothing.

    Every hour from the first to the newest, none twice; each above zero and
    what the stand-in sent for it; each ``sum`` step equal to its hour, the
    first ``sum`` its own hour; and, when ``expected``, the first and newest
    hours where they should be.
    """
    if not rows:
        return ["no rows"]
    problems = _sequence_problems(rows) + _hour_problems(kind, rows)
    if expected is not None:
        problems += _span_problems(rows, expected)
    if len(problems) > SHOWN_PROBLEMS:
        problems[SHOWN_PROBLEMS:] = [f"and {len(problems) - SHOWN_PROBLEMS} more"]
    return problems


def changed_rows(before: Sequence[Row], after: Sequence[Row]) -> list[str]:
    """Return every hour of ``before`` that ``after`` lost or holds otherwise."""
    now = {row.start: row for row in after}
    changed: list[str] = []
    for row in before:
        later = now.get(row.start)
        if later is None:
            changed.append(f"{row.start:%Y-%m-%dT%H:%M}Z is gone")
        elif (
            abs(later.state - row.state) > TOLERANCE
            or abs(later.sum - row.sum) > TOLERANCE
        ):
            changed.append(
                f"{row.start:%Y-%m-%dT%H:%M}Z was {row.state}/{row.sum}, "
                f"now {later.state}/{later.sum}"
            )
    return changed


# -------------------------------------------------------------------- screens


@dataclass(frozen=True, slots=True)
class Screen:
    """One screen a journey met, as Home Assistant's dialog shows it."""

    kind: str
    step: str
    title: str
    text: str
    error: str
    fields: tuple[str, ...]
    missing: tuple[str, ...]

    def lines(self) -> list[str]:
        """Return the screen as markdown lines."""
        lines = [
            f"- **{self.kind} `{self.step}`**"
            + (f": {self.title}" if self.title else "")
        ]
        lines.extend(
            f"  > {line}" if line else "  >" for line in self.text.splitlines()
        )
        if self.error:
            lines.append(f"  - Error line: {self.error}")
        if self.fields:
            lines.append(f"  - Fields: {', '.join(self.fields)}")
        if self.missing:
            lines.append(f"  - No translation for: {', '.join(self.missing)}")
        return lines


def fill(text: str, placeholders: dict[str, Any] | None) -> str:
    """Return a translation with its placeholders filled in, as the frontend does."""
    for name, value in (placeholders or {}).items():
        text = text.replace(f"{{{name}}}", str(value))
    return text


def render(result: dict[str, Any], translations: dict[str, str]) -> Screen:
    """Return a flow result as Home Assistant's dialog shows it."""
    domain = result.get("translation_domain") or result.get("handler") or DOMAIN
    base = f"component.{domain}.config"
    placeholders = result.get("description_placeholders")
    missing: list[str] = []

    def text(key: str, *, required: bool = True) -> str:
        if key in translations:
            return fill(translations[key], placeholders)
        if required:
            missing.append(key.removeprefix(f"{base}."))
        return ""

    kind = str(result.get("type"))
    title = body = error = ""
    fields: tuple[str, ...] = ()
    if kind == "form":
        step = str(result["step_id"])
        title = text(f"{base}.step.{step}.title")
        body = text(f"{base}.step.{step}.description", required=False)
        if base_error := (result.get("errors") or {}).get("base"):
            error = text(f"{base}.error.{base_error}") or str(base_error)
        fields = tuple(
            text(f"{base}.step.{step}.data.{item['name']}") or str(item["name"])
            for item in result.get("data_schema") or []
        )
    elif kind == "progress":
        step = str(result.get("progress_action"))
        body = text(f"{base}.progress.{step}")
    elif kind == "abort":
        step = str(result.get("reason"))
        body = text(f"{base}.abort.{step}") or step
    elif kind == "create_entry":
        step = str(result.get("description") or "default")
        title = str(result.get("title") or "")
        body = text(f"{base}.create_entry.{step}")
    else:
        step = str(result.get("step_id", "-"))
    return Screen(kind, step, title, body, error, fields, tuple(missing))


def offered(result: dict[str, Any], name: str) -> list[str]:
    """Return the values a form's select field offers."""
    for item in result.get("data_schema") or []:
        if item.get("name") == name:
            select = (item.get("selector") or {}).get("select") or {}
            return [str(option["value"]) for option in select.get("options", [])]
    return []


# --------------------------------------------------------------------- report


@dataclass(slots=True)
class Check:
    """One check and its outcome."""

    name: str
    ok: bool
    detail: str = ""


@dataclass(slots=True)
class JourneyResult:
    """What one journey did and found."""

    name: str
    title: str
    checks: list[Check] = field(default_factory=list)
    screens: list[Screen] = field(default_factory=list)
    error: str | None = None
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        """Return whether every check passed and nothing stopped the journey."""
        return (
            self.error is None and bool(self.checks) and all(c.ok for c in self.checks)
        )


def markdown(results: Sequence[JourneyResult], header: dict[str, str]) -> str:
    """Return the run's report."""
    passed = sum(result.ok for result in results)
    lines = ["# Meridian Energy journeys", ""]
    lines += [f"- {name}: {value}" for name, value in header.items()]
    lines += [f"- Result: {passed} of {len(results)} journeys passed", ""]
    for result in results:
        lines.append(
            f"## {'PASS' if result.ok else 'FAIL'}: {result.title} "
            f"(`{result.name}`, {result.seconds:.0f} s)"
        )
        lines.append("")
        if result.error:
            lines += [f"Stopped: {result.error}", ""]
        lines += ["| Result | Check | Detail |", "| --- | --- | --- |"]
        lines += [
            f"| {'PASS' if check.ok else 'FAIL'} | {check.name} | "
            f"{check.detail.replace('|', '/')} |"
            for check in result.checks
        ]
        lines += ["", "Screens met:", ""]
        for screen in result.screens:
            lines += screen.lines()
        lines.append("")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------- client


class Client:
    """Talk to the instance as its owner, through REST and the websocket."""

    def __init__(self, instance: live_ha.Instance) -> None:
        self.instance = instance
        self._token = ""
        self._token_at = 0.0

    def token(self) -> str:
        """Return an access token, minted again once it is ten minutes old."""
        if not self._token or time.monotonic() - self._token_at > TOKEN_LIFETIME_S:
            self._token = live_ha.access_token(self.instance.state, self.instance.port)
            self._token_at = time.monotonic()
        return self._token

    def forget_token(self) -> None:
        """Mint a new token next time, as after a restart."""
        self._token = ""

    def rest(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        """Send one REST request and return its JSON; raise on an error status."""
        try:
            return live_ha.call(
                self.instance.port,
                method,
                path,
                token=self.token(),
                data=data,
                timeout=REQUEST_TIMEOUT_S,
            )
        except urllib.error.HTTPError as err:
            body = err.read().decode(errors="replace")[:300]
            message = f"{method} {path} returned HTTP {err.code}: {body}"
            raise JourneyError(message) from None

    def ws(self, *messages: dict[str, Any]) -> list[Any]:
        """Send websocket commands in turn and return their results."""
        return asyncio.run(self._ws(list(messages)))

    async def _ws(self, messages: list[dict[str, Any]]) -> list[Any]:
        url = f"http://{live_ha.HOST}:{self.instance.port}/api/websocket"
        results: list[Any] = []
        async with aiohttp.ClientSession() as session, session.ws_connect(url) as ws:
            await ws.receive_json(timeout=30)
            await ws.send_json({"type": "auth", "access_token": self.token()})
            if (auth := await ws.receive_json(timeout=30)).get("type") != "auth_ok":
                raise JourneyError(f"websocket sign-in failed: {auth.get('type')}")
            for number, message in enumerate(messages, start=1):
                await ws.send_json({"id": number, **message})
                while True:
                    reply = await ws.receive_json(timeout=REQUEST_TIMEOUT_S)
                    if reply.get("id") == number and reply.get("type") == "result":
                        break
                if not reply.get("success"):
                    raise JourneyError(
                        f"{message['type']} failed: {reply.get('error')}"
                    )
                results.append(reply.get("result"))
        return results


def wait(what: str, ready: Callable[[], Any], timeout: float) -> Any:
    """Poll ``ready`` until it returns a value; raise after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while True:
        if value := ready():
            return value
        if time.monotonic() >= deadline:
            raise JourneyError(f"timed out after {timeout:.0f} s waiting for {what}")
        time.sleep(POLL_S)


# -------------------------------------------------------------------- journey


@dataclass(slots=True)
class Journey:
    """One journey's context: the instance, the stand-in and what it found."""

    client: Client
    folder: Path
    result: JourneyResult
    translations: dict[str, str] = field(default_factory=dict)
    entries: list[str] = field(default_factory=list)
    flows: list[str] = field(default_factory=list)

    # ------------------------------------------------------------- recording

    def check(self, name: str, ok: bool, detail: str = "") -> bool:  # noqa: FBT001
        """Record and print one check; return whether it passed."""
        self.result.checks.append(Check(name, ok, detail))
        say(
            f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else "")
        )
        return ok

    def require(self, name: str, ok: bool, detail: str = "") -> None:  # noqa: FBT001
        """Record one check; stop the journey if it failed."""
        if not self.check(name, ok, detail):
            raise JourneyError(f"{name}: {detail}" if detail else name)

    def screen(self, result: dict[str, Any]) -> Screen:
        """Record a screen the journey met."""
        if not self.translations:
            (loaded,) = self.client.ws(
                {
                    "type": "frontend/get_translations",
                    "language": "en",
                    "category": "config",
                    "integration": [DOMAIN],
                }
            )
            self.translations = dict(loaded["resources"])
        shown = render(result, self.translations)
        self.result.screens.append(shown)
        say(
            f"  ...  {shown.kind} {shown.step}"
            + (f": {shown.error}" if shown.error else "")
        )
        return shown

    # -------------------------------------------------------------- stand-in

    def scenario(self, scenario: Scenario) -> None:
        """Set how the stand-in answers from now on."""
        path = self.folder / standin.SCENARIO
        partial = path.with_suffix(".partial")
        partial.write_text(json.dumps(scenario.to_json()))
        partial.replace(path)

    def journal(self, since: datetime | None = None) -> list[dict[str, Any]]:
        """Return the requests the stand-in answered, from ``since`` on."""
        path = self.folder / standin.JOURNAL
        if not path.is_file():
            return []
        records = [json.loads(line) for line in path.read_text().splitlines() if line]
        if since is None:
            return records
        moment = since.replace(microsecond=0)
        return [r for r in records if datetime.fromisoformat(r["at"]) >= moment]

    # ------------------------------------------------------------------ flows

    def start_flow(self, data: dict[str, Any]) -> dict[str, Any]:
        """Start a config flow and record its first screen."""
        result: dict[str, Any] = self.client.rest(
            "POST", "/api/config/config_entries/flow", {"handler": DOMAIN, **data}
        )
        if flow_id := result.get("flow_id"):
            self.flows.append(str(flow_id))
        self.screen(result)
        return result

    def submit(self, flow_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Submit a flow's form, follow any progress, and record each screen."""
        result: dict[str, Any] = self.client.rest(
            "POST", f"/api/config/config_entries/flow/{flow_id}", data
        )
        self.screen(result)
        if result.get("type") == "progress":
            result = self.follow(flow_id)
        self._note_entry(result)
        return result

    def follow(self, flow_id: str) -> dict[str, Any]:
        """Wait for a flow's progress to end, as the dialog does, and record it."""

        def moved_on() -> dict[str, Any] | None:
            result: dict[str, Any] = self.client.rest(
                "GET", f"/api/config/config_entries/flow/{flow_id}"
            )
            return None if result.get("type") == "progress" else result

        result: dict[str, Any] = wait("the import to end", moved_on, FLOW_TIMEOUT_S)
        self.screen(result)
        self._note_entry(result)
        return result

    def get_flow(self, flow_id: str) -> dict[str, Any]:
        """Return a flow's current step, and record it."""
        result: dict[str, Any] = self.client.rest(
            "GET", f"/api/config/config_entries/flow/{flow_id}"
        )
        self.screen(result)
        return result

    def _note_entry(self, result: dict[str, Any]) -> None:
        if result.get("type") == "create_entry":
            self.entries.append(str(result["result"]["entry_id"]))

    def set_up(self, email: str) -> tuple[dict[str, Any], datetime, datetime]:
        """Sign in with the stand-in's code; return the step after it and its times."""
        first = self.start_flow({})
        self.require(
            "the setup starts at the email form", first.get("step_id") == "user"
        )
        flow_id = str(first["flow_id"])
        code = self.submit(flow_id, {"email": email})
        self.require("the code form follows", code.get("step_id") == "otp")
        started = datetime.now(UTC)
        after = self.submit(flow_id, {"otp": OTP})
        return after, started, datetime.now(UTC)

    # -------------------------------------------------------------- entries

    def entry(self, entry_id: str) -> dict[str, Any] | None:
        """Return a config entry as the REST API lists it, or None."""
        entries = self.client.rest(
            "GET", f"/api/config/config_entries/entry?domain={DOMAIN}"
        )
        return next((e for e in entries if e["entry_id"] == entry_id), None)

    def diagnostics(self, entry_id: str) -> dict[str, Any]:
        """Return the coordinator's part of the entry's diagnostics."""
        loaded = self.client.rest("GET", f"/api/diagnostics/config_entry/{entry_id}")
        coordinator: dict[str, Any] = loaded["data"]["coordinator"]
        return coordinator

    def wait_synced(self, entry_id: str, after: str | None = None) -> dict[str, Any]:
        """Wait for the entry to finish a sync newer than ``after``."""

        def synced() -> dict[str, Any] | None:
            entry = self.entry(entry_id)
            if entry is None or entry["state"] != "loaded":
                return None
            coordinator = self.diagnostics(entry_id)
            if coordinator.get("first_sync_pending"):
                return None
            if coordinator.get("synced_at") == after:
                return None
            return coordinator

        return dict(wait("the entry's sync", synced, SYNC_TIMEOUT_S))

    def entity_ids(self, entry_id: str) -> list[str]:
        """Return the entry's enabled entities."""
        (registry,) = self.client.ws({"type": "config/entity_registry/list"})
        return sorted(
            item["entity_id"]
            for item in registry
            if item.get("config_entry_id") == entry_id and not item.get("disabled_by")
        )

    def devices(self, entry_id: str) -> int:
        """Return how many devices the entry has."""
        (registry,) = self.client.ws({"type": "config/device_registry/list"})
        return sum(entry_id in item.get("config_entries", []) for item in registry)

    def statistics(self, ids: Iterable[str]) -> dict[str, list[Row]]:
        """Return every hourly row of the statistics, from 120 days back."""
        wanted = sorted(ids)
        start = datetime.now(UTC) - timedelta(days=120)
        (loaded,) = self.client.ws(
            {
                "type": "recorder/statistics_during_period",
                "start_time": start.isoformat(),
                "statistic_ids": wanted,
                "period": "hour",
                "types": ["state", "sum"],
            }
        )
        return {sid: parse_rows(loaded.get(sid, [])) for sid in wanted}

    def remove_entries(self) -> None:
        """Remove the journey's entries and close its flows, as they stand."""
        for flow_id in self.flows:
            with contextlib.suppress(JourneyError, OSError):
                self.client.rest("DELETE", f"/api/config/config_entries/flow/{flow_id}")
        for entry_id in self.entries:
            with contextlib.suppress(JourneyError, OSError):
                self.client.rest(
                    "DELETE", f"/api/config/config_entries/entry/{entry_id}"
                )

    # ---------------------------------------------------------- common checks

    def check_entry(
        self,
        entry_id: str,
        histories: Mapping[str, Expected],
        *,
        feed_in: Iterable[str] = (),
    ) -> dict[str, list[Row]]:
        """
        Run the checks every journey that made an entry ends with.

        The entry is loaded; its sensors exist and have states; each
        account's statistics hold the hours they should, with no gap and no
        zero, each sum step equal to its hour and each hour what the
        stand-in sent; and the stand-in refused no request. Returns the rows.
        """
        entry = self.entry(entry_id)
        self.check(
            "the entry is loaded",
            entry is not None and entry["state"] == "loaded",
            "" if entry is None else str(entry["state"]),
        )
        entities = self.entity_ids(entry_id)
        unavailable = [
            entity
            for entity in entities
            if self.client.rest("GET", f"/api/states/{entity}")["state"]
            == "unavailable"
        ]
        self.check(
            "the sensors exist and have states",
            bool(entities) and not unavailable,
            f"{len(entities)} sensors"
            + (f"; unavailable: {', '.join(unavailable)}" if unavailable else ""),
        )
        feeds = set(feed_in)
        rows: dict[str, list[Row]] = {}
        for account, expected in histories.items():
            ids = statistic_ids(account, feed_in=account in feeds)
            loaded = self.statistics(ids.values())
            for kind, sid in ids.items():
                problems = history_problems(kind, loaded[sid], expected)
                found = loaded[sid]
                span = (
                    f"{len(found)} hours, {found[0].start:%Y-%m-%dT%H:%M}Z to "
                    f"{found[-1].start:%Y-%m-%dT%H:%M}Z"
                    if found
                    else "none"
                )
                self.check(
                    f"{account} {kind}: every hour, none zero, sums consistent",
                    not problems,
                    "; ".join(problems) if problems else span,
                )
                rows[sid] = found
        self.check_nothing_refused()
        return rows

    def check_nothing_refused(self) -> None:
        """Check that the stand-in refused nothing and answered every request."""
        path = self.folder / standin.REFUSED
        refused = path.read_text().splitlines() if path.is_file() else []
        self.check(
            "the stand-in refused no outbound request",
            not refused,
            f"{len(refused)} refused: {'; '.join(refused[:3])}" if refused else "",
        )
        errors = [r["error"] for r in self.journal() if "error" in r]
        self.check(
            "the fake answered every request the integration sent",
            not errors,
            "; ".join(sorted(set(errors))[:3]),
        )

    def check_screens(self) -> None:
        """
        Check that every screen met has its words.

        Where a title, label, error or abort reason has no translation, the
        dialog shows nothing or the raw key, such as ``reauth_successful``.
        """
        if not self.result.screens:
            return
        missing = [
            f"{screen.kind} {screen.step}: {', '.join(screen.missing)}"
            for screen in self.result.screens
            if screen.missing
        ]
        self.check(
            "every screen met has its words, no raw keys",
            not missing,
            "no translation for " + "; ".join(missing) if missing else "",
        )


# ------------------------------------------------------------------- journeys


def first_setup(j: Journey) -> None:
    """First-time setup with one account, then the entry's first update."""
    account = "A-JOURNEY-1"
    j.scenario(Scenario(accounts=(account,)))
    done, started, ended = j.set_up("journey-1@example.com")
    j.require("progress, then the entry", done.get("type") == "create_entry")
    j.check(
        "the screens met",
        [(s.kind, s.step) for s in j.result.screens]
        == [
            ("form", "user"),
            ("form", "otp"),
            ("progress", "initial_import"),
            ("create_entry", "default"),
        ],
        " → ".join(f"{s.kind} {s.step}" for s in j.result.screens),
    )
    entry_id = j.entries[-1]
    coordinator = j.wait_synced(entry_id)
    modes = [r["sync_mode"] for r in coordinator.get("property_results", [])]
    days = [
        r["requested_days"]["consumption"]
        for r in coordinator.get("property_results", [])
    ]
    j.check(
        "the first update imported only the tip",
        coordinator.get("sync_mode") == "tip"
        and modes == ["tip"]
        and max(days) <= TIP_DAYS,
        f"mode {coordinator.get('sync_mode')}, days asked {days}",
    )
    oldest = date.fromisoformat(
        min(
            (r["start_on"] for r in j.journal(ended) if r.get("start_on")),
            default=ended.astimezone(NZ).date().isoformat(),
        )
    )
    j.check(
        "after the import, no window older than yesterday",
        oldest >= ended.astimezone(NZ).date() - timedelta(days=1),
        f"oldest window starts {oldest}",
    )
    rows = j.check_entry(
        entry_id, {account: Expected(started, ended, datetime.now(UTC))}
    )
    usage = rows[statistic_ids(account)["consumption"]]
    at_import = sum(row.start < floor_hour(ended) for row in usage)
    crossed = floor_hour(started) != floor_hour(ended)
    j.check(
        "90 days of hourly statistics at the import",
        at_import in ({2160, 2161} if crossed else {2159, 2160}),
        f"{at_import} hours before {floor_hour(ended):%H:%M}Z",
    )


def two_accounts(j: Journey) -> None:
    """Two accounts: choose one, then add the other with Reconfigure."""
    first, second = "A-JOURNEY-2A", "A-JOURNEY-2B"
    j.scenario(Scenario(accounts=(first, second)))
    choice = j.start_flow({})
    flow_id = str(choice["flow_id"])
    j.submit(flow_id, {"email": "journey-2@example.com"})
    accounts = j.submit(flow_id, {"otp": OTP})
    j.require("the accounts form follows", accounts.get("step_id") == "accounts")
    j.check(
        "the accounts form offers both",
        offered(accounts, "selected_accounts") == [first, second],
        ", ".join(offered(accounts, "selected_accounts")),
    )
    started = datetime.now(UTC)
    done = j.submit(flow_id, {"selected_accounts": [first]})
    ended = datetime.now(UTC)
    j.require("progress, then the entry", done.get("type") == "create_entry")
    entry_id = j.entries[-1]
    coordinator = j.wait_synced(entry_id)
    j.check("one device", j.devices(entry_id) == 1, str(j.devices(entry_id)))
    before = j.check_entry(
        entry_id, {first: Expected(started, ended, datetime.now(UTC))}
    )
    j.check(
        "no statistics for the account left out",
        not any(j.statistics(statistic_ids(second).values()).values()),
    )
    reconfigure = j.start_flow({"entry_id": entry_id})
    j.require(
        "Reconfigure shows the accounts", reconfigure.get("step_id") == "reconfigure"
    )
    j.check(
        "Reconfigure offers both, the chosen one selected",
        offered(reconfigure, "selected_accounts") == [first, second],
        ", ".join(offered(reconfigure, "selected_accounts")),
    )
    added = datetime.now(UTC)
    result = j.submit(
        str(reconfigure["flow_id"]), {"selected_accounts": [first, second]}
    )
    j.check(
        "Reconfigure saves and reloads",
        result.get("type") == "abort"
        and result.get("reason") == "reconfigure_successful",
        str(result.get("reason")),
    )
    j.wait_synced(entry_id, coordinator.get("synced_at"))
    devices = j.devices(entry_id)
    j.check("two devices", devices == TWO_DEVICES, str(devices))
    now = datetime.now(UTC)
    after = j.check_entry(
        entry_id,
        {
            first: Expected(started, ended, now),
            second: Expected(added, now, now),
        },
    )
    for kind, sid in statistic_ids(first).items():
        changed = changed_rows(before[sid], after[sid])
        j.check(
            f"{first} {kind} unchanged by the second account",
            not changed,
            "; ".join(changed[:3]) or f"{len(before[sid])} hours identical",
        )


def _detail(result: dict[str, Any]) -> str:
    """Return the failure form's ``detail`` placeholder."""
    return str((result.get("description_placeholders") or {}).get("detail"))


def _no_statistics(j: Journey, ids: Iterable[str]) -> bool:
    """Return once the statistics have no rows (the rollback has run)."""
    wanted = list(ids)
    try:
        wait(
            "the rollback",
            lambda: not any(j.statistics(wanted).values()),
            60,
        )
    except JourneyError:
        return False
    return True


def _wait_out_retry(failed_at: datetime) -> None:
    """Wait until the setup flow allows a retry after a failure."""
    remaining = (
        failed_at + RETRY_DELAY + RETRY_MARGIN - datetime.now(UTC)
    ).total_seconds()
    if remaining > 0:
        say(f"  ...  waiting {remaining:.0f} s, as the flow asks before a retry")
        time.sleep(remaining)


def retry(j: Journey) -> None:
    """Import fails, then a retry after the real delay works."""
    account = "A-JOURNEY-3"
    feed_in = (account,)
    j.scenario(
        Scenario(
            accounts=(account,),
            feed_in=feed_in,
            measurements_status=503,
            failing_directions=(standin.GENERATION,),
        )
    )
    failed, started, _ended = j.set_up("journey-3@example.com")
    failed_at = datetime.now(UTC)
    j.require("the failure form", failed.get("step_id") == "import_failed")
    shown = j.result.screens[-1]
    j.check(
        "the failure form's error line",
        (failed.get("errors") or {}).get("base") == "upstream_unavailable"
        and "503" in _detail(failed),
        f"{shown.error} / {_detail(failed)}",
    )
    j.check(
        "the failure form offers Continue without history",
        "continue_without_history"
        in [i["name"] for i in failed.get("data_schema") or []],
        ", ".join(shown.fields),
    )
    usage = [
        r
        for r in j.journal(started)
        if r.get("operation") == "measurements"
        and r.get("direction") == standin.CONSUMPTION
    ]
    j.check(
        "the failed attempt imported usage before it failed",
        any(r.get("status") == HTTP_OK for r in usage),
        f"{len(usage)} usage windows answered",
    )
    ids = statistic_ids(account, feed_in=True)
    j.check(
        "the failed attempt's statistics were removed before the retry",
        _no_statistics(j, ids.values()),
    )
    j.scenario(Scenario(accounts=(account,), feed_in=feed_in))
    _wait_out_retry(failed_at)
    retried = datetime.now(UTC)
    done = j.submit(str(failed["flow_id"]), {})
    ended = datetime.now(UTC)
    j.require("progress, then the entry", done.get("type") == "create_entry")
    j.check(
        "the entry says nothing is left to import",
        j.result.screens[-1].step == "default",
        j.result.screens[-1].step,
    )
    entry_id = j.entries[-1]
    j.wait_synced(entry_id)
    j.check_entry(
        entry_id,
        {account: Expected(retried, ended, datetime.now(UTC))},
        feed_in=feed_in,
    )


def without_history(j: Journey) -> None:
    """Meridian down: Continue without history, then the history fills in."""
    account = "A-JOURNEY-4"
    j.scenario(
        Scenario(accounts=(account,), measurements_status=504, measurements_page="html")
    )
    failed, _started, _ended = j.set_up("journey-4@example.com")
    failed_at = datetime.now(UTC)
    j.require("the failure form", failed.get("step_id") == "import_failed")
    shown = j.result.screens[-1]
    j.check(
        "the failure form's error line",
        (failed.get("errors") or {}).get("base") == "upstream_timeout",
        f"{shown.error} / {_detail(failed)}",
    )
    j.check(
        "the failure form offers Continue without history",
        "continue_without_history"
        in [i["name"] for i in failed.get("data_schema") or []],
        ", ".join(shown.fields),
    )
    ids = statistic_ids(account)
    j.check("nothing imported", _no_statistics(j, ids.values()))
    _wait_out_retry(failed_at)
    # Meridian is back before the entry exists: its first update starts the
    # moment it is created, and a failed one waits 10 minutes to try again.
    j.scenario(Scenario(accounts=(account,)))
    created = datetime.now(UTC)
    done = j.submit(str(failed["flow_id"]), {"continue_without_history": True})
    j.require("the entry is created", done.get("type") == "create_entry")
    j.check(
        'the entry says the history finishes in the background ("partial")',
        done.get("description") == "partial"
        and "background" in j.result.screens[-1].text,
        j.result.screens[-1].text,
    )
    entry_id = j.entries[-1]
    coordinator = j.wait_synced(entry_id)
    j.check(
        "the entry's first update imported the history",
        coordinator.get("sync_mode") == "initial",
        f"mode {coordinator.get('sync_mode')}",
    )
    now = datetime.now(UTC)
    j.check_entry(entry_id, {account: Expected(created, now, now)})


def reauth(j: Journey) -> None:
    """Meridian ends the sign-in; reconnect from the reauthentication."""
    account = "A-JOURNEY-5"
    j.scenario(Scenario(accounts=(account,)))
    done, started, ended = j.set_up("journey-5@example.com")
    j.require("progress, then the entry", done.get("type") == "create_entry")
    entry_id = j.entries[-1]
    coordinator = j.wait_synced(entry_id)
    ids = statistic_ids(account)
    before = j.statistics(ids.values())
    j.scenario(Scenario(accounts=(account,), session_ended=True))
    entity = j.entity_ids(entry_id)[0]
    j.client.rest(
        "POST", "/api/services/homeassistant/update_entity", {"entity_id": entity}
    )

    def reauth_flow() -> dict[str, Any] | None:
        (flows,) = j.client.ws({"type": "config_entries/flow/progress"})
        return next(
            (
                flow
                for flow in flows
                if flow["handler"] == DOMAIN
                and flow["context"].get("source") == "reauth"
                and flow["context"].get("entry_id") == entry_id
            ),
            None,
        )

    found = wait("reauthentication to start", reauth_flow, 300)
    j.check("the update started reauthentication", bool(found))
    flow_id = str(found["flow_id"])
    j.flows.append(flow_id)
    confirm = j.get_flow(flow_id)
    j.check(
        "the reauthentication asks to send a new code",
        confirm.get("step_id") == "reauth_confirm"
        and "journey-5@example.com" in j.result.screens[-1].text,
        j.result.screens[-1].text,
    )
    j.scenario(Scenario(accounts=(account,)))
    code = j.submit(flow_id, {})
    j.check(
        "the code form follows", code.get("step_id") == "otp", str(code.get("step_id"))
    )
    result = j.submit(flow_id, {"otp": OTP})
    j.check(
        "the sign-in is renewed",
        result.get("type") == "abort" and result.get("reason") == "reauth_successful",
        str(result.get("reason")),
    )
    j.wait_synced(entry_id, coordinator.get("synced_at"))
    after = j.check_entry(
        entry_id, {account: Expected(started, ended, datetime.now(UTC))}
    )
    for kind, sid in ids.items():
        changed = changed_rows(before[sid], after[sid])
        j.check(
            f"{kind}: the history before is unchanged",
            not changed,
            "; ".join(changed[:3]) or f"{len(before[sid])} hours identical",
        )


def restart(j: Journey, relaunch: Callable[[], None]) -> None:
    """Reload the entry, then restart Home Assistant: the history stays whole."""
    account = "A-JOURNEY-6"
    j.scenario(Scenario(accounts=(account,)))
    done, started, ended = j.set_up("journey-6@example.com")
    j.require("progress, then the entry", done.get("type") == "create_entry")
    entry_id = j.entries[-1]
    coordinator = j.wait_synced(entry_id)
    ids = statistic_ids(account)
    expected = {account: Expected(started, ended, datetime.now(UTC))}
    before = j.check_entry(entry_id, expected)
    for when, act in (("the reload", None), ("the restart", relaunch)):
        if act is None:
            j.client.rest("POST", f"/api/config/config_entries/entry/{entry_id}/reload")
        else:
            act()
        coordinator = j.wait_synced(entry_id, coordinator.get("synced_at"))
        j.check(
            f"{when}'s update ran",
            coordinator.get("sync_mode") == "restart",
            f"mode {coordinator.get('sync_mode')}",
        )
        expected = {account: Expected(started, ended, datetime.now(UTC))}
        after = j.check_entry(entry_id, expected)
        for kind, sid in ids.items():
            changed = changed_rows(before[sid], after[sid])
            j.check(
                f"{kind}: the hours before {when} are unchanged",
                not changed,
                "; ".join(changed[:3]) or f"{len(before[sid])} hours identical",
            )
        before = after


JOURNEYS: dict[str, str] = {
    "first_setup": "First-time setup, one account",
    "two_accounts": "Two accounts: choose one, add the other later",
    "retry": "Import fails, then a retry works",
    "without_history": "Meridian down, Continue without history, history fills in",
    "reauth": "Meridian ends the sign-in, reconnect",
    "restart": "Restart and reload, history unbroken",
}


# --------------------------------------------------------------------- runner


def site_files(folder: Path) -> dict[str, bytes]:
    """Return the stand-in's files for the instance's site-packages."""
    package = standin.PACKAGE
    files = {
        f"{package}/__init__.py": (ROOT / "tests" / "meridian_standin.py").read_bytes(),
        f"{package}/{standin.SETTINGS}": json.dumps({"folder": str(folder)}).encode(),
        f"{package}.pth": standin.PTH_LINE.encode(),
    }
    for name in _FAKES:
        files[f"{package}/{name}"] = (ROOT / "tests" / name).read_bytes()
    return files


@dataclass(slots=True)
class Run:
    """One run of the journeys against one instance."""

    instance: live_ha.Instance
    folder: Path
    results: list[JourneyResult] = field(default_factory=list)

    def check_standin(self) -> None:
        """Stop unless the stand-in is installed in Home Assistant's process."""
        marker = self.folder / standin.INSTALLED.format(pid=self.instance.pid)
        if not marker.is_file():
            raise SystemExit(
                "The stand-in is not installed in Home Assistant's process: stopping"
            )

    def relaunch(self) -> None:
        """Restart Home Assistant on the same folder and check it again."""
        self.instance = live_ha.relaunch(
            self.instance, DOMAIN, live_ha.SYSTEM_ENTRIES | {DOMAIN}
        )
        self.check_standin()

    def run(self, name: str) -> JourneyResult:
        """Run one journey, record what it found, and remove what it made."""
        result = JourneyResult(name, JOURNEYS[name])
        # Recorded first, so that the report has it whatever happens.
        self.results.append(result)
        say(f"Journey {name}: {result.title}")
        client = Client(self.instance)
        journey = Journey(client, self.folder, result)
        began = time.monotonic()
        try:
            if name == "restart":

                def relaunch() -> None:
                    self.relaunch()
                    client.instance = self.instance
                    client.forget_token()

                restart(journey, relaunch)
            else:
                globals()[name](journey)
        except (JourneyError, OSError, KeyError, ValueError) as err:
            result.error = f"{type(err).__name__}: {err}"
            say(f"  FAIL  stopped: {result.error}")
        except SystemExit as err:
            # Home Assistant failed to start again: the run cannot go on.
            result.error = f"the run stopped: {err}"
            say(f"  FAIL  {result.error}")
            raise
        finally:
            journey.check_screens()
            journey.remove_entries()
            result.seconds = time.monotonic() - began
        say(
            f"{'PASS' if result.ok else 'FAIL'} journey {name} ({result.seconds:.0f} s)"
        )
        return result


def write_report(results: Sequence[JourneyResult], header: dict[str, str]) -> Path:
    """Write the report to the system's temporary directory; return its path."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = Path(tempfile.gettempdir()) / f"meridian-journeys-{stamp}.md"
    path.write_text(markdown(results, header))
    return path


def journeys_port(value: str) -> int:
    """Return ``value`` as a port for the journeys, never the live test's."""
    port = live_ha.port_number(value)
    if port == live_ha.DEFAULT_PORT:
        message = f"{port} is the live test's port; use another"
        raise argparse.ArgumentTypeError(message)
    return port


def parse(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0].strip())
    parser.add_argument("--ref", help="a release tag or commit to test")
    parser.add_argument("--ha-version", help="default: the newest release")
    parser.add_argument("--port", type=journeys_port, default=DEFAULT_PORT)
    parser.add_argument("--only", nargs="+", choices=list(JOURNEYS), metavar="NAME")
    parser.add_argument(
        "--clean-up", action="store_true", help="only delete a left-over instance"
    )
    return parser.parse_args(argv)


def fail(message: str) -> NoReturn:
    """Stop the run with a message."""
    raise SystemExit(message)


def main(argv: Sequence[str] | None = None) -> int:
    """Build the instance, run the journeys, report, and delete it all."""
    args = parse(argv)
    if args.clean_up:
        live_ha.stop(NAME)
        return 0
    domain = live_ha.integration_domain()
    if domain != DOMAIN:
        fail(f"These journeys are for {DOMAIN}, not {domain}")
    names = [name for name in JOURNEYS if not args.only or name in args.only]
    folder = live_ha.config_dir(live_ha.state_dir(domain, NAME)) / STANDIN_FOLDER
    began = time.monotonic()
    header = {
        "Started": datetime.now(UTC).isoformat(timespec="seconds"),
        "Integration": args.ref or "the working tree",
    }
    results: list[JourneyResult] = []
    try:
        instance = live_ha.launch(
            args.ref,
            args.ha_version,
            args.port,
            name=NAME,
            site_files=site_files(folder),
        )
        header["Home Assistant"] = instance.version
        run = Run(instance, folder)
        results = run.results
        run.check_standin()
        say(f"The stand-in answers for Meridian in Home Assistant {instance.version}")
        for name in names:
            run.run(name)
    finally:
        live_ha.stop(NAME)
        header["Duration"] = f"{time.monotonic() - began:.0f} s"
        if results:
            report = write_report(results, header)
            passed = sum(result.ok for result in results)
            say(f"{passed} of {len(results)} journeys passed; report: {report}")
    return 0 if results and all(result.ok for result in results) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

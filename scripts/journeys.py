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
answers the integration's requests from the tests' own fake. In Home
Assistant's process it refuses every other TCP connection and name lookup
that would leave this computer, and records it with its host only. Every
value is synthetic, and so is the sign-in: there is none. Building the
instance, and Home Assistant's first start, download Home Assistant and the
requirements it pins from PyPI, as for the live test.

Each journey drives Home Assistant through its REST and websocket APIs, as
the frontend would, with the owner's token, and checks what came of it: the
screens met, the config entry, its devices and sensors, the measurements
windows the stand-in answered, repair notices, every line the integration
logged above debug level, any warning Home Assistant's recorder, websocket
API or sensor platform logged, and its statistics hour by hour against what
the stand-in sent. ``--only`` runs some
of them, in their usual order. Each check and each journey is printed as
PASS or FAIL, and a report (markdown) with every check and every screen, as
Home Assistant shows it, is written to the system's temporary directory. The
instance is always stopped and deleted at the end, and the exit status is
non-zero if anything failed. A run holds a lock for as long as it lasts, so
neither a second run nor ``--clean-up`` can start meanwhile. A run does not
start either while the journeys' folder exists: a run that was killed left
it behind, and ``--clean-up`` deletes it.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, NoReturn
from zoneinfo import ZoneInfo

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # run as a script, not as scripts.journeys
    sys.path.insert(0, str(ROOT))

from scripts import live_ha  # noqa: E402
from tests import meridian_standin as standin  # noqa: E402
from tests.meridian_standin import Scenario  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

NAME = "journeys"
DEFAULT_PORT = 8125
DOMAIN = "meridian_energy"
NZ = ZoneInfo("Pacific/Auckland")
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
# The history reaches back to New Zealand midnight 90 dates before today.
HISTORY_DAYS = 90
# Measurements windows hold up to 7 dates. A first sync asks for 13, newest
# first, back to the target; the first sync after a start asks for the 3 of
# the last 21 dates, and nothing older when the history is whole.
WINDOW_DATES = 7
FIRST_SYNC_WINDOWS = 13
RECENT_WINDOWS = 3
# The bill's day totals come in requests of this frequency, not windows.
DAY_FREQUENCY = "DAY_INTERVAL"
# The sensors of Meridian's own figures, with a feed-in register and an
# all-day plan, as the stand-in has.
FIGURES = (
    "bill_usage",
    "bill_cost",
    "bill_export",
    "bill_credit",
    "bill_estimate",
    "total_due",
    "last_bill",
    "last_payment",
    "daily_charge",
    "anytime_price",
)
# Without a feed-in register: no export and no credit so far.
FIGURES_WITHOUT_FEED_IN = tuple(
    key for key in FIGURES if key not in {"bill_export", "bill_credit"}
)
BILL = ("bill_usage", "bill_cost", "bill_estimate")
# Total due's credit when the stand-in has the account in credit, in dollars.
CREDIT = standin.CREDIT_CENTS / 100
# What the integration says when Meridian's own figures fail. (Not named for
# the billing period: CodeQL takes a name with "billing" in it for private
# data, and these lines are printed.)
PERIOD_LOST = (
    "Meridian Energy couldn't get the billing period; the billing period and bill "
    "sensors show unknown until it can"
)
DAYS_LOST = (
    "Meridian Energy couldn't get the bill's day totals; the bill sensors show "
    "unknown until it can"
)
PRICES_UNANSWERED = "Meridian Energy couldn't get the prices; it asks again in a day"
# The repair notice appears after this many failed updates in a row.
NOTICE_AFTER = 3
# The outage's answer to every measurements request, from a gateway.
GATEWAY_TIMEOUT = 504
# Home Assistant runs an update asked for within 10 s of the last one only
# after those 10 s, and does not wait for it.
UPDATE_COOLDOWN_S = 11.0
TWO_DEVICES = 2
OTP = "123456"
# The setup asks Meridian for no history: the entry follows the code at once.
SIGN_IN_LIMIT = timedelta(seconds=30)
# Readings and costs are compared to the stand-in's within this.
TOLERANCE = 1e-6
POLL_S = 2.0
SYNC_TIMEOUT_S = 1200
REQUEST_TIMEOUT_S = 150
TOKEN_LIFETIME_S = 600
# Problems shown for one statistic before the rest are only counted.
SHOWN_PROBLEMS = 5
# The stand-in's folder, in the instance's configuration folder.
STANDIN_FOLDER = "journeys"
# Home Assistant's log, in the instance's configuration folder, and its lines:
# "<date> <time>.<ms> <LEVEL> (<thread>) [<logger>] <message>". From 2026.11
# the time ends with the UTC offset, as in "12:00:00.123+13:00". A start
# moves the log before it to LOG_FILE.1.
LOG_FILE = "home-assistant.log"
LOG_LINE = re.compile(
    r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}(?:[+-]\d\d:\d\d)? (?P<level>[A-Z]+) "
    r"\(.*?\) \[(?P<name>[^\]]+)\] (?P<message>.*)$"
)
_FAKES = ("meridian_http.py", "kraken_fake.py")
# Home Assistant's loggers whose warnings fail a journey: they say when a
# state the integration wrote could not be recorded or shown, as when it
# can't be written as JSON.
WATCHED_LOGGERS = (
    "homeassistant.components.recorder",
    "homeassistant.components.websocket_api",
    "homeassistant.components.sensor",
)
WARNING_LEVELS = frozenset({"WARNING", "ERROR", "CRITICAL"})


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


def floor_hour(value: datetime) -> datetime:
    """Return the start of the hour a time is in."""
    return value.replace(minute=0, second=0, microsecond=0)


def nz_date(moment: datetime) -> date:
    """Return the New Zealand date at a moment."""
    return moment.astimezone(NZ).date()


def backfill_start(now: datetime) -> datetime:
    """Return New Zealand midnight at the start of the date 90 dates back."""
    day = nz_date(now) - timedelta(days=HISTORY_DAYS)
    return datetime.combine(day, datetime.min.time(), tzinfo=NZ).astimezone(UTC)


def newest_windows(today: date, count: int) -> list[tuple[str, str]]:
    """Return the first and last dates of ``count`` windows back from ``today``."""
    return [
        (
            (today - timedelta(days=n * WINDOW_DATES + WINDOW_DATES - 1)).isoformat(),
            (today - timedelta(days=n * WINDOW_DATES)).isoformat(),
        )
        for n in range(count)
    ]


@dataclass(frozen=True, slots=True)
class Expected:
    """
    What a statistic's history should hold.

    Its first hour is New Zealand midnight at the start of the date 90 dates
    before an import that ran between ``imported_from`` and
    ``imported_to``; its newest is the last whole hour before
    ``newest_by``, or the one before.
    """

    imported_from: datetime
    imported_to: datetime
    newest_by: datetime

    @property
    def first_range(self) -> tuple[datetime, datetime]:
        """Return the earliest and latest first hour allowed."""
        return backfill_start(self.imported_from), backfill_start(self.imported_to)


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
    if not before:
        return ["no hours to compare"]
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


_PLACEHOLDER = re.compile(r"\{\w+\}")


def render(result: dict[str, Any], translations: dict[str, str]) -> Screen:
    """
    Return a flow result as Home Assistant's dialog shows it.

    A form with no title of its own shows the integration's title, as in the
    dialog, but one with neither a title nor a description of its own lacks
    its words.
    """
    handler = result.get("handler") or DOMAIN
    domain = result.get("translation_domain") or handler
    base = f"component.{domain}.config"
    placeholders = result.get("description_placeholders")
    missing: list[str] = []

    def text(key: str, *, required: bool = True) -> str:
        short = key.removeprefix(f"component.{handler}.config.")
        short = short.removeprefix(f"{base}.")
        if key in translations:
            filled = fill(translations[key], placeholders)
            if unfilled := _PLACEHOLDER.findall(filled):
                # The dialog shows a translation error instead.
                missing.append(f"{short} (no value for {', '.join(unfilled)})")
            return filled
        if required:
            missing.append(short)
        return ""

    kind = str(result.get("type"))
    title = body = error = ""
    fields: tuple[str, ...] = ()
    if kind == "form":
        step = str(result["step_id"])
        body = text(f"{base}.step.{step}.description", required=False)
        # A form needs words of its own, a title or a description: without
        # them, its step was renamed or its texts dropped.
        title = text(f"{base}.step.{step}.title", required=not body) or text(
            f"component.{handler}.title"
        )
        if base_error := (result.get("errors") or {}).get("base"):
            error = text(f"{base}.error.{base_error}") or str(base_error)
        # Field labels come from the handler's texts alone, as in the dialog.
        fields = tuple(
            text(f"component.{handler}.config.step.{step}.data.{item['name']}")
            or str(item["name"])
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


def summary(results: Sequence[JourneyResult], wanted: int, refused: int = 0) -> str:
    """Return the run's result in a line; a journey that never ran did not pass."""
    passed = sum(result.ok for result in results)
    line = f"{passed} of {max(wanted, len(results))} journeys passed"
    # A refusal fails the run, even one made after the journeys' own checks.
    return f"{line}; FAIL: the stand-in refused {refused}" if refused else line


def markdown(
    results: Sequence[JourneyResult],
    header: dict[str, str],
    wanted: int = 0,
    refused: int = 0,
) -> str:
    """Return the run's report."""
    lines = ["# Meridian Energy journeys", ""]
    lines += [f"- {name}: {value}" for name, value in header.items()]
    lines += [f"- Result: {summary(results, wanted, refused)}", ""]
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


class LogMark(NamedTuple):
    """Where the instance's log ended: its file's inode and size."""

    inode: int
    size: int


def read_refused(folder: Path) -> list[str]:
    """Return the stand-in's record of what it refused in this run, a line each."""
    path = folder / standin.REFUSED
    return path.read_text().splitlines() if path.is_file() else []


@dataclass(slots=True)
class Journey:
    """One journey's context: the instance, the stand-in and what it found."""

    client: Client
    folder: Path
    result: JourneyResult
    translations: dict[str, str] = field(default_factory=dict)
    translated: set[tuple[str, str]] = field(default_factory=set)
    entries: list[str] = field(default_factory=list)
    flows: list[str] = field(default_factory=list)
    # When the last update asked for ended, by the monotonic clock.
    updated_at: float = 0.0

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
        # The texts the dialog loads: the integration's texts and title, and
        # the config texts of any translation_domain a step names (Home
        # Assistant's own for "reauth_successful", for example).
        wanted = [(DOMAIN, "config"), (DOMAIN, "title")]
        if other := result.get("translation_domain"):
            wanted.append((other, "config"))
        for domain, kind in wanted:
            self.translate(domain, kind)
        shown = render(result, self.translations)
        self.result.screens.append(shown)
        say(
            f"  ...  {shown.kind} {shown.step}"
            + (f": {shown.error}" if shown.error else "")
        )
        return shown

    def translate(self, domain: str, kind: str) -> None:
        """Load an integration's texts of one category, as the frontend does, once."""
        if (domain, kind) in self.translated:
            return
        (loaded,) = self.client.ws(
            {
                "type": "frontend/get_translations",
                "language": "en",
                "category": kind,
                "integration": [domain],
            }
        )
        self.translations.update(loaded["resources"])
        self.translated.add((domain, kind))

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
        return [r for r in records if datetime.fromisoformat(r["at"]) >= since]

    def hourly(self, since: datetime) -> list[dict[str, Any]]:
        """Return the hourly measurements requests since ``since``."""
        return [
            record
            for record in self.journal(since)
            if record.get("operation") == "measurements"
            and record.get("frequency") != DAY_FREQUENCY
        ]

    def windows(
        self, since: datetime, direction: str = standin.CONSUMPTION
    ) -> list[tuple[str, str]]:
        """Return the first and last dates of a direction's windows since ``since``."""
        return [
            (str(record["start_on"]), str(record["end_on"]))
            for record in self.hourly(since)
            if record.get("direction") == direction and not record.get("continuation")
        ]

    def check_windows(
        self,
        when: str,
        since: datetime,
        count: int,
        direction: str = standin.CONSUMPTION,
    ) -> None:
        """Check that a direction asked for ``count`` windows, newest first, alone."""
        asked = self.windows(since, direction)
        # Today is the New Zealand date when the sync began, or now.
        wanted = [
            newest_windows(nz_date(moment), count)
            for moment in (since, datetime.now(UTC))
        ]
        self.check(
            f"{when}: {direction.lower()} asked for {count} windows of "
            f"{WINDOW_DATES} dates back from today, newest first, and no other",
            asked in wanted,
            f"{len(asked)} windows"
            + (f", {asked[0][1]} back to {asked[-1][0]}" if asked else ""),
        )

    # -------------------------------------------------------------------- log

    def log_mark(self) -> LogMark:
        """Return where the instance's log ends now, to read ``own_log`` from."""
        try:
            found = self._log_file().stat()
        except FileNotFoundError:
            return LogMark(0, 0)
        return LogMark(found.st_ino, found.st_size)

    def own_log(self, since: LogMark) -> list[tuple[str, str]]:
        """Return the integration's log lines after ``since``: level, message."""
        own = f"custom_components.{DOMAIN}"
        return [
            (level, message)
            for name, level, message in self.log_lines(since)
            if name == own or name.startswith(f"{own}.")
        ]

    def log_lines(self, since: LogMark) -> list[tuple[str, str, str]]:
        """
        Return every log line after ``since``: logger, level, message.

        From the instance's log, where the integration logs at debug level.
        When a start has moved the marked log to ``LOG_FILE.1`` since, its
        lines after the mark come first, then the new log's.
        """
        current = self._log_file()
        texts: list[str] = []
        for path in (current.with_name(f"{LOG_FILE}.1"), current):
            try:
                with path.open("rb") as log:
                    marked = os.fstat(log.fileno()).st_ino == since.inode
                    if path != current and not marked:
                        continue
                    log.seek(since.size if marked else 0)
                    texts.append(log.read().decode(errors="replace"))
            except FileNotFoundError:
                continue
        return [
            (line["name"], line["level"], line["message"])
            for text in texts
            for line in map(LOG_LINE.match, text.splitlines())
            if line is not None
        ]

    def check_log(
        self, when: str, since: LogMark, expected: Sequence[tuple[str, str]] = ()
    ) -> None:
        """
        Check the integration's log lines above debug level since ``since``.

        They must be ``expected``, in order, and no other. Its debug lines
        must be there too, so a log that could not be read fails. Home
        Assistant's watched loggers must have logged no warning either.
        """
        lines = self.own_log(since)
        loud = [(level, message) for level, message in lines if level != "DEBUG"]

        def shown(found: Sequence[tuple[str, str]]) -> str:
            return "; ".join(f'{level} "{message}"' for level, message in found)

        self.check(
            f"{when}: the integration logged "
            + (f"only {shown(expected)}" if expected else "nothing above debug level"),
            bool(lines) and loud == list(expected),
            (shown(loud) or "nothing above debug level")
            if lines
            else f"no line from it in {LOG_FILE}",
        )
        self.check_warnings(when, since)

    def check_warnings(
        self, when: str, since: LogMark, *, every_logger: bool = False
    ) -> None:
        """Check that no watched logger, or none at all, logged a warning or worse."""
        found = [
            (name, level, message)
            for name, level, message in self.log_lines(since)
            if level in WARNING_LEVELS
            and (
                every_logger
                or any(
                    name == watched or name.startswith(f"{watched}.")
                    for watched in WATCHED_LOGGERS
                )
            )
        ]
        self.check(
            f"{when}: "
            + (
                "nothing logged a warning"
                if every_logger
                else "Home Assistant's recorder, websocket API and sensor logged "
                "no warning"
            ),
            not found,
            "; ".join(
                f"{level} [{name}] {message}" for name, level, message in found[:3]
            ),
        )

    def _log_file(self) -> Path:
        return live_ha.config_dir(self.client.instance.state) / LOG_FILE

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
        """Submit a flow's form, and record the screen that follows."""
        result: dict[str, Any] = self.client.rest(
            "POST", f"/api/config/config_entries/flow/{flow_id}", data
        )
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

    def created(self, entry_id: str) -> datetime:
        """Return when Home Assistant created the entry."""
        entry = self.entry(entry_id)
        if entry is None:
            raise JourneyError(f"entry {entry_id} is gone")
        return datetime.fromtimestamp(float(entry["created_at"]), UTC)

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

    def wait_failed(self, entry_id: str) -> dict[str, Any]:
        """Wait for the entry's first sync to fail."""

        def failed() -> dict[str, Any] | None:
            entry = self.entry(entry_id)
            if entry is None or entry["state"] != "loaded":
                return None
            coordinator = self.diagnostics(entry_id)
            return coordinator if coordinator.get("last_failure") else None

        return dict(wait("the entry's first sync to fail", failed, SYNC_TIMEOUT_S))

    def unavailable(self, entry_id: str) -> list[str]:
        """Return the entry's entities whose state is unavailable."""
        return [
            entity
            for entity in self.entity_ids(entry_id)
            if self.client.rest("GET", f"/api/states/{entity}")["state"]
            == "unavailable"
        ]

    def update(self, entry_id: str) -> None:
        """
        Ask for an update, as the entity's menu does, and wait for it to end.

        Home Assistant runs it at once, and answers when it has ended, unless
        the last one asked for ended less than 10 s before: that is waited out.
        """
        if (wait_s := self.updated_at + UPDATE_COOLDOWN_S - time.monotonic()) > 0:
            time.sleep(wait_s)
        entity = self.entity_ids(entry_id)[0]
        try:
            self.client.rest(
                "POST",
                "/api/services/homeassistant/update_entity",
                {"entity_id": entity},
            )
        finally:
            self.updated_at = time.monotonic()

    def notice(self, entry_id: str) -> tuple[str, str] | None:
        """Return the entry's repair notice, its title and text as shown, or None."""
        (listed,) = self.client.ws({"type": "repairs/list_issues"})
        issue = next(
            (
                item
                for item in listed["issues"]
                if item["domain"] == DOMAIN
                and item["issue_id"] == f"sync_failing_{entry_id}"
            ),
            None,
        )
        if issue is None:
            return None
        self.translate(DOMAIN, "issues")
        base = f"component.{DOMAIN}.issues.{issue['translation_key']}"
        placeholders = issue.get("translation_placeholders")
        return (
            fill(self.translations.get(f"{base}.title", ""), placeholders),
            fill(self.translations.get(f"{base}.description", ""), placeholders),
        )

    def sensor_states(self, entry_id: str) -> dict[str, str]:
        """Return the state of each of the entry's enabled sensors, by its key."""
        (registry,) = self.client.ws({"type": "config/entity_registry/list"})
        return {
            str(item.get("translation_key")): self.client.rest(
                "GET", f"/api/states/{item['entity_id']}"
            )["state"]
            for item in registry
            if item.get("config_entry_id") == entry_id and not item.get("disabled_by")
        }

    def sensor(self, entry_id: str, key: str) -> dict[str, Any] | None:
        """Return the state of the entry's enabled sensor with this key, or None."""
        (registry,) = self.client.ws({"type": "config/entity_registry/list"})
        entity = next(
            (
                item["entity_id"]
                for item in registry
                if item.get("config_entry_id") == entry_id
                and item.get("translation_key") == key
                and not item.get("disabled_by")
            ),
            None,
        )
        if entity is None:
            return None
        state: dict[str, Any] = self.client.rest("GET", f"/api/states/{entity}")
        return state

    def check_unknown(self, when: str, entry_id: str, figures: Iterable[str]) -> None:
        """Check that each of these figures' sensors shows unknown."""
        states = self.sensor_states(entry_id)
        wanted = sorted(figures)
        self.check(
            f"{when}: "
            + ", ".join(key.replace("_", " ") for key in wanted)
            + " show unknown",
            all(states.get(key) == "unknown" for key in wanted),
            ", ".join(f"{key} {states.get(key)}" for key in wanted),
        )

    def check_figures(self, entry_id: str, figures: Iterable[str]) -> None:
        """Check that each of the account's figures has a sensor with a value."""
        states = self.sensor_states(entry_id)
        wanted = sorted(figures)
        missing = [key for key in wanted if key not in states]
        unknown = [key for key in wanted if states.get(key) == "unknown"]
        self.check(
            "every figure from Meridian has a value: "
            + ", ".join(key.replace("_", " ") for key in wanted),
            not missing and not unknown,
            "; ".join(
                part
                for part in (
                    f"missing: {', '.join(missing)}" if missing else "",
                    f"unknown: {', '.join(unknown)}" if unknown else "",
                )
                if part
            ),
        )

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
        """
        Return every hourly row of the statistics, from 120 days back.

        The integration queues its writes without waiting for the recorder to
        save them, so the rows are read once nothing is queued and two reads,
        a poll apart, agree.
        """
        wanted = sorted(ids)
        start = datetime.now(UTC) - timedelta(days=120)

        def read() -> dict[str, list[Row]]:
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

        def saved() -> tuple[dict[str, list[Row]]] | None:
            (info,) = self.client.ws({"type": "recorder/info"})
            if info.get("backlog"):
                return None
            first = read()
            time.sleep(POLL_S)
            again = read()
            return (again,) if again == first else None

        (rows,) = wait("the recorder to save the statistics", saved, SYNC_TIMEOUT_S)
        return dict(rows)

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
        unavailable = self.unavailable(entry_id)
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

    def check_complete(self, coordinator: Mapping[str, Any], units: int) -> None:
        """Check that each of the entry's units found its history complete."""
        found = coordinator.get("units") or []
        self.check(
            f"each unit's history is complete, {units} in all",
            len(found) == units
            and all(unit.get("history_complete") is True for unit in found),
            ", ".join(
                f"{unit.get('direction')}: {unit.get('history_complete')}"
                for unit in found
            )
            or "no units",
        )

    def check_nothing_refused(self) -> None:
        """Check that the stand-in refused nothing and answered every request."""
        refused = read_refused(self.folder)
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


def _created_at_once(j: Journey, done: dict[str, Any], took: timedelta) -> str:
    """Check that the code alone created the entry, at once; return its id."""
    j.require("the entry follows the code", done.get("type") == "create_entry")
    j.check(
        "the screens met: no account choice, no import",
        [(s.kind, s.step) for s in j.result.screens]
        == [("form", "user"), ("form", "otp"), ("create_entry", "default")],
        " → ".join(f"{s.kind} {s.step}" for s in j.result.screens),
    )
    j.check(
        "the entry is created within seconds of the code",
        took < SIGN_IN_LIMIT,
        f"{took.total_seconds():.1f} s",
    )
    return j.entries[-1]


def first_setup(j: Journey) -> None:
    """
    First-time setup with a feed-in register: the entry at once, then history.

    The setup asks Meridian for no history, so it creates the entry at once
    and says the history comes in the background. The first sync asks for
    13 windows of each direction, newest first, back to New Zealand midnight
    90 dates ago, imports them, and logs nothing above debug level. Every
    figure from Meridian has a value, the export and credit so far included.
    The account is in credit: Total due is 0 and its credit a number, and
    nothing in Home Assistant logs a warning about any state.
    """
    account = "A-JOURNEY-1"
    j.scenario(Scenario(accounts=(account,), feed_in=(account,), credit=(account,)))
    mark = j.log_mark()
    done, started, ended = j.set_up("journey-1@example.com")
    entry_id = _created_at_once(j, done, ended - started)
    j.check(
        "the entry says the history comes in the background",
        "background" in j.result.screens[-1].text,
        j.result.screens[-1].text,
    )
    coordinator = j.wait_synced(entry_id)
    for direction in (standin.CONSUMPTION, standin.GENERATION):
        j.check_windows("the first sync", started, FIRST_SYNC_WINDOWS, direction)
    j.check_complete(coordinator, 2)
    now = datetime.now(UTC)
    j.check_entry(entry_id, {account: Expected(ended, now, now)}, feed_in=(account,))
    j.check_figures(entry_id, FIGURES)
    due = j.sensor(entry_id, "total_due") or {}
    credit = due.get("attributes", {}).get("credit")
    j.check(
        "in credit, Total due is 0 and its credit a number",
        due.get("state") == "0" and credit == CREDIT,
        f"{due.get('state')}, credit {credit!r}",
    )
    j.check_log("the setup and the first sync", mark)
    j.check_warnings("the setup and the first sync", mark, every_logger=True)


def figures(j: Journey) -> None:
    """
    Meridian's own figures fail at the first sync, then come.

    The first sync's billing period, bill day totals and prices requests
    fail: the entry loads and its update works, the bill shows unknown, and
    the integration says so and that it asks for the prices again in a day.
    Each is next due hours later, so a reload asks again instead: with the
    day totals still failing, the bill shows unknown and says so; with every
    request answered, every figure has a value.
    """
    account = "A-JOURNEY-7"
    failing = tuple(sorted(standin.FIGURES))
    j.scenario(Scenario(accounts=(account,), failing_figures=failing))
    mark = j.log_mark()
    done, started, ended = j.set_up("journey-7@example.com")
    entry_id = _created_at_once(j, done, ended - started)
    coordinator = j.wait_synced(entry_id)
    imported = datetime.now(UTC)
    j.check(
        "the first sync works while Meridian's own figures fail",
        coordinator.get("last_update_success") is True,
        f"last failure {coordinator.get('last_failure')}",
    )
    j.check_unknown("the first sync", entry_id, BILL)
    j.check_log(
        "the first sync", mark, [("INFO", PERIOD_LOST), ("INFO", PRICES_UNANSWERED)]
    )
    steps = (
        ("a reload with the day totals failing", ("days",), [("INFO", DAYS_LOST)]),
        ("a reload with every request answered", (), []),
    )
    for when, still_failing, lines in steps:
        j.scenario(Scenario(accounts=(account,), failing_figures=still_failing))
        mark = j.log_mark()
        j.client.rest("POST", f"/api/config/config_entries/entry/{entry_id}/reload")
        coordinator = j.wait_synced(entry_id, coordinator.get("synced_at"))
        if still_failing:
            j.check_unknown(when, entry_id, BILL)
            j.check_figures(entry_id, ("daily_charge", "anytime_price"))
        else:
            j.check_figures(entry_id, FIGURES_WITHOUT_FEED_IN)
        j.check_log(when, mark, lines)
    j.check_entry(entry_id, {account: Expected(ended, imported, datetime.now(UTC))})


def outage(j: Journey) -> None:
    """
    Meridian times out from the start: the repair notice, then the history.

    The setup creates the entry all the same. Its first sync fails, sending
    the newest window twice, and Home Assistant logs one ERROR line; the
    sensors are unavailable, and the third failed update in a row raises the
    repair notice. Once Meridian answers, an update imports the 90 dates,
    the notice goes, and Home Assistant says the updates recovered.
    """
    account = "A-JOURNEY-3"
    j.scenario(
        Scenario(
            accounts=(account,),
            measurements_status=GATEWAY_TIMEOUT,
            measurements_page="html",
        )
    )
    mark = j.log_mark()
    done, started, ended = j.set_up("journey-3@example.com")
    entry_id = _created_at_once(j, done, ended - started)
    failed = j.wait_failed(entry_id)
    j.check(
        "the first sync fails while Meridian times out",
        failed.get("first_sync_pending") is True
        and failed["last_failure"].get("key") == "cannot_connect",
        f"last failure {failed['last_failure']}",
    )
    entities = j.entity_ids(entry_id)
    unavailable = j.unavailable(entry_id)
    j.check(
        "the sensors are unavailable until the history arrives",
        bool(unavailable) and unavailable == entities,
        f"{len(unavailable)} of {len(entities)} unavailable",
    )
    reason = (
        "Couldn't get an answer from Meridian (HTTP 504). Meridian Energy tries "
        "again automatically"
    )
    for failures in range(1, NOTICE_AFTER):
        notice = j.notice(entry_id)
        j.check(
            f"no repair notice after {failures} of {NOTICE_AFTER} failed updates",
            notice is None,
            str(notice),
        )
        j.update(entry_id)
    notice = j.notice(entry_id)
    j.check(
        f"the repair notice appears after {NOTICE_AFTER} failed updates in a row",
        notice
        == (
            "Meridian Energy isn't updating",
            f"{reason}.\n\nThis notice goes away by itself once an update works.",
        ),
        str(notice),
    )
    sent = j.hourly(started)
    newest = [
        newest_windows(nz_date(moment), 1)[0] for moment in (started, datetime.now(UTC))
    ]
    j.check(
        "each failed update sent the newest window, and once more",
        len(sent) == 2 * NOTICE_AFTER
        and all(
            (r.get("start_on"), r.get("end_on")) in newest
            and r.get("status") == GATEWAY_TIMEOUT
            for r in sent
        ),
        "; ".join(
            f"{r.get('start_on')}..{r.get('end_on')} {r.get('status')}" for r in sent
        ),
    )
    j.check_log(
        "while Meridian times out",
        mark,
        [("ERROR", f"Error fetching Meridian Energy data: {reason}")],
    )
    j.scenario(Scenario(accounts=(account,)))
    mark = j.log_mark()
    synced_from = datetime.now(UTC)
    j.update(entry_id)
    coordinator = j.wait_synced(entry_id)
    j.check_windows("the update once Meridian answers", synced_from, FIRST_SYNC_WINDOWS)
    j.check_complete(coordinator, 1)
    notice = j.notice(entry_id)
    j.check("the repair notice goes once an update works", notice is None, str(notice))
    now = datetime.now(UTC)
    j.check_entry(entry_id, {account: Expected(synced_from, now, now)})
    j.check_log(
        "once Meridian answers",
        mark,
        [("INFO", "Fetching Meridian Energy data recovered")],
    )


def two_accounts(j: Journey) -> None:
    """Two accounts on one login: both are followed, with no choice to make."""
    first, second = "A-JOURNEY-2A", "A-JOURNEY-2B"
    j.scenario(Scenario(accounts=(first, second)))
    mark = j.log_mark()
    done, started, ended = j.set_up("journey-2@example.com")
    entry_id = _created_at_once(j, done, ended - started)
    j.wait_synced(entry_id)
    devices = j.devices(entry_id)
    j.check("two devices", devices == TWO_DEVICES, str(devices))
    now = datetime.now(UTC)
    j.check_entry(
        entry_id,
        {first: Expected(ended, now, now), second: Expected(ended, now, now)},
    )
    j.check_log("the setup and the first sync", mark)


def reauth(j: Journey) -> None:
    """Meridian ends the sign-in; reconnect from the reauthentication."""
    account = "A-JOURNEY-5"
    j.scenario(Scenario(accounts=(account,)))
    mark = j.log_mark()
    done, started, ended = j.set_up("journey-5@example.com")
    entry_id = _created_at_once(j, done, ended - started)
    coordinator = j.wait_synced(entry_id)
    imported = datetime.now(UTC)
    j.check_log("the setup and the first sync", mark)
    ids = statistic_ids(account)
    before = j.statistics(ids.values())
    j.scenario(Scenario(accounts=(account,), session_ended=True))
    mark = j.log_mark()
    j.update(entry_id)

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

    # The journey stops here if no reauthentication starts.
    found = wait("reauthentication to start", reauth_flow, 300)
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
        entry_id, {account: Expected(ended, imported, datetime.now(UTC))}
    )
    for kind, sid in ids.items():
        changed = changed_rows(before[sid], after[sid])
        j.check(
            f"{kind}: the history before is unchanged",
            not changed,
            "; ".join(changed[:3]) or f"{len(before[sid])} hours identical",
        )
    ended_sign_in = (
        "Authentication failed while fetching Meridian Energy data: Meridian "
        "has ended the sign-in. Reconnect Meridian Energy to sign in again"
    )
    j.check_log(
        "from the ended sign-in to the renewed one", mark, [("ERROR", ended_sign_in)]
    )


def restart(j: Journey, relaunch: Callable[[], None]) -> None:
    """
    Reload the entry, then restart Home Assistant: the history stays whole.

    After each, the first sync asks for the last 21 dates, in 3 windows, and
    nothing older, since the history is whole; no hour changes.
    """
    account = "A-JOURNEY-6"
    j.scenario(Scenario(accounts=(account,)))
    mark = j.log_mark()
    done, started, ended = j.set_up("journey-6@example.com")
    entry_id = _created_at_once(j, done, ended - started)
    coordinator = j.wait_synced(entry_id)
    imported = datetime.now(UTC)
    ids = statistic_ids(account)
    expected = {account: Expected(ended, imported, datetime.now(UTC))}
    before = j.check_entry(entry_id, expected)
    j.check_log("the setup and the first sync", mark)
    for when, act in (("the reload", None), ("the restart", relaunch)):
        mark = j.log_mark()
        since = datetime.now(UTC)
        if act is None:
            j.client.rest("POST", f"/api/config/config_entries/entry/{entry_id}/reload")
        else:
            act()
        coordinator = j.wait_synced(entry_id, coordinator.get("synced_at"))
        j.check_windows(f"the update after {when}", since, RECENT_WINDOWS)
        expected = {account: Expected(ended, imported, datetime.now(UTC))}
        after = j.check_entry(entry_id, expected)
        for kind, sid in ids.items():
            changed = changed_rows(before[sid], after[sid])
            j.check(
                f"{kind}: the hours before {when} are unchanged",
                not changed,
                "; ".join(changed[:3]) or f"{len(before[sid])} hours identical",
            )
        j.check_log(f"{when} and the update after it", mark)
        before = after


JOURNEYS: dict[str, str] = {
    "first_setup": "First-time setup with a feed-in register, the history",
    "figures": "Meridian's own figures fail at the first sync, then come",
    "outage": "Meridian times out from the start, the notice, then the history",
    "two_accounts": "Two accounts on one login, both followed",
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
        except Exception as err:  # noqa: BLE001
            # Any failure fails this journey; the next one still runs.
            result.error = f"{type(err).__name__}: {err}"
            say(f"  FAIL  stopped: {result.error}")
        except BaseException as err:
            # Home Assistant failed to start again, or the run was interrupted:
            # the run cannot go on.
            result.error = f"the run stopped: {type(err).__name__}" + (
                f": {err}" if str(err) else ""
            )
            say(f"  FAIL  {result.error}")
            raise
        finally:
            try:
                journey.check_screens()
                journey.remove_entries()
            except Exception as err:  # noqa: BLE001
                # What is left could change the next journey: this one fails.
                failed = f"the clean-up failed: {type(err).__name__}: {err}"
                result.error = f"{result.error}; {failed}" if result.error else failed
                say(f"  FAIL  {failed}")
            result.seconds = time.monotonic() - began
        say(
            f"{'PASS' if result.ok else 'FAIL'} journey {name} ({result.seconds:.0f} s)"
        )
        return result


def write_report(
    results: Sequence[JourneyResult],
    header: dict[str, str],
    wanted: int,
    refused: int = 0,
) -> Path:
    """Write the report to the system's temporary directory; return its path."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = Path(tempfile.gettempdir()) / f"meridian-journeys-{stamp}.md"
    path.write_text(markdown(results, header, wanted, refused))
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


@contextlib.contextmanager
def only_run(state: Path) -> Iterator[None]:
    """
    Hold the journeys' lock, beside their folder, or stop if a run holds it.

    The system releases the lock when the process ends, however it ends.
    """
    lock = state.with_name(f"{state.name}.lock")
    with lock.open("a") as file:
        try:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fail(f"Another run is using {state}")
        yield


def main(argv: Sequence[str] | None = None) -> int:
    """Build the instance, run the journeys, report, and delete it all."""
    args = parse(argv)
    domain = live_ha.integration_domain()
    if domain != DOMAIN:
        fail(f"These journeys are for {DOMAIN}, not {domain}")
    state = live_ha.state_dir(domain, NAME)
    with only_run(state):
        if args.clean_up:
            live_ha.stop(NAME)
            return 0
        if state.exists():
            fail(
                f"{state} exists, left by a run that was killed: --clean-up deletes it"
            )
        return run_journeys(args, state)


def run_journeys(args: argparse.Namespace, state: Path) -> int:
    """Run the journeys in a new instance in ``state``, report, and delete it all."""
    names = [name for name in JOURNEYS if not args.only or name in args.only]
    folder = live_ha.config_dir(state) / STANDIN_FOLDER
    began = time.monotonic()
    header = {
        "Started": datetime.now(UTC).isoformat(timespec="seconds"),
        "Integration": args.ref or "the working tree",
    }
    results: list[JourneyResult] = []
    # Everything the stand-in refused in the run, up to Home Assistant's
    # shutdown: the journeys check it only up to their own last checks.
    refused: list[str] = []

    def read_at_the_end(state: Path) -> None:
        refused.extend(read_refused(live_ha.config_dir(state) / STANDIN_FOLDER))

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
        live_ha.stop(NAME, before_delete=read_at_the_end)
        header["Duration"] = f"{time.monotonic() - began:.0f} s"
        if results:
            header["Refused by the stand-in"] = (
                f"{len(refused)}: {'; '.join(refused[:3])}" if refused else "nothing"
            )
            if refused:
                say(f"FAIL the stand-in refused {len(refused)}: {refused[0]}")
            report = write_report(results, header, len(names), len(refused))
            say(f"{summary(results, len(names), len(refused))}; report: {report}")
    ran_all = len(results) == len(names) and all(result.ok for result in results)
    return 0 if ran_all and not refused else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

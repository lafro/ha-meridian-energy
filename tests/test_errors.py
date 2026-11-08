"""
How each kind of failure reads: its category, its key and detail, and the notice.

Each case runs the real client and transport in front of a synthetic
Meridian service, and checks the key and detail of the failure Home
Assistant shows, and when the repair notice comes and goes. Every value is
synthetic.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import patch

import pytest
from aiohttp import ClientConnectionError
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers import translation
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.meridian_energy import async_remove_entry
from custom_components.meridian_energy.const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
)

from .kraken_fake import FakeKraken
from .meridian_http import (
    ACCOUNT_NUMBER,
    FakeMeridianSession,
    Response,
    json_response,
    property_id,
)
from .syncs import DAY, HOUR, NOW, TODAY, coordinator_for, refresh, sync

_HTML = {"Content-Type": "text/html"}
_GATEWAY = Response(b"<html>504</html>", status=504, headers=_HTML)
_DORMANT = "A-TEST-2"


@pytest.fixture(autouse=True)
async def translations(hass: HomeAssistant) -> None:
    """Load the integration's texts, so failures read as users see them."""
    hass.config.components.add(DOMAIN)
    await translation.async_load_integrations(hass, {DOMAIN})


def _coded(*codes: str | None) -> Response:
    return json_response(
        {
            "errors": [
                {"message": "Synthetic"}
                if code is None
                else {"message": "Synthetic", "extensions": {"errorCode": code}}
                for code in codes
            ]
        }
    )


def _measurements(answer: Any) -> Any:
    return lambda operation, _variables: answer if operation == "measurements" else None


@pytest.mark.parametrize(
    ("answer", "key", "detail"),
    [
        (
            Response(b"<html>504</html>", status=504, headers=_HTML),
            "cannot_connect",
            "HTTP 504",
        ),
        (json_response({}, status=503), "cannot_connect", "HTTP 503"),
        (json_response({}, status=500), "cannot_connect", "HTTP 500"),
        (
            Response(b"", error=TimeoutError()),
            "cannot_connect",
            "timed out",
        ),
        (
            Response(b"", error=ClientConnectionError()),
            "cannot_connect",
            "network error",
        ),
        (_coded(None), "cannot_connect", "a Meridian service error"),
        (_coded("KT-CT-1188"), "invalid_data", "KT-CT-1188"),
        (_coded("KT-CT-1189"), "invalid_data", "KT-CT-1189"),
        # A list mixing coded and uncoded errors is a refusal.
        (_coded(None, "KT-CT-4321"), "invalid_data", "KT-CT-4321"),
        (json_response({}, status=400), "invalid_data", "HTTP 400"),
        (
            json_response(
                {
                    "errors": [
                        {"extensions": {"errorCode": "KT-CT-4321"}},
                        {"extensions": {"errorCode": "not a code"}},
                        {"extensions": {"errorCode": "KT-CT-4321"}},
                    ]
                },
                status=422,
            ),
            "invalid_data",
            "HTTP 422, KT-CT-4321",
        ),
        (json_response({"data": None}), "invalid_data", "unexpected data"),
        (Response(b"<html>ok</html>", headers=_HTML), "invalid_data", "not JSON"),
    ],
)
async def test_each_failure_has_its_category_and_detail(
    hass: HomeAssistant, answer: Response, key: str, detail: str
) -> None:
    session = FakeMeridianSession(NOW)
    session.hook = _measurements(answer)
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == key
    assert failure.value.translation_placeholders == {"detail": detail}
    assert f"({detail})" in str(failure.value)


@pytest.mark.parametrize("amount", [["12.5"], {"amount": "12.5"}])
async def test_a_cost_of_the_wrong_json_type_is_unusable_data(
    hass: HomeAssistant, amount: object
) -> None:
    """A list or object where an hour's cost belongs fails its unit, no more."""

    def cost(_start: Any, _channel: Any) -> Any:
        return amount

    session = FakeMeridianSession(NOW, kraken=FakeKraken(NOW, days=30, cost=cost))
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "invalid_data"
    assert failure.value.translation_placeholders == {"detail": "unexpected data"}


@pytest.mark.parametrize("field", ["value", "cost"])
async def test_an_hour_too_large_to_add_is_unusable_data(
    hass: HomeAssistant, field: str
) -> None:
    """A finite number past any hour's kWh or cost fails its unit, not every update."""

    def huge(_start: Any, _channel: Any) -> str:
        return "1E+1000000"

    kraken = (
        FakeKraken(NOW, days=30, value=huge)
        if field == "value"
        else FakeKraken(NOW, days=30, cost=huge)
    )
    session = FakeMeridianSession(NOW, kraken=kraken)
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "invalid_data"
    assert failure.value.translation_placeholders == {"detail": "unexpected data"}


async def test_a_json_rejection_after_a_fresh_session_ends_the_sign_in(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW)
    session.hook = _measurements(json_response({"error": "Synthetic"}, status=401))
    coordinator = coordinator_for(hass, session)

    with pytest.raises(ConfigEntryAuthFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "invalid_auth"
    # The session was renewed once before.
    assert session.operations.count("measurements") == 2


async def test_a_gateway_page_refusing_a_fresh_session_is_a_refusal(
    hass: HomeAssistant,
) -> None:
    """A 403 page from a gateway is not Meridian ending the sign-in."""
    session = FakeMeridianSession(NOW)
    session.hook = _measurements(
        Response(b"<html>Forbidden</html>", status=403, headers=_HTML)
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "invalid_data"
    assert failure.value.translation_placeholders == {"detail": "HTTP 403"}


def _entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        unique_id="synthetic-user-0123",
        data={
            CONF_REFRESH_TOKEN: "synthetic-refresh-token",
            CONF_FIREBASE_USER_ID: "synthetic-user-0123",
        },
    )
    entry.add_to_hass(hass)
    return entry


async def test_the_notice_comes_after_three_failed_updates_and_goes_after_one(
    hass: HomeAssistant,
) -> None:
    entry = _entry(hass)
    session = FakeMeridianSession(NOW)
    session.hook = _measurements(json_response({}, status=503))
    coordinator = coordinator_for(hass, session, config_entry=entry)
    registry = ir.async_get(hass)
    issue_id = f"sync_failing_{entry.entry_id}"

    for update in range(3):
        assert registry.async_get_issue(DOMAIN, issue_id) is None
        with pytest.raises(UpdateFailed):
            await sync(coordinator, NOW + update * HOUR, session)

    issue = registry.async_get_issue(DOMAIN, issue_id)
    assert issue is not None
    assert (issue.severity, issue.is_fixable) == (ir.IssueSeverity.WARNING, False)
    assert issue.translation_key == "sync_failing"
    assert issue.translation_placeholders == {
        "reason": "Couldn't get an answer from Meridian (HTTP 503). Meridian Energy "
        "tries again automatically"
    }
    texts = await translation.async_get_translations(hass, "en", "issues", {DOMAIN})
    description = texts[f"component.{DOMAIN}.issues.sync_failing.description"]
    assert description.format(**issue.translation_placeholders) == (
        "Couldn't get an answer from Meridian (HTTP 503). Meridian Energy tries "
        "again automatically.\n\nThis notice goes away by itself once an update works."
    )

    session.hook = lambda _operation, _variables: None
    await sync(coordinator, NOW + 3 * HOUR, session)
    assert registry.async_get_issue(DOMAIN, issue_id) is None

    # The count starts again.
    session.hook = _measurements(json_response({}, status=503))
    with pytest.raises(UpdateFailed):
        await sync(coordinator, NOW + 4 * HOUR, session)
    assert registry.async_get_issue(DOMAIN, issue_id) is None


@pytest.mark.parametrize("accounts_fail", [False, True])
async def test_a_quiet_unit_never_hides_that_the_others_fail(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture, *, accounts_fail: bool
) -> None:
    """A dormant account beside one Meridian no longer answers: every update fails."""
    caplog.set_level(logging.INFO)
    entry = _entry(hass)
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, _DORMANT))
    dormant = FakeKraken(NOW, withdrawn_from=NOW - 400 * DAY)
    failing = False

    def answer(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation == "accountsList" and failing and accounts_fail:
            return json_response({}, status=503)
        if operation != "measurements":
            return None
        if variables["propertyId"] == property_id(_DORMANT):
            return json_response(dormant.graphql(variables))
        return _GATEWAY if failing else None

    session.hook = answer
    coordinator = coordinator_for(hass, session, config_entry=entry)
    await refresh(coordinator, NOW, session)
    assert [unit["quiet"] for unit in coordinator.unit_diagnostics()] == [False, True]

    failing = True
    # A day on, the accounts are asked for again; when that fails too, the
    # cached ones are kept and the quiet unit is not woken.
    start = NOW + DAY if accounts_fail else NOW
    registry = ir.async_get(hass)
    issue_id = f"sync_failing_{entry.entry_id}"
    for update in range(1, 4):
        await refresh(coordinator, start + update * HOUR, session)
        failure = coordinator.last_exception
        assert isinstance(failure, UpdateFailed)
        assert failure.translation_key == "cannot_connect"
        assert (registry.async_get_issue(DOMAIN, issue_id) is None) == (update < 3)
    assert len(dormant.requests) == 13

    failing = False
    await refresh(coordinator, start + 4 * HOUR, session)
    assert coordinator.last_update_success
    assert registry.async_get_issue(DOMAIN, issue_id) is None
    error = (
        "Error fetching Meridian Energy data: Couldn't get an answer from Meridian "
        "(HTTP 504). Meridian Energy tries again automatically"
    )
    assert [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ] == [(logging.ERROR, error)]


async def test_an_empty_grid_export_never_hides_a_failing_grid_import(
    hass: HomeAssistant,
) -> None:
    session = FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
    empty = FakeKraken(NOW, withdrawn_from=NOW - 400 * DAY)
    session.hook = lambda operation, variables: (
        None
        if operation != "measurements"
        else json_response(empty.graphql(variables))
        if variables["readingDirectionType"] == "GENERATION"
        else _GATEWAY
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(UpdateFailed) as failure:
        await sync(coordinator, NOW, session)

    assert failure.value.translation_key == "cannot_connect"
    # A failed first sync is tried again after 10 minutes.
    assert failure.value.retry_after == 600


@pytest.mark.parametrize(
    ("feed_in", "cut", "answer", "key", "retry_after", "fill_failure"),
    [
        (True, 21, _GATEWAY, "cannot_connect", 600, "cannot_connect (HTTP 504)"),
        (True, 21, _coded("KT-CT-1199"), "rate_limited", 3600, None),
        (False, 56, _GATEWAY, "cannot_connect", 600, "cannot_connect (HTTP 504)"),
    ],
)
async def test_a_unit_without_readings_cut_short_never_hides_a_failing_one(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    *,
    feed_in: bool,
    cut: int,
    answer: Response,
    key: str,
    retry_after: int,
    fill_failure: str | None,
) -> None:
    """An empty grid export, or a dormant account, whose search fails part-way."""
    caplog.set_level(logging.INFO)
    entry = _entry(hass)
    session = (
        FakeMeridianSession(NOW, feed_in=(ACCOUNT_NUMBER,))
        if feed_in
        else FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, _DORMANT))
    )
    empty = FakeKraken(NOW, withdrawn_from=NOW - 400 * DAY)
    cut_on = (TODAY - cut * DAY).isoformat()

    def measurements(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation != "measurements":
            return None
        if variables["propertyId"] == property_id(ACCOUNT_NUMBER) and (
            variables["readingDirectionType"] == "CONSUMPTION"
        ):
            return _GATEWAY
        if variables["endOn"] == cut_on:
            return answer
        return json_response(empty.graphql(variables))

    session.hook = measurements
    coordinator = coordinator_for(hass, session, config_entry=entry)

    await refresh(coordinator, NOW, session)

    failure = coordinator.last_exception
    assert isinstance(failure, UpdateFailed)
    assert (failure.translation_key, failure.retry_after) == (key, retry_after)
    lines = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert [r.levelno for r in lines] == [logging.ERROR]
    assert lines[0].getMessage().startswith("Error fetching Meridian Energy data: ")
    unit = coordinator.unit_diagnostics()[1]
    assert (unit["quiet"], unit["fill_failure"]) == (False, fill_failure)


@pytest.mark.parametrize("all_fail_first", [False, True])
async def test_a_unit_that_failed_then_went_quiet_shows_no_failure(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture, *, all_fail_first: bool
) -> None:
    caplog.set_level(logging.INFO)
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, _DORMANT))
    dormant = FakeKraken(NOW, withdrawn_from=NOW - 400 * DAY)
    first = True

    def measurements(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation != "measurements":
            return None
        if variables["propertyId"] == property_id(_DORMANT):
            return _GATEWAY if first else json_response(dormant.graphql(variables))
        return _GATEWAY if first and all_fail_first else None

    session.hook = measurements
    coordinator = coordinator_for(hass, session)
    await refresh(coordinator, NOW, session)
    assert [unit["last_failure"] for unit in coordinator.unit_diagnostics()] == [
        "cannot_connect" if all_fail_first else None,
        "cannot_connect",
    ]

    first = False
    await refresh(coordinator, NOW + HOUR, session)

    assert coordinator.last_update_success
    unit = coordinator.unit_diagnostics()[1]
    assert (unit["quiet"], unit["last_failure"]) == (True, None)
    # It has not recovered, so it never says it is updating again.
    assert not [r for r in caplog.records if "is updating" in r.getMessage()]

    # A day on it has readings again: it says so only if it warned.
    dormant.withdrawn_from = None
    await refresh(coordinator, NOW + DAY + HOUR, session)

    assert coordinator.last_update_success
    assert coordinator.unit_diagnostics()[1]["quiet"] is False
    again = [r.getMessage() for r in caplog.records if "is updating" in r.getMessage()]
    assert again == (
        []
        if all_fail_first
        else ["Meridian Energy is updating grid import for property 2 of 2 again"]
    )


async def test_a_code_shaped_like_an_identifier_is_never_shown(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Meridian's codes are shown only in Kraken's own form.

    Answers carry a code shaped like an account number: first for one
    account's import, the other's older history (with a query limit's code,
    so its window waits), the balance and the prices, then for every request.
    """
    caplog.set_level(logging.DEBUG)
    entry = _entry(hass)
    session = FakeMeridianSession(NOW, accounts=(ACCOUNT_NUMBER, _DORMANT))
    session.billing_period = (TODAY - 10 * DAY, TODAY + 20 * DAY)
    hostile = _coded(ACCOUNT_NUMBER)
    older = (TODAY - 35 * DAY).isoformat()
    everything = False

    def answer(operation: str, variables: dict[str, Any]) -> Response | None:
        if operation in {"balance", "prices"}:
            return hostile
        if operation != "measurements":
            return None
        if everything or variables["propertyId"] == property_id(ACCOUNT_NUMBER):
            return hostile
        if variables["endOn"] == older:
            return _coded(ACCOUNT_NUMBER, "KT-CT-1188")
        return None

    session.hook = answer
    coordinator = coordinator_for(hass, session, config_entry=entry)
    await refresh(coordinator, NOW, session)
    assert coordinator.last_update_success
    fill_failures = [unit["fill_failure"] for unit in coordinator.unit_diagnostics()]
    assert fill_failures == [None, "invalid_data (an error code, KT-CT-1188)"]
    diagnostics = [coordinator.unit_diagnostics()]
    for update in range(1, 3):
        await refresh(coordinator, NOW + update * HOUR, session)
        assert coordinator.last_update_success
    assert "Couldn't get the balance (an error code)" in caplog.messages
    assert (
        "Meridian Energy couldn't update grid import for property 1 of 2 (Meridian "
        "refused the request or sent an answer Meridian Energy can't read: an "
        "error code); the rest keeps updating"
    ) in caplog.messages
    diagnostics.append(coordinator.extras_diagnostics())

    everything = True
    for update in range(3, 6):
        await refresh(coordinator, NOW + update * HOUR, session)

    failure = coordinator.last_exception
    assert isinstance(failure, UpdateFailed)
    assert failure.translation_placeholders == {"detail": "an error code"}
    issue = ir.async_get(hass).async_get_issue(DOMAIN, f"sync_failing_{entry.entry_id}")
    assert issue is not None
    assert "(an error code)" in issue.translation_placeholders["reason"]
    shown = (
        str(failure),
        str(issue.translation_placeholders),
        str(diagnostics),
        str(coordinator.unit_diagnostics()),
        *(
            logging.Formatter().format(record)
            for record in caplog.records
            if record.name.startswith("custom_components.meridian_energy")
        ),
    )
    for text in shown:
        assert ACCOUNT_NUMBER not in text, text


async def test_no_account_raises_the_notice_at_once_and_removal_clears_it(
    hass: HomeAssistant,
) -> None:
    entry = _entry(hass)
    session = FakeMeridianSession(NOW)
    session.accounts = ()
    coordinator = coordinator_for(hass, session, config_entry=entry)
    registry = ir.async_get(hass)
    issue_id = f"sync_failing_{entry.entry_id}"

    with pytest.raises(UpdateFailed):
        await sync(coordinator, NOW, session)
    issue = registry.async_get_issue(DOMAIN, issue_id)
    assert issue is not None
    assert "no active electricity account" in issue.translation_placeholders["reason"]

    await async_remove_entry(hass, entry)  # type: ignore[arg-type]
    assert registry.async_get_issue(DOMAIN, issue_id) is None


async def test_an_ended_sign_in_never_raises_the_notice(hass: HomeAssistant) -> None:
    entry = _entry(hass)
    session = FakeMeridianSession(NOW)
    session.hook = _measurements(json_response({}, status=401))
    coordinator = coordinator_for(hass, session, config_entry=entry)

    for update in range(4):
        with pytest.raises(ConfigEntryAuthFailed):
            await sync(coordinator, NOW + update * HOUR, session)

    assert (
        ir.async_get(hass).async_get_issue(DOMAIN, f"sync_failing_{entry.entry_id}")
        is None
    )


@pytest.mark.parametrize(
    ("answer", "accounts", "error", "key"),
    [
        (None, (), ConfigEntryNotReady, "no_accounts"),
        (json_response({}, status=401), None, ConfigEntryAuthFailed, "invalid_auth"),
        (Response(b"", status=429), None, ConfigEntryNotReady, "rate_limited"),
        (json_response({}, status=504), None, ConfigEntryNotReady, "cannot_connect"),
        (_coded("KT-CT-4321"), None, ConfigEntryNotReady, "invalid_data"),
    ],
)
async def test_setup_maps_each_failure_of_the_account_list(
    hass: HomeAssistant,
    answer: Response | None,
    accounts: tuple[str, ...] | None,
    error: type[Exception],
    key: str,
) -> None:
    session = FakeMeridianSession(NOW)
    if accounts is not None:
        session.accounts = accounts
    session.hook = lambda operation, _variables: (
        answer if operation == "accountsList" else None
    )
    coordinator = coordinator_for(hass, session)

    with pytest.raises(error) as failure:
        await coordinator.async_prepare_topology()

    assert getattr(failure.value, "translation_key", None) == key
    assert str(failure.value) != key


async def test_an_entry_that_cannot_reach_meridian_retries_setup(
    hass: HomeAssistant,
) -> None:
    entry = _entry(hass)
    session = FakeMeridianSession(NOW)
    session.hook = lambda operation, _variables: (
        json_response({}, status=503) if operation == "accountsList" else None
    )
    with patch(
        "custom_components.meridian_energy.async_get_clientsession",
        return_value=session,
    ):
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_RETRY
    assert entry.error_reason_translation_key == "cannot_connect"

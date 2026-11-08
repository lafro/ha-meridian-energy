"""
How each kind of failure reads: its category, its key and detail, and the notice.

Each case runs the real client and transport in front of a synthetic
Meridian service, and checks the key and detail of the failure Home
Assistant shows, and when the repair notice comes and goes. Every value is
synthetic.
"""

from __future__ import annotations

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

from .meridian_http import FakeMeridianSession, Response, json_response
from .syncs import HOUR, NOW, coordinator_for, sync

_HTML = {"Content-Type": "text/html"}


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
            "no answer in time",
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
        (json_response({"data": None}), "invalid_data", "an answer it can't read"),
        (
            Response(b"<html>ok</html>", headers=_HTML),
            "invalid_data",
            "an answer it can't read",
        ),
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

    session.hook = lambda _operation, _variables: None
    await sync(coordinator, NOW + 3 * HOUR, session)
    assert registry.async_get_issue(DOMAIN, issue_id) is None

    # The count starts again.
    session.hook = _measurements(json_response({}, status=503))
    with pytest.raises(UpdateFailed):
        await sync(coordinator, NOW + 4 * HOUR, session)
    assert registry.async_get_issue(DOMAIN, issue_id) is None


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

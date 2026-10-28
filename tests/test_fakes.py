"""Contracts for the synthetic Meridian the tests and the journeys use."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from custom_components.meridian_energy.const import (
    AUTH_EMAIL_URL,
    AUTH_OTP_URL,
    FIREBASE_API_KEY,
    FIREBASE_CUSTOM_TOKEN_URL,
    FIREBASE_REFRESH_URL,
    GRAPHQL_URL,
)

from .kraken_fake import FakeKraken, cursor
from .meridian_http import (
    ACCOUNT_NUMBER,
    MEASUREMENTS_NAME,
    MEASUREMENTS_URL,
    PROPERTY_ID,
    FakeMeridianSession,
    Response,
    json_response,
    property_address,
    property_id,
)

_NOW = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)


async def _body(response: Response) -> Any:
    async with response as answered:
        return json.loads(await answered.read())


def _graphql(name: str, variables: dict[str, Any]) -> dict[str, Any]:
    return {
        "operationName": name,
        "query": f"query {name}($x: Int) {{ x }}",
        "variables": variables,
    }


def _window(account: str, **extra: Any) -> dict[str, Any]:
    return {
        "accountNumber": account,
        "propertyId": property_id(account),
        "startOn": "2026-07-10",
        "endOn": "2026-07-16",
        "last": 24,
        "readingFrequencyType": "HOUR_INTERVAL",
        "readingDirectionType": "CONSUMPTION",
        "readingQualityType": "COMBINED",
        **extra,
    }


async def test_the_default_login_has_the_one_synthetic_account() -> None:
    session = FakeMeridianSession(_NOW)
    listed = await _body(session.post(GRAPHQL_URL, json=_graphql("accountsList", {})))
    assert listed["data"]["viewer"]["accounts"] == [
        {"number": ACCOUNT_NUMBER, "status": "ACTIVE", "id": "1"}
    ]
    account = await _body(
        session.post(
            GRAPHQL_URL, json=_graphql("account", {"accountNumber": ACCOUNT_NUMBER})
        )
    )
    (prop,) = account["data"]["account"]["properties"]
    assert (prop["id"], prop["address"]) == (PROPERTY_ID, "1 Synthetic Street")
    (meter,) = prop["meterPoints"]
    assert [register["isFeedIn"] for register in meter["registers"]] == [False]


async def test_a_login_can_have_several_accounts_and_a_feed_in() -> None:
    session = FakeMeridianSession(
        _NOW, accounts=("A-TEST-1", "A-TEST-2"), feed_in=("A-TEST-2",)
    )
    listed = await _body(session.post(GRAPHQL_URL, json=_graphql("accountsList", {})))
    assert [
        (item["number"], item["id"]) for item in listed["data"]["viewer"]["accounts"]
    ] == [("A-TEST-1", "1"), ("A-TEST-2", "2")]
    second = await _body(
        session.post(
            GRAPHQL_URL, json=_graphql("account", {"accountNumber": "A-TEST-2"})
        )
    )
    (prop,) = second["data"]["account"]["properties"]
    assert prop["id"] == property_id("A-TEST-2") != PROPERTY_ID
    assert prop["address"] == property_address("A-TEST-2")
    (meter,) = prop["meterPoints"]
    assert [register["isFeedIn"] for register in meter["registers"]] == [False, True]
    # The account set can change between requests.
    session.accounts = ("A-TEST-2",)
    with pytest.raises(AssertionError, match="does not have"):
        session.post(
            GRAPHQL_URL, json=_graphql("account", {"accountNumber": "A-TEST-1"})
        )


async def test_measurements_name_the_property_asked_for() -> None:
    session = FakeMeridianSession(_NOW, accounts=("A-TEST-1", "A-TEST-2"))
    body = await _body(
        session.post(
            MEASUREMENTS_URL, json=_graphql(MEASUREMENTS_NAME, _window("A-TEST-2"))
        )
    )
    measured = body["data"]["account"]["property"]
    assert measured["id"] == property_id("A-TEST-2")
    assert measured["measurements"]["edges"]


async def test_the_auth_hook_replaces_login_and_session_answers() -> None:
    session = FakeMeridianSession(_NOW)
    urls = {
        "send_code": AUTH_EMAIL_URL,
        "check_code": AUTH_OTP_URL,
        "session_sign_in": f"{FIREBASE_CUSTOM_TOKEN_URL}?key={FIREBASE_API_KEY}",
        "session_refresh": f"{FIREBASE_REFRESH_URL}?key={FIREBASE_API_KEY}",
    }
    defaults = {name: await _body(session.post(url)) for name, url in urls.items()}
    assert defaults["send_code"] == {"success": True}
    assert defaults["session_refresh"]["expires_in"] == "3600"
    seen: list[str] = []

    def reject_refresh(operation: str) -> Response | None:
        seen.append(operation)
        if operation == "session_refresh":
            return json_response({"error": {"message": "synthetic"}}, status=400)
        return None

    session.auth_hook = reject_refresh
    rejected = session.post(urls["session_refresh"])
    assert rejected.status == 400
    assert await _body(session.post(urls["check_code"])) == defaults["check_code"]
    assert seen == ["session_refresh", "check_code"]
    # GraphQL answers are the GraphQL hook's alone.
    await _body(session.post(GRAPHQL_URL, json=_graphql("accountsList", {})))
    assert seen == ["session_refresh", "check_code"]


def _page(kraken: FakeKraken, **extra: Any) -> dict[str, Any]:
    variables = _window(ACCOUNT_NUMBER, last=24, **extra)
    page: dict[str, Any] = kraken.graphql(variables)["data"]["account"]["property"][
        "measurements"
    ]
    return page


def test_a_cursor_may_be_sent_again_only_when_the_fake_allows_it() -> None:
    for repeat in (False, True):
        kraken = FakeKraken(_NOW, repeat_cursors=repeat)
        first = _page(kraken)
        start = first["pageInfo"]["startCursor"]
        _page(kraken, before=start)
        if repeat:
            _page(kraken, before=start)
            assert kraken.violations == []
        else:
            with pytest.raises(AssertionError, match="sent twice"):
                _page(kraken, before=start)
    # A cursor no page issued under these arguments is refused either way.
    kraken = FakeKraken(_NOW, repeat_cursors=True)
    with pytest.raises(AssertionError, match="arguments differ"):
        _page(kraken, before=cursor(3))


def test_rows_follow_now_and_a_fixed_history_start() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    kraken = FakeKraken(_NOW, history_start=start)
    today = _NOW.date()
    before = kraken.window_rows(today, today)
    kraken.now = _NOW + timedelta(days=1)
    after = kraken.window_rows(today + timedelta(days=1), today + timedelta(days=1))
    assert before
    assert after
    assert kraken.history_start == start
    assert kraken.window_rows(date(2025, 12, 1), date(2025, 12, 7)) == []

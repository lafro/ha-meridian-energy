"""Config flow for Meridian Energy."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.const import CONF_EMAIL
from homeassistant.core import callback
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import SelectOptionDict

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianOtpError,
)
from .const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
)
from .coordinator import MeridianDataCoordinator
from .models import MeridianAccount, MeridianTokenSet

OTP_LENGTH = 6
_LOGGER = logging.getLogger(__name__)


class MeridianEnergyConfigFlow(ConfigFlow, domain=DOMAIN):
    """Set up a Meridian Energy account."""

    VERSION = 2
    MINOR_VERSION = 0

    def __init__(self) -> None:
        self._email: str | None = None
        self._journey_id: str | None = None
        self._reauth_entry: ConfigEntry | None = None
        self._pending_data: dict[str, Any] | None = None
        self._tokens: MeridianTokenSet | None = None
        self._accounts: tuple[MeridianAccount, ...] = ()
        self._selected_accounts: frozenset[str] = frozenset()
        self._initial_import_task: asyncio.Task[None] | None = None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Collect the account email and send a login code."""
        errors: dict[str, str] = {}
        if user_input is not None:
            email = str(user_input[CONF_EMAIL]).strip().lower()
            await self.async_set_unique_id(email)
            self._abort_if_unique_id_configured()
            self._email = email
            self._journey_id = str(uuid4())
            try:
                await self._client.async_send_otp(email, self._journey_id)
            except MeridianConnectionError:
                errors["base"] = "cannot_connect"
            except MeridianAuthenticationError:
                errors["base"] = "email_not_found"
            else:
                return await self.async_step_otp()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({vol.Required(CONF_EMAIL): str}),
            errors=errors,
        )

    async def async_step_otp(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Validate the six-digit emailed code."""
        if self._email is None or self._journey_id is None:
            return self.async_abort(reason="login_expired")
        errors: dict[str, str] = {}
        if user_input is not None:
            otp = str(user_input["otp"]).strip()
            if len(otp) != OTP_LENGTH or not otp.isascii() or not otp.isdigit():
                errors["base"] = "otp_invalid"
                return self._show_otp_form(errors)
            try:
                tokens = await self._client.async_validate_otp(
                    self._email, otp, self._journey_id
                )
            except MeridianOtpError as err:
                errors["base"] = _otp_error_key(err.code)
            except MeridianConnectionError:
                errors["base"] = "cannot_connect"
            except MeridianAuthenticationError:
                errors["base"] = "invalid_auth"
            else:
                data = {
                    CONF_EMAIL: self._email,
                    CONF_REFRESH_TOKEN: tokens.refresh_token,
                    CONF_FIREBASE_USER_ID: tokens.user_id,
                }
                if self._reauth_entry is not None:
                    return self.async_update_reload_and_abort(
                        self._reauth_entry,
                        data_updates=data,
                        reason="reauth_successful",
                    )
                return await self._async_prepare_accounts(tokens, data)

        return self._show_otp_form(errors)

    async def _async_prepare_accounts(
        self, tokens: MeridianTokenSet, data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Discover accounts after authentication and route to selection."""
        self._pending_data = data
        self._tokens = tokens
        try:
            self._accounts = await self._authenticated_client(
                tokens
            ).async_get_accounts()
        except MeridianConnectionError:
            return self._show_otp_form({"base": "cannot_connect"})
        except MeridianAuthenticationError, ValueError:
            return self._show_otp_form({"base": "invalid_auth"})
        if not self._accounts:
            return self.async_abort(reason="no_accounts")
        if len(self._accounts) > 1:
            return await self.async_step_accounts()
        self._selected_accounts = frozenset({self._accounts[0].number})
        return await self._start_initial_import()

    async def async_step_accounts(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Let multi-account customers choose the accounts to import."""
        if self._tokens is None or not self._accounts:
            return self.async_abort(reason="login_expired")
        errors: dict[str, str] = {}
        if user_input is not None:
            selected = frozenset(
                str(item) for item in user_input[CONF_SELECTED_ACCOUNTS]
            )
            available = {account.number for account in self._accounts}
            if not selected or not selected.issubset(available):
                errors["base"] = "select_account"
            else:
                self._selected_accounts = selected
                return await self._start_initial_import()

        options = [
            SelectOptionDict(value=account.number, label=_account_label(account))
            for account in self._accounts
        ]
        return self.async_show_form(
            step_id="accounts",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SELECTED_ACCOUNTS,
                        default=[account.number for account in self._accounts],
                    ): selector.SelectSelector(
                        selector.SelectSelectorConfig(
                            options=options,
                            multiple=True,
                            mode=selector.SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
        )

    async def _start_initial_import(self) -> ConfigFlowResult:
        """Start the visible history import after account selection."""
        if self._tokens is None or self._pending_data is None:
            return self.async_abort(reason="login_expired")
        self._pending_data[CONF_SELECTED_ACCOUNTS] = sorted(self._selected_accounts)
        self._initial_import_task = self.hass.async_create_task(
            self._async_initial_import(self._tokens, self._selected_accounts)
        )
        return await self.async_step_initial_import()

    async def async_step_initial_import(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Show progress while importing the initial 90 days of history."""
        del user_input
        if self._initial_import_task is None or self._pending_data is None:
            return self.async_abort(reason="login_expired")
        if not self._initial_import_task.done():
            return self.async_show_progress(
                step_id="initial_import",
                progress_action="initial_import",
                progress_task=self._initial_import_task,
            )
        try:
            self._initial_import_task.result()
        except Exception:
            _LOGGER.exception("Initial Meridian history import failed")
            return self.async_abort(reason="initial_import_failed")
        return self.async_show_progress_done(next_step_id="finish")

    async def async_step_finish(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Create the config entry after the initial import completes."""
        del user_input
        if self._email is None or self._pending_data is None:
            return self.async_abort(reason="login_expired")
        return self.async_create_entry(title=self._email, data=self._pending_data)

    async def _async_initial_import(
        self, tokens: MeridianTokenSet, selected_accounts: frozenset[str]
    ) -> None:
        """Import history before entry creation so progress is visible to the user."""
        client = self._authenticated_client(tokens)
        coordinator = MeridianDataCoordinator(
            self.hass, client, selected_accounts=selected_accounts
        )
        await coordinator.async_fetch_and_import()

    def _authenticated_client(self, tokens: MeridianTokenSet) -> MeridianApiClient:
        """Create a client using the session validated by this flow."""
        return MeridianApiClient(async_get_clientsession(self.hass), tokens=tokens)

    def _show_otp_form(self, errors: dict[str, str]) -> ConfigFlowResult:
        """Show the serializable six-digit login-code form."""
        return self.async_show_form(
            step_id="otp",
            data_schema=vol.Schema(
                {
                    vol.Required("otp"): selector.TextSelector(
                        selector.TextSelectorConfig(
                            type=selector.TextSelectorType.TEL,
                            autocomplete="one-time-code",
                        )
                    )
                }
            ),
            errors=errors,
            description_placeholders={"email": self._email or ""},
        )

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> ConfigFlowResult:
        """Begin reauthentication for an expired Firebase session."""
        self._reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        if self._reauth_entry is None:
            return self.async_abort(reason="reauth_entry_missing")
        self._email = str(entry_data[CONF_EMAIL]).strip().lower()
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm before sending a new login code."""
        errors: dict[str, str] = {}
        if user_input is not None:
            if self._email is None:
                return self.async_abort(reason="login_expired")
            self._journey_id = str(uuid4())
            try:
                await self._client.async_send_otp(self._email, self._journey_id)
            except MeridianConnectionError:
                errors["base"] = "cannot_connect"
            except MeridianAuthenticationError:
                errors["base"] = "email_not_found"
            else:
                return await self.async_step_otp()
        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema({}),
            errors=errors,
            description_placeholders={"email": self._email or ""},
        )

    @property
    def _client(self) -> MeridianApiClient:
        return MeridianApiClient(async_get_clientsession(self.hass))

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> MeridianOptionsFlow:
        """Return an account-selection options flow."""
        return MeridianOptionsFlow(config_entry)


class MeridianOptionsFlow(OptionsFlow):
    """Allow selected Meridian accounts to be changed safely."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        self._entry = config_entry
        self._accounts: tuple[MeridianAccount, ...] = ()

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Refresh topology and save the selected account set."""
        errors: dict[str, str] = {}
        if not self._accounts:
            tokens = MeridianTokenSet(
                id_token="",
                refresh_token=str(self._entry.data[CONF_REFRESH_TOKEN]),
                expires_at=datetime.fromtimestamp(0, UTC),
                user_id=str(self._entry.data[CONF_FIREBASE_USER_ID]),
            )
            client = MeridianApiClient(
                async_get_clientsession(self.hass), tokens=tokens
            )
            try:
                self._accounts = await client.async_get_accounts()
            except MeridianConnectionError:
                errors["base"] = "cannot_connect"
            except MeridianAuthenticationError:
                errors["base"] = "invalid_auth"

        available = {account.number for account in self._accounts}
        if user_input is not None and available:
            selected = frozenset(
                str(item) for item in user_input[CONF_SELECTED_ACCOUNTS]
            )
            if not selected or not selected.issubset(available):
                errors["base"] = "select_account"
            else:
                return self.async_create_entry(
                    data={CONF_SELECTED_ACCOUNTS: sorted(selected)}
                )

        current = self._entry.options.get(
            CONF_SELECTED_ACCOUNTS,
            self._entry.data.get(CONF_SELECTED_ACCOUNTS, sorted(available)),
        )
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SELECTED_ACCOUNTS, default=list(current)
                    ): selector.SelectSelector(
                        selector.SelectSelectorConfig(
                            options=[
                                SelectOptionDict(
                                    value=account.number,
                                    label=_account_label(account),
                                )
                                for account in self._accounts
                            ],
                            multiple=True,
                            mode=selector.SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
        )


def _otp_error_key(code: str) -> str:
    return {
        "OTP_EXPIRED": "otp_expired",
        "OTP_INVALID": "otp_invalid",
        "OTP_TOO_MANY_ATTEMPTS": "otp_too_many_attempts",
        "OTP_NOT_FOUND": "otp_not_found",
    }.get(code, "invalid_auth")


def _account_label(account: MeridianAccount) -> str:
    """Return a recognisable local-only label without exposing meter IDs."""
    address = (
        account.properties[0].address if account.properties else "Meridian account"
    )
    return f"{address} · account ending {account.number[-4:]}"

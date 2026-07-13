"""Config flow for Meridian Energy."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import uuid4

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_EMAIL
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianOtpError,
)
from .const import CONF_FIREBASE_USER_ID, CONF_REFRESH_TOKEN, DOMAIN
from .coordinator import MeridianDataCoordinator
from .models import MeridianTokenSet

OTP_LENGTH = 6
_LOGGER = logging.getLogger(__name__)


class MeridianEnergyConfigFlow(ConfigFlow, domain=DOMAIN):
    """Set up a Meridian Energy account."""

    VERSION = 1
    MINOR_VERSION = 1

    def __init__(self) -> None:
        self._email: str | None = None
        self._journey_id: str | None = None
        self._reauth_entry: ConfigEntry | None = None
        self._pending_data: dict[str, Any] | None = None
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
                self._pending_data = data
                self._initial_import_task = self.hass.async_create_task(
                    self._async_initial_import(tokens)
                )
                return await self.async_step_initial_import()

        return self._show_otp_form(errors)

    async def async_step_initial_import(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Show progress while importing the initial year of history."""
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

    async def _async_initial_import(self, tokens: MeridianTokenSet) -> None:
        """Import history before entry creation so progress is visible to the user."""
        client = MeridianApiClient(
            async_get_clientsession(self.hass),
            tokens=tokens,
        )
        coordinator = MeridianDataCoordinator(self.hass, client)
        await coordinator.async_fetch_and_import()

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


def _otp_error_key(code: str) -> str:
    return {
        "OTP_EXPIRED": "otp_expired",
        "OTP_INVALID": "otp_invalid",
        "OTP_TOO_MANY_ATTEMPTS": "otp_too_many_attempts",
        "OTP_NOT_FOUND": "otp_not_found",
    }.get(code, "invalid_auth")

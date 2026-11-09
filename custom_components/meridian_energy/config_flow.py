"""Config flow for Meridian Energy."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any
from uuid import uuid4

import probatio
from homeassistant.config_entries import (
    SOURCE_IGNORE,
    SOURCE_REAUTH,
    ConfigEntryState,
    ConfigFlow,
    ConfigFlowResult,
)
from homeassistant.const import CONF_EMAIL
from homeassistant.core import callback
from homeassistant.data_entry_flow import AbortFlow, FlowResultType
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianOtpError,
    MeridianRateLimitError,
    MeridianRejectedError,
    http_status,
)
from .const import (
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    NAME,
)
from .graphql import is_service_failure
from .models import MeridianTokenSet

OTP_LENGTH = 6
_LOGGER = logging.getLogger(__name__)
# How a submission ended, for one that waited on it: the abort it returned or
# raised, the entry it created, or None when the flow went on to another step.
type _SubmissionEnd = ConfigFlowResult | AbortFlow | None


class MeridianEnergyConfigFlow(ConfigFlow, domain=DOMAIN):
    """
    Set up a Meridian login: email, emailed code, then the entry.

    The entry follows every active or dormant account on the login. Its
    history is fetched by the entry's own first update, in the background,
    so a slow Meridian never fails setup.
    """

    VERSION = CONFIG_ENTRY_VERSION
    MINOR_VERSION = 1

    def __init__(self) -> None:
        self._email: str | None = None
        self._journey_id: str | None = None
        self._pending_data: dict[str, Any] | None = None
        self._discovery_client: MeridianApiClient | None = None
        # The submission waiting on Meridian, if any.
        self._submission: asyncio.Future[_SubmissionEnd] | None = None
        # Set once Home Assistant has removed the flow, finished or aborted.
        self._ended = asyncio.Event()
        # Whether the flow has created its entry, which Home Assistant sets
        # up before it removes the flow.
        self._created = False

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Collect the login email and send a login code."""
        if user_input is None:
            return self._show_user_form({})
        return await self._async_submit(
            partial(self._async_send_login_code, user_input)
        )

    async def _async_send_login_code(
        self, user_input: dict[str, Any]
    ) -> ConfigFlowResult:
        """
        Email the login a code, unless it is set up already.

        A setup of the same email left at a form, as in a closed browser
        tab, does not block this one: Home Assistant aborts it once either
        creates the entry. An entry an earlier version made, refused as too
        old, must be deleted first, so the abort says so.
        """
        email = str(user_input[CONF_EMAIL]).strip().lower()
        existing = await self.async_set_unique_id(email, raise_on_progress=False)
        if (
            existing is not None
            and existing.state is ConfigEntryState.MIGRATION_ERROR
            and existing.source != SOURCE_IGNORE
        ):
            return self.async_abort(reason="earlier_entry")
        self._abort_if_unique_id_configured()
        self._email = email
        self._journey_id = str(uuid4())
        try:
            await self._client.async_send_otp(email, self._journey_id)
        except MeridianConnectionError as err:
            error = _connection_error_key(err)
        except MeridianAuthenticationError:
            error = "email_not_found"
        except ValueError:
            error = _unreadable_answer()
        else:
            return await self.async_step_otp()
        return self._show_user_form({"base": error})

    def _show_user_form(self, errors: dict[str, str]) -> ConfigFlowResult:
        """Show the email form."""
        return self.async_show_form(
            step_id="user",
            data_schema=probatio.Schema(
                {
                    probatio.Required(CONF_EMAIL): selector.TextSelector(
                        selector.TextSelectorConfig(
                            type=selector.TextSelectorType.EMAIL,
                            autocomplete="email",
                        )
                    )
                }
            ),
            errors=errors,
        )

    @callback
    def async_remove(self) -> None:
        """Note that Home Assistant removed this flow."""
        self._ended.set()

    async def async_step_otp(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Validate the six-digit emailed code."""
        if self._email is None or self._journey_id is None:
            return self.async_abort(reason="login_expired")
        if user_input is None:
            return self._show_otp_form({})
        return await self._async_submit(
            partial(self._async_check_code, user_input, self._email, self._journey_id)
        )

    async def _async_check_code(
        self, user_input: dict[str, Any], email: str, journey_id: str
    ) -> ConfigFlowResult:
        """Check the emailed code, then renew the entry's sign-in or find accounts."""
        otp = str(user_input["otp"]).strip()
        if len(otp) != OTP_LENGTH or not otp.isascii() or not otp.isdigit():
            return self._show_otp_form({"base": "otp_invalid"})
        try:
            tokens = await self._client.async_validate_otp(email, otp, journey_id)
        except MeridianOtpError as err:
            error = _otp_error_key(err.code)
        except MeridianConnectionError as err:
            error = _connection_error_key(err)
        except MeridianAuthenticationError:
            error = "invalid_auth"
        except ValueError:
            error = _unreadable_answer()
        else:
            data = {
                CONF_EMAIL: email,
                CONF_REFRESH_TOKEN: tokens.refresh_token,
                CONF_FIREBASE_USER_ID: tokens.user_id,
            }
            if self.source == SOURCE_REAUTH:
                return self.async_update_reload_and_abort(
                    self._get_reauth_entry(), data_updates=data
                )
            self._pending_data = data
            self._discovery_client = self._authenticated_client(tokens)
            return await self._async_discover_accounts()
        return self._show_otp_form({"base": error})

    async def async_step_discovery_retry(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Look for the accounts again with the same sign-in, without a new code."""
        del user_input
        return await self._async_submit(self._async_discover_accounts)

    async def _async_discover_accounts(self) -> ConfigFlowResult:
        """Create the entry once the login is known to have an account."""
        if self._discovery_client is None or self._pending_data is None:
            return self.async_abort(reason="login_expired")
        try:
            accounts = await self._discovery_client.async_get_accounts()
        except MeridianAuthenticationError:
            return self.async_abort(reason="login_expired")
        except MeridianGraphQLError as err:
            # Errors without a code, as for a failure inside Meridian's
            # service, are Meridian not answering; a coded one is a refusal.
            _log_discovery_failure(err)
            error = (
                "cannot_connect" if is_service_failure(err.codes) else "invalid_data"
            )
        except (MeridianRejectedError, ValueError) as err:
            # Before MeridianConnectionError, as a refusal is one too: a
            # retry asks the same, so the form says so rather than to wait.
            _log_discovery_failure(err)
            error = "invalid_data"
        except MeridianConnectionError as err:
            _log_discovery_failure(err)
            error = _connection_error_key(err)
        else:
            if not accounts:
                return self.async_abort(reason="no_accounts")
            if not any(account.properties for account in accounts):
                # Listed, but with no property with a supply yet.
                return self.async_abort(reason="no_properties")
            # Another setup of the same login may have created its entry.
            self._abort_if_unique_id_configured()
            self._created = True
            return self.async_create_entry(title=NAME, data=self._pending_data)
        return self._show_discovery_retry_form({"base": error})

    def _show_discovery_retry_form(self, errors: dict[str, str]) -> ConfigFlowResult:
        """Show the form that looks for the accounts again with the same sign-in."""
        return self.async_show_form(
            step_id="discovery_retry",
            data_schema=probatio.Schema({}),
            errors=errors,
        )

    def _authenticated_client(self, tokens: MeridianTokenSet) -> MeridianApiClient:
        """Create a client using the session validated by this flow."""
        return MeridianApiClient(
            async_get_clientsession(self.hass),
            tokens=tokens,
            token_update_callback=self._async_store_discovery_tokens,
        )

    async def _async_store_discovery_tokens(self, tokens: MeridianTokenSet) -> None:
        """Keep a session Meridian renewed while the accounts were found."""
        if self._pending_data is not None:
            self._pending_data.update(
                {
                    CONF_REFRESH_TOKEN: tokens.refresh_token,
                    CONF_FIREBASE_USER_ID: tokens.user_id,
                }
            )

    async def _async_submit(
        self, handle: Callable[[], Awaitable[ConfigFlowResult]]
    ) -> ConfigFlowResult:
        """
        Handle one submission at a time; one sent meanwhile waits for it.

        The frontend sends a submission for each press of Enter, even while
        an earlier one is still waiting on Meridian, and the flow API accepts
        any number. One that arrives meanwhile makes no request and changes
        nothing: a second email would start a new sign-in the first code
        might not match, and a second code would sign in twice. It waits for
        the other, then gets the step the flow is at, or the same abort: the
        frontend shows whichever answer reaches the dialog first, so both
        must carry the real next step.

        When the other created the entry, Home Assistant sets the entry up
        before it ends the flow, and a copy of the created entry would make
        it create a second one: the waiting submission answers once the flow
        has ended, with an abort the dialog drops, as it comes second. So
        does a submission that arrives after the entry was created.
        """
        if self._created:
            return await self._answer_after_entry()
        if (running := self._submission) is not None:
            # Shielded: a waiting request that is cancelled leaves the other.
            ended = await asyncio.shield(running)
            if isinstance(ended, AbortFlow):
                raise ended
            if ended is not None and ended["type"] is FlowResultType.CREATE_ENTRY:
                return await self._answer_after_entry()
            if ended is not None:
                return ended.copy()
            # A submission answers a step the flow showed, so there is one.
            step = self.cur_step
            assert step is not None  # noqa: S101
            return step.copy()
        running = self._submission = self.hass.loop.create_future()
        ended = None
        try:
            result = await handle()
        except AbortFlow as err:
            ended = err
            raise
        else:
            if result["type"] in {FlowResultType.ABORT, FlowResultType.CREATE_ENTRY}:
                ended = result
            return result
        finally:
            self._submission = None
            running.set_result(ended)

    async def _answer_after_entry(self) -> ConfigFlowResult:
        """Answer a submission that came after the entry, once the flow has ended."""
        await self._ended.wait()
        return self.async_abort(reason="already_configured")

    def _show_otp_form(self, errors: dict[str, str]) -> ConfigFlowResult:
        """Show the serializable six-digit login-code form."""
        return self.async_show_form(
            step_id="otp",
            data_schema=probatio.Schema(
                {
                    probatio.Required("otp"): selector.TextSelector(
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
        self._email = str(entry_data[CONF_EMAIL]).strip().lower()
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm before sending a new login code."""
        if user_input is None:
            return self._show_reauth_confirm_form({})
        return await self._async_submit(self._async_send_reauth_code)

    async def _async_send_reauth_code(self) -> ConfigFlowResult:
        """Email a new login code to the entry's address."""
        if self._email is None:
            return self.async_abort(reason="login_expired")
        self._journey_id = str(uuid4())
        try:
            await self._client.async_send_otp(self._email, self._journey_id)
        except MeridianConnectionError as err:
            error = _connection_error_key(err)
        except MeridianAuthenticationError:
            # There is no email to correct here: the entry's is the one sent.
            error = "reauth_email_refused"
        except ValueError:
            error = _unreadable_answer()
        else:
            return await self.async_step_otp()
        return self._show_reauth_confirm_form({"base": error})

    def _show_reauth_confirm_form(self, errors: dict[str, str]) -> ConfigFlowResult:
        """Show the form that confirms sending a new login code."""
        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=probatio.Schema({}),
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


def _connection_error_key(err: MeridianConnectionError) -> str:
    """Return the form error for a request to Meridian that got no usable answer."""
    if isinstance(err, MeridianRateLimitError):
        return "rate_limited"
    return "cannot_connect"


def _unreadable_answer() -> str:
    """Log a sign-in answer that can't be read, and return its form error."""
    _LOGGER.debug("A sign-in answer couldn't be read; the form asks to try again")
    return "cannot_connect"


def _log_discovery_failure(err: Exception) -> None:
    """
    Log why account discovery failed, without identifiers.

    A refusal of the request (a client error, or a GraphQL error with a
    code), or data Home Assistant cannot use, is a defect worth reporting,
    so it gets a warning with its traceback. A connection failure, a rate
    limit or GraphQL errors without a code are already on the form, and the
    transport's debug lines describe the answer, so they get a debug line.
    """
    if isinstance(err, MeridianRejectedError | ValueError) or (
        isinstance(err, MeridianGraphQLError) and not is_service_failure(err.codes)
    ):
        _LOGGER.warning(
            "Meridian refused account discovery or returned data Home Assistant "
            "could not use: %s",
            err,
            exc_info=err,
        )
        return
    status = http_status(err)
    _LOGGER.debug(
        "Meridian account discovery failed: error=%s status=%s",
        type(err).__name__,
        "-" if status is None else status,
    )

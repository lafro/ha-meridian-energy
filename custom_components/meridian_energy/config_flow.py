"""Config flow for Meridian Energy."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial
from time import monotonic
from typing import Any
from uuid import uuid4

import probatio
from homeassistant.config_entries import (
    SOURCE_REAUTH,
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
)
from homeassistant.const import CONF_EMAIL, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import CALLBACK_TYPE, Event, callback
from homeassistant.data_entry_flow import AbortFlow, FlowResultType
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import SelectOptionDict
from homeassistant.helpers.update_coordinator import UpdateFailed

from .api import (
    MeridianApiClient,
    MeridianAuthenticationError,
    MeridianConnectionError,
    MeridianGraphQLError,
    MeridianOtpError,
    MeridianRateLimitError,
    MeridianUpstreamError,
    exception_chain,
    http_status,
)
from .const import (
    CONF_AUTO_ADD_ACCOUNTS,
    CONF_FIREBASE_USER_ID,
    CONF_REFRESH_TOKEN,
    CONF_SELECTED_ACCOUNTS,
    DOMAIN,
    NAME,
    READING_CONSUMPTION,
    READING_GENERATION,
    SETUP_HANDOFF,
    SETUP_IMPORT_BUDGET_SECONDS,
    SETUP_IMPORT_MAX_ATTEMPTS,
    SETUP_IMPORT_RETRY_DELAY_SECONDS,
    SETUP_IMPORT_TIMEOUT_SECONDS,
)
from .coordinator import MeridianDataCoordinator, RunBudgetError, format_seconds
from .models import (
    MeridianAccount,
    MeridianSyncData,
    MeridianTokenSet,
    RequestContext,
    SetupHandoff,
)
from .statistics import (
    StatisticsAnchorError,
    StatisticsWriteError,
    async_clear_statistics,
    async_confirm_visible,
    async_has_statistics,
    async_wait_for_recorder,
    consumption_ids,
    generation_ids,
    property_key,
)
from .transport import (
    MeridianTimeoutError,
    MeridianTransportError,
    MeridianUnreadableResponseError,
)
from .units import SyncMemory

OTP_LENGTH = 6
_LOGGER = logging.getLogger(__name__)
# Barriers in each wait for the recorder before an attempt: the second, queued
# once the first has run, is behind an import the recorder queued again. Each
# barrier gets one 30 s wait; a timeout shows the form's cleanup_pending.
_RECORDER_PASSES = 2
# The failure form's option to finish setup without importing any history,
# and the failures it is offered after: Meridian's service failing or not
# answering in time, which the entry's own updates may get past later.
_WITHOUT_HISTORY = "continue_without_history"
_WITHOUT_HISTORY_FAILURES = frozenset({"upstream_timeout", "upstream_unavailable"})
# The same failures on the form after the last attempt allowed, which has no
# retry: its error says so, rather than to wait and try again.
_LAST_FAILURE_ERRORS = {
    "upstream_timeout": "last_upstream_timeout",
    "upstream_unavailable": "last_upstream_unavailable",
}

# The coordinator's UpdateFailed translation keys, mapped to the failure form's
# own error keys. A key missing here shows as "unknown". Kept as an explicit
# table so a test can check that every value exists in strings.json.
_IMPORT_ERRORS = {
    "cannot_connect": "import_cannot_connect",
    "invalid_data": "import_invalid_data",
    "rate_limited": "import_rate_limited",
    "statistics_write": "import_statistics_write",
    "statistics_refused": "import_statistics_refused",
    "upstream_unavailable": "upstream_unavailable",
    "upstream_timeout": "upstream_timeout",
}
_CALL_LABELS = {
    "accounts": "the account list",
    "billing": "the billing period",
    READING_CONSUMPTION: "grid import history",
    READING_GENERATION: "grid export history",
}
# Steps a user-started flow can wait at after its email is known: the forms
# (the email form itself holds the email once a send has failed), and the
# progress-done result an import that ended leaves until the frontend fetches
# the next step, "finish" or "import_failed". A flow left at one of them (for
# example in a closed browser tab) is replaced by a new setup for the same
# email; a flow still importing ("initial_import") is not.
_PARKED_STEPS = frozenset(
    {
        "user",
        "otp",
        "discovery_retry",
        "accounts",
        "import_failed",
        "import_failed_last",
        "finish",
    }
)
# How a submission ended, for one that waited on it: the abort it returned or
# raised, the entry it created, or None when the flow went on to another step.
type _SubmissionEnd = ConfigFlowResult | AbortFlow | None


class SetupTimeLimitError(TimeoutError):
    """An attempt at the setup import was still running at its time limit."""


class MeridianEnergyConfigFlow(ConfigFlow, domain=DOMAIN):
    """Set up a Meridian Energy account."""

    VERSION = 3
    MINOR_VERSION = 1

    def __init__(self) -> None:
        self._email: str | None = None
        self._journey_id: str | None = None
        self._pending_data: dict[str, Any] | None = None
        self._tokens: MeridianTokenSet | None = None
        self._discovery_client: MeridianApiClient | None = None
        self._reconfigure_tokens: MeridianTokenSet | None = None
        self._accounts: tuple[MeridianAccount, ...] = ()
        self._selected_accounts: frozenset[str] = frozenset()
        self._initial_import_task: asyncio.Task[None] | None = None
        # The submission waiting on Meridian or the recorder, if any.
        self._submission: asyncio.Future[_SubmissionEnd] | None = None
        # Set once Home Assistant has removed the flow, finished or aborted.
        self._ended = asyncio.Event()
        # Whether the flow has created its entry, which Home Assistant sets
        # up before it removes the flow.
        self._created = False
        self._initial_statistic_ids: set[str] = set()
        self._existing_statistic_ids: set[str] = set()
        # The failed attempt, as the failure form and its retry need it. The
        # coordinator of an attempt is never kept: a retry builds a new one.
        self._import_attempts = 0
        self._import_error: str | None = None
        self._failure_detail = ""
        self._last_request: RequestContext | None = None
        self._median_request: float | None = None
        # Whether the form shows the option to go on without history ticked,
        # whether setup went on without history, and whether the import that
        # succeeded left some of the history for the entry's updates.
        self._without_history_chosen = False
        self._without_history = False
        self._partial = False
        # The oldest hour of each unit's deep replay that was not confirmed,
        # kept across attempts: each attempt's coordinator updates it in
        # place, and the next reaches back to it (``_note_unconfirmed``).
        self._unconfirmed: dict[tuple[str, str], datetime] = {}
        # When the successful attempt's sync started, and what its
        # coordinator remembered of each direction, handed to the entry.
        self._imported_at: datetime | None = None
        self._memory: SyncMemory | None = None
        # What a successful attempt wrote, read back before the entry exists,
        # and whether its import has returned: a cancellation after that came
        # while Home Assistant was saving the rows, not during the requests.
        self._imported_newest: dict[str, datetime] = {}
        self._import_returned = False
        # When the attempt started and when its import ended (before the
        # rollback, which can wait 30 s on a busy recorder): the warnings time
        # the requests by these. The retry gap counts from _failed_at, once
        # the rollback has run and the failure can be shown.
        self._attempt_started_at = 0.0
        self._attempt_ended_at = 0.0
        self._failed_at = 0.0
        self._retry_after = 0.0
        # Whether the failed attempt's rollback finished within its wait, or
        # the removal is still queued behind other recorder work.
        self._rollback_removed = False
        # Why an attempt was cancelled, for its warning: Home Assistant removed
        # the flow (the dialog was closed), or Home Assistant is stopping.
        self._removed = False
        self._stopping = False

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Collect the account email and send a login code."""
        if user_input is None:
            return self._show_user_form({})
        return await self._async_submit(
            self._show_user_form, partial(self._async_send_login_code, user_input)
        )

    async def _async_send_login_code(
        self, user_input: dict[str, Any]
    ) -> ConfigFlowResult:
        """Replace parked setups for the email, then email it a login code."""
        email = str(user_input[CONF_EMAIL]).strip().lower()
        # Refuse a configured email before touching other flows: a setup
        # whose entry is still being set up waits at "finish" until that
        # returns, and must not be taken for a parked one.
        await self.async_set_unique_id(email, raise_on_progress=False)
        self._abort_if_unique_id_configured()
        self._async_abort_parked_flows(email)
        await self.async_set_unique_id(email)
        self._email = email
        self._journey_id = str(uuid4())
        try:
            await self._client.async_send_otp(email, self._journey_id)
        except MeridianConnectionError as err:
            error = _connection_error_key(err)
        except MeridianAuthenticationError:
            error = "email_not_found"
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
    def _async_abort_parked_flows(self, email: str) -> None:
        """
        Abort earlier setups for this email that wait at a form or ended.

        Closing the browser tab instead of the dialog leaves a flow parked, and
        Home Assistant does not list user-started flows, so it can be neither
        resumed nor dismissed. It holds the email as its unique ID, so until
        Home Assistant restarted every new setup for that email would abort as
        already in progress. A flow whose import ended with nobody watching
        waits at progress done the same way. Statistics a successful import
        wrote are kept: the new setup finds them as existing history and
        continues them, as it would after a restart. A running import is left
        alone; the unique-ID check that follows still refuses to start beside
        it. Two kinds of flow are never reached here, because their entry
        already exists and the new setup has aborted as already configured: a
        flow at "finish" whose entry is being set up, and a reauthentication,
        which carries its entry's unique ID and which Home Assistant aborts
        when that entry is removed or reloaded. A reconfiguration carries no
        unique ID, so every flow found here is a setup.
        """
        for flow in self._async_in_progress(
            include_uninitialized=True, match_context={"unique_id": email}
        ):
            if flow.get("step_id") in _PARKED_STEPS:
                self.hass.config_entries.flow.async_abort(flow["flow_id"])

    @callback
    def async_remove(self) -> None:
        """Note that Home Assistant removed this flow, so no import starts later."""
        self._removed = True
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
            self._show_otp_form,
            partial(self._async_check_code, user_input, self._email, self._journey_id),
        )

    async def _async_check_code(
        self, user_input: dict[str, Any], email: str, journey_id: str
    ) -> ConfigFlowResult:
        """Check the emailed code, then sign in again or find the accounts."""
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
        else:
            data = {
                CONF_EMAIL: email,
                CONF_REFRESH_TOKEN: tokens.refresh_token,
                CONF_FIREBASE_USER_ID: tokens.user_id,
            }
            if self.source == SOURCE_REAUTH:
                # Quality-scale pattern: reauthentication always uses the
                # entry's own stored email, which is its unique ID, so this
                # check guards only against an inconsistent entry.
                await self.async_set_unique_id(email)
                self._abort_if_unique_id_mismatch()
                return self.async_update_reload_and_abort(
                    self._get_reauth_entry(), data_updates=data
                )
            return await self._async_prepare_accounts(tokens, data)
        return self._show_otp_form({"base": error})

    async def _async_prepare_accounts(
        self, tokens: MeridianTokenSet, data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Discover accounts after authentication and route to selection."""
        self._pending_data = data
        self._tokens = tokens
        self._discovery_client = self._authenticated_client(tokens)
        return await self._async_discover_accounts()

    async def async_step_discovery_retry(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Retry what stopped setup after the login, without reusing the OTP."""
        del user_input
        return await self._async_submit(
            self._show_discovery_retry_form, self._async_continue_after_login
        )

    async def _async_continue_after_login(self) -> ConfigFlowResult:
        """
        Repeat the account discovery, or only the wait on the recorder.

        Once the single account is known, this form comes back only while the
        recorder is behind (cleanup_pending), so a retry waits on it again
        without asking Meridian for the accounts again.
        """
        if self._selected_accounts:
            return await self._async_start_attempt(
                self._show_discovery_retry_form, selected=self._selected_accounts
            )
        return await self._async_discover_accounts()

    async def _async_discover_accounts(self) -> ConfigFlowResult:
        """Find the login's accounts, then ask which to import or start importing."""
        if self._discovery_client is None:
            return self.async_abort(reason="login_expired")
        errors: dict[str, str] = {}
        try:
            self._accounts = await self._discovery_client.async_get_accounts()
        except MeridianRateLimitError as err:
            errors["base"] = "rate_limited"
            _log_discovery_failure(err)
        except MeridianConnectionError as err:
            errors["base"] = _connection_error_key(err)
            _log_discovery_failure(err)
        except MeridianAuthenticationError:
            return self.async_abort(reason="login_expired")
        except (MeridianGraphQLError, ValueError) as err:
            errors["base"] = "invalid_data"
            _log_discovery_failure(err)
        if errors:
            return self._show_discovery_retry_form(errors)
        if not self._accounts:
            return self.async_abort(reason="no_accounts")
        if len(self._accounts) > 1:
            return await self.async_step_accounts()
        return await self._async_start_attempt(
            self._show_discovery_retry_form,
            selected=frozenset({self._accounts[0].number}),
        )

    def _show_discovery_retry_form(self, errors: dict[str, str]) -> ConfigFlowResult:
        """Show the form that repeats account discovery with the same login."""
        return self.async_show_form(
            step_id="discovery_retry",
            data_schema=probatio.Schema({}),
            errors=errors,
        )

    async def async_step_accounts(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Let multi-account customers choose the accounts to import."""
        if self._tokens is None or not self._accounts:
            return self.async_abort(reason="login_expired")
        if user_input is None:
            return self._show_accounts_form({})
        return await self._async_submit(
            self._show_accounts_form, partial(self._async_select_accounts, user_input)
        )

    async def _async_select_accounts(
        self, user_input: dict[str, Any]
    ) -> ConfigFlowResult:
        """Start importing the chosen accounts, if they are a valid choice."""
        selected = frozenset(str(item) for item in user_input[CONF_SELECTED_ACCOUNTS])
        available = {account.number for account in self._accounts}
        if not selected or not selected.issubset(available):
            return self._show_accounts_form({"base": "select_account"})
        return await self._async_start_attempt(
            self._show_accounts_form, selected=selected
        )

    def _show_accounts_form(self, errors: dict[str, str]) -> ConfigFlowResult:
        """
        Show the account selection form.

        It starts with every account selected, or, when it comes back after a
        valid selection (cleanup_pending), with that selection: the frontend
        fills the form from its defaults, so a plain resubmission must not
        import accounts the user had left out.
        """
        options = [
            SelectOptionDict(value=account.number, label=_account_label(account))
            for account in self._accounts
        ]
        selected = [
            account.number
            for account in self._accounts
            if not self._selected_accounts or account.number in self._selected_accounts
        ]
        return self.async_show_form(
            step_id="accounts",
            data_schema=probatio.Schema(
                {
                    probatio.Required(
                        CONF_SELECTED_ACCOUNTS, default=selected
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

    async def _async_submit(
        self,
        form: Callable[[dict[str, str]], ConfigFlowResult],
        handle: Callable[[], Awaitable[ConfigFlowResult]],
    ) -> ConfigFlowResult:
        """
        Handle one submission at a time; one sent meanwhile waits for it.

        The frontend sends a submission for each press of Enter, even while
        an earlier one is still waiting, and the flow API accepts any number.
        One that arrives while another waits on Meridian or the recorder
        makes no request and changes nothing: it waits for the other, then
        gets the step the flow is at (the next form, or the progress of the
        import it started), or the same abort. The frontend shows whichever
        answer reaches the dialog first and drops the other, so both must
        carry the real next step: an earlier form shown again could not be
        submitted to a flow that had moved on. When the other created the
        entry, Home Assistant still sets the entry up before the flow ends,
        and the flow is at its earlier form until then: the waiting one
        answers only once the flow has ended, with an abort the dialog
        drops, as it comes second. A copy of the created entry would make
        Home Assistant create a second entry and replace the first. Home
        Assistant ends the flow once the entry is added, or aborts it, as
        when the dialog is closed; either ends that wait.

        A submission that arrives once the entry is created, while Home
        Assistant still sets it up, as Enter pressed again or a slow setup
        gives, changes nothing either and gets that abort once the flow has
        ended (``_answer_after_entry``): handled, it would repeat the
        rollback under the new entry's first update and create a second
        entry.

        Handled side by side, a second answer could come back after the
        first had started the import: as a form or an abort it made Home
        Assistant cancel the import, leaving the flow at a form that could
        not start another; as an accepted code it replaced the login data
        the import was filling in; as an account selection or retry it
        started a second import of the same statistics. A second email would
        also start a new login, which the code in the first email might not
        match.
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
            return ended.copy() if ended is not None else self._current_step(form)
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
        """
        Answer a submission that came after the entry was created.

        Once Home Assistant has set the entry up and ended the flow, with an
        abort the dialog drops, as it comes second: a copy of the created
        entry would make Home Assistant create a second entry and replace
        the first.
        """
        await self._ended.wait()
        return self.async_abort(reason="already_configured")

    def _current_step(
        self, form: Callable[[dict[str, str]], ConfigFlowResult]
    ) -> ConfigFlowResult:
        """
        Return the step the flow is at, for a submission that changes nothing.

        While an import runs that is its progress step: a form result would
        make Home Assistant cancel the import's task.
        """
        return self.cur_step.copy() if self.cur_step is not None else form({})

    async def _async_start_attempt(
        self,
        form: Callable[[dict[str, str]], ConfigFlowResult],
        *,
        selected: frozenset[str] | None = None,
    ) -> ConfigFlowResult:
        """
        Start an import attempt once the statistics it may replace are settled.

        With ``selected``, start the first attempt for those accounts, after
        recording which statistics existed before it; without, retry the
        failed attempt after repeating its rollback. Either waits on the
        recorder, and while the recorder is still behind, the submitted form
        comes back with cleanup_pending. The submission that calls this holds
        the flow until it returns (see ``_async_submit``), so attempts never
        overlap.
        """
        if selected is None:
            ready = await self._async_rollback_complete()
        else:
            self._selected_accounts = selected
            ready = await self._async_prepare_import_rollback()
        # A flow removed while it waited, on Meridian or the recorder (the
        # dialog was closed, or a new setup for the same email replaced it),
        # must not start importing statistics that no flow would then own or
        # roll back. Nor may an attempt start once Home Assistant has started
        # to stop: the stop event has fired, so nothing would cancel it, and
        # its rollback would not run once the recorder had stopped. The flow
        # does not outlive the restart, so it ends even while the recorder is
        # still behind.
        if self._removed or self.hass.is_stopping:
            if selected is None:
                self._log_retry_cancelled(removed=ready)
            return self.async_abort(reason="login_expired")
        if not ready:
            return form({"base": "cleanup_pending"})
        return await self._start_initial_import()

    async def _start_initial_import(self) -> ConfigFlowResult:
        """Start one attempt at the visible history import."""
        if self._tokens is None or self._pending_data is None:
            return self.async_abort(reason="login_expired")
        self._pending_data[CONF_SELECTED_ACCOUNTS] = sorted(self._selected_accounts)
        self._pending_data[CONF_AUTO_ADD_ACCOUNTS] = self._selected_accounts == {
            account.number for account in self._accounts
        }
        self._import_attempts += 1
        self._import_error = None
        self._failure_detail = ""
        self._last_request = None
        self._median_request = None
        self._partial = False
        self._imported_at = None
        self._memory = None
        self._imported_newest = {}
        self._import_returned = False
        self._retry_after = 0.0
        self._rollback_removed = False
        self._attempt_started_at = monotonic()
        task = self.hass.async_create_task(
            self._async_initial_import_with_rollback(
                self._tokens, self._selected_accounts
            )
        )
        self._initial_import_task = task
        self._cancel_when_home_assistant_stops(task)
        return await self.async_step_initial_import()

    def _cancel_when_home_assistant_stops(self, task: asyncio.Task[None]) -> None:
        """
        Cancel the attempt as soon as Home Assistant starts to stop.

        Home Assistant cancels a task still running at shutdown only after the
        recorder has made its final write and stopped, so a removal the
        rollback queued then would never run, and the statistics the attempt
        created would stay without an entry. Cancelled at the stop event, the
        rollback queues the removal while the recorder still runs, and the
        final write includes it.
        """
        unsubscribe: CALLBACK_TYPE | None = None

        @callback
        def async_cancel(_event: Event) -> None:
            nonlocal unsubscribe
            unsubscribe = None  # a one-time listener removes itself
            self._stopping = True
            task.cancel()

        @callback
        def async_stop_listening(_task: asyncio.Task[None]) -> None:
            if unsubscribe is not None:
                unsubscribe()

        unsubscribe = self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP, async_cancel
        )
        task.add_done_callback(async_stop_listening)

    async def async_step_initial_import(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """
        Show progress while importing the initial 90 days of history.

        A progress step may only return more progress or progress done, so a
        failure moves on to the import_failed step, which shows it.
        """
        del user_input
        task = self._initial_import_task
        if task is None or self._pending_data is None:
            self._import_error = None
            return self.async_show_progress_done(next_step_id="import_failed")
        if not task.done():
            return self.async_show_progress(
                step_id="initial_import",
                progress_action="initial_import",
                progress_task=task,
            )
        self._initial_import_task = None
        self._failed_at = monotonic()
        if task.cancelled():
            # Cancelled by something other than closing the dialog, which
            # would have removed the flow. The task has run the rollback and
            # logged the cancellation, with any failure whose rollback it
            # interrupted, so only the form is left to fill in.
            self._import_error = "unknown"
            self._failure_detail = self._failure_text(None)
            return self.async_show_progress_done(next_step_id="import_failed")
        try:
            task.result()
        except ConfigEntryAuthFailed as err:
            # The session was rejected: a retry would fail the same way, so the
            # failure step ends the flow and the user signs in again.
            self._import_error = None
            self._record_import_failure(err, reason="login_expired")
        except UpdateFailed as err:
            self._import_error = _IMPORT_ERRORS.get(
                err.translation_key or "", "unknown"
            )
            if err.translation_key == "rate_limited":
                self._retry_after = float(err.retry_after or 0)
            self._record_import_failure(err, reason=self._import_error)
        except SetupTimeLimitError as err:
            self._import_error = "import_timeout"
            self._record_import_failure(err, reason=self._import_error)
        except Exception as err:  # noqa: BLE001 - every failure must reach the form
            self._import_error = "unknown"
            self._record_import_failure(err, reason=self._import_error)
        else:
            return self.async_show_progress_done(next_step_id="finish")
        return self.async_show_progress_done(next_step_id="import_failed")

    async def async_step_import_failed(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Explain a failed history import and offer a retry with the same login."""
        # Only a submission of this step's own form is a retry. When a progress
        # step finishes, the flow manager hands the next step the input of the
        # step that started the import (the login code), which is not one.
        if user_input is not None and self._showing_own_form("import_failed"):
            return await self._async_submit(
                self._show_import_failed_form,
                partial(self._async_retry_import, user_input),
            )
        if (
            self._import_error is None
            or self._tokens is None
            or self._pending_data is None
        ):
            return self.async_abort(reason="login_expired")
        if self._import_attempts >= SETUP_IMPORT_MAX_ATTEMPTS:
            if self._offers_without_history():
                # No retry is left, but going on without history needs none.
                return self._show_last_failure_form(
                    {
                        "base": _LAST_FAILURE_ERRORS.get(
                            self._import_error, "last_upstream_unavailable"
                        )
                    }
                )
            # The last attempt allowed has failed: say what failed and end
            # setup, rather than offer a retry that could only abort.
            return self._too_many_attempts()
        return self._show_import_failed_form({"base": self._import_error})

    async def async_step_import_failed_last(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """
        Offer to go on without history once the last attempt allowed has failed.

        Only after Meridian's service failed or did not answer in time
        (``_offers_without_history``): no retry is left, but going on needs
        none. Submitted without it, setup ends as after any last failure.
        Reached otherwise, it shows what the failure step would.
        """
        if user_input is not None and self._showing_own_form("import_failed_last"):
            return await self._async_submit(
                self._show_last_failure_form,
                partial(self._async_last_choice, user_input),
            )
        return await self.async_step_import_failed()

    async def _async_last_choice(self, user_input: dict[str, Any]) -> ConfigFlowResult:
        """Go on without history once time allows, or end setup."""
        self._without_history_chosen = bool(user_input.get(_WITHOUT_HISTORY, False))
        if not self._without_history_chosen:
            return self._too_many_attempts()
        if seconds := self._seconds_until_retry():
            return self._show_last_failure_form(
                {"base": "continue_too_soon"}, seconds=seconds
            )
        return await self._async_continue_without_history(self._show_last_failure_form)

    def _too_many_attempts(self) -> ConfigFlowResult:
        """End setup after the last attempt allowed failed, saying what failed."""
        return self.async_abort(
            reason="too_many_attempts",
            description_placeholders={"detail": self._failure_detail},
        )

    async def _async_retry_import(self, user_input: dict[str, Any]) -> ConfigFlowResult:
        """
        Start the next attempt, or go on without history, once time allows.

        Either waits the same time after the failure: going on without
        history creates the entry, whose first update asks Meridian for the
        history at once (``_async_continue_without_history``). The form
        then says to wait before doing what was chosen.
        """
        self._without_history_chosen = self._offers_without_history() and bool(
            user_input.get(_WITHOUT_HISTORY, False)
        )
        if seconds := self._seconds_until_retry():
            return self._show_import_failed_form(
                {
                    "base": (
                        "continue_too_soon"
                        if self._without_history_chosen
                        else "retry_too_soon"
                    )
                },
                seconds=seconds,
            )
        if self._without_history_chosen:
            return await self._async_continue_without_history(
                self._show_import_failed_form
            )
        return await self._async_start_attempt(self._show_import_failed_form)

    def _offers_without_history(self) -> bool:
        """
        Return whether the failure form offers to go on without history.

        Only after Meridian's service failed or did not answer in time: the
        entry's updates then ask for the history in the background, a few
        weeks at a time, and may get past what stopped setup. After any
        other failure, as data Home Assistant could not use or a rate limit,
        they would most likely fail the same way, with no form to say why.
        """
        return self._import_error in _WITHOUT_HISTORY_FAILURES

    async def _async_continue_without_history(
        self, form: Callable[[dict[str, str]], ConfigFlowResult]
    ) -> ConfigFlowResult:
        """
        Create the entry without history, once the failed attempt is removed.

        The rollback is repeated and checked as before a retry
        (``_async_rollback_complete``): the entry's first update must not
        read rows the failed attempt left, nor lose rows of its own to a
        removal still queued. That update finds no statistics, and imports
        the history as a new one does, keeping what it fetched; the updates
        after it ask for the rest. Statistics that existed before the flow
        keep what the failed attempts added: the oldest hour of a deep
        replay of them that was not confirmed goes to the entry (``memory``),
        whose walks reach back to it, as they would after a retry.
        """
        ready = await self._async_rollback_complete()
        if self._removed or self.hass.is_stopping:
            self._log_retry_cancelled(removed=ready)
            return self.async_abort(reason="login_expired")
        if not ready:
            return form({"base": "cleanup_pending"})
        self._without_history = self._partial = True
        self._imported_at = None
        self._memory = (
            SyncMemory(
                unit_states={},
                measurements={},
                row_density={},
                unconfirmed=dict(self._unconfirmed),
            )
            if self._unconfirmed
            else None
        )
        return await self.async_step_finish()

    def _show_import_failed_form(
        self, errors: dict[str, str], *, seconds: int = 0
    ) -> ConfigFlowResult:
        """
        Show the failure form, which offers a retry with the same login.

        After Meridian's service failed or did not answer in time, it also
        offers to finish setup without history, which the entry's updates
        then import in the background (``_offers_without_history``).
        """
        schema: dict[Any, Any] = {}
        if self._offers_without_history():
            schema[
                probatio.Optional(
                    _WITHOUT_HISTORY, default=self._without_history_chosen
                )
            ] = selector.BooleanSelector()
        return self.async_show_form(
            step_id="import_failed",
            data_schema=probatio.Schema(schema),
            errors=errors,
            description_placeholders={
                "detail": self._failure_detail,
                "seconds": str(seconds),
            },
        )

    async def async_step_finish(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """
        Create the config entry after the initial import completes.

        Or without one, when the user chose to go on without history. When
        some of the history is left for the entry's updates to import, the
        dialog says so (``partial``). Asked for again while Home Assistant
        sets the entry up, it creates no second one (``_answer_after_entry``).
        """
        del user_input
        if self._created:
            return await self._answer_after_entry()
        if self._email is None or self._pending_data is None:
            return self.async_abort(reason="login_expired")
        if self._imported_at is not None or self._without_history:
            # The entry's setup reads this once, so it does not ask Meridian
            # for the accounts again, nor, after an import, repeat a 14-day
            # reconciliation of the history just imported.
            handoffs = self.hass.data.setdefault(DOMAIN, {}).setdefault(
                SETUP_HANDOFF, {}
            )
            handoffs[self._email] = SetupHandoff(
                accounts=self._accounts,
                imported_at=self._imported_at or datetime.now(UTC),
                memory=self._memory,
                imported=self._imported_at is not None,
            )
        self._created = True
        return self.async_create_entry(
            title=NAME,
            data=self._pending_data,
            description="partial" if self._partial else None,
        )

    def _show_last_failure_form(
        self, errors: dict[str, str], *, seconds: int = 0
    ) -> ConfigFlowResult:
        """Show the form after the last attempt allowed: go on, or end setup."""
        return self.async_show_form(
            step_id="import_failed_last",
            data_schema=probatio.Schema(
                {
                    probatio.Optional(
                        _WITHOUT_HISTORY, default=self._without_history_chosen
                    ): selector.BooleanSelector()
                }
            ),
            errors=errors,
            description_placeholders={
                "detail": self._failure_detail,
                "seconds": str(seconds),
            },
        )

    def _showing_own_form(self, step_id: str) -> bool:
        """Return whether the step being submitted is this step's own form."""
        return (
            self.cur_step is not None
            and self.cur_step.get("type") is FlowResultType.FORM
            and self.cur_step.get("step_id") == step_id
        )

    def _seconds_until_retry(self) -> int:
        """Return how long to wait before the next attempt; 0 when it may start."""
        delay = max(float(SETUP_IMPORT_RETRY_DELAY_SECONDS), self._retry_after)
        return max(0, math.ceil(delay - (monotonic() - self._failed_at)))

    def _flow_created_statistic_ids(self) -> set[str]:
        """Return the statistics this flow may remove: only those it created."""
        return self._initial_statistic_ids - self._existing_statistic_ids

    async def _async_rollback_complete(self) -> bool:
        """
        Repeat the rollback, then confirm no statistic this flow created has rows.

        The removal is idempotent. A retry must not start while rows from the
        failed attempt remain, or while their removal is still queued behind
        other recorder work after its 30 s wait: the new attempt could read
        them back as existing history, import only a short catch-up, and then
        lose them to the queued removal. While the recorder is that far behind,
        the rows may not even be committed, so the removal's own result is
        the check that counts. With nothing of its own to remove, because
        every statistic existed before the flow, the retry still waits for
        the recorder as the first attempt did: rows the failed attempt added
        to those statistics may still be queued, and a retry that read the
        history without them could start its window after theirs and leave
        a gap in the sums.

        Either way the wait has two passes, as the entry's first sync's
        does: on MySQL or MariaDB, the failed attempt's import can meet a
        lock error once the recorder catches up and queue itself again,
        behind the removal or barrier, which then finishes first. The second
        barrier, queued once the first has run, is behind that second try,
        so rows it writes back to a statistic the flow created are found
        below, and rows it adds to a kept one are read as its history.

        Once nothing is left of a unit whose statistics the flow created
        both, nothing of it is half saved either: its unconfirmed hour, if
        any, is forgotten, and the retry walks its own 90 days for it.
        """
        flow_created = self._flow_created_statistic_ids()
        if flow_created:
            settled = await async_clear_statistics(
                self.hass, flow_created
            ) and await async_wait_for_recorder(self.hass)
        else:
            settled = await async_wait_for_recorder(
                self.hass, attempts=_RECORDER_PASSES, passes=_RECORDER_PASSES
            )
        if not settled:
            return False
        for statistic_id in sorted(flow_created):
            if await async_has_statistics(self.hass, statistic_id):
                return False
        for cache_key in list(self._unconfirmed):
            key, direction = cache_key
            ids = (
                consumption_ids(key)
                if direction == READING_CONSUMPTION
                else generation_ids(key)
            )
            if flow_created.issuperset(ids):
                del self._unconfirmed[cache_key]
        return True

    def _failure_text(self, err: BaseException | None) -> str:
        """Describe a failed attempt for the form, without identifiers."""
        call = self._last_request
        cause = _describe_failure(err)
        if call is None:
            return cause
        # A call that had returned did not fail: the failure came after it,
        # for example while saving the statistics it fetched.
        when = "while" if call.finished_at is None else "after"
        return f"{cause} {when} requesting {_describe_call(call)}"

    def _record_import_failure(self, err: BaseException, *, reason: str) -> None:
        """Keep a safe description of a failed attempt and log it with its traceback."""
        status = http_status(err)
        call = self._last_request
        self._failure_detail = self._failure_text(err)
        _LOGGER.warning(
            "Initial Meridian history import failed (attempt %d of %d): reason=%s "
            "cause=%s root=%s status=%s %s total_elapsed=%.1fs median_request=%s "
            "rollback=%s",
            self._import_attempts,
            SETUP_IMPORT_MAX_ATTEMPTS,
            reason,
            _cause_name(err),
            _root_name(err),
            status if status is not None else "-",
            _log_call(call, self._attempt_ended_at),
            self._attempt_ended_at - self._attempt_started_at,
            format_seconds(self._median_request),
            "removed" if self._rollback_removed else "queued",
            exc_info=err,
        )

    async def _async_prepare_import_rollback(self) -> bool:
        """
        Record pre-existing statistics so failed setup removes only new data.

        First wait for the recorder to commit the work queued so far: an
        earlier setup of these accounts, cancelled or replaced, may have left
        a removal of its rows queued, and those rows must not count as
        existing history. Return False, recording nothing, while it is still
        queued. The wait has two passes, as a retry's does
        (``_async_rollback_complete``), so work the recorder queued again
        behind the first barrier is done too.
        """
        if not await async_wait_for_recorder(
            self.hass, attempts=_RECORDER_PASSES, passes=_RECORDER_PASSES
        ):
            return False
        selected = {
            account.number: account
            for account in self._accounts
            if account.number in self._selected_accounts
        }
        statistic_ids: set[str] = set()
        for account in selected.values():
            for property_data in account.properties:
                key = property_key(account.number, property_data.id)
                statistic_ids.update(consumption_ids(key))
                if any(meter.has_feed_in for meter in property_data.meter_points):
                    statistic_ids.update(generation_ids(key))
        self._initial_statistic_ids = statistic_ids
        self._existing_statistic_ids = {
            statistic_id
            for statistic_id in statistic_ids
            if await async_has_statistics(self.hass, statistic_id)
        }
        return True

    async def _async_initial_import(
        self,
        tokens: MeridianTokenSet,
        selected_accounts: frozenset[str],
        time_limit: asyncio.Timeout | None = None,
    ) -> MeridianSyncData:
        """
        Import history before entry creation so progress is visible to the user.

        Billing is left out: this coordinator is discarded, and the entry's
        first sync fetches it. The flow has waited for the recorder just
        before every attempt, in two passes (``_async_prepare_import_rollback``
        before the first, ``_async_rollback_complete`` before a retry), so
        the coordinator does not wait again. Its budget, 4 minutes, ends
        before the attempt's 5-minute time limit, so the last request it
        starts can end within the limit, and what came back is imported,
        newest weeks first: the entry's updates fetch the rest. It moves the
        progress ring on after each window of history. The time it spends
        waiting for the recorder to save and confirm each direction's rows
        counts towards neither its budget nor the limit (``time_limit``),
        which the coordinator moves on by that time.
        """
        client = self._authenticated_client(tokens)
        coordinator = MeridianDataCoordinator(
            self.hass,
            client,
            selected_accounts=selected_accounts,
            # The accounts the flow just found: the import does not ask
            # Meridian for them again.
            topology=self._accounts or None,
            # Shared across attempts: the rollback keeps what a failed one
            # added to statistics that existed before, perhaps usage without
            # its cost, and this coordinator reads the newest stored hours,
            # which that usage has moved on.
            unconfirmed=self._unconfirmed,
        )
        try:
            data = await coordinator.async_fetch_and_import(
                setup_import=True,
                time_budget=SETUP_IMPORT_BUDGET_SECONDS,
                time_limit=time_limit,
                progress=self.async_update_progress,
            )
        finally:
            # Keep copies of the call in flight and the request times, never
            # the coordinator: its memory of what it imported is wrong once
            # the rollback has run. The unconfirmed hours it shares stay only
            # for the statistics the rollback leaves
            # (``_async_rollback_complete``).
            self._last_request = coordinator.last_request
            self._median_request = coordinator.median_request_seconds
        self._imported_newest = coordinator.imported_newest
        self._imported_at = data.synced_at
        # Setup succeeds once every direction of a property imported its
        # newest week: what is left, the entry's updates import.
        self._partial = bool(
            data.units_failed or data.units_degraded or data.units_deferred
        )
        # A copy, taken once the import has succeeded: no rollback runs for
        # this attempt, so it matches what is stored.
        self._memory = coordinator.memory
        return data

    async def _async_capped_import(
        self, tokens: MeridianTokenSet, selected_accounts: frozenset[str]
    ) -> None:
        """
        Import within the setup time limit, then wait until it can be read.

        The wait for the recorder is part of the attempt, so closing the
        dialog during it still rolls the import back. It never fails setup,
        and gives up after 30 s at a time, twice at most: the recorder saves
        the rows later all the same, and the entry's first sync waits for the
        recorder again, in two passes, before it reads any statistics. So it
        imports over rows still to be saved only when the recorder had to
        retry them twice in a row. The wait counts towards the time limit,
        which bounds the whole attempt but for the time the import's own
        replays wait for the recorder, which moves it on
        (``_async_initial_import``); but the import has finished by then, so
        the limit running out during the wait only ends the wait, as the
        wait's own timeout would. The rollback would otherwise remove a
        complete history because the recorder was busy, with a removal
        queued behind the same busy recorder.
        """
        time_limit = asyncio.timeout(SETUP_IMPORT_TIMEOUT_SECONDS)
        data: MeridianSyncData | None = None
        try:
            async with time_limit:
                data = await self._async_initial_import(
                    tokens, selected_accounts, time_limit
                )
                self._import_returned = True
                saved = await async_confirm_visible(self.hass, self._imported_newest)
        except RunBudgetError as err:
            # Too little of the import's own budget was left to start another
            # request, and no property had its newest week: it fails as the
            # time limit, the backstop, does.
            raise SetupTimeLimitError from err
        except TimeoutError as err:
            if not time_limit.expired():
                raise
            if data is None:
                raise SetupTimeLimitError from err
            _LOGGER.info(
                "The setup time limit ran out while Home Assistant was saving the "
                "imported Meridian history; setup continues, and the first update "
                "waits until it is saved"
            )
            saved = False
        _LOGGER.info(
            "Initial Meridian history import finished (attempt %d of %d): "
            "duration=%.1fs requests=%d directions=%d median_request=%s "
            "median_measurements=%s windows_ok=%d windows_empty=%d "
            "windows_failed=%d retries=%d splits=%d history_start=%s "
            "units_failed=%d partial=%s saved=%s",
            self._import_attempts,
            SETUP_IMPORT_MAX_ATTEMPTS,
            monotonic() - self._attempt_started_at,
            data.request_count,
            data.direction_count,
            format_seconds(data.median_request_seconds),
            format_seconds(data.median_measurements_seconds),
            data.windows_ok,
            data.windows_empty,
            data.windows_failed,
            data.retries,
            data.splits,
            "yes" if data.history_start else "no",
            data.units_failed,
            "yes" if self._partial else "no",
            "yes" if saved else "no",
        )

    async def _async_initial_import_with_rollback(
        self, tokens: MeridianTokenSet, selected_accounts: frozenset[str]
    ) -> None:
        """Run setup import and remove only newly created statistics on failure."""
        try:
            await self._async_capped_import(tokens, selected_accounts)
        except BaseException as err:
            self._attempt_ended_at = monotonic()
            # Closing the setup dialog removes the flow, which cancels this
            # task; Home Assistant stopping cancels it too, at its stop event.
            # The flow may be gone by the time the task ends, so a
            # cancellation is logged here, once the rollback has finished or
            # was itself interrupted. A failure's warning, logged by the flow,
            # says whether the removal finished.
            failure = None if isinstance(err, asyncio.CancelledError) else err
            try:
                removed = await async_clear_statistics(
                    self.hass, self._flow_created_statistic_ids()
                )
            except asyncio.CancelledError:
                self._log_cancelled(failure, removed=False)
                raise
            self._rollback_removed = removed
            if failure is None:
                self._log_cancelled(None, removed=removed)
            raise

    def _log_cancelled(self, failure: BaseException | None, *, removed: bool) -> None:
        """
        Log one WARNING for a cancelled attempt, once its rollback has ended.

        ``failure`` is the error whose rollback the cancellation interrupted:
        the flow never gets to log it, so its traceback goes here. Without
        one, the warning says whether the import had returned, so a busy
        recorder is not taken for a slow Meridian. The times end when the
        attempt ended, leaving out the rollback's wait.
        """
        if failure is not None:
            phase = "while removing the statistics from a failed attempt"
        elif self._import_returned:
            phase = "while Home Assistant was saving the imported history"
        else:
            phase = "before the history import finished"
        _LOGGER.warning(
            "Meridian setup was cancelled %s, %s; %s (attempt %d of %d, "
            "failure=%s %s total_elapsed=%.1fs median_request=%s)",
            phase,
            (
                "because the setup dialog was closed"
                if self._removed
                else "because Home Assistant is stopping"
                if self._stopping
                else "while the setup dialog was still open"
            ),
            _removal_text(removed=removed),
            self._import_attempts,
            SETUP_IMPORT_MAX_ATTEMPTS,
            type(failure).__name__ if failure is not None else "-",
            _log_call(self._last_request, self._attempt_ended_at),
            self._attempt_ended_at - self._attempt_started_at,
            format_seconds(self._median_request),
            exc_info=failure,
        )

    def _log_retry_cancelled(self, *, removed: bool) -> None:
        """
        Log one WARNING when setup ends while a retry repeats the rollback.

        No attempt runs then, so ``_log_cancelled`` never does. The failed
        attempt's own warning has its failure, request and traceback.
        """
        _LOGGER.warning(
            "Meridian setup was cancelled while removing the statistics from a "
            "failed attempt before a retry, %s; %s (attempt %d of %d)",
            (
                # A new setup for the same email replaces a flow at this form
                # as well, so the flow cannot tell which removed it.
                "because the setup dialog was closed or a new setup replaced it"
                if self._removed
                else "because Home Assistant is stopping"
            ),
            _removal_text(removed=removed),
            self._import_attempts,
            SETUP_IMPORT_MAX_ATTEMPTS,
        )

    def _authenticated_client(self, tokens: MeridianTokenSet) -> MeridianApiClient:
        """Create a client using the session validated by this flow."""
        return MeridianApiClient(
            async_get_clientsession(self.hass),
            tokens=tokens,
            token_update_callback=self._async_store_discovery_tokens,
        )

    async def _async_store_discovery_tokens(self, tokens: MeridianTokenSet) -> None:
        """Retain credentials rotated during discovery or the initial import."""
        self._tokens = tokens
        if self._pending_data is not None:
            self._pending_data.update(
                {
                    CONF_REFRESH_TOKEN: tokens.refresh_token,
                    CONF_FIREBASE_USER_ID: tokens.user_id,
                }
            )

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
        return await self._async_submit(
            self._show_reauth_confirm_form, self._async_send_reauth_code
        )

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
            error = "email_not_found"
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

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Change the selected accounts without renewing credentials."""
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if not self._accounts:
            discovery_result = await self._async_discover_reconfigure_accounts(entry)
            if discovery_result is not None:
                return discovery_result
        if not self._accounts:
            return self.async_abort(reason="no_accounts")

        available = {account.number for account in self._accounts}
        if user_input is not None and available:
            selected = frozenset(
                str(item) for item in user_input[CONF_SELECTED_ACCOUNTS]
            )
            if not selected or not selected.issubset(available):
                errors["base"] = "select_account"
            else:
                data_updates: dict[str, Any] = {
                    CONF_SELECTED_ACCOUNTS: sorted(selected),
                    CONF_AUTO_ADD_ACCOUNTS: selected == available,
                }
                if self._reconfigure_tokens is not None:
                    data_updates.update(
                        {
                            CONF_REFRESH_TOKEN: self._reconfigure_tokens.refresh_token,
                            CONF_FIREBASE_USER_ID: self._reconfigure_tokens.user_id,
                        }
                    )
                return self.async_update_reload_and_abort(
                    entry,
                    data_updates=data_updates,
                )

        current = entry.data.get(CONF_SELECTED_ACCOUNTS, sorted(available))
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=probatio.Schema(
                {
                    probatio.Required(
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

    async def async_step_reconfigure_discovery(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Retry account discovery while reconfiguring."""
        del user_input
        return await self.async_step_reconfigure()

    async def _async_discover_reconfigure_accounts(
        self, entry: ConfigEntry
    ) -> ConfigFlowResult | None:
        """Discover accounts and preserve any credentials rotated during refresh."""
        tokens = self._reconfigure_tokens or MeridianTokenSet(
            id_token="",
            refresh_token=str(entry.data[CONF_REFRESH_TOKEN]),
            expires_at=datetime.fromtimestamp(0, UTC),
            user_id=str(entry.data[CONF_FIREBASE_USER_ID]),
        )

        async def async_capture_tokens(updated: MeridianTokenSet) -> None:
            self._reconfigure_tokens = updated
            self.hass.config_entries.async_update_entry(
                entry,
                data={
                    **entry.data,
                    CONF_REFRESH_TOKEN: updated.refresh_token,
                    CONF_FIREBASE_USER_ID: updated.user_id,
                },
            )

        runtime_data = getattr(entry, "runtime_data", None)
        client = getattr(runtime_data, "client", None)
        if client is None:
            client = MeridianApiClient(
                async_get_clientsession(self.hass),
                tokens=tokens,
                token_update_callback=async_capture_tokens,
            )
        try:
            self._accounts = await client.async_get_accounts()
        except MeridianRateLimitError as err:
            error = "rate_limited"
            _log_discovery_failure(err)
        except MeridianConnectionError as err:
            error = _connection_error_key(err)
            _log_discovery_failure(err)
        except MeridianAuthenticationError:
            # The stored session is no longer accepted: continue this flow as a
            # reauthentication of the same entry (context keeps its entry_id).
            self.context["source"] = SOURCE_REAUTH
            self._email = str(entry.data[CONF_EMAIL]).strip().lower()
            return await self.async_step_reauth_confirm()
        except (MeridianGraphQLError, ValueError) as err:
            error = "invalid_data"
            _log_discovery_failure(err)
        else:
            if isinstance(client.tokens, MeridianTokenSet):
                self._reconfigure_tokens = client.tokens
            return None
        return self.async_show_form(
            step_id="reconfigure_discovery",
            data_schema=probatio.Schema({}),
            errors={"base": error},
        )


def _otp_error_key(code: str) -> str:
    return {
        "OTP_EXPIRED": "otp_expired",
        "OTP_INVALID": "otp_invalid",
        "OTP_TOO_MANY_ATTEMPTS": "otp_too_many_attempts",
        "OTP_NOT_FOUND": "otp_not_found",
    }.get(code, "invalid_auth")


def _connection_error_key(err: MeridianConnectionError) -> str:
    """
    Return the form error for a request to Meridian that got no usable answer.

    Meridian's service, or a gateway in front of it, failing or not answering
    in time has its own message; anything else, such as a connection that
    could not be made or a failure of Google's session service, is the
    general message about reaching Meridian.
    """
    if isinstance(err, MeridianUpstreamError):
        return "upstream_timeout" if err.gateway else "upstream_unavailable"
    return "cannot_connect"


def _account_label(account: MeridianAccount) -> str:
    """Return a recognisable local-only label without exposing meter IDs."""
    if len(account.properties) == 1:
        label = " ".join(account.properties[0].address.split())
    elif account.properties:
        label = f"{len(account.properties)} properties"
    else:
        label = "Meridian account"
    return f"{label} · account ending {account.number[-4:]}"


def _removal_text(*, removed: bool) -> str:
    """Say, for a cancellation warning, whether the attempt's removal finished."""
    return (
        "the statistics it created were removed"
        if removed
        else "removal of the statistics it created is queued"
    )


def _log_discovery_failure(err: Exception) -> None:
    """
    Log why account discovery failed, without identifiers.

    Data Home Assistant cannot use is a defect worth reporting, so it gets a
    warning with its traceback; before 0.3.0 it left no trace at any level. A
    connection failure or rate limit is already on the form, and the
    transport's debug lines describe the answer, so it gets a debug line.
    """
    if isinstance(err, MeridianGraphQLError | ValueError):
        _LOGGER.warning(
            "Meridian account discovery returned data Home Assistant could not use "
            "(%s)",
            type(err).__name__,
            exc_info=err,
        )
        return
    status = http_status(err)
    _LOGGER.debug(
        "Meridian account discovery failed: error=%s status=%s",
        type(err).__name__,
        "-" if status is None else status,
    )


def _failure_chain(err: BaseException | None) -> list[BaseException]:
    """
    Return the chain of a failure, outermost first, as a description reads it.

    A ``TimeoutError`` that ``asyncio.timeout`` raised is chained from the
    cancellation it stopped the task with. That cancellation is how the limit
    worked, not what failed, and its class reads like a closed dialog, so it
    is left out.
    """
    chain = exception_chain(err)
    if len(chain) > 1 and isinstance(chain[-1], asyncio.CancelledError):
        chain.pop()
    return chain


def _root_cause(err: BaseException | None) -> BaseException | None:
    """Return the innermost exception of a failure's chain."""
    chain = _failure_chain(err)
    return chain[-1] if chain else None


def _cause_name(err: BaseException) -> str:
    """Return the class of the error a failure was raised from, or ``-``."""
    cause = err.__cause__
    if cause is None or cause not in _failure_chain(err):
        return "-"
    return type(cause).__name__


def _root_name(err: BaseException | None) -> str:
    """
    Return the class of the innermost error in a chain, for a log line.

    A network error is not chained, so its class is read from the transport
    error that stands for it. The setup time limit names itself: what lies
    behind it is only how ``asyncio.timeout`` stopped the attempt, wherever
    it was waiting, and a request timeout has a ``TimeoutError`` root too.
    """
    if isinstance(err, SetupTimeLimitError):
        return type(err).__name__
    root = _root_cause(err)
    return getattr(root, "error_class", None) or type(root).__name__


def _first[ErrorT: BaseException](
    err: BaseException | None, kind: type[ErrorT]
) -> ErrorT | None:
    """Return the first exception of a kind in a chain, outermost first."""
    return next((item for item in exception_chain(err) if isinstance(item, kind)), None)


def _describe_statistics_failure(err: BaseException | None) -> str | None:
    """Describe a replay that refused to write, or was not confirmed, if it was."""
    if _first(err, StatisticsAnchorError) is not None:
        return "older statistics have no running total the import could continue"
    if _first(err, StatisticsWriteError) is not None:
        return "Home Assistant did not confirm that it had saved the statistics"
    return None


def _describe_failure(  # noqa: PLR0911 - one wording per kind of failure
    err: BaseException | None,
) -> str:
    """Describe what failed for the form, in words rather than class names."""
    status = http_status(err)
    upstream = _first(err, MeridianUpstreamError)
    unreadable = _first(err, MeridianUnreadableResponseError)
    transport = _first(err, MeridianTransportError)
    if isinstance(err, SetupTimeLimitError):
        if isinstance(err.__cause__, RunBudgetError):
            return (
                f"too little of the import's {SETUP_IMPORT_BUDGET_SECONDS // 60} "
                "minutes was left for another request"
            )
        return "the setup time limit ran out"
    if (statistics := _describe_statistics_failure(err)) is not None:
        return statistics
    if upstream is not None:
        # "HTTP 503", or "no answer within the time limit" for a timeout.
        return upstream.detail
    if unreadable is not None:
        return f"an unreadable answer (HTTP {unreadable.status}, {unreadable.content})"
    if status is not None:
        return f"HTTP {status}"
    if isinstance(transport, MeridianTimeoutError):
        # A service other than Meridian's, such as the session's, timed out.
        return "no answer within the time limit"
    if transport is not None and transport.error_class:
        return f"a network error ({transport.error_class})"
    if err is not None:
        return type(_root_cause(err)).__name__
    return "the import was cancelled"


def _describe_call(call: RequestContext) -> str:
    """Describe a Meridian call for the failure form, without identifiers."""
    label = _CALL_LABELS.get(call.direction or call.operation, call.operation)
    if call.window is not None:
        label = f"{label}, window {call.window} of {call.window_count}"
    if call.page is not None:
        label = f"{label}, page {call.page}"
    places = [
        f"{name} {index} of {count}"
        for name, index, count in (
            ("account", call.account_index, call.account_count),
            ("property", call.property_index, call.property_count),
        )
        if index is not None
    ]
    return f"{label} ({', '.join(places)})" if places else label


def _log_call(call: RequestContext | None, now: float) -> str:
    """Describe a Meridian call as log fields, without identifiers."""
    if call is None:
        return "operation=-"

    def position(index: int | None, count: int | None) -> str:
        return "-" if index is None else f"{index}/{count}"

    # For a call that has returned, its own duration; else the time so far.
    end = now if call.finished_at is None else call.finished_at
    return (
        f"operation={call.operation} direction={call.direction or '-'} "
        f"account={position(call.account_index, call.account_count)} "
        f"property={position(call.property_index, call.property_count)} "
        f"window={position(call.window, call.window_count)} "
        f"page={call.page if call.page is not None else '-'} "
        f"bound={call.bound or '-'} calls={call.calls} "
        f"in_flight={'yes' if call.finished_at is None else 'no'} "
        f"request_elapsed={max(0.0, end - call.started_at):.1f}s"
    )

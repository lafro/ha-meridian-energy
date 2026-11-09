# Architecture

How the integration fits together, for people changing it. What users see and do is in the [README](../README.md).

## Components

| Module | Role |
|---|---|
| `api.py`, `transport.py`, `graphql.py`, `parsers.py` | The Meridian client: the emailed-code sign-in, session renewal, the GraphQL documents Meridian's web app sends, under its names at `?opName=<name>` and trimmed to the fields used, and strict parsing of the answers. Accepted exceptions, as since 0.2.x: `accountsList`, `account` and `billingPeriods` keep their own names without `?opName=`, asking only for fields the web app's queries ask for. |
| `config_flow.py` | Setup (email, code, then the entry) and reauthentication. |
| `__init__.py` | Entry setup, migration and removal. |
| `coordinator.py` | `MeridianDataCoordinator`: the hourly sync of every unit, each account's other figures, failures and the repair notice. |
| `windows.py` | New Zealand dates and hours, request windows, and the readings kept from an answer. |
| `statistics.py` | External statistic IDs and metadata, and the one write, `async_write_hours`. |
| `sensor.py` | One service device per account, and its sensors. |
| `diagnostics.py` | Counts, flags and times only. |

## Setup

1. The flow sends a code to the email and exchanges it for a renewable session. It never asks for a password.
2. It lists the accounts (`async_get_accounts`, which keeps the ACTIVE and DORMANT ones) and creates the entry at once. The entry holds only the email and the session (the refresh token and the Firebase user ID). It follows every account on the login: no account choice, no reconfiguration. A failed list shows `discovery_retry`, which asks again with the same session; an ended session aborts with `login_expired`; no account, or no property yet, with `no_accounts` or `no_properties`.
3. `async_setup_entry` renews the session and caches the accounts (`async_prepare_topology`), then sets up the sensors. It asks for no history.
4. The first sync runs as an entry background task once Home Assistant has started. The entities are unavailable until it succeeds. A failed first sync is tried again after 10, 30 and 60 minutes, then hourly (`FIRST_SYNC_RETRY_SCHEDULE`).

## The life of an update

The coordinator runs hourly. A **unit** is one direction of one property: grid import always, and grid export when the property has an active feed-in register. Units run one after another. For each unit:

1. **Recent dates.** The last 21 New Zealand dates when its daily request hasn't succeeded for a day, as in the first sync after a start or reload; otherwise the last 3. A request window holds at most 7 dates, both ends included. Every query is resent once after a timeout, a network error or 502–504.
2. **Fill.** One read of the stored hours of both statistics, from New Zealand midnight 90 days back up to the recent dates. A date is missing when an hour of it is absent from either statistic and Meridian's last answer for that date didn't leave that hour out. The missing dates go into windows, newest first, at most 13 per sync.
   - After each answered window, the hours Meridian didn't return are noted, so they aren't asked for again. The notes live in memory, so after a restart or reload those dates are asked for once more, at most 10 windows per direction: cost from before Meridian's current system, say.
   - Two answered windows in a row with no readings, older than every reading, mark where the history starts for this run.
   - A refusal (another 4xx, or a coded GraphQL error but KT-CT-1188 or 1189) of a window older than every reading stops the fill until a restart or reload, with one INFO line.
   - Any other failure makes the window's dates wait an hour, doubling up to a day, and ends the fill for this sync. One INFO line says so once a date has failed for a day.
   - A unit whose search back to the target found nothing is quiet, sending no requests, until the accounts are next refreshed.
3. **Write** its energy and cost (below).

The same steps do the first import and every catch-up. What the coordinator learns about a unit is kept in memory only (`_Memory`) and built again after a start.

**Outcome.** An update succeeds when a unit with readings got its recent dates, or when no unit failed. A unit without readings never counts as imported, and counts as failed only when its recent dates fail. A unit that fails while another imports logs one WARNING, naming the property only when there are several, then one INFO when it works again. When none imports, the update raises `UpdateFailed` with the failure's key, and Home Assistant logs one ERROR. A rate limit (429 or KT-CT-1199) stops the requests, and what was fetched is written; the next update waits Meridian's Retry-After (an hour when it gives none), doubled for each rate limit in a row, up to a day; when something was imported, the next update also comes no sooner than the usual hour. The first rate limit of a run logs one INFO line. An ended sign-in raises `ConfigEntryAuthFailed`, which starts reauthentication.

**Account figures.** After the units come each account's other figures, each kind at its own interval: the billing period (daily; once it ends, at the next update, then every 6 hours until a new one comes), the bill's day totals and the balance (every 6 hours), and the prices (daily, and only for an account with one property and one meter point). A failed kind is asked again at the next update if the failure may pass, and keeps its last value for two intervals (Bill estimate until its period ends), then shows unknown. One INFO line says which sensors show unknown, again if that changes, and one says when the kind is back; prices unanswered since a start say instead that they are asked again in a day. These requests never change whether an update succeeds, except that an ended sign-in raises `ConfigEntryAuthFailed`. The day totals need every property's grid import, so they aren't asked for while one failed its recent dates. When the grid export's day totals fail, or a grid export unit failed, only the export and credit so far show unknown. After a rate limit, none of these is asked for.

**Repair notice.** The coordinator counts failed updates in a row itself, because Home Assistant doesn't call listeners for a failure that follows a failure. After 3, or at once for `no_accounts` or `no_properties`, it raises `sync_failing_<entry_id>` (a warning, not fixable) with the failure's message as its reason; never for an ended sign-in, which Home Assistant's reauthentication covers. The next success and the entry's removal delete it.

**Failures** map to seven translation keys: `invalid_auth` (reauthentication), `cannot_connect` (no answer: a network error, a timeout, 408 or 5xx, or a GraphQL error without a code), `rate_limited`, `invalid_data` (another 4xx, a coded GraphQL error, or an answer that can't be parsed), `no_accounts`, `no_properties` and `entry_too_old`. Each message carries a detail without identifiers, such as `HTTP 504`. After a fresh token, a 401 or 403 with a JSON body starts reauthentication, and one without is a refusal.

## The statistics write

Each property has external statistics `meridian_energy:<kind>_<key>`, where `<kind>` is `consumption`, `consumption_cost`, `generation` or `generation_credit`, and `<key>` is the first 12 hex digits of a SHA-256 hash of the account number and property ID, so IDs carry no identifiers. Display names add the property's address, or its position, only when the login has more than one property.

An hour's cost adds up the cost types Meridian's app adds (`COST_STATISTIC_TYPES`), each with its sign, so a credit lowers it. The export credit is `GENERATION_VALUE`.

Each unit writes once per sync: one `statistics.async_write_hours` call for its energy and cost.

1. **Barrier.** Queue an empty `async_clear_statistics` and wait up to 30 s for it, so the reads see the writes queued before. If it times out, nothing is written; the update still succeeds, and a later one fills the gap.
2. **Read** each statistic's stored rows from 14 days before its earliest fetched hour.
3. **Span.** It starts at the earlier of the first fetched hour not stored with that value and the first stored row whose sum doesn't step from the row before by its own state (a seam). It ends at the later of the newest fetched hour and the newest stored hour.
4. **Sums** continue from the newest stored row with a sum before the span, searched back without limit; from zero only when there is none.
5. **Rows.** Each hour of the span takes its fetched state, else its stored one; an hour that is neither is left out.
6. **Queue** one `async_add_external_statistics` call per statistic, one recorder transaction each, with no await between energy and cost.

A write can be repeated safely, and a dropped one heals at the next, which finds the stored rows differ. An Adjust sum in the span, or anywhere in the rows read, where it is a seam, is overwritten: that reaches about five weeks back at the daily update, and 14 days before any filled window. Per-statistic locks (`hass.data[DOMAIN]["write_locks"]`), held from the barrier until the rows are queued, keep a refresh that outlives a reload from interleaving with the new coordinator's writes.

Recorder writes, deletes and barriers go only through the public Recorder API on the recorder's own thread (`get_instance(hass).async_clear_statistics`, `async_add_external_statistics`), never from an executor. Reads use `statistics_during_period` on the recorder's executor. The write is tested against a real recorder (`recorder_mock`).

## Entities

One service device per account, always named "Meridian Energy", so entity IDs never carry an address. The [README](../README.md#sensors) lists the sensors. Only Provisional data intervals has a state class: the external statistics are the long-term record. The export sensors exist only with a feed-in register, and the price sensors only for one property with one meter point on an all-day plan; sensors are added and removed as that changes, but registered price sensors stay, showing unknown, until an answer says whether the plan is all-day. Devices for accounts Meridian no longer lists are detached.

## Config entries

Entries are version 4, minor version 1. `async_migrate_entry` refuses an entry below version 4 with a translated `ConfigEntryError` (`entry_too_old`); such an entry is removed and added again. It accepts a later 4.x minor version unchanged, so going back to an earlier 4.x release still loads the entry. Home Assistant refuses a higher major version, so a change earlier 4.x releases can't read needs a major-version bump.

## Diagnostics

- **Per entry:** the entry's version, `last_update_success`, `last_failure` (its `key`, and its `cause` as a class name), whether the first sync is pending, counts, the last sync's duration and number of requests, and, for each kind of account figure, how many fail and the oldest one's age.
- **Per unit,** by its account's and property's positions (`1/2`) and its direction: `newest_stored_hour`, `oldest_stored_hour`, `target`, `history_complete`, `fill_stopped`, `fill_failure` (the last fill failure's key and detail, such as `cannot_connect (HTTP 504)`, cleared once no date waits), `quiet` and `last_failure`.
- **Per account:** the billing period's type and whether it is fixed, whether the account has feed-in and price sensors, and whether each figure is known, never its value.

## Privacy

- The entry holds only the email and the session; short-lived tokens stay in memory.
- Diagnostics hold versions, counts, flags and times: never emails, account or meter numbers, ICPs, addresses, tokens, usage or amounts. A snapshot test (`tests/snapshots/test_sensor_diagnostics.ambr`) holds the whole payload, so a new field shows in review.
- Logs never include payloads, headers, URLs or identifiers. At DEBUG, each request logs its operation, window (as days from today), status, answer's kind and size, and time taken. Above DEBUG, logs name a unit as grid import or grid export and a property by its position. `tests/test_privacy_logs.py` checks this end to end.
- The account list's property addresses are used only for statistic display names, shown locally in Home Assistant. The balance and price queries ask for no address and no bill link.

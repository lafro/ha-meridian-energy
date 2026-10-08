# Architecture

## Components

| Module | Role |
|---|---|
| `api.py`, `transport.py`, `graphql.py`, `parsers.py` | Meridian client: emailed-code login, Firebase session renewal, GraphQL queries and response parsing. No external Python requirements. |
| `config_flow.py` | Setup (email, six-digit code, account selection, visible initial import), reauthentication and reconfiguration. |
| `coordinator.py` | `MeridianDataCoordinator`: topology cache, sync-mode selection, measurement fetches, billing metadata and statistics import. |
| `statistics.py` | External statistics: IDs, import, running-sum baselines, billing-period totals and rollback clearing. |
| `sensor.py` | One service device per selected account with diagnostic, provisional-data and current-bill sensors. |
| `diagnostics.py` | Counts, flags and timings only. |

## Setup and start-up

1. The config flow sends a login code to the account email and exchanges it for a renewable session. No password is ever requested or stored; the entry keeps the refresh token and the Firebase user ID.
2. After account selection the flow imports up to 90 days of history while showing progress, then creates the entry. If that import fails, the statistics it created are cleared through the recorder's public API (`get_instance(hass).async_clear_statistics`); statistics that existed before the flow are left alone.
3. `async_setup_entry` only renews the session and caches the account topology (`async_prepare_topology`). A rejected session raises `ConfigEntryAuthFailed` (reauthentication); an unreachable Meridian, a rate limit or unusable data raises `ConfigEntryNotReady` (Home Assistant retries).
4. The first sync runs as an entry background task once Home Assistant has started. Entities exist from the cached topology but are unavailable until it finishes. A first sync that fails for connection or data reasons is retried after 10 minutes; a rate limit uses Meridian's `retry_after`.

## Sync modes

The coordinator polls hourly and picks a mode for each fetch:

| Mode | When | Window requested |
|---|---|---|
| `initial` | A property or direction has no statistics yet (or they are more than 14 days old at start-up) | 90 days |
| `restart` | First sync after Home Assistant starts, when statistics exist | 14 days |
| `full_reconciliation` | Every 7 days | 14 days, the provisional-data correction window |
| `targeted_reconciliation` | Daily | From 6 hours before the oldest provisional interval; at least 48 hours, at most 14 days |
| `tip` | Otherwise | The last 24 hours |

Topology and billing metadata are cached for a day.

## Statistics

Each property gets external statistics `meridian_energy:<kind>_<key>`, where `<kind>` is `consumption`, `consumption_cost`, `generation` or `generation_credit`, and `<key>` is the first 12 hex digits of a SHA-256 hash of the account number and property ID. IDs therefore carry no identifiers. Generation statistics exist only for feed-in meters.

Imports anchor their running sums to the last stored statistic before the window, so re-importing a window rewrites its rows without shifting the running total. Current-bill totals are summed from the stored hourly rows for the billing period and are reported only when those rows cover every hour from the start of the period; missing cost is never treated as zero. All recorder writes and deletes go through the public Recorder API on the recorder's own thread; tests cover this against a real recorder (`recorder_mock`).

## Entities

- **Last data update**, **Latest usage data**: timestamps (diagnostic).
- **Provisional data intervals**: count of non-actual intervals in the correction window (`state_class: measurement`).
- **Current bill usage / cost / export / export credit**: bill-to-date totals computed from the statistics for the current billing period, with the period dates and a completeness flag as attributes. They have no state class: Home Assistant keeps no long-term statistics for them and does not offer them in the Energy dashboard, because the external statistics are the long-term record.
- **Billing period start / end**, **Next billing date**: dates, disabled by default.

Feed-in sensors are created only for accounts with feed-in metering and removed when that stops. Devices for accounts that are no longer selected are detached.

## Config entries

Entries are version 3, minor version 1. `async_migrate_entry` promotes completed 3.0 entries (from 0.2.4) to 3.1, raises a translated `ConfigEntryError` for anything it cannot use, and accepts a higher 3.x minor version unchanged so that rolling back to an earlier release still loads the entry. A future change that earlier 3.x releases cannot read needs a major-version bump.

## Privacy

- Diagnostics contain versions, counts, flags, timestamps and timings; never emails, account or meter numbers, ICPs, addresses, tokens, usage or cost.
- Logs never include payloads, headers or identifiers.
- Account selection labels show an address and the last four digits of the account number locally in the Home Assistant UI only.
- A snapshot test (`tests/snapshots/test_sensor_diagnostics.ambr`) holds the full diagnostics payload, so any new field shows up in review.

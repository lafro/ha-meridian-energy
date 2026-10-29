# Meridian Energy for Home Assistant

An unofficial Home Assistant integration for Meridian electricity usage, cost and billing-period insights.

[![Open your Home Assistant instance and open this repository in HACS.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=lafro&repository=ha-meridian-energy&category=integration)

> [!IMPORTANT]
> This project is not affiliated with, endorsed by or supported by Meridian Energy. It uses Meridian customer-service endpoints that may change without notice.

## What it provides

- Passwordless setup using Meridian's emailed six-digit code.
- Automatic session renewal and selection of one or more electricity accounts.
- Hourly grid-import and cost statistics for Home Assistant's Energy dashboard.
- Current-billing-period usage and cost.
- Grid-export and export-credit data when Meridian reports feed-in metering.
- Hourly updates, provisional-reading reconciliation and privacy-safe diagnostics.

## Installation

1. Use the HACS button above, or add `lafro/ha-meridian-energy` as a custom **Integration** repository.
2. Download **Meridian Energy** in HACS and restart Home Assistant.
3. Go to **Settings → Devices & services → Add integration → Meridian Energy**.
4. Enter your MyMeridian email address and the six-digit code Meridian sends you.
5. Select the accounts to import, if prompted.
6. Keep the setup dialog open while it imports your last 90 days of usage. It asks Meridian for one week at a time, newest first, and shows how many weeks are done; it can take a few minutes. If Meridian is too slow for the import to finish in the time setup allows, setup keeps what came back once at least one property has its newest week of grid import (and of export, if it has any), says so, and the updates after setup import the rest in the background. If the import fails, the dialog says what went wrong and offers to try again (see [Limitations and troubleshooting](#limitations-and-troubleshooting)). Closing the dialog cancels setup and removes the statistics it created; statistics kept from an earlier installation stay.

Beta versions are GitHub pre-releases: HACS offers them only when beta versions are turned on for this repository.

Home Assistant renews the saved session automatically. Another code is needed only if Meridian invalidates it.

Home Assistant 2026.10.0 or newer is required.

Upgrading from v0.2.4 or earlier requires a completed v0.2.4 installation.
Older or incomplete config entries intentionally stop at a migration error that
says so; restore or complete them on v0.2.4 before upgrading, or remove the
integration and add it again.

## Entities

Each selected account is represented by one service device.

| Entity | Meaning |
|---|---|
| **Last data update** | Last successful Meridian sync. |
| **Latest usage data** | End of the newest completed usage interval received. |
| **Provisional data intervals** | Non-actual consumption intervals in the rolling 14-day correction window. |
| **Current bill usage** | Grid import recorded in Meridian's current billing period. |
| **Current bill cost** | Available tax-inclusive interval cost for that period, added up as Meridian's app does: usage, the daily charge, and any other charges or credits Meridian assigns to intervals. |
| **Current bill export** | Grid export in the current period; feed-in accounts only. |
| **Current bill export credit** | Retailer credit for that export; feed-in accounts only. |

**Billing period start**, **Billing period end** and **Next billing date** are disabled by default. Current-bill values stay unknown until Home Assistant has complete data from the start of the period; missing cost is never treated as zero.

The **Current bill** sensors are running totals for the current period, with the period dates as attributes. They have no state class, so Home Assistant keeps no long-term statistics for them and does not offer them in the Energy dashboard; the statistics below are the long-term record.

The period follows Meridian's billing cycle, as Meridian's apps do, so its dates can differ by a day or two from the period printed on your bill. The totals also include Meridian's estimates for past hours it has not read yet, which the app's "so far" total leaves out, so they can be higher than it until those hours are read.

## Energy dashboard statistics

For each property, the integration creates:

- **Meridian grid import** (kWh)
- **Meridian grid import cost** (NZD)
- **Meridian grid export** (kWh, when available)
- **Meridian grid export credit** (NZD, when available)

An address is added only when multiple properties need disambiguation. These are external long-term statistics, so Home Assistant does not associate them with a Device.

After the first import, add grid import and cost under **Settings → Dashboards → Energy → Grid consumption**. Add grid export and return credit when available. Do not add a second whole-home source for the same energy flow.

Grid export is not total solar production: Meridian cannot see electricity used directly in the home. Use an inverter or production-meter integration for Home Assistant's separate **Solar production** source.

## Data timing

- **First setup:** up to 90 days; what setup could not fetch in its time comes with the updates after it.
- **Older history:** each update, but the first after a start, a reload or setup, completes the history back to midnight 90 days ago, or to the start of the current billing period when that is older (up to 400 days), a few weeks at a time, newest first (below).
- **Hourly:** recent usage.
- **Daily:** delayed or provisional data, up to 14 days, and hours missing between stored hours of those days.
- **Weekly and after restart:** full 14-day reconciliation.
- **After an outage:** every update reaches back to the newest hour Home Assistant has stored, up to 90 days, so an outage longer than 14 days no longer leaves a gap. It reaches back to the newest hour with a stored cost too, up to 2 days further, so the hours whose cost Meridian had not published yet get it as well. While an update keeps finding readings newer than those stored, as when Meridian publishes them more than a week late, the next one searches back again. For a supply that ended, once that search has found nothing newer, the hourly and daily updates ask for the newest week alone, and look further back as soon as it has readings again; the weekly one searches again, with up to 13 requests.

- **Monthly:** older estimates corrected (below).

Meridian data is delayed rather than real-time. Revisions older than 14 days are outside the normal correction window, which covers them only once a month, back to 90 days or the billing period's start: older ones are never corrected.

After its own requests, each update asks for the history still missing back to midnight 90 days ago, or to midnight at the start of the current billing period when that is older, so the **Current bill** sensors get every hour from the period's start: at most 6 more windows of a week for each grid import and export, newest first, each request starting 2 to 5 seconds after the last one ended. The first update after a start of Home Assistant, a reload of the integration or setup asks for none, so the Meridian entities do not wait for them, and the update after it knows the billing period by then. The update after a 90-day import, as setup's, asks once for the hours of the day 90 days back before that time, which that import leaves out. A window can take more than one request: a week Meridian refuses as before your supply started is asked for by its days, and when a week's last day comes back without readings while the history goes on after it, that day is asked for again with the next one, so a week read without its last day fails rather than leave that day out; so is a week's first day, or that day's first hour, with the day before, while the history may go on before it. Each is imported behind the newer hours already stored, with running totals that stay consistent, and once nothing is missing the updates ask for none. Two weeks in a row with no readings before the oldest reading, or a week Meridian refuses as before your supply started, are taken as the start of your history; a shorter gap, as a meter that sent nothing for a week, is asked across. A request that fails fails nothing else: its days wait an hour and are then asked for one at a time, while the older weeks go on; a day that keeps failing waits 2 hours, then twice as long each time, up to a day. Days whose readings an import had to leave out, as when Meridian no longer returns an hour already stored next to them, wait the same way and are asked for again, and so do days still missing an hour for which Meridian returns no readings, although some are stored. When Meridian asks Home Assistant to wait, no more history is asked for until then (below). Home Assistant reads what it holds from its statistics at every start, with no other record, so a restart goes on from where the updates left off; days Meridian had nothing for are asked for once more after a restart. On one day of each month, spread so that not every grid import and export asks on the same day, or at the first update in the 3 days after it when none ran that day, the updates also ask again for every stored day from 90 days back, or the billing period's start, to 14 days back, 6 weeks at each update, and write again only the hours Meridian has changed since: about 11 more requests a month for each. Diagnostics show for each grid import and export whether its history is complete (`reaches_target`, yes too when there is nothing to complete, as for an export register that never exported, and no after a start, a reload or setup until the update after the first one has looked; a monthly correction still running, or a day whose correction keeps failing, does not change it) and whether Meridian showed where it starts (`history_start_found`, which the monthly correction does not change either).

Each request asks Meridian for one week of history at most, by New Zealand date, newest first. For each property, and each of grid import and export, the first setup sends up to 13, as does a restart after more than 14 days without updates; a restart or the weekly reconciliation sends 3, the daily reconciliation of estimates usually 1 (up to 3 once the oldest estimate, or the oldest hour missing between stored hours of the last 14 days, is more than about a week old; a gap that started before those 14 days counts from 14 days back), and any other hourly update 1. A meter with more than one register needs up to one more for each extra register for the newest week, at setup and after each restart. A request holds 744 readings at most, one for each register and hour, so a meter with more than four registers needs more than one for every week: two for five to eight registers, three for nine to thirteen. For five registers that is 28 requests at setup and 5 for the weekly reconciliation; for nine, 44 and 7. Up to 70 registers are supported. A grid import or export with no readings in the last 90 days, such as that of a property whose supply ended or an export register that never exported, sends 13 at setup. Setup tells the first update after it that it searched there, so that update sends 1, the hourly update; but each restart or reload sends 13 again: Home Assistant keeps no record that setup found nothing there, and cannot tell it from an account added later through **Reconfigure**, whose first update imports its 90 days the same way. A grid import or export whose readings stopped while Home Assistant has statistics for it, as for a property whose supply ended, sends 1 for its hourly and daily updates, but its weekly reconciliation searches back to its newest stored readings, up to 90 days: up to 13 requests every week while the property is still listed, with the 15-minute time limit. Requests start at least a second apart.

When a week comes back with no readings and no statistics are stored for it, the import goes on to the next; two such weeks in a row after a reading are taken as the start of your history, unless readings are stored from before them, as after more than 14 days without updates: the update then goes on back to them. Empty weeks before any reading, as for a supply that ended some weeks ago or a meter that sent nothing for a while, are not taken as the start either: the import goes on through the 90 days and imports the readings before them. Meridian's estimates for the rest of today do not count as readings. When Meridian refuses outright a week older than every reading, at setup or in a later update, it is asked for the refused week's days alone, newest first, up to 8 more requests. A refusal of the newest day too, or no readings for it, makes the refused week the start of your history. A day refused after the days after it came back, as when your supply started in the middle of the week, is the start when Meridian also refuses, or has no readings for, the day before it, and the days after it are imported. When every day comes back, the week is imported and the import goes on. Readings for the day before a refused day mean the refusal was about something else, so the update fails, to be tried again, and so does a refusal without an error code, or a start with statistics stored before it. When Meridian refuses the newest week and then today alone, with an error code, before any reading, as it may for a property whose supply has not started yet, such as a new address added before a move, and Home Assistant has no statistics for it, there is no history yet: nothing is imported, one INFO line gives the code, and the other properties sync as usual. Refused there with an HTTP client error instead, or when statistics are stored for it, however old, the update still fails.

A request for usage history that fails because Meridian's service is briefly unavailable is sent once more after a few seconds; if that one times out, it is a gateway timeout like any other. After a gateway timeout of a week, setup waits 75 seconds, so Meridian is not asked twice while it may still be working on the first request, then asks for that week's newest day alone. If the day comes back, the week's other days follow one at a time and the import goes back to weeks; if a second week needs that too, the rest of the import asks for days. If the day times out as well, smaller requests do not help, and that grid import or export stops after those 2 requests. An hourly update stops at the first gateway timeout and leaves the retry to its next run, which asks for that week by day, or to the run after if the time limit stops it first; if another week then times out too, or the same one asked for whole again, the updates in the week after that ask for those weeks, and any older ones, by day, rather than ask for them whole again, whether or not the newest week is among them. A week Meridian refuses as too large is asked for by day, and so is the rest of that import or update, up to 92 requests for the 90 days; a second refusal stops. A rate limit is never retried early: hourly updates then wait as long as Meridian asks, an hour for its own in-band limit, and twice as long each time it comes again in a row, up to a day. So they do after one met while completing the older history, though never sooner than the hourly update. That one fails nothing: the update shows its own data and asks Meridian for nothing more, not even the billing details, and no more history is asked for until the wait has passed. Requests for your accounts and billing details are not sent again.

Each property's grid import and grid export is updated on its own. Every one's newest week is fetched first, then the older weeks, so a property whose supply ended, listed first, no longer holds up the others: once its first newest week comes back empty, the other properties' newest weeks go before its second; after a gateway timeout, the rest of that property waits out its 75 seconds behind the other properties' requests, not before them, and once the pause has passed, a newest week that waited with it goes before that property's older weeks. One that fails logs one warning, and Home Assistant shows the others' data as usual; it is tried again after 1 hour, then 2, doubling up to a day, and logs one INFO line once it works again. Until the first update after a restart or reload has succeeded, each of that update's retries tries every one again, and the warning says so. One that fails on an older week still imports the newer weeks, with statistics or without, and asks for the rest again after the same wait, while its hourly updates go on; until that reconciliation has run, diagnostics show it as degraded, and the days older than its 14 days come with the older history (above). If the reconciliation then fails on its newest week, that update imports nothing for it, but the hourly updates still go on, and the reconciliation waits longer. If one of those hourly updates fails, the next waits 1 hour, then 2, doubling up to a day, counting those failures alone, while the reconciliation keeps its own wait. A rejected session or a rate limit stops the whole update. So does the update's 4-minute time limit, for the requests still to come; what came back before it is imported by the same rules, and those it did not reach go first at the next update, and still get any reconciliation they missed. An update that reaches back more than 14 days, to import a grid import or export for the first time or after a long outage, has 15 minutes instead, so that a search for a supply that ended can finish; it imports what came back, whether or not it is done. So does one whose readings come back after they stopped for more than 14 days, once its newest week has them. Diagnostics show each one's state.

Each import writes the hours it fetched with running totals that follow on from the newest hour stored before them, so the Energy dashboard's running totals stay consistent. A correction you made with **Adjust sum** in **Settings → Tools → Statistics** on the hours an import writes again (the last 14 days at every restart or reload of the integration and at the weekly reconciliation, further after a long outage, and, when missing days are filled in or at the monthly correction, any hour from the first to the last hour of a week that it writes again, and the hour after them) is overwritten; one made on other hours is kept. An update also leaves stored statistics as they are, and logs one INFO line, when Meridian stops returning readings Home Assistant has already stored, or returns them without a cost it had: it imports only the hours it can import safely. For hours that come back without their cost, it imports their usage and keeps the stored cost of those hours and of the hours before them, while the cost of the hours after them is still written. Home Assistant must confirm that it saved what an import wrote: when it does not within about 90 seconds, as when it is busy with a backup, that grid import or export counts as failed, and the next update first waits until Home Assistant has saved what it wrote, then writes the same hours again. So is one that a reload of the integration interrupted while Home Assistant was saving it. One that reached back more than 14 days, as setup's does, is written again from its oldest hour, by setup's next try or the next update, after a reload of the integration too, but not after a restart of Home Assistant: if Home Assistant had saved only its usage or only its cost, some of its hours more than 14 days back can then keep only one of the two. An import that completes the older history, or corrects it monthly, fails nothing when it is not confirmed: its days wait an hour and are then written again in full, one at a time, after a reload of the integration too, and whether or not they are still within the 90 days or the billing period, and a day Meridian then returns no readings for, or only some of, waits again rather than count as done, but not after a restart of Home Assistant, after which the running totals can keep a jump where that week ends.

At start-up the integration only renews its session and loads the account list, so it does not hold up Home Assistant. Right after setup it does not even do that: it uses the accounts setup found. The restart reconciliation runs in the background once Home Assistant has started; until it finishes, the Meridian entities show as unavailable. It asks for none of the older history still missing, so the entities do not wait for that: the update after it does. Right after setup, this first sync continues the history setup imported with an hourly update instead of reconciling 14 days of it again, and does not search again a grid import or export setup found no readings for. Before its first request, this first sync, after setup, a restart or a reload, waits for Home Assistant to finish saving the statistics queued before it, such as the history setup imported, for up to 5 minutes; a backup, for example, can keep Home Assistant busy that long. If Home Assistant is still busy after that, it is retried after 10 minutes. If Meridian cannot be reached or returns unusable data, it is retried after 10 minutes, then 30, then hourly, rather than every 10 minutes; a Meridian rate limit is honoured as usual.

## Automation examples

Entity IDs below assume the default device name; check yours under **Settings → Devices & services → Meridian Energy**.

Notify when the current bill passes a budget:

```yaml
alias: Electricity bill over budget
triggers:
  - trigger: numeric_state
    entity_id: sensor.meridian_energy_current_bill_cost
    above: 250
actions:
  - action: notify.notify
    data:
      title: Electricity bill
      message: >-
        This bill has reached
        ${{ states('sensor.meridian_energy_current_bill_cost') }}.
```

Notify when Meridian data has stopped updating, for example after Meridian revokes the session:

```yaml
alias: Meridian data unavailable
triggers:
  - trigger: state
    entity_id: sensor.meridian_energy_last_data_update
    to: unavailable
    for:
      hours: 3
actions:
  - action: notify.notify
    data:
      title: Meridian Energy
      message: >-
        Meridian data has not updated for 3 hours. Check Settings → Devices &
        services for a reauthentication request.
```

For Energy dashboard sources, use the **Meridian grid import** statistics described above; the **Current bill** sensors are not offered there.

## Limitations and troubleshooting

The integration supports multiple accounts, properties, registers and time-of-use interval costs. It does not expose tariff-rate entities, fixed-charge entities, bills or payment data. Live testing has primarily used one property on an all-day plan without feed-in; other account types are covered by synthetic automated tests.

- If setup cannot connect, confirm MyMeridian is available and try again later.
- If the history import fails, the setup dialog says what failed and offers to try again with the same login, so no new emailed code is needed. Wait at least 2 minutes between attempts, or longer if Meridian asks Home Assistant to wait. After 5 failed attempts setup stops and says what failed last; start it again later. When Meridian's service failed or did not answer in time, it still offers to continue without history first (below). Each failure also leaves a warning from Meridian Energy in the Home Assistant log, saying which request it happened during or after; it contains no account details.
- The import has 4 minutes, not counting the time Home Assistant takes to save it; when they run out, setup keeps what came back, as below. As a backstop, setup stops an import still running after 5 minutes, not counting the time Home Assistant takes to save it, removes the statistics it created and offers to try again.
- If the import stopped part-way, setup keeps what came back once every grid import and export of a property has at least its newest week, and finishes: the updates after it import the rest in the background, and the **Current bill** sensors stay unknown until the history reaches the start of the billing period. Rolling back to an earlier version leaves an incomplete history as it is, so let it fill in first (diagnostics show `reaches_target`).
- If the import failed because Meridian's service failed or did not answer in time, as when a gateway timeout, or answers too slow, left setup too little time for any property's newest weeks, the dialog also offers to continue without history (**Continue without history**), after the last attempt too: setup removes what the failed import created and finishes, and the integration's updates import your history in the background, the last 90 days at the first update, then the rest a few weeks at a time. It is not offered after other failures, which those updates would most likely meet too.
- With several properties, setup finishes when at least one of them imported its newest weeks; a property that failed, such as one whose supply ended, logs a warning and is tried again by the first update after setup. A property with no readings in its 90 days does not count as one that imported: when it is listed beside one that fails, setup fails as it would for that one alone.
- If Home Assistant requests reauthentication, complete the emailed-code flow.
- If current-bill cost shows unknown, with its `data_complete` attribute false, Meridian may not have supplied cost for every interval yet. It also shows unknown for a billing period in which the stored running total of the cost does not follow the hourly costs, as after **Adjust sum** was used on the cost statistic: the total could not be trusted. The same holds for current-bill usage and export and their statistics, and the cost or export credit that goes with them then shows unknown too.
- If setup or an update says that older Meridian Energy statistics can't be continued, Home Assistant already has Meridian Energy statistics for that property, such as from an earlier installation, that are more than 800 days older than the history to import, or that have no running total in those 800 days. Nothing is imported for them, rather than start the running total again from zero, and trying again gives the same result until they are deleted in **Settings → Tools → Statistics**: select them, then **Delete selected statistics**.
- For a reproducible problem, use the integration page's three-dot menu to **Enable debug logging**, reproduce the issue, then disable it to download the log. Download diagnostics from the same menu, review both files, and follow the redaction warning in the issue form.
- If setup itself fails, that menu does not exist yet, so turn on debug logging for the integration first and then try again, so the new attempt is logged. Leave the setup dialog open on the failure: closing it ends the setup, and starting again needs a new emailed code. In a second browser tab, or the companion app, run the **Logger: Set level** action (`logger.set_level`) from **Settings → Tools → Actions**, then go back to the setup dialog and submit the failure form to try again with the same login:

  ```yaml
  action: logger.set_level
  data:
    custom_components.meridian_energy: debug
  ```

  This lasts until Home Assistant restarts. If the dialog was closed already, debug logging still covers the next setup, which needs a new emailed code. To keep debug logging across restarts, add this to `configuration.yaml` instead and restart Home Assistant. The restart ends the setup in progress, so start setup again afterwards, with a new emailed code:

  ```yaml
  logger:
    logs:
      custom_components.meridian_energy: debug
  ```

  Afterwards, undo the action by restarting Home Assistant, or by running it again with `notset`, which returns the integration to Home Assistant's default level: `warning`, unless your `logger:` configuration sets a `default`. Undo the YAML by removing it and restarting. The debug lines describe each request to Meridian: its kind, the HTTP status, the size and type of the answer, which server wrote an error page and how long it took; and for usage history, which week it asked for, as days before today, and what came back. They contain no email, account, address, token, usage or cost, and give times as hours or days before now rather than dates. With the time of each log line, though, they show how far back your usage history goes: when it is shorter than the 90 days, or the billing period, that the updates complete, they show the hour of your first reading. Check the log before posting it publicly.

To remove the integration, first remove its sources from the Energy dashboard, then delete **Meridian Energy** from **Settings → Devices & services** and uninstall it from HACS. Existing external statistics can be deleted separately in **Settings → Tools → Statistics** (select them, then **Delete selected statistics**) if you intend to discard that history.

## Security, support and licence

The integration never asks for or stores your Meridian password. It stores a renewable session token in Home Assistant; short-lived tokens remain in memory. Diagnostics exclude credentials, email, account, meter, address, usage and cost data by design.

Use [GitHub Issues](https://github.com/lafro/ha-meridian-energy/issues) for support and [private vulnerability reporting](SECURITY.md) for security problems. Development guidance is in [CONTRIBUTING.md](CONTRIBUTING.md).

This clean implementation acknowledges the earlier MIT-licensed [`codyc1515/ha-meridian-energy`](https://github.com/codyc1515/ha-meridian-energy) project. See [NOTICE](NOTICE). Licensed under the MIT License.

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
6. Keep the setup dialog open during the initial 90-day import. It can take several minutes.

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
| **Current bill cost** | Available tax-inclusive interval cost for that period, including any standing-charge components Meridian assigns to intervals. |
| **Current bill export** | Grid export in the current period; feed-in accounts only. |
| **Current bill export credit** | Retailer credit for that export; feed-in accounts only. |

**Billing period start**, **Billing period end** and **Next billing date** are disabled by default. Current-bill values remain unavailable until Home Assistant has complete data from the start of the period; missing cost is never treated as zero.

The **Current bill** sensors are running totals for the current period, with the period dates as attributes. They have no state class, so Home Assistant keeps no long-term statistics for them and does not offer them in the Energy dashboard; the statistics below are the long-term record.

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

- **First setup:** up to 90 days.
- **Hourly:** recent usage.
- **Daily:** delayed or provisional data, up to 14 days.
- **Weekly and after restart:** full 14-day reconciliation.

Meridian data is delayed rather than real-time. Revisions older than 14 days are outside the normal correction window.

At start-up the integration only renews its session and loads the account list, so it does not hold up Home Assistant. The restart reconciliation runs in the background once Home Assistant has started; until it finishes, the Meridian entities show as unavailable. If Meridian cannot be reached or returns unusable data, that first sync is retried after 10 minutes rather than at the next hourly update; a Meridian rate limit is honoured as usual.

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
- If Home Assistant requests reauthentication, complete the emailed-code flow.
- If current-bill cost is unavailable, Meridian may not have supplied cost for every interval yet.
- For a reproducible problem, use the integration page's three-dot menu to **Enable debug logging**, reproduce the issue, then disable it to download the log. Download diagnostics from the same menu, review both files, and follow the redaction warning in the issue form.

To remove the integration, first remove its sources from the Energy dashboard, then delete **Meridian Energy** from **Settings → Devices & services** and uninstall it from HACS. Existing external statistics can be deleted separately from **Developer tools → Statistics** if you intend to discard that history.

## Security, support and licence

The integration never asks for or stores your Meridian password. It stores a renewable session token in Home Assistant; short-lived tokens remain in memory. Diagnostics exclude credentials, email, account, meter, address, usage and cost data by design.

Use [GitHub Issues](https://github.com/lafro/ha-meridian-energy/issues) for support and [private vulnerability reporting](SECURITY.md) for security problems. Development guidance is in [CONTRIBUTING.md](CONTRIBUTING.md).

This clean implementation acknowledges the earlier MIT-licensed [`codyc1515/ha-meridian-energy`](https://github.com/codyc1515/ha-meridian-energy) project. See [NOTICE](NOTICE). Licensed under the MIT License.

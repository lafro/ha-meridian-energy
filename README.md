# Meridian Energy for Home Assistant

An unofficial Home Assistant integration for Meridian electricity usage, cost and billing-period insights.

[![Open your Home Assistant instance and open this repository in HACS.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=lafro&repository=ha-meridian-energy&category=integration)

> [!IMPORTANT]
> This project is not affiliated with, endorsed by or supported by Meridian Energy. It uses the same private customer-service endpoints as MyMeridian, which Meridian may change without notice.

## Features

- Passwordless setup using Meridian's emailed six-digit code.
- Automatic session renewal; another code is only needed if Meridian rejects or revokes the saved session.
- Account selection when a login has more than one electricity account.
- Hourly grid-consumption and consumption-cost statistics for Home Assistant's Energy dashboard.
- Current billing-period usage and cost entities using Meridian's own period dates.
- Conditional solar-export and export-credit support when Meridian reports a feed-in register.
- Adaptive hourly updates with reconciliation of provisional readings.
- Privacy-preserving diagnostics.

## Installation

1. Use the button above to add this repository to HACS, or add `lafro/ha-meridian-energy` as a custom **Integration** repository.
2. Download **Meridian Energy** in HACS and restart Home Assistant.
3. Go to **Settings → Devices & services → Add integration** and select **Meridian Energy**.
4. Enter your MyMeridian email address and the six-digit code Meridian sends you.
5. If offered a choice, select the accounts to import.
6. Keep the setup dialog open while up to 90 days of history is imported. This can take several minutes.

## Entities

Each selected account is represented as a separate Home Assistant device.

| Entity | Purpose |
|---|---|
| **Last data update** | When Home Assistant last completed a successful Meridian sync. |
| **Latest usage data** | The end of the newest completed usage interval received from Meridian. |
| **Provisional data intervals** | Number of consumption intervals in the rolling 14-day window that Meridian does not yet mark as actual. Attributes provide non-sensitive reconciliation detail. |
| **Current bill usage** | Consumption recorded in the current Meridian billing period. |
| **Current bill cost** | Available Meridian cost data for the current billing period, including tax. |
| **Current bill export** | Current-period grid export; only created for feed-in accounts. |
| **Current bill export credit** | Current-period export value; only created for feed-in accounts. |

Billing period start, end and next billing date entities are also available but disabled by default. The same dates are included as attributes on the current-bill entities.

Current-bill values are only published when Home Assistant has data from the start of Meridian's current period. A cost remains unknown if any required interval has no cost, rather than being treated as zero.

## Long-term statistics and Energy dashboard

For each property, the integration imports:

| Statistic | Unit |
|---|---:|
| **Meridian electricity consumption — `<property>`** | kWh |
| **Meridian electricity cost — `<property>`** | NZD |
| **Meridian solar export — `<property>`** | kWh |
| **Meridian solar export credit — `<property>`** | NZD |

Solar statistics are only created when feed-in data is available. These are external long-term statistics, not live sensor entities.

Add the consumption and cost statistics to Home Assistant's Energy dashboard after the first import. Do not add them alongside another whole-home source measuring the same grid import, as that would double-count usage.

## Updates and data timing

- **First setup:** imports up to 90 days.
- **Every hour:** retrieves the latest 24 hours.
- **Daily:** revisits delayed or provisional data, up to 14 days.
- **Weekly and after restart:** reconciles the complete previous 14 days.

The wider daily and weekly sync replaces that hour's normal request. Meridian data is not real-time and may arrive hours or days after electricity was used. Revisions older than 14 days are outside the normal correction window.

Removing the integration stops future imports but may leave existing long-term statistics. Delete those separately from **Developer tools → Statistics** only if you intend to discard the history.

## Compatibility and limitations

The integration supports multiple selected accounts, properties and meter registers, including combined time-of-use costs returned by Meridian. It does not expose separate tariff-rate entities, fixed-charge entities, bills or payment data.

Automated tests cover multi-account, multi-property, multi-register, time-of-use-style interval aggregation and feed-in behavior. Live testing to date has used one property on an all-day plan without solar, so reports from other account types are welcome.

## Security and support

The integration stores a renewable session token in Home Assistant, but never asks for or stores your Meridian password. Short-lived tokens remain in memory. Logs and downloadable diagnostics are designed to exclude credentials, email addresses, account and meter identifiers, addresses, usage and cost values.

Review diagnostics before sharing them publicly. See [SECURITY.md](SECURITY.md) for private vulnerability reporting and [CONTRIBUTING.md](CONTRIBUTING.md) for development instructions.

## Credits and licence

This is a clean implementation for Meridian's current application. The earlier archived [`codyc1515/ha-meridian-energy`](https://github.com/codyc1515/ha-meridian-energy) project established the original Home Assistant use case. See [NOTICE](NOTICE).

Licensed under the MIT License.

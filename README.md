# Meridian Energy for Home Assistant

An unofficial Home Assistant integration for Meridian Energy electricity accounts in New Zealand: hourly usage and cost for the Energy dashboard, and sensors for your bill.

[![Open this repository in the Marketplace, or in HACS before Home Assistant 2026.11.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=lafro&repository=ha-meridian-energy&category=integration)

> [!IMPORTANT]
> This project is not affiliated with, endorsed by or supported by Meridian Energy. It uses the service behind Meridian's web app, which may change without notice.

## What it provides

- Sign-in with the six-digit code Meridian emails you; no password.
- Hourly grid import and cost in the Energy dashboard, from 90 days back; grid export and credit too with a feed-in register.
- Sensors for your bill so far and Meridian's estimate of it, what you owe, your last bill and payment, and your plan's prices.
- Every account Meridian lists as active or dormant on your login, including one added later.

## Installation

You need Home Assistant 2026.10 or newer and a GitHub account: HACS needs one, and so does the Marketplace for a repository outside its catalog.

1. Install Meridian Energy, or use the button above:
   - **Home Assistant 2026.11 or newer:** go to **Settings → Marketplace → Add from link**, enter `https://github.com/lafro/ha-meridian-energy` and select **Add**, connecting GitHub when asked. Then open **Meridian Energy** and select **Install**; setup opens by itself, normally with no restart.
   - **Home Assistant 2026.10:** in HACS, open the menu (⋮), choose **Custom repositories**, add `https://github.com/lafro/ha-meridian-energy` with type **Integration**, download **Meridian Energy**, and restart Home Assistant.
2. If setup didn't open, go to **Settings → Devices & services → Add integration → Meridian Energy**.
3. Enter the email you use for MyMeridian, then the code Meridian sends to it.

Setup finishes once the code is accepted. Meridian Energy then fetches your last 90 days in the background, which can take several minutes; until then its sensors show as unavailable. The sign-in renews itself.

Updates appear under **Settings → Updates**: install one there, then restart Home Assistant when asked.

Beta versions are GitHub pre-releases, offered only if you opt in: in the Marketplace, **Meridian Energy → ⋮ → Include pre-releases**; in HACS on 2026.10, enable and turn on the **Pre-release** switch on Meridian Energy's device under **Settings → Devices & services → HACS**.

If an entry says it was set up by an earlier version, remove it and add Meridian Energy again, ideally with a [clean start](#starting-again).

## Energy dashboard

For each property, Meridian Energy keeps two statistics, and two more with a feed-in register:

| Statistic | Contents |
|---|---|
| Meridian grid import | kWh from the grid, each hour |
| Meridian grid import cost | Its cost in NZD, including GST and the daily charge |
| Meridian grid export | kWh sent to the grid (feed-in register only) |
| Meridian grid export credit | Meridian's credit for it, in NZD (feed-in register only) |

With several properties on the login, each name ends with the property's address.

In **Settings → Dashboards → Energy → Electricity grid → Add grid connection**, choose *Meridian grid import* as the energy imported and *Meridian grid import cost* as the entity tracking the total costs. With export, choose *Meridian grid export* as the energy exported and *Meridian grid export credit* as the entity tracking the total compensation.

Grid export isn't solar production: Meridian can't see what your home uses straight from your panels. Use your inverter's integration for **Solar production**.

## Sensors

Each account has one device, **Meridian Energy**.

| Sensor | Shows |
|---|---|
| Bill usage so far | Grid import in the current billing period, counting actual readings only, as Meridian's app does |
| Bill cost so far | The cost of that usage |
| Bill export so far, Bill credit so far | Grid export in the period, and its credit (feed-in register only) |
| Bill estimate | Meridian's estimate of the whole period's cost |
| Total due | What you owe: 0 when you're in credit, with the credit as an attribute |
| Last bill | When your latest bill was issued, with the period it covers |
| Last payment | Your latest payment's amount, with its date and status |
| Daily charge, Anytime price | Your plan's prices, including GST (only for one property with one meter on an all-day plan) |
| Last data update | When the last update succeeded (diagnostic) |
| Latest usage data | The end of the newest hour of usage received (diagnostic) |
| Provisional data intervals | Hours of grid import in the last three weeks that are still estimates (diagnostic) |
| Billing period start, Billing period end, Next billing date | The current billing period (diagnostic, disabled by default) |

Example (default names): notify when this bill passes a budget.

```yaml
alias: Electricity bill over budget
triggers:
  - trigger: numeric_state
    entity_id: sensor.meridian_energy_bill_cost_so_far
    above: 250
actions:
  - action: notify.notify
    data:
      message: >-
        This bill has reached
        ${{ states('sensor.meridian_energy_bill_cost_so_far') }} so far.
```

## How data updates

- Meridian Energy asks Meridian for new data every hour.
- Meridian's readings usually arrive 2–3 days late, as estimates until then, and gaps usually fill in within a week or two. Each update asks again for the last three days, and once a day (and after each restart or reload) for the last three weeks, so readings replace the estimates.
- The statistics go back 90 days. Hours missing in that time, as after an outage, are filled in by the next updates.
- The **so far** sensors count actual readings only, as Meridian's app does, so they can read lower than the Energy dashboard, which shows estimates too.
- Each hour's cost includes its share of the daily charge, which Meridian spreads over the day.
- The billing period follows Meridian's billing cycle, as its app does, so its dates can differ by a day or two from those printed on your bill.
- Some hours have no figure at Meridian, such as cost from before your account moved to Meridian's current system. Meridian Energy asks for them once more after each restart or reload.
- If Meridian refuses a request for older dates, Meridian Energy stops filling in older history until Home Assistant restarts.
- An **Adjust sum** (in **Settings → Tools → Statistics**) made in about the last five weeks, or anywhere in history still being filled in, is replaced by Meridian's figures at a later update.

## Troubleshooting

- **Reconnect requested:** Meridian ended the sign-in. Open the request under **Settings → Devices & services**, select **Submit**, and enter the new code Meridian emails you.
- **"Meridian Energy isn't updating":** this repair notice appears after three failed updates in a row, or at once when Meridian stops listing any active or dormant account for the login. It gives the reason, for example:

  > Couldn't get an answer from Meridian (HTTP 504). Meridian Energy tries again automatically.
  >
  > This notice goes away by itself once an update works.

  An ended sign-in asks you to reconnect instead.
- **Asked to wait:** at setup, try again later; after that, Meridian Energy waits as asked, then carries on by itself.
- **A warning that Meridian Energy couldn't update grid import or export:** that part failed while the rest kept updating; the next update tries it again.
- **An answer Meridian Energy can't read:** it tries again by itself; if this keeps happening, check for an update to Meridian Energy.
- **Reporting a problem:** in the integration's ⋮ menu, select **Enable debug logging**, reproduce the problem, then **Disable debug logging** to download the log. Download diagnostics from the same menu. Both leave out your email, account and meter numbers, address, tokens, usage and amounts, but check them before sharing.

### Starting again

For a clean start, delete **Meridian Energy** under **Settings → Devices & services**, delete its statistics in **Settings → Tools → Statistics** (select them, then **Delete selected statistics**), and add it again. The statistics keep their IDs, so the Energy dashboard needs no change.

## Removal

1. Remove the Meridian statistics from **Settings → Dashboards → Energy**.
2. Delete **Meridian Energy** under **Settings → Devices & services**.
3. On 2026.11, choose **Uninstall** when asked (or **Meridian Energy → ⋮ → Uninstall** in the Marketplace); on 2026.10, remove it in HACS. Then restart Home Assistant.

Its statistics stay; delete them under **Settings → Tools → Statistics** if you no longer want the history.

## Limitations

Solar export, time-of-use plans and several accounts or properties are tested only with synthetic data.

## Security, support and licence

Meridian Energy never asks for or stores your Meridian password: its entry holds only your email and a renewable sign-in.

Use [GitHub Issues](https://github.com/lafro/ha-meridian-energy/issues) for support and [private vulnerability reporting](SECURITY.md) for security problems. Development notes are in [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/](docs/README.md).

This clean implementation acknowledges the earlier MIT-licensed [codyc1515/ha-meridian-energy](https://github.com/codyc1515/ha-meridian-energy); see [NOTICE](NOTICE). Licensed under the MIT License; see [LICENSE](LICENSE).

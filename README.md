# Meridian Energy for Home Assistant

An unofficial Home Assistant custom integration that imports electricity usage from Meridian Energy's current MyMeridian service into Home Assistant's long-term statistics and Energy dashboard.

[![Open your Home Assistant instance and open this repository in HACS.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=lafro&repository=ha-meridian-energy&category=integration)

> [!IMPORTANT]
> This project is not affiliated with, endorsed by, or supported by Meridian Energy. It uses the same private customer-service endpoints as the current MyMeridian application. Meridian can change those endpoints without notice.

## What it provides

- Passwordless setup using Meridian's emailed six-digit login code.
- Automatically renewed Firebase sessions, with UI reauthentication only if Meridian rejects or revokes the stored session.
- Hourly grid-consumption statistics in kWh.
- Consumption-cost statistics in NZD, including Meridian's consumption and standing-charge values.
- Solar-export and export-credit statistics when a feed-in register is present.
- A one-year initial history import, followed by a fourteen-day overlap on each update so estimated readings can be replaced safely by actual readings.
- Privacy-preserving diagnostics and diagnostic entities for sync health and meter-data freshness.

The integration polls every three hours. Meridian's meter data can itself be delayed by several days.

## Security and privacy

The integration never asks for or stores your Meridian password.

During setup, Meridian emails a single-use code. The integration exchanges it for a Firebase refresh token. Home Assistant stores that token in the config entry, as it does credentials for other integrations, and uses it to renew short-lived sessions automatically. Routine polling and Home Assistant restarts do not require another code. A new code is only required if Meridian rejects or revokes the refresh session, or the integration is removed and configured again. Short-lived ID tokens are held in memory only.

The integration deliberately avoids logging or diagnostics containing:

- email addresses;
- login codes;
- Firebase tokens or user IDs;
- Meridian account numbers or ICPs;
- property addresses; or
- electricity usage and billing values.

See [SECURITY.md](SECURITY.md) before reporting a security issue.

## Installation

1. Select the **Open your Home Assistant instance** button above to add this repository to HACS, or add `lafro/ha-meridian-energy` manually as a custom **Integration** repository.
2. In HACS, download **Meridian Energy** and restart Home Assistant when prompted.
3. Go to **Settings → Devices & services → Add integration**.
4. Search for **Meridian Energy** and enter the email used for MyMeridian.
5. Enter the six-digit code emailed by Meridian.
6. Keep the setup dialog open while the integration imports up to one year of history. The progress step normally takes 2–5 minutes and advances automatically.

## Energy dashboard

The integration creates external long-term statistics rather than pretending Meridian's delayed settlement data is a live power sensor.

After the first successful import, configure the Energy dashboard with the generated consumption statistic and its cost statistic. Solar export statistics are created only when Meridian reports a feed-in register.

Do not add a Meridian statistic alongside another whole-home meter that measures the same grid import; doing so would double-count consumption.

## Data handling

Meridian returns hourly interval values. For each property, the integration:

1. aggregates multiple registers at the same hour;
2. converts timestamps to UTC while preserving New Zealand daylight-saving boundaries;
3. builds monotonic cumulative kWh and NZD statistics;
4. converts Meridian's cost values from cents to dollars;
5. upserts existing timestamps, allowing estimates to be corrected later; and
6. uses stable hashed statistic identifiers that do not expose account or property IDs.

The initial history window is 365 days. Subsequent updates re-import the latest 14 days. This bounds API load while covering Meridian's normal estimate-to-actual revision period.

## Removal

Removing the integration stops future imports. Home Assistant may retain previously imported long-term statistics. Remove those statistics separately from **Developer tools → Statistics** only if you intend to discard the historical data.

## Troubleshooting

- **Code not found:** codes are single-use. Restart setup to request another.
- **Reauthentication required:** this is not routine. It means Meridian rejected or revoked the renewable session; use the integration's **Reconfigure/Reauthenticate** action and enter the new emailed code.
- **Latest meter data is old:** Meridian commonly publishes data after a delay; first check the MyMeridian application.
- **No Energy data immediately after setup:** the initial historical import is queued through Home Assistant's recorder and may take a short time to appear.

Download diagnostics from the integration entry before opening an issue. The diagnostics are designed to exclude credentials and household data, but review any file before sharing it publicly.

## Development

This integration targets Home Assistant 2026.7.2 or newer and Python 3.14.2 or newer. Quality gates include Ruff, mypy, pytest, coverage, Hassfest and HACS validation.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the local workflow.

## Credits and licence

This is a clean implementation for Meridian's current application architecture. The earlier archived [`codyc1515/ha-meridian-energy`](https://github.com/codyc1515/ha-meridian-energy) project established the original Home Assistant use case and statistic naming approach. See [NOTICE](NOTICE).

Licensed under the MIT License.

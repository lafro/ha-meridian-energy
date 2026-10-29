# Changelog

Notable changes to Meridian Energy for Home Assistant. Releases from v0.2.6 onwards are listed here; earlier release notes are on the [GitHub releases page](https://github.com/lafro/ha-meridian-energy/releases).

## 0.3.0

Requires Home Assistant 2026.10.0 or newer, the same as 0.2.6. Your sensors and settings carry over, so there's nothing to change after updating.

This release fills in more of your energy history and stops setup getting stuck. It also changes when Meridian sensors show unknown or unavailable: see **Sensors** below.

### Energy history

- **Older gaps in your history are filled in.** The integration now asks Meridian for hours missing from your Energy dashboard from 14 to 90 days ago, including gaps from before you updated, and fills them in over the hourly updates that follow. Gaps Meridian has no readings for stay empty.
- **Later corrections from Meridian are picked up.** About once a month, the integration now asks Meridian again for the days from 14 to 90 days ago that are already in your history, and saves readings Meridian has changed, such as an estimate replaced by an actual reading. Past days' usage and cost may change as a result.

### Sensors

- **Sensors can take longer to come back after an outage.** If updates keep failing, the integration now waits longer each time before asking Meridian for your usage again, up to a day. So once Meridian or your internet is back, your Meridian sensors can stay unavailable for many hours; in 0.2.6 they came back within about an hour. The usage you missed is filled in once they're back. To try again straight away, reload the integration: **Settings → Devices & services → Meridian Energy → ⋮ → Reload**.
- **Sensors can go unavailable while Home Assistant is busy.** If Home Assistant takes too long to save Meridian's readings, for example during a long backup, or fails to save some hours, your Meridian sensors may now show unavailable until a later update, when the integration tries those hours again. In 0.2.6 they stayed available, but hours that failed to save could stay missing from your Energy dashboard.
- **If you've set up more than one property, or you export solar.** When updates keep failing for just one property, or just the export, your Meridian sensors now stay available; in 0.2.6 they all became unavailable. Until the failing one catches up, the Current bill sensors that count it may be incomplete or show unknown.
- **If you use Adjust sum on a Meridian statistic.** After you adjust an hour in your current bill, the matching Current bill sensor now shows unknown, and so does its cost or credit if you adjusted usage or export. They come back with your next bill's first readings, or sooner if the integration writes that hour again from Meridian, which undoes your adjustment: it writes the last 14 days again at every restart and now also once a week.

### Setting up

These apply when you add the integration.

- **Setup no longer gets stuck.** In 0.2.6, if downloading your history at the end of setup failed, the setup window kept spinning and never said what had gone wrong. It now says what went wrong and, unless your sign-in has expired, lets you try again from the same window up to 4 times.
- **Setup can finish with part of your history.** If Meridian is too slow to send all of it, setup can finish once your most recent week has arrived, and the rest is fetched in the background. Your Current bill sensors may show unknown until then.
- **You can finish setup without your history.** If the **History import failed** screen says Meridian's service returned an error or didn't answer in time, you can turn on **Continue without history**. The integration then fetches your history by itself later.

## 0.2.6

Requires Home Assistant 2026.10.0 or newer.

### Fixed

- A failed first import now removes the partial statistics it created. The rollback called a recorder-internal function from the wrong thread, so on a real recorder it raised an error, left the partial statistics in place and hid the original failure. It now uses the recorder's public API.

### Changed

- Start-up no longer waits for Meridian. Setup renews the session and loads the account list; the restart reconciliation, which imports statistics, runs in the background after Home Assistant has started. Entities show as unavailable until it finishes. If that first sync fails because Meridian is unreachable or returns unusable data, it is retried after 10 minutes instead of at the next hourly update.
- The **Current bill** sensors no longer have a state class or `last_reset`, so Home Assistant stops recording long-term statistics for them and no longer offers them in the Energy dashboard. The Meridian grid statistics remain the long-term record, and each sensor still shows its billing period as attributes. After updating, Home Assistant shows a repair saying each of these sensors no longer has a state class; delete their old statistics in **Developer tools → Statistics** to clear it.
- Reauthentication uses Home Assistant's reauth-entry helper and unique-ID check.
- Unsupported or incomplete config entries stop at a migration error with a translated explanation instead of an unexplained failure.
- A config entry from a later release with the same major config version loads unchanged, so rolling back to this release does not stop at a migration error.
- The success messages for reauthentication and reconfiguration come from Home Assistant's shared translations.
- The README has automation examples.

### Maintenance

- Tests run against Home Assistant 2026.10 (pytest-homeassistant-custom-component 0.13.370), plus a weekly compatibility run against the newest release and beta.
- Dependabot uses the `uv` ecosystem, so updates change `uv.lock` together with `pyproject.toml`, and CI installs with `uv sync --locked`.
- Development dependencies upgraded; the Pillow and PyJWT overrides were removed because Home Assistant 2026.10 pins patched versions.
- Release notes are this file's section for the version, and CI fails when the version has no section here.
- Workflow checkouts no longer keep the job token, and the release gates install without a dependency cache.
- Snapshot tests cover entity states and diagnostics, and developer notes are in `docs/`.

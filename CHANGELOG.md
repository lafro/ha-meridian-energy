# Changelog

Notable changes to Meridian Energy for Home Assistant. Releases from v0.2.6 onwards are listed here; earlier release notes are on the [GitHub releases page](https://github.com/lafro/ha-meridian-energy/releases).

## 0.3.0

Requires Home Assistant 2026.10.0 or newer. Every change is listed in [0.3.0 in detail](https://github.com/lafro/ha-meridian-energy/blob/v0.3.0/docs/changes/0.3.0.md).

### Fixed

- Setup no longer spins forever when Meridian is slow. Usage history is asked for a week at a time, and a slow answer gets up to 75 seconds instead of 30.
- When the history import fails, setup says why and offers to try again without a new code. After a Meridian timeout or service error it also offers **Continue without history**.
- If Meridian is too slow, setup finishes once the newest week of history is in, and the rest imports in the background.
- A property, or its grid import or export, that fails no longer stops the others updating. It is tried again on its own after 1 hour, then 2, doubling up to a day; reload the integration to try sooner.
- A property whose supply has not started yet no longer stops setup and updates for the others.
- Missing history is filled in automatically, after a partial setup or an outage, back 90 days or to the start of the current billing period when that is older (up to 400 days). Within that range, estimates more than 14 days old are corrected once a month.
- An import counts only once Home Assistant confirms it saved it; hours it did not save are written again instead of skipped.
- The **Current bill** sensors show unknown instead of a wrong total for a billing period whose stored history is inconsistent, as after **Adjust sum** or where an earlier version left a wrong hour.
- A change in the registers Meridian returns for a meter no longer fails every update for up to 14 days.

### Changed

- Shorter setup screens, and error messages that say when Meridian timed out or returned an error.
- With debug logging on, each request to Meridian is logged, with no personal details. Each failed history import during setup logs one warning saying where it failed.
- Diagnostics show the state of each property's grid import and export, including whether its history is complete (`reaches_target`).
- An **Adjust sum** correction on hours an import writes again is undone, now at the weekly reconciliation too.
- Statistics with no running total to continue, as from another tool or a long-ago installation, are not built on: the import is refused and says so.

### Before rolling back

Let the history fill in first (diagnostics show `reaches_target`): earlier versions do not fill in gaps more than 14 days old.

### Maintenance

- Tests run against Home Assistant 2026.10.4, and end-to-end journeys run setup in a disposable Home Assistant before every release.

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

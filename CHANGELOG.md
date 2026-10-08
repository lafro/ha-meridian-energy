# Changelog

Notable changes to Meridian Energy for Home Assistant. Releases from v0.2.6 onwards are listed here; earlier release notes are on the [GitHub releases page](https://github.com/lafro/ha-meridian-energy/releases).

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

# CLAUDE.md: ha-meridian-energy

Public HACS custom integration (`custom_components/meridian_energy`, domain `meridian_energy`) for Meridian Energy (New Zealand). It is a custom repository in HACS and is deliberately not listed in the HACS default store. This repository is public: everything committed here, and every PR or issue text, is world-readable.

## Rules

- **Synthetic data only.** Fixtures, tests, docs, commits, PR text and logs never contain real emails, account numbers, ICPs, meter IDs, addresses, tokens, usage or cost from any real installation. Use values such as `A-TEST-1`, `synthetic-account`, `person@example.com`.
- **No secrets.** CI runs a secret scan on every pull request; run the same locally with `gitleaks git -c .gitleaks.toml .` and `gitleaks dir -c .gitleaks.toml .`. `.gitleaks.toml` allows `FIREBASE_API_KEY` in `const.py`, the Meridian web app's public client key (see SECURITY.md); anything else blocks the pull request.
- **Pull requests only.** Never push to `main`, force-push, or create, move or delete tags or releases. Tags and releases are immutable by ruleset and come only from the Release workflow, which the maintainer runs.

## Development

```bash
uv sync --locked --all-groups        # never edit uv.lock by hand
uv run ruff format --check .
uv run ruff check .                  # ruff "ALL" with the ignores in pyproject.toml
uv run mypy custom_components/meridian_energy   # strict
uv run pytest                        # branch coverage gate 98% overall
uv run python scripts/check_module_coverage.py  # 95% per module
uv run python scripts/check_versions.py vX.Y.Z  # manifest = pyproject = tag, CHANGELOG entry
```

- Use the global uv cache (`~/.cache/uv`); never create `.uv-cache/` in the repo. If `.venv` is broken, delete it and run `uv sync --locked --all-groups`.
- `pytest-homeassistant-custom-component` is pinned exactly and pins Home Assistant itself, so bump that one pin to move the Home Assistant version under test. Do not add a separate `homeassistant` pin.
- The harness also pins `pytest`, `pytest-asyncio`, `pytest-cov` and `syrupy` exactly, so Dependabot ignores them: an update to one of them alone cannot resolve, so Dependabot skips it and records an error that fails the uv job. They move with the harness. The ignore also stops Dependabot security pull requests for them, though their alerts still show; fix such an alert by bumping the harness pin. `test_dependabot_leaves_the_harness_pins_to_the_harness` fails unless the ignore list is exactly the direct dev dependencies the installed harness pins with `==`.
- A harness bump that changes which direct dev dependencies the harness pins with `==` fails that test on the pull request that bumps it, normally Dependabot's uv pull request, until the ignore list is updated in that same pull request. This is intended: the list follows the harness. The weekly compat run tests the newest harness, so it can fail on the same test first. Never narrow an entry with `versions:` or `update-types:`: Dependabot would then try that package's other updates, which cannot resolve and fail the uv job as before. The test fails on such an entry.
- `.github/workflows/compat.yml` runs weekly against the newest harness (stable and beta) unpinned and opens an issue when it fails. GitHub disables scheduled workflows in a public repository after 60 days without activity; check with `gh run list --workflow compat.yml` and re-enable with `gh workflow enable compat.yml` (and `validate.yml`).
- Tests that touch statistics must also pass on the real recorder (`recorder_mock`); see `tests/test_statistics_recorder.py`.
- Snapshot tests (syrupy) cover entity states and diagnostics: `tests/snapshots/`. After an intended change, run `uv run pytest --snapshot-update`, then review the `.ambr` diff before committing it.
- Every behaviour change needs a test; keep `strings.json` and `translations/en.json` identical.

## Design notes

Fuller, public notes are in [`docs/`](docs/README.md).

- Statistics are external (`meridian_energy:consumption_<key>` and friends); keys are truncated SHA-256 hashes, so IDs carry no identifiers.
- Recorder writes and deletes must go through the public Recorder API on its own thread (`get_instance(hass).async_clear_statistics`, `async_adjust_statistics`, `async_add_external_statistics`). Never call `recorder.statistics.*` write helpers from an executor.
- Every statistics import, the flow's and every entry's, goes through one primitive, `statistics.async_replay`: running sums from the newest stored row with a sum before the span (by time, hourly periods only, passing over rows without a sum as `async_latest_numeric_statistic_start` does), stored states for the hours in the span it was not given, one `async_adjust_statistics` shift of the rows after it (so users' "Adjust sum" corrections after its first row survive; one at that row is undone, since its step becomes its state), all queued with no await between them under per-statistic locks in `hass.data[DOMAIN]["write_locks"]`, then confirmed behind a barrier by reading back the joins it wrote. The next replay of statistics whose last replay was not confirmed first waits behind two barriers (`_async_settle`), so it never reads running sums the recorder has yet to save. An unconfirmed replay raises `StatisticsWriteError` (`statistics_write`), and one refused for want of a running total to continue its subclass `StatisticsAnchorError` (`statistics_refused`, which a retry does not clear); either way nothing that records what was imported moves on. Seams from older releases are not searched for or repaired at start-up; one inside a span a replay writes, as in the 14 days of every restart, reload and weekly reconciliation, is rewritten with it.
- The setup flow imports with a new coordinator for each attempt, under a 240 s budget and a 300 s backstop that leaves out the time the recorder takes to save it. A failed attempt rolls back the statistics the flow created (`async_clear_statistics`, never those that existed before the flow) and shows the `import_failed` form, which retries with the same session (120 s apart, 5 attempts) and, after Meridian's service failed or timed out (as when slow answers left the budget too short for any property's newest windows), offers to continue without history. Before each attempt the flow waits for the recorder behind two barriers.
- Setup only renews the session and caches the topology (`MeridianDataCoordinator.async_prepare_topology`); right after the setup flow it makes no request at all: it takes the flow's `SetupHandoff` (the accounts, when the import ran, and the import coordinator's `SyncMemory`), so after an import its first sync continues it in the mode the cadence asks for, normally `tip`, instead of a 14-day `restart`. The first sync runs as an entry background task after Home Assistant has started, and waits for the recorder to save what was queued before it before any request or statistics read; entities are unavailable until it finishes.
- Each sync plans every unit (one direction of one property), fetches every unit's newest window before any older one, and walks each unit on its own: a failure stops only its unit, which backs off (`units.py`, in memory only), while a rejected session or a rate limit stops the sync. Measurements requests ask for windows of at most 7 New Zealand dates (`windows.py`), with one resend after a passing failure, a 75 s cooldown per property after a gateway timeout, and the size ladder (by day) after it.
- Setup and updates import what they fetched when a walk stops part-way (partial setup creates the entry once every direction of a property has its newest window and one of them found readings). The rest of the history is filled in by lanes (`lanes.py`): after its walks, each update asks for at most 6 more windows per unit of the days missing back to its target (New Zealand midnight 90 days back, or at the billing period's start when older, at most 400 days, and never after a date whose coverage import was not confirmed), from the update after its initial import on and never in a coordinator's first sync, which the entities wait for, and once a month asks again for older stored days to correct revised estimates. A lane's state is rebuilt from the recorder at every start; it has no Store, and only the dates whose coverage import was not confirmed are kept per entry in `hass.data` across reloads (`UNSAVED_COVERAGE`, handed over at the end of every sync), as unconfirmed deep replays are. A rate limit the lanes meet fails nothing, so the coordinator honours it by setting its own `update_interval` for the next interval only, never below the hourly one, and that update sends no billing request (`_honour_coverage_rate_limit`, `_async_billing_period`).
- The current-bill sensors have no state class, so Home Assistant keeps no long-term statistics for them; the external statistics are the long-term record.
- Config entries are version 3. A minor-version bump must stay loadable by every earlier 3.x release, because the rollback is to re-release earlier code: `async_migrate_entry` accepts a higher minor version unchanged. Anything that is not backward compatible needs a major-version bump.
- Diagnostics and logs must never contain identifiers, payloads or headers.

## Branches, pull requests and releases

- Branches: `<type>/<slug>` (`fix/`, `feat/`, `chore/`, `docs/`, `release/`). `main` is squash-only with required checks `python`, `hassfest`, `hacs`, `dependency-review`, `Analyze Python`, `secrets` and `private-terms`.
- A shippable change bumps `version` in `manifest.json` and `pyproject.toml` together and adds a `## X.Y.Z` section to `CHANGELOG.md`; `check_versions.py` fails without it. HACS only offers an update when the version string changes, so never cut a release for CI-only changes.
- Release (maintainer only): after the PR is merged, run the **Release** workflow from `main` with tag `vX.Y.Z` (`vX.Y.ZbN` publishes a beta pre-release; see `docs/releasing.md`). The workflow re-runs the gates, creates the immutable tag and publishes the release with that version's CHANGELOG section as its notes. Never create or move tags by hand.
- Users install a GitHub release through HACS. Never copy files into `/config/custom_components` by hand.
- A defect found after release is fixed forward with a new patch version; existing releases are never replaced.

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
- A harness bump that changes which direct dev dependencies the harness pins with `==` fails that test on the pull request that bumps it, normally Dependabot's uv pull request, until the ignore list is updated in that same pull request. This is intended: the list follows the harness. Never narrow an entry with `versions:` or `update-types:`: Dependabot would then try that package's other updates, which cannot resolve and fail the uv job as before. The test fails on such an entry.
- Dependabot's uv updates run on the 12th of each month, when the harness pins the month's Home Assistant release rather than a beta (`test_pull_requests_are_tested_on_a_supported_release` refuses a beta). Don't move that day outside the 9th to the 21st.
- `.github/workflows/compat.yml` checks every 6 hours and tests only when something changed: `main` and the newest release on the newest harness (`stable`, and `beta` while the harness pins one), and `main` on the minimum in `hacs.json` (`minimum`). Each target uploads a `compat-result-*` artifact (schema in `docs/releasing.md`); the report keeps a `compat` issue open while anything is broken and a `deprecation` issue while Home Assistant reports a deprecation. Snapshot-only differences are not breaks. Its scripts are `scripts/compat_*.py`, tested in `tests/test_compat_scripts.py`. GitHub disables scheduled workflows in a public repository after 60 days without activity; check with `gh run list --workflow compat.yml` and re-enable with `gh workflow enable compat.yml` (and `validate.yml`).
- `tests/ha_deprecations.py` fails any test run in which Home Assistant reports a deprecation for this integration, or its own code raises a deprecation warning. Fix the use in a patch release; wrap a test that triggers one on purpose in `COLLECTOR.expected()`.
- Raise the minimum in `hacs.json` only when the code needs something newer (or Home Assistant drops the Python it runs on), in the pull request that needs it.
- Tests that touch statistics must also pass on the real recorder (`recorder_mock`); see `tests/test_statistics_recorder.py`.
- Snapshot tests (syrupy) cover entity states and diagnostics: `tests/snapshots/`. After an intended change, run `uv run pytest --snapshot-update`, then review the `.ambr` diff before committing it.
- Every behaviour change needs a test; keep `strings.json` and `translations/en.json` identical.

## Design notes

Fuller, public notes are in [`docs/`](docs/README.md).

- Statistics are external (`meridian_energy:consumption_<key>` and friends); keys are truncated SHA-256 hashes, so IDs carry no identifiers.
- Every write is one `statistics.async_write_hours` call per unit (energy and cost), through the Recorder API on its own thread: a barrier, the stored rows read from 14 days before the first hour, sums continued from the newest stored row before the span, and the rows queued with no await between them under per-statistic locks (`hass.data[DOMAIN]["write_locks"]`). Repeating a write is safe; a dropped one heals at the next.
- Setup signs in and creates the entry at once (`user` → `otp`), following every ACTIVE or DORMANT account; a login with none aborts the flow, and at setup raises `ConfigEntryNotReady` (`no_accounts`). The first sync runs in the background once Home Assistant has started; entities are unavailable until one succeeds, tried again after 10, 30 and 60 minutes, then hourly.
- Each hourly sync takes the units (one direction of one property) in turn, requests at least 1 s apart. A unit asks for its recent dates, the last 21 once a day and the last 3 otherwise, then fills the dates missing back to New Zealand midnight 90 dates ago, at most 13 windows of 7 dates a sync, newest first. A failed fill window waits an hour, doubling to a day. What a unit learns is kept in memory only (`_Memory`), so a restart asks again for the last 21 dates.
- A sync succeeds when any unit imported; a failing unit logs one WARNING until it recovers. A rate limit stops the sync and the next waits as Meridian asked. Failures map to `cannot_connect`, `invalid_data`, `rate_limited`, `no_accounts` or `invalid_auth` (reauthentication). After 3 failed updates in a row, or at once for `no_accounts`, a repair notice (`sync_failing_<entry_id>`) shows until an update works.
- The current-bill sensors have no state class; the external statistics are the long-term record.
- Config entries are version 4; earlier ones are refused (`entry_too_old`) and must be added again. A minor bump must stay loadable by every earlier 4.x release (the rollback is to re-release earlier code), so `async_migrate_entry` accepts a higher minor version unchanged; anything incompatible needs a major bump.
- Diagnostics and logs must never contain identifiers, payloads or headers.

## Branches, pull requests and releases

- Branches: `<type>/<slug>` (`fix/`, `feat/`, `chore/`, `docs/`, `release/`). `main` is squash-only with required checks `python`, `hassfest`, `hacs`, `dependency-review`, `Analyze Python`, `secrets` and `private-terms`.
- A shippable change bumps `version` in `manifest.json` and `pyproject.toml` together and adds a `## X.Y.Z` section to `CHANGELOG.md`; `check_versions.py` fails without it. HACS only offers an update when the version string changes, so never cut a release for CI-only changes.
- Release (maintainer only): after the PR is merged, run the **Release** workflow from `main` with tag `vX.Y.Z` (`vX.Y.ZbN` publishes a beta pre-release; see `docs/releasing.md`). The workflow re-runs the gates, creates the immutable tag and publishes the release with that version's CHANGELOG section as its notes. Never create or move tags by hand.
- Users install a GitHub release through HACS. Never copy files into `/config/custom_components` by hand.
- A defect found after release is fixed forward with a new patch version; existing releases are never replaced.

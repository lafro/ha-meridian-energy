# CLAUDE.md: ha-meridian-energy

Public custom integration (`custom_components/meridian_energy`, domain `meridian_energy`) for Meridian Energy (New Zealand): a custom repository in HACS and the Marketplace, deliberately in neither catalog. Everything committed here, and every PR or issue text, is world-readable.

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
- Dependabot ignores the dev dependencies the harness pins with `==`, which can't update alone; they move with the harness. `test_dependabot_leaves_the_harness_pins_to_the_harness` requires the ignore list to match them exactly. Never narrow an entry with `versions:` or `update-types:`.
- Dependabot's uv updates run on the 12th, when the harness pins a release, not a beta, which pull requests must not test on; keep that day between the 9th and the 21st.
- `.github/workflows/compat.yml` tests new Home Assistant versions and keeps a `compat` or `deprecation` issue open while something breaks (see `docs/releasing.md`). GitHub disables scheduled workflows after 60 days without activity; re-enable with `gh workflow enable compat.yml` (and `validate.yml`).
- `tests/ha_deprecations.py` fails any run in which Home Assistant reports a deprecation for this integration. Wrap a test that triggers one on purpose in `COLLECTOR.expected()`.
- Raise the minimum in `hacs.json` only in the pull request whose code needs it.
- Tests that touch statistics must also pass on the real recorder (`recorder_mock`).
- Snapshot tests (`tests/snapshots/`) cover entity states and diagnostics; after an intended change, run `uv run pytest --snapshot-update` and review the `.ambr` diff.
- Every behaviour change needs a test; keep `strings.json` and `translations/en.json` identical.
- `tests/test_repository_contracts.py` holds the docs to their word limits and checks their links and the journeys table. The README says what users see and do: no request counts, timings or recorder internals.

## Design notes

Fuller, public notes are in [`docs/`](docs/README.md).

- The entry holds only the email and the session (refresh token and Firebase user ID); `tests/test_config_flow.py` pins this.
- Statistics are external (`meridian_energy:consumption_<key>` and friends); keys are truncated SHA-256 hashes, so IDs carry no identifiers.
- Every write is one `statistics.async_write_hours` call per unit (energy and cost), under per-statistic locks (`hass.data[DOMAIN]["write_locks"]`). Repeating a write is safe; a dropped one heals at the next. Recorder writes, deletes and barriers go only through the public Recorder API on its own thread; never call `recorder.statistics.*` write helpers from an executor.
- Setup signs in and creates the entry at once, following every ACTIVE or DORMANT account; with none, the flow aborts and entry setup raises `ConfigEntryNotReady` (`no_accounts`). The first sync runs in the background once Home Assistant has started.
- Each hourly sync takes the units (one direction of one property) in turn. A unit asks for its recent dates, then fills the dates missing back to New Zealand midnight 90 dates ago, newest first; a date is missing when an hour of it is absent from either statistic. What a unit learns is kept in memory only (`_Memory`), so a restart asks again for the last 21 dates.
- A sync succeeds when a unit with readings imported its recent dates, or none failed. A unit without readings never counts as imported, and counts as failed only when its recent dates fail. A rate limit stops the sync, which fails only when nothing was imported; the next waits as Meridian asked. A failing unit logs one WARNING while another updates. After 3 failed updates in a row, or at once for `no_accounts`, a repair notice (`sync_failing_<entry_id>`) shows until an update works.
- Only the diagnostic count has a state class; the external statistics are the long-term record.
- Config entries are version 4; earlier ones are refused (`entry_too_old`) and added again. `async_migrate_entry` accepts a higher 4.x minor version unchanged, so a minor bump must stay loadable by earlier 4.x releases; anything else needs a major bump.
- Diagnostics and logs must never contain identifiers, amounts, payloads or headers.

## Branches, pull requests and releases

- Branches: `<type>/<slug>` (`fix/`, `feat/`, `chore/`, `docs/`, `release/`). `main` is squash-only with required checks `python`, `hassfest`, `hacs`, `dependency-review`, `Analyze Python`, `secrets` and `private-terms`.
- A shippable change bumps `version` in `manifest.json` and `pyproject.toml` together and adds a `## X.Y.Z` section to `CHANGELOG.md`; `check_versions.py` fails without it. Updates are offered only when the version string changes, so never cut a release for CI-only changes.
- Release (maintainer only): after the PR is merged, run the **Release** workflow from `main` with tag `vX.Y.Z`, or `vX.Y.ZbN` for a beta (`docs/releasing.md`).
- Users install a GitHub release through HACS or the Marketplace. Never copy files into `/config/custom_components` by hand.
- A defect found after release is fixed forward with a new patch version; existing releases are never replaced.

# CLAUDE.md: ha-meridian-energy

Public HACS custom integration (`custom_components/meridian_energy`, domain `meridian_energy`) for Meridian Energy (New Zealand). It is a custom repository in HACS and is deliberately not listed in the HACS default store. This repository is public: everything committed here, and every PR or issue text, is world-readable.

## Rules

- **Synthetic data only.** Fixtures, tests, docs, commits, PR text and logs never contain real emails, account numbers, ICPs, meter IDs, addresses, tokens, usage or cost from any real installation. Use values such as `A-TEST-1`, `synthetic-account`, `person@example.com`.
- **No secrets.** CI runs a secret scan on every pull request; you can run the same locally with `gitleaks git --log-opts="origin/main..HEAD"` and `gitleaks dir .`. The one expected finding is `FIREBASE_API_KEY` in `const.py`, the Meridian web app's public client key (see SECURITY.md).
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
- Recorder writes and deletes must go through the public Recorder API on its own thread (`get_instance(hass).async_clear_statistics`, `async_add_external_statistics`). Never call `recorder.statistics.*` write helpers from an executor.
- Setup only renews the session and caches the topology (`MeridianDataCoordinator.async_prepare_topology`). The first sync runs as an entry background task after Home Assistant has started; entities are unavailable until then.
- The current-bill sensors have no state class, so Home Assistant keeps no long-term statistics for them; the external statistics are the long-term record.
- Config entries are version 3. A minor-version bump must stay loadable by every earlier 3.x release, because the rollback is to re-release earlier code: `async_migrate_entry` accepts a higher minor version unchanged. Anything that is not backward compatible needs a major-version bump.
- Diagnostics and logs must never contain identifiers, payloads or headers.

## Branches, pull requests and releases

- Branches: `<type>/<slug>` (`fix/`, `feat/`, `chore/`, `docs/`, `release/`). `main` is squash-only with required checks `python`, `hassfest`, `hacs`, `dependency-review`, `Analyze Python`, `secrets` and `private-terms`.
- A shippable change bumps `version` in `manifest.json` and `pyproject.toml` together and adds a `## X.Y.Z` section to `CHANGELOG.md`; `check_versions.py` fails without it. HACS only offers an update when the version string changes, so never cut a release for CI-only changes.
- Release (maintainer only): after the PR is merged, run the **Release** workflow from `main` with tag `vX.Y.Z`. The workflow re-runs the gates, creates the immutable tag and publishes the release with that version's CHANGELOG section as its notes. Never create or move tags by hand.
- Users install a GitHub release through HACS. Never copy files into `/config/custom_components` by hand.
- A defect found after release is fixed forward with a new patch version; existing releases are never replaced.

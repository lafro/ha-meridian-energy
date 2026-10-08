# CLAUDE.md: ha-meridian-energy

Public HACS custom integration (`custom_components/meridian_energy`, domain `meridian_energy`) for Meridian Energy (New Zealand). It is a custom repository in HACS and is deliberately not listed in the HACS default store.

## Rules

- **Synthetic data only.** Fixtures, tests, docs, commits, PR text and logs never contain real emails, account numbers, ICPs, meter IDs, addresses, tokens, usage or cost. Use values such as `A-TEST-1`, `synthetic-account`, `person@example.com`.
- **Secret scan before every push**: `gitleaks git --log-opts="origin/main..HEAD"` and `gitleaks dir .` (or a grep for the same patterns if gitleaks is missing). The one expected finding is `FIREBASE_API_KEY` in `const.py`, the Meridian web app's public client key (see SECURITY.md); anything else blocks the push.
- **No Home Assistant access from this repo.** Sessions here never call Home Assistant tools; `.claude/settings.json` denies them. Live checks, HACS downloads and restarts happen in an `ha-home` session.
- **Never delete** tags, releases or the repository, and never force-push. Tags and releases are immutable by ruleset; only Dan deletes anything.
- **Dan merges.** Open a pull request, wait for CI (`gh pr checks --watch`), and stop.
- Never use Linear or the Rectangle website systems from this repo.

## Development

```bash
uv sync --locked --all-groups        # never edit uv.lock by hand
uv run ruff format --check .
uv run ruff check .                  # ruff "ALL" with the ignores in pyproject.toml
uv run mypy custom_components/meridian_energy   # strict
uv run pytest                        # branch coverage gate 98% overall
uv run python scripts/check_module_coverage.py  # 95% per module
uv run python scripts/check_versions.py vX.Y.Z  # manifest = pyproject = tag
```

- Use the global uv cache (`~/.cache/uv`); never create `.uv-cache/` in the repo. If `.venv` is broken, delete it and run `uv sync --locked --all-groups`.
- `pytest-homeassistant-custom-component` is pinned exactly and pins Home Assistant itself, so bump that one pin to move the Home Assistant version under test. Do not add a separate `homeassistant` pin.
- `.github/workflows/compat.yml` runs weekly against the newest harness (stable and beta) unpinned and opens an issue when it fails.
- Tests that touch statistics must also pass on the real recorder (`recorder_mock`); see `tests/test_statistics_recorder.py`.
- Every behaviour change needs a test; keep `strings.json` and `translations/en.json` identical.

## Design notes

- Statistics are external (`meridian_energy:consumption_<key>` and friends); keys are truncated SHA-256 hashes, so IDs carry no identifiers.
- Recorder writes and deletes must go through the public Recorder API on its own thread (`get_instance(hass).async_clear_statistics`, `async_add_external_statistics`). Never call `recorder.statistics.*` write helpers from an executor.
- Setup only renews the session and caches the topology (`MeridianDataCoordinator.async_prepare_topology`). The first sync runs as an entry background task after Home Assistant has started; entities are unavailable until then.
- Diagnostics and logs must never contain identifiers, payloads or headers.

## Branches, pull requests and releases

- Branches: `<type>/<slug>` (`fix/`, `feat/`, `chore/`, `docs/`, `release/`). `main` is squash-only with required checks `python`, `hassfest`, `hacs`, `dependency-review` and `Analyze Python`.
- A shippable change bumps `version` in `manifest.json` and `pyproject.toml` together and adds a `CHANGELOG.md` entry. HACS only offers an update when the version string changes, so never cut a release for CI-only changes.
- Release (Dan): after the PR is merged, run the **Release** workflow from `main` with tag `vX.Y.Z` (GitHub Mobile can dispatch it). The workflow re-runs the gates, creates the immutable tag and publishes the release with generated notes. Never create or move tags by hand.
- Deploy: the GitHub release, then a HACS download of that version and a Home Assistant restart, done from an `ha-home` session. Never copy files into `/config/custom_components` by hand.
- A defect found after release is fixed forward with a new patch version; existing releases are never replaced.

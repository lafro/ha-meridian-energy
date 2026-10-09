# CLAUDE.md: ha-meridian-energy

Public HACS custom integration (`custom_components/meridian_energy`, domain `meridian_energy`) for Meridian Energy (New Zealand). It is a custom repository in HACS and is deliberately not listed in the HACS default store. This repository is public: everything committed here, and every PR or issue text, is world-readable.

## Rules

- **Synthetic data only.** Fixtures, tests, docs, commits, PR text and logs never contain real emails, account numbers, ICPs, meter IDs, addresses, tokens, usage or cost, nor details of the maintainer's own installation. Use values such as `A-TEST-1`, `synthetic-account`, `person@example.com`.
- **Secret scan before every push**: `gitleaks git --log-opts="origin/main..HEAD"` and `gitleaks dir .` (or a grep for the same patterns if gitleaks is missing). The one expected finding is `FIREBASE_API_KEY` in `const.py`, the Meridian web app's public client key (see SECURITY.md); anything else blocks the push.
- **No Home Assistant access from this repo.** Sessions here never call Home Assistant tools. Live checks, HACS downloads and restarts happen in sessions on the maintainer's private home-configuration repository.
- **GitHub only.** Use no connector other than GitHub from this repo (no issue trackers, email, chat, calendars, website or marketing tools).
- **Never delete** tags, releases or the repository, and never force-push. Tags and releases are immutable by ruleset; only the maintainer deletes anything.
- **The maintainer merges and releases.** Open a pull request, wait for CI (`gh pr checks --watch`), and stop. Never create or push tags, publish releases or dispatch the Release workflow.

## How the rules are enforced

`.claude/settings.json` has three layers; none of them is relied on alone.

1. **PreToolUse hooks** (the main layer):
   - any tool whose name matches `mcp__.*__ha_.*` is refused with exit 2, whatever the connector's server name;
   - every Bash call goes through `.claude/hooks/guard.sh` → `guard.py`, which blocks pushes to `main`, tag pushes and tag writes, force pushes and remote-ref deletion, merges, release writes and Release workflow dispatches, including `gh api`/curl spellings and `git -C`, `sh -c`, `$(...)` forms. The cases are in `tests/test_agent_guard.py`; extend them with every rule change.
2. **Deny rules** for the same commands as written, and for the Home Assistant connector by its cloud tool prefix (`mcp__claude_ai_Home_Assistant`). Deny rules match a tool or command only as spelled, and connector tool prefixes differ by surface: in a desktop session the same connector's tools carry a per-installation ID prefix, which the cloud prefix does not match. Keep such IDs out of this public repo; a per-machine deny belongs in the gitignored `.claude/settings.local.json` or in user settings.
3. **`deniedMcpServers`** by connector display name. Whether repository-scope entries reach a session's connectors has not been verified.

The hooks match on tool names, so they work on any surface, but the guard has only been tested by feeding it events (`tests/test_agent_guard.py`), not yet in a live session. Treat this file's rules as binding either way.

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
- The harness also pins `pytest`, `pytest-asyncio`, `pytest-cov` and `syrupy` exactly, so Dependabot ignores them (an update to one alone cannot resolve and fails the whole grouped update); they move with the harness. `test_dependabot_leaves_the_harness_pins_to_the_harness` keeps the ignore list in step with the installed harness.
- `.github/workflows/compat.yml` runs weekly against the newest harness (stable and beta) unpinned and opens an issue when it fails. GitHub disables scheduled workflows in a public repository after 60 days without activity; check with `gh run list --workflow compat.yml` and re-enable with `gh workflow enable compat.yml` (and `validate.yml`).
- Tests that touch statistics must also pass on the real recorder (`recorder_mock`); see `tests/test_statistics_recorder.py`.
- Snapshot tests (syrupy) cover entity states and diagnostics: `tests/snapshots/`. After an intended change, run `uv run pytest --snapshot-update`, then review the `.ambr` diff before committing it.
- Every behaviour change needs a test; keep `strings.json` and `translations/en.json` identical.
- `.claude/hooks/guard.py` must stay runnable on Python 3.9 with the standard library only (ruff checks it with a py39 target; a test parses it with the 3.9 grammar).

## Design notes

Fuller, public notes are in [`docs/`](docs/README.md).

- Statistics are external (`meridian_energy:consumption_<key>` and friends); keys are truncated SHA-256 hashes, so IDs carry no identifiers.
- Recorder writes and deletes must go through the public Recorder API on its own thread (`get_instance(hass).async_clear_statistics`, `async_add_external_statistics`). Never call `recorder.statistics.*` write helpers from an executor.
- Setup only renews the session and caches the topology (`MeridianDataCoordinator.async_prepare_topology`). The first sync runs as an entry background task after Home Assistant has started; entities are unavailable until then.
- The current-bill sensors have no state class, so Home Assistant keeps no long-term statistics for them; the external statistics are the long-term record.
- Config entries are version 3. A minor-version bump must stay loadable by every earlier 3.x release, because the rollback is to re-release earlier code: `async_migrate_entry` accepts a higher minor version unchanged. Anything that is not backward compatible needs a major-version bump.
- Diagnostics and logs must never contain identifiers, payloads or headers.

## Branches, pull requests and releases

- Branches: `<type>/<slug>` (`fix/`, `feat/`, `chore/`, `docs/`, `release/`). `main` is squash-only with required checks `python`, `hassfest`, `hacs`, `dependency-review` and `Analyze Python`.
- A shippable change bumps `version` in `manifest.json` and `pyproject.toml` together and adds a `## X.Y.Z` section to `CHANGELOG.md`; `check_versions.py` fails without it. HACS only offers an update when the version string changes, so never cut a release for CI-only changes.
- Release (maintainer only): after the PR is merged, run the **Release** workflow from `main` with tag `vX.Y.Z` (GitHub Mobile can dispatch it). The workflow re-runs the gates, creates the immutable tag and publishes the release with that version's CHANGELOG section as its notes. Never create or move tags by hand.
- Deploy: the GitHub release, then a HACS download of that version and a Home Assistant restart, done from the home-configuration repository's sessions. Never copy files into `/config/custom_components` by hand.
- A defect found after release is fixed forward with a new patch version; existing releases are never replaced.

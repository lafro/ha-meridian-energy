# Releasing

## Versions and the CHANGELOG

- The version lives in `custom_components/meridian_energy/manifest.json` and `pyproject.toml`, and they must match.
- Every version has a `## X.Y.Z` section in `CHANGELOG.md`. `scripts/check_versions.py` fails without it, on every pull request and in the Release workflow.
- HACS and the Marketplace offer an update only when the version string changes, so CI-only and documentation-only changes are merged without a release.

## Cutting a release (maintainer)

1. Run the [journeys](../CONTRIBUTING.md#journeys) on the pull request's branch; every journey must pass.
2. Merge the pull request that bumps the version and adds the CHANGELOG section. `main` is squash-only and requires `python`, `hassfest`, `hacs`, `dependency-review`, `Analyze Python`, `secrets` and `private-terms`.
3. Run the **Release** workflow from `main` with the tag `vX.Y.Z` (or `vX.Y.ZbN` for a beta). After the gates pass, publishing waits in the `release` environment for the maintainer's approval (the run's **Review deployments** button).
4. The workflow re-runs every gate without a cache, checks the tag against the version and the CHANGELOG, tags the commit it tested, and publishes the release, with the version's CHANGELOG section as its notes.
5. Install the published release and confirm it loads and completes a sync.

Tags and releases are immutable by repository ruleset. A tag is never created, moved or deleted by hand; if the workflow fails after creating the tag, re-run it on the same commit.

## Beta pre-releases (maintainer)

1. In the pull request, set the version to `X.Y.ZbN` in `manifest.json` and `pyproject.toml`, and run `uv lock`. The CHANGELOG keeps the `## X.Y.Z` section of the release it previews; `check_versions.py` reads a beta's notes from it.
2. Merge, then run the **Release** workflow with `vX.Y.ZbN`. It publishes a GitHub pre-release, not marked latest, offered only to installations that opted in to pre-releases.
3. A beta that changes sign-in, setup or the first import is also tried as a new installation, with the [live test](../CONTRIBUTING.md#live-test) (`--ref vX.Y.ZbN`).
4. Once the beta is verified on a real installation, a pull request sets the version to `X.Y.Z` with no code change, and the stable release follows. A defect found in the beta is fixed in the pull request for `X.Y.ZbN+1`.

## After release

Users update from **Settings → Updates** and restart Home Assistant. A problem found after release is fixed forward with a new patch version. To return to earlier code, install the previous release through the Marketplace or HACS, or release the earlier code again as a new patch version. Config-entry minor versions stay backward compatible, so either route loads existing entries (see [Config entries](architecture.md#config-entries)). Releases before 0.3.0, its first two betas included, can't load a version-4 entry, so going back across 0.3.0 means removing the integration and adding it again.

## Compatibility runs

- `validate.yml` tests the pinned harness on every pull request, on `main` and weekly.
- `compat.yml` plans every 6 hours (`scripts/compat_plan.py`) and tests when a target changed: `main` and the newest release on the newest harness (`stable`, and `beta` while one pins a beta), and `main` on the minimum in `hacs.json` (`minimum`). Each target runs its own tests, unpinned, without the coverage gates or the contract tests. `scripts/compat_pytest_plugin.py` sorts the outcome: `passed`, `snapshot-differences` (not a break) or `broken` (retried once). Each target uploads `compat-result-<target>-<channel>`, whose schema is in the docstring of `scripts/compat_results.py`.
- The report keeps an issue labelled `compat` open while any target is broken, and one labelled `deprecation` while any reports a deprecation.
- A break on a beta is fixed before that Home Assistant version ships; a break in the newest release is fixed in a patch release.
- `tests/ha_deprecations.py` fails any run in which Home Assistant reports a deprecation for this integration.

### Versions under test

- Pull requests test the harness pinned in `pyproject.toml`. Move it by bumping that one exact pin and running `uv lock`; it must select a final Home Assistant release no older than the minimum.
- The minimum in `hacs.json` rises only when the code needs something newer, in the pull request that needs it.
- GitHub disables scheduled workflows in a public repository after 60 days without activity. Check with `gh run list --workflow compat.yml`, and re-enable with `gh workflow enable compat.yml` (and `validate.yml`).

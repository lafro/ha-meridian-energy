# Releasing

## Versions and the CHANGELOG

- The version lives in `custom_components/meridian_energy/manifest.json` and `pyproject.toml`, and they must match.
- Every version has a `## X.Y.Z` section in `CHANGELOG.md`. `scripts/check_versions.py` fails without it, on every pull request and in the Release workflow.
- HACS offers an update only when the version string changes, so CI-only or documentation-only changes are merged without a release.

## Cutting a release (maintainer)

1. Merge the pull request that bumps the version and adds the CHANGELOG section. `main` is squash-only and requires `python`, `hassfest`, `hacs`, `dependency-review`, `Analyze Python`, `secrets` and `private-terms`.
2. Run the **Release** workflow from `main` with the tag `vX.Y.Z` (or `vX.Y.ZbN` for a beta, below). After the gates pass, the publishing job waits in the `release` environment until the maintainer approves it (the run's **Review deployments** button).
3. The workflow re-runs every gate without a dependency cache, checks the tag against the committed version and the CHANGELOG, creates the tag on the exact commit it tested, and publishes the release. The release notes are the version's CHANGELOG section, followed by GitHub's generated list of pull requests.

## Beta pre-releases (maintainer)

A change that is risky to verify only with fakes ships first as a beta:

1. In the pull request, set the version to `X.Y.ZbN` (for example `0.3.0b1`) in `manifest.json` and `pyproject.toml`, and run `uv lock`. The CHANGELOG keeps the `## X.Y.Z` section of the release it previews; `check_versions.py` reads a beta's notes from it.
2. Merge, then run the **Release** workflow with the tag `vX.Y.ZbN`. It publishes a GitHub pre-release, not marked latest. HACS offers it only to installations that turn on beta versions for this repository, or that download that version by name.
3. Once the beta is verified on a real installation, a pull request sets the version to `X.Y.Z` (no code change), and the stable release follows as above. A defect found in the beta is fixed in the pull request for `X.Y.ZbN+1`.

Tags and releases are immutable by repository ruleset. A tag is never created, moved or deleted by hand; if the workflow fails after creating the tag, re-run it on the same commit.

## After release

Users update through HACS and restart Home Assistant. A problem found after release is fixed forward with a new patch version. To return to earlier code, either install the previous release from HACS, or release the earlier code again as a new patch version. Config-entry minor versions stay backward compatible so either route loads existing entries (see [architecture](architecture.md#config-entries)). Running totals stay consistent either way, but a history that 0.3.0 or later is still completing stays incomplete under earlier code, which asks only for the hours after the newest one stored and for the last 14 days: let it fill in first (diagnostics show `reaches_target` for each grid import and export).

## Compatibility runs

- `validate.yml` tests the pinned harness on every pull request, on `main` and weekly.
- `compat.yml` tests the newest `pytest-homeassistant-custom-component` weekly, unpinned, in a `stable` channel and a `beta` channel, and opens or updates a tracking issue when a scheduled run fails. The stable channel skips itself while the newest harness is older than the minimum Home Assistant version in `hacs.json`.
- To move the tested Home Assistant version, bump the exact `pytest-homeassistant-custom-component` pin in `pyproject.toml` and run `uv lock`.
- GitHub disables scheduled workflows in a public repository after 60 days without activity. Check with `gh run list --workflow compat.yml` and `gh run list --workflow validate.yml`, and re-enable with `gh workflow enable <file>`.

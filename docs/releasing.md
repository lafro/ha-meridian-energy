# Releasing

## Versions and the CHANGELOG

- The version lives in `custom_components/meridian_energy/manifest.json` and `pyproject.toml`, and they must match.
- Every version has a `## X.Y.Z` section in `CHANGELOG.md`. `scripts/check_versions.py` fails without it, on every pull request and in the Release workflow.
- HACS offers an update only when the version string changes, so CI-only or documentation-only changes are merged without a release.

## Cutting a release (maintainer)

1. Run the [journeys](../CONTRIBUTING.md#journeys) on the pull request's branch, for a beta and a stable release alike; all six must pass.
2. Merge the pull request that bumps the version and adds the CHANGELOG section. `main` is squash-only and requires `python`, `hassfest`, `hacs`, `dependency-review`, `Analyze Python`, `secrets` and `private-terms`.
3. Run the **Release** workflow from `main` with the tag `vX.Y.Z` (or `vX.Y.ZbN` for a beta, below). After the gates pass, the publishing job waits in the `release` environment until the maintainer approves it (the run's **Review deployments** button).
4. The workflow re-runs every gate without a dependency cache, checks the tag against the committed version and the CHANGELOG, creates the tag on the exact commit it tested, and publishes the release. The release notes are the version's CHANGELOG section, followed by GitHub's generated list of pull requests.

## Beta pre-releases (maintainer)

A change that is risky to verify only with fakes ships first as a beta:

1. In the pull request, set the version to `X.Y.ZbN` (for example `0.3.0b1`) in `manifest.json` and `pyproject.toml`, and run `uv lock`. The CHANGELOG keeps the `## X.Y.Z` section of the release it previews; `check_versions.py` reads a beta's notes from it.
2. Merge, then run the **Release** workflow with the tag `vX.Y.ZbN`. It publishes a GitHub pre-release, not marked latest. HACS offers it only to installations that turn on beta versions for this repository, or that download that version by name.
3. A beta that changes sign-in, setup or the first import is also tried as a new installation, with the live test in [CONTRIBUTING](../CONTRIBUTING.md#live-test) (`--ref vX.Y.ZbN`).
4. Once the beta is verified on a real installation, a pull request sets the version to `X.Y.Z` (no code change), and the stable release follows as above. A defect found in the beta is fixed in the pull request for `X.Y.ZbN+1`.

Tags and releases are immutable by repository ruleset. A tag is never created, moved or deleted by hand; if the workflow fails after creating the tag, re-run it on the same commit.

## After release

Users update through HACS and restart Home Assistant. A problem found after release is fixed forward with a new patch version. To return to earlier code, either install the previous release from HACS, or release the earlier code again as a new patch version. Config-entry minor versions stay backward compatible so either route loads existing entries (see [architecture](architecture.md#config-entries)). Running totals stay consistent either way, but a history that 0.3.0 or later is still completing stays incomplete under earlier code, which asks only for the hours after the newest one stored and for the last 14 days: let it fill in first (diagnostics show `reaches_target` for each grid import and export).

## Compatibility runs

- `validate.yml` tests the pinned harness on every pull request, on `main` and weekly. Every run fails on a deprecation Home Assistant reports for this integration (see Deprecations below).
- `compat.yml` tests each new Home Assistant version, beta, release and patch alike, within hours of its harness (`pytest-homeassistant-custom-component` publishes one a day, 12 to 18 hours after each Home Assistant version). Every 6 hours `scripts/compat_plan.py` works out the targets:
  - `main` and the newest published release (what HACS installs), each on the newest harness for a final Home Assistant release (`stable`) and, while the newest harness pins a beta, on that one too (`beta`);
  - `main` on exactly the minimum Home Assistant in `hacs.json` (`minimum`), with the harness uv matches to it.

  A scheduled run tests only when the targets changed since a run that tested them: it leaves the artifact `compat-done-<fingerprint>` (kept 30 days), and the next scheduled run with the same targets stops after the plan. A manual run, or a pull request that changes the workflow or `scripts/compat_*.py`, always tests.
- Each target runs its own test suite on its harness, unpinned from `uv.lock`, without the coverage gates and without `tests/test_repository_contracts.py`, which checks the pinned harness on purpose. `scripts/compat_pytest_plugin.py` sorts the failures:
  - `passed`;
  - `snapshot-differences`: only syrupy snapshots differ, as when Home Assistant adds a field to what a snapshot stores. Not a break; the snapshots move with the harness pin (`uv run pytest --snapshot-update`, then review the `.ambr` diff);
  - `broken`: any other failure, an error in setup or collection, or no record at all. A broken target runs its failed tests once more, and only the second outcome counts.
- Each target uploads `compat-result-<target>-<channel>` (kept 90 days) holding `result.json`, schema 1: `integration`, `integration_version`, `target` (`main` or `release`), `tag`, `ref`, `channel` (`stable`, `beta` or `minimum`), `homeassistant`, `harness`, `python`, `outcome`, `retried`, `broken_tests`, `snapshot_tests` (at most 20 each), `deprecations` (a list, or `null` for a release older than the detector) and `run_url`.
- The report keeps one issue labelled `compat` open while any target is broken and one labelled `deprecation` while any target reports a deprecation. It comments only when what the issue lists changes, and closes the issue when a run finds it clear. Pull requests never touch the issues.
- Targets: a break on a beta is fixed and released before that Home Assistant version ships; a break in the newest release on a final Home Assistant release is fixed in a patch release.

### Deprecations

`tests/ha_deprecations.py`, a pytest plugin loaded by `tests/conftest.py`, collects what Home Assistant reports for this integration: its "Detected that custom integration 'meridian_energy' …" and "The deprecated … from meridian_energy." log messages, and the `DeprecationWarning`, `PendingDeprecationWarning` and `FutureWarning` raised from `custom_components/meridian_energy/`. Any of them fails a run that would otherwise pass and is listed under "Home Assistant deprecations" in the summary, so a harness bump that brings one in cannot be merged until the use is fixed. The fix is integration code and ships as a normal patch release, well before the version Home Assistant names. In `compat.yml` the plugin writes the list to the result instead (`HA_DEPRECATIONS_REPORT`). A test that triggers a report on purpose wraps it in `COLLECTOR.expected()`.

### Versions under test

- Pull requests test the harness pinned in `pyproject.toml`. To move it, bump that one exact pin and run `uv lock`; the pin must select a final Home Assistant release no older than the minimum (`test_pull_requests_are_tested_on_a_supported_release`).
- Dependabot's uv updates run once a month, on the 12th. The harness pins the newest Home Assistant, betas included, and from about the 23rd until the day after the release (by the 8th) that is a beta; by the 12th it is the month's release. `compat.yml` tests the patches between. Any change to `.github/dependabot.yml` makes Dependabot run at once: merged during a beta week, that opens a pull request pinned to a beta, which fails its checks and is closed.
- The minimum in `hacs.json` is raised only when the code needs something newer, in the pull request that needs it, never just because a new Home Assistant came out. The `minimum` channel keeps it true: when it breaks, the fix raises the minimum. The one other reason to raise it is a Python version: the `minimum` channel runs on the oldest Python that version of Home Assistant supports, and the minimum moves up when Home Assistant moves to a Python that version cannot run on.
- Security alerts for packages only the harness brings in (Home Assistant's own pins, such as Pillow) need no action: HACS does not install them and the integration does not use them. They clear when a harness bump reaches a Home Assistant that moves on.
- GitHub disables scheduled workflows in a public repository after 60 days without activity. Check with `gh run list --workflow compat.yml` and `gh run list --workflow validate.yml`, and re-enable with `gh workflow enable <file>`.

# Contributing

## Requirements

- Python 3.14.2 or newer
- `uv`
- `lsof`, for the live test's checks (macOS and most Linux systems have it)

Never use production credentials in automated tests or commit captured Meridian responses. Fixtures must be synthetic and must not contain real account numbers, ICPs, addresses, tokens or usage.

## Setup

```bash
uv sync --locked --all-groups
uv run pytest
uv run python scripts/check_module_coverage.py
uv run ruff check .
uv run ruff format --check .
uv run mypy custom_components/meridian_energy
uv run python scripts/check_versions.py
```

`uv sync --locked` fails when `uv.lock` is out of date; update it with `uv lock` (or `uv lock --upgrade-package <name>`) rather than editing it. The exact `pytest-homeassistant-custom-component` pin selects the Home Assistant version under test.

Run Hassfest and HACS validation through the GitHub Actions workflow before releasing. The weekly **Compatibility** workflow tests against the newest Home Assistant release and beta.

## Live test

The tests use a fake Meridian. To try the integration against the real service, run a disposable Home Assistant:

```bash
uv run python scripts/live_ha.py start                 # the working tree
uv run python scripts/live_ha.py start --ref v0.3.0b1  # or a release tag or commit
uv run python scripts/live_ha.py stop                  # remove it all
```

`start` builds a new Home Assistant (the newest release, or `--ha-version`) in a folder of its own in the system's temporary directory, completes onboarding, and checks before it reports ready that the instance listens on 127.0.0.1 only and loads nothing that discovers or announces devices (no `default_config`). Open the printed link, add the integration and sign in with your own MyMeridian email: the browser on the same computer is signed in to the instance without a password, as is any other program on it (and, in principle, a web page open in that browser), so run `stop` as soon as you are done. The integration logs at debug level to the instance's log. `stop` removes the integration's entries, stops Home Assistant and deletes the folder with its database, log and stored session. Never commit anything from a live test.

## Pull requests

- Add tests for every behavior change.
- Preserve config-entry migration and reauthentication paths.
- Treat API responses as untrusted input.
- Never log request payloads, response payloads, headers or identifiers.
- Update the README for user-visible changes.
- Snapshot tests (`tests/snapshots/`) cover entity states and diagnostics. After an intended change, run `uv run pytest --snapshot-update` and review the `.ambr` diff.

Pull requests must pass the Python, Hassfest, HACS, dependency-review and CodeQL checks, and the content checks: a secret scan, and a check against a private list of terms that must never be published (pull requests from forks and Dependabot skip it, because they get no repository secrets). The repository uses squash merges and automatically deletes merged branches.

Developer notes are in [`docs/`](docs/README.md).

## Release checklist

1. Confirm `manifest.json`, `pyproject.toml` and the proposed `v…` tag contain the same version, and `CHANGELOG.md` has a `## X.Y.Z` section for it (`check_versions.py` enforces this). That section becomes the release notes.
2. Run the full local commands above and review the branch-coverage report.
3. Merge only through a protected pull request with all conversations resolved.
4. Run the protected **Release** workflow from `main`; do not create or move release tags manually.
5. Install the published release through HACS on a Home Assistant instance and confirm that it loads and completes a sync.
6. If that finds a material defect, fix it through a new pull request and publish a new patch version; never replace an existing release in place.

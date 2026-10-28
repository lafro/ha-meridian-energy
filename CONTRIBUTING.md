# Contributing

## Requirements

- Python 3.14.2 or newer
- `uv`
- `lsof`, for the live test's and the journeys' checks (macOS and most Linux systems have it)

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

## Journeys

The journeys run the integration end to end in a real Home Assistant against a stand-in Meridian, with no real sign-in:

```bash
uv run python scripts/journeys.py                      # the working tree, all six
uv run python scripts/journeys.py --ref v0.3.0b2       # or a release tag or commit
uv run python scripts/journeys.py --only retry reauth  # some of them
uv run python scripts/journeys.py --clean-up           # delete a left-over instance
```

It builds a disposable Home Assistant as the live test does (the newest release, or `--ha-version`), but in a folder of its own (`journeys-meridian_energy`) on port 8125, so the two never meet. A run does not start while that folder exists, as another run may be using it; `--clean-up` deletes one a killed run left behind. The stand-in (`tests/meridian_standin.py`, with the fakes the tests use) goes into that instance's Python through a `.pth` file. It answers the integration's Meridian requests from the scenario the journey sets, with synthetic data that depends only on the hour. In Home Assistant's process it refuses and records every other aiohttp request, TCP connection and name lookup that would leave the computer; Home Assistant does not start without it. It does not cover UDP or reverse lookups, nor other programs: building the instance, and Home Assistant's first start, download Home Assistant and the requirements it pins from PyPI (through uv), as for the live test.

| Journey | What it does |
| --- | --- |
| `first_setup` | Email, code, import, entry; the first update asks only for the tip; 90 days of hours |
| `two_accounts` | A login with two accounts: choose one, add the other later by reconfiguring |
| `retry` | The import fails (Meridian answers 503), nothing is kept; a retry after the wait works |
| `without_history` | Meridian times out; **Continue without history**; the history fills in afterwards |
| `reauth` | Meridian ends the sign-in; the next update asks to reconnect; email code; history unbroken |
| `restart` | Reload the entry and restart Home Assistant; each update asks again for the last 14 days at least; history unbroken |

Every journey also checks that its entry is loaded and its sensors have states; that each statistic has every hour, none zero, each the value the stand-in sent and each sum step equal to its hour; that every screen it met has its words (no raw keys); and that the stand-in refused nothing; the run checks that once more after Home Assistant has stopped. The run prints PASS or FAIL for each check and journey, writes a report with every check and every screen as the dialog shows it to the system's temporary directory, deletes the instance whatever happens, and exits non-zero if anything failed. It takes several minutes: two journeys wait out the integration's 120-second retry delay.

Run the journeys before every beta and stable release, and in every pull request that changes sign-in, setup or the import; say in the pull request that they passed. They are not part of CI.

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
2. Run the full local commands above and the [journeys](#journeys), and review the branch-coverage report.
3. Merge only through a protected pull request with all conversations resolved.
4. Run the protected **Release** workflow from `main`; do not create or move release tags manually.
5. Install the published release through HACS on a Home Assistant instance and confirm that it loads and completes a sync.
6. If that finds a material defect, fix it through a new pull request and publish a new patch version; never replace an existing release in place.

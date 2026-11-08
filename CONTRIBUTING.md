# Contributing

## Requirements

- Python 3.14.2 or newer, below 3.15
- `uv`
- `lsof`, for the live test and the journeys

Never use real credentials in tests or commit captured Meridian answers. Fixtures are synthetic: no real emails, account numbers, ICPs, addresses, tokens, usage or cost.

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

`uv sync --locked` fails on a stale `uv.lock`; update it with `uv lock`, never by hand. The exact `pytest-homeassistant-custom-component` pin selects the Home Assistant version under test; the **Compatibility** workflow tests new releases and betas ([Compatibility runs](docs/releasing.md#compatibility-runs)).

## Live test

To try the real Meridian, run a disposable Home Assistant:

```bash
uv run python scripts/live_ha.py start                 # the working tree
uv run python scripts/live_ha.py start --ref v0.3.0b1  # or a release tag or commit
uv run python scripts/live_ha.py stop                  # remove it all
```

`start` builds and onboards a new Home Assistant (the newest release, or `--ha-version`) in its own temporary folder. Before reporting ready, it checks that the instance listens on 127.0.0.1 only, loads nothing that discovers or announces devices, and has the Marketplace turned off. Open the printed link and add the integration with your own MyMeridian email; it logs at debug level. Every program on that computer, its browser included (and, in principle, a web page open in it), is signed in to the instance without a password, so run `stop` as soon as you are done: it removes the entries, stops Home Assistant and deletes the folder with its database, log and stored session. Never commit anything from a live test.

## Journeys

The journeys run the integration end to end in a real Home Assistant against a stand-in Meridian:

```bash
uv run python scripts/journeys.py                        # the working tree, all of them
uv run python scripts/journeys.py --ref v0.3.0b2         # or a release tag or commit
uv run python scripts/journeys.py --only reauth restart  # some of them
uv run python scripts/journeys.py --clean-up             # delete a left-over instance
```

A run builds its own disposable Home Assistant, as the live test does, on port 8125. The stand-in (`tests/meridian_standin.py`) answers Meridian requests with synthetic data and refuses every other aiohttp request, TCP connection and forward name lookup from Home Assistant that would leave the computer, but not UDP or other programs; building the instance and its first start download from PyPI.

| Journey | What it does |
| --- | --- |
| `first_setup` | A feed-in register and an account in credit: the entry at once; the first sync asks 13 windows per direction back to midnight 90 dates ago, completing both histories; every figure has a value |
| `figures` | Billing period, day totals and prices fail at the first sync: the update works, the bill shows unknown and the log says so; reloads bring every figure; a restart with the prices failing leaves only them unknown |
| `outage` | Meridian times out from the start: the entry loads, sensors unavailable; a notice after 3 failed updates; once Meridian answers, one update imports 90 dates and the notice goes |
| `two_accounts` | Two accounts on one login, both followed |
| `reauth` | Meridian ends the sign-in; reconnecting with a new code keeps the history unbroken |
| `restart` | Reload, then restart: each first sync asks only for the last 21 dates; no hour changes |

Every journey also checks its entry and sensors; that each statistic has every hour, none zero, with the value sent and consistent sums; that every screen has its words; that the integration's log above debug level is exactly as expected, with no warning from Home Assistant's recorder, websocket API or sensors; and that the stand-in refused nothing. The run prints PASS or FAIL for each check, writes a report, deletes the instance and exits non-zero if anything failed.

Run the journeys before every release, betas included, and in every pull request that changes sign-in, setup or the import; say in it that they passed; CI doesn't run them.

## Pull requests

- Add tests for every behaviour change, and keep reauthentication working.
- Treat Meridian's answers as untrusted input, and never log payloads, headers or identifiers.
- Update the README for changes users see, in their terms; `tests/test_repository_contracts.py` holds the docs to their word limits.
- After an intended change to entity states or diagnostics, run `uv run pytest --snapshot-update` and review the `.ambr` diff.

Pull requests must pass the required checks, among them a secret scan and a check for private terms (skipped for forks and Dependabot, which get no repository secrets). Merges are squashed.

Developer notes and release steps are in [`docs/`](docs/README.md).

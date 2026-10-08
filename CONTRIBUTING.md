# Contributing

## Requirements

- Python 3.14.2 or newer
- `uv`
- A disposable Home Assistant development instance for manual testing

Never use production credentials in automated tests or commit captured Meridian responses. Fixtures must be synthetic and must not contain real account numbers, ICPs, addresses, tokens or usage.

## Setup

```bash
uv sync --locked --all-groups
uv run pytest
uv run python scripts/check_module_coverage.py
uv run ruff check .
uv run ruff format --check .
uv run mypy custom_components/meridian_energy
```

`uv sync --locked` fails when `uv.lock` is out of date; update it with `uv lock` (or `uv lock --upgrade-package <name>`) rather than editing it. The exact `pytest-homeassistant-custom-component` pin selects the Home Assistant version under test.

Run Hassfest and HACS validation through the GitHub Actions workflow before releasing. The weekly **Compatibility** workflow tests against the newest Home Assistant release and beta.

## Pull requests

- Add tests for every behavior change.
- Preserve config-entry migration and reauthentication paths.
- Treat API responses as untrusted input.
- Never log request payloads, response payloads, headers or identifiers.
- Update the README for user-visible changes.

Pull requests must pass Python, Hassfest, HACS, dependency-review and CodeQL checks. The repository uses squash merges and automatically deletes merged branches.

## Release checklist

1. Confirm `manifest.json`, `pyproject.toml` and the proposed `v…` tag contain the same version, and `CHANGELOG.md` has an entry for it.
2. Run the full local commands above and review the branch-coverage report.
3. Merge only through a protected pull request with all conversations resolved.
4. Run the protected **Release** workflow from `main`; do not create or move release tags manually.
5. Install the published release through HACS and complete the documented canary on that exact release.
6. If the canary finds a material defect, fix it through a new protected pull request and publish a new patch version; never replace an existing release in place.

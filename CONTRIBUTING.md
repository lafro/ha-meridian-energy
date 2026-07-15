# Contributing

## Requirements

- Python 3.14.2 or newer
- `uv`
- A disposable Home Assistant development instance for manual testing

Never use production credentials in automated tests or commit captured Meridian responses. Fixtures must be synthetic and must not contain real account numbers, ICPs, addresses, tokens or usage.

## Setup

```bash
uv sync --all-groups
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy custom_components/meridian_energy
```

Run Hassfest and HACS validation through the GitHub Actions workflow before releasing.

## Pull requests

- Add tests for every behavior change.
- Preserve config-entry migration and reauthentication paths.
- Treat API responses as untrusted input.
- Never log request payloads, response payloads, headers or identifiers.
- Update the README for user-visible changes.

Pull requests must pass Python, Hassfest, HACS, dependency-review and CodeQL checks. The repository uses squash merges and automatically deletes merged branches.

## Release checklist

1. Confirm `manifest.json`, `pyproject.toml` and the proposed `v…` tag contain the same version.
2. Run the full local commands above and review the branch-coverage report.
3. Merge only through a protected pull request with all conversations resolved.
4. Publish beta versions as GitHub prereleases; never replace a beta release with the stable release.
5. Install the prerelease through HACS and complete the documented canary before promoting it.

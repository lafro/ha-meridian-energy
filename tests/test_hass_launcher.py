"""Contracts for the launcher that runs the instance's Home Assistant."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from homeassistant.const import __version__

from scripts import hass_launcher

ROOT = Path(__file__).resolve().parents[1]

# Home Assistant's entry point, faked: it leaves output in the buffers, a log
# record in a buffering handler and an exit hook, then asks for a restart. The
# record goes to standard output through a stream of its own, so flushing the
# log handlers does not also flush sys.stdout.
FAKE_MAIN = """\
import atexit, logging, logging.handlers, sys
from scripts import hass_launcher

def main():
    target = logging.StreamHandler(open(1, "w", closefd=False))
    logging.getLogger().addHandler(logging.handlers.MemoryHandler(100, target=target))
    logging.getLogger().warning("logged")
    sys.stdout.write("printed")
    sys.stderr.write("written")
    atexit.register(print, "teardown")
    return 100

hass_launcher.run(main)
"""


def test_the_process_ends_with_its_code_and_output_but_no_teardown() -> None:
    result = subprocess.run(
        [sys.executable, "-c", FAKE_MAIN],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 100
    assert result.stdout == "logged\nprinted"
    assert result.stderr == "written"


@pytest.mark.parametrize(
    ("args", "code", "stdout", "stderr"),
    [
        (["--version"], 0, f"{__version__}\n", ""),
        (["--config", "missing"], 1, "", "does not exist"),
    ],
)
def test_it_runs_home_assistants_own_entry_point(
    tmp_path: Path, args: list[str], code: int, stdout: str, stderr: str
) -> None:
    result = subprocess.run(
        [sys.executable, "-P", ROOT / "scripts" / "hass_launcher.py", *args],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == code
    assert result.stdout == stdout
    assert stderr in result.stderr


def _raise(error: BaseException) -> int:
    raise error


@pytest.mark.parametrize(
    ("error", "code", "stderr"),
    [
        (SystemExit(None), 0, ""),
        (SystemExit(2), 2, ""),
        (SystemExit("Fatal Error"), 1, "Fatal Error\n"),
        (RuntimeError("synthetic"), 1, "RuntimeError: synthetic\n"),
    ],
)
def test_the_exit_code_is_the_one_the_interpreter_would_give(
    capsys: pytest.CaptureFixture[str], error: BaseException, code: int, stderr: str
) -> None:
    assert hass_launcher.exit_code(lambda: 100) == 100
    assert hass_launcher.exit_code(lambda: _raise(error)) == code
    err = capsys.readouterr().err
    assert err.endswith(stderr)
    assert bool(err) == bool(stderr)

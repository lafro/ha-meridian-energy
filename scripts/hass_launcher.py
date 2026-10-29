"""
Run Home Assistant, then end its process without the interpreter's teardown.

``live_ha.spawn`` starts the instance's Home Assistant through this file, in
the instance's own Python, instead of through Home Assistant's ``hass``
script. It runs the same entry point (``homeassistant.__main__.main``) with
the same arguments, and takes the same exit code: 0 once Home Assistant has
stopped, or 100 when it was asked to restart, which nothing here acts on, as
nothing acted on it before.

By the time that entry point returns, Home Assistant has shut down and saved
what it keeps. The process then flushes its log handlers and its output, and
ends with that code through ``os._exit``, so the interpreter's own teardown
never runs: on macOS, Python 3.14 has crashed in it after Home Assistant
stopped (a segmentation fault in the garbage collection that clears the
modules), and every such crash leaves a crash report.
"""

from __future__ import annotations

import logging
import os
import sys
import traceback
from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from collections.abc import Callable


def exit_code(main: Callable[[], int]) -> int:
    """Run ``main`` and return the exit code the interpreter would give it."""
    try:
        return main()
    except SystemExit as stop:
        code = stop.code
    except BaseException:  # noqa: BLE001 - the interpreter would print it too
        traceback.print_exc()
        return 1
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    print(code, file=sys.stderr)  # noqa: T201
    return 1


def run(main: Callable[[], int]) -> NoReturn:
    """Run ``main``, flush what it wrote, and end the process with its code."""
    code = exit_code(main)
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == "__main__":
    from homeassistant.__main__ import main

    run(main)

"""Hold Home Assistant's recorder thread, as a backlog of other work would."""

from __future__ import annotations

import threading

from homeassistant.components.recorder import Recorder, get_instance
from homeassistant.components.recorder.tasks import RecorderTask
from homeassistant.core import HomeAssistant


class _HoldTask(RecorderTask):
    """A recorder task that blocks the recorder thread until released."""

    commit_before = False

    def __init__(self, release: threading.Event) -> None:
        self._release = release

    def run(self, instance: Recorder) -> None:
        del instance
        self._release.wait(30)


class RecorderHold:
    """
    Hold the recorder thread, as a backlog of other recorder work would.

    Work queued after the hold, such as an import's rows or a barrier, waits
    until ``release``; reads on the recorder's executor do not.
    """

    def __init__(self, hass: HomeAssistant) -> None:
        self._instance = get_instance(hass)
        self._release = threading.Event()
        self._held = False

    def hold_once(self) -> None:
        """Queue the hold, once; work queued after it waits until release."""
        if not self._held:
            self._held = True
            self._instance.queue_task(_HoldTask(self._release))

    def release(self) -> None:
        self._release.set()

    def release_after(self, seconds: float) -> threading.Timer:
        """Release the hold from another thread after a while."""
        timer = threading.Timer(seconds, self.release)
        timer.start()
        return timer

"""Filesystem watcher for DataPoint changes."""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


class DataPointWatcher:
    """Watch DataPoint files and trigger sync with debounce."""

    def __init__(
        self,
        datapoints_dir: str | Path,
        on_change: Callable[[], None],
        debounce_seconds: float = 5.0,
    ) -> None:
        self._datapoints_dir = Path(datapoints_dir)
        self._on_change = on_change
        self._debounce_seconds = debounce_seconds
        self._observer: Observer | None = None
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        if self._observer is not None:
            return
        self._datapoints_dir.mkdir(parents=True, exist_ok=True)
        handler = _DataPointEventHandler(self._handle_event)
        observer = Observer()
        observer.schedule(handler, str(self._datapoints_dir), recursive=True)
        observer.start()
        self._observer = observer

    def stop(self) -> None:
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None

    def _handle_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        if not event.src_path.endswith(".json"):
            return
        self._schedule_sync()

    def _schedule_sync(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(self._debounce_seconds, self._on_change)
            self._timer.daemon = True
            self._timer.start()


class _DataPointEventHandler(FileSystemEventHandler):
    def __init__(self, callback: Callable[[FileSystemEvent], None]) -> None:
        self._callback = callback

    def on_created(self, event: FileSystemEvent) -> None:
        self._callback(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._callback(event)

    def on_deleted(self, event: FileSystemEvent) -> None:
        self._callback(event)

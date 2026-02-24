"""Unit tests for DataPointWatcher."""

from __future__ import annotations

import time
from pathlib import Path

from watchdog.events import FileSystemEvent

from backend.sync.watcher import DataPointWatcher


def _event(path: str) -> FileSystemEvent:
    event = FileSystemEvent(path)
    event.is_directory = False
    return event


def test_detects_file_creation(tmp_path: Path):
    calls = []

    watcher = DataPointWatcher(tmp_path, lambda: calls.append("sync"), debounce_seconds=0.01)
    watcher._handle_event(_event(str(tmp_path / "test.json")))

    time.sleep(0.05)
    assert len(calls) == 1


def test_detects_file_modification(tmp_path: Path):
    calls = []

    watcher = DataPointWatcher(tmp_path, lambda: calls.append("sync"), debounce_seconds=0.01)
    watcher._handle_event(_event(str(tmp_path / "test.json")))

    time.sleep(0.05)
    assert len(calls) == 1


def test_detects_file_deletion(tmp_path: Path):
    calls = []

    watcher = DataPointWatcher(tmp_path, lambda: calls.append("sync"), debounce_seconds=0.01)
    watcher._handle_event(_event(str(tmp_path / "test.json")))

    time.sleep(0.05)
    assert len(calls) == 1


def test_debounces_events(tmp_path: Path):
    calls = []

    watcher = DataPointWatcher(tmp_path, lambda: calls.append("sync"), debounce_seconds=0.05)
    watcher._handle_event(_event(str(tmp_path / "test.json")))
    watcher._handle_event(_event(str(tmp_path / "test.json")))

    time.sleep(0.1)
    assert len(calls) == 1

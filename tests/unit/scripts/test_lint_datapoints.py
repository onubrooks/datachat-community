"""Tests for scripts/lint_datapoints.py."""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
LINT_SCRIPT_PATH = ROOT / "scripts" / "lint_datapoints.py"
SPEC = importlib.util.spec_from_file_location("lint_datapoints_module", LINT_SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
LINT_SCRIPT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LINT_SCRIPT)

FIXTURES_DIR = ROOT / "tests" / "fixtures" / "datapoints"


def _prepare_fixture_dir(tmp_path: Path) -> Path:
    target = tmp_path / "datapoints"
    target.mkdir(parents=True, exist_ok=True)
    for file_name in (
        "metric_revenue_001.json",
        "table_fact_sales_001.json",
        "proc_daily_etl_001.json",
    ):
        shutil.copy(FIXTURES_DIR / file_name, target / file_name)
    return target


def test_lint_datapoints_passes_without_contract_errors(monkeypatch, tmp_path):
    target = _prepare_fixture_dir(tmp_path)
    monkeypatch.setattr(
        "sys.argv",
        ["lint_datapoints.py", "--path", str(target), "--recursive"],
    )
    assert LINT_SCRIPT.main() == 0


def test_lint_datapoints_can_fail_on_warnings(monkeypatch, tmp_path):
    target = _prepare_fixture_dir(tmp_path)
    monkeypatch.setattr(
        "sys.argv",
        [
            "lint_datapoints.py",
            "--path",
            str(target),
            "--recursive",
            "--fail-on-warnings",
        ],
    )
    assert LINT_SCRIPT.main() == 1


def test_lint_datapoints_strict_fails_for_advisory_gaps(monkeypatch, tmp_path):
    target = _prepare_fixture_dir(tmp_path)
    monkeypatch.setattr(
        "sys.argv",
        ["lint_datapoints.py", "--path", str(target), "--recursive", "--strict"],
    )
    assert LINT_SCRIPT.main() == 1

"""Validation tests for fintech sample DataPoints."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from backend.knowledge.datapoints import DataPointLoader

ROOT = Path(__file__).resolve().parents[3]
FINTECH_DATAPOINT_DIR = ROOT / "datapoints" / "examples" / "fintech_bank"


def _expected_datapoint_counts(datapoint_dir: Path) -> tuple[int, Counter[str]]:
    expected_by_type: Counter[str] = Counter()
    datapoint_files = sorted(datapoint_dir.glob("*.json"))
    for datapoint_file in datapoint_files:
        payload = json.loads(datapoint_file.read_text())
        expected_by_type[payload["type"]] += 1
    return len(datapoint_files), expected_by_type


def test_fintech_datapoints_load_successfully():
    loader = DataPointLoader()
    datapoints = loader.load_directory(FINTECH_DATAPOINT_DIR)
    stats = loader.get_stats()
    expected_count, expected_by_type = _expected_datapoint_counts(FINTECH_DATAPOINT_DIR)

    assert stats["failed_count"] == 0
    assert len(datapoints) == expected_count

    by_type = Counter(dp.type for dp in datapoints)
    assert by_type == expected_by_type


def test_fintech_datapoints_have_unique_ids():
    loader = DataPointLoader()
    datapoints = loader.load_directory(FINTECH_DATAPOINT_DIR)

    ids = [dp.datapoint_id for dp in datapoints]
    assert len(ids) == len(set(ids))
    assert "table_bank_transactions_001" in ids
    assert "metric_total_deposits_bank_001" in ids
    assert "proc_nightly_credit_risk_snapshot_bank_001" in ids

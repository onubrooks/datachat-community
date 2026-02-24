"""Validation tests for grocery sample DataPoints."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from backend.knowledge.datapoints import DataPointLoader

ROOT = Path(__file__).resolve().parents[3]
GROCERY_DATAPOINT_DIR = ROOT / "datapoints" / "examples" / "grocery_store"


def _expected_datapoint_counts(datapoint_dir: Path) -> tuple[int, Counter[str]]:
    expected_by_type: Counter[str] = Counter()
    datapoint_files = sorted(datapoint_dir.glob("*.json"))
    for datapoint_file in datapoint_files:
        payload = json.loads(datapoint_file.read_text())
        expected_by_type[payload["type"]] += 1
    return len(datapoint_files), expected_by_type


def test_grocery_datapoints_load_successfully():
    loader = DataPointLoader()
    datapoints = loader.load_directory(GROCERY_DATAPOINT_DIR)
    stats = loader.get_stats()
    expected_count, expected_by_type = _expected_datapoint_counts(GROCERY_DATAPOINT_DIR)

    assert stats["failed_count"] == 0
    assert len(datapoints) == expected_count

    by_type = Counter(dp.type for dp in datapoints)
    assert by_type == expected_by_type


def test_grocery_datapoints_have_unique_ids():
    loader = DataPointLoader()
    datapoints = loader.load_directory(GROCERY_DATAPOINT_DIR)

    ids = [dp.datapoint_id for dp in datapoints]
    assert len(ids) == len(set(ids))
    assert "table_grocery_sales_transactions_001" in ids
    assert "metric_total_revenue_grocery_001" in ids
    assert "proc_nightly_inventory_snapshot_001" in ids

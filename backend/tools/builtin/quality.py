"""Built-in DataPoint quality report tool."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.tools.base import ToolCategory, ToolContext, tool


def _load_datapoints_from_managed() -> list[dict[str, Any]]:
    managed_dir = Path("datapoints") / "managed"
    datapoints: list[dict[str, Any]] = []
    if not managed_dir.exists():
        return datapoints

    for path in managed_dir.rglob("*.json"):
        try:
            payload = json.loads(path.read_text())
            if isinstance(payload, dict):
                datapoints.append(payload)
        except Exception:
            continue
    return datapoints


@tool(
    name="datapoint_quality_report",
    description="Report weak or duplicate DataPoints in managed storage.",
    category=ToolCategory.KNOWLEDGE,
)
def datapoint_quality_report(limit: int = 20, ctx: ToolContext | None = None) -> dict[str, Any]:
    datapoints = _load_datapoints_from_managed()

    schema_items = [dp for dp in datapoints if dp.get("type") == "Schema"]
    business_items = [dp for dp in datapoints if dp.get("type") == "Business"]

    weak_schema = []
    for dp in schema_items:
        business_purpose = dp.get("business_purpose") or ""
        key_columns = dp.get("key_columns") or []
        if len(business_purpose) < 10 or len(key_columns) < 2:
            weak_schema.append(
                {
                    "datapoint_id": dp.get("datapoint_id"),
                    "table_name": dp.get("table_name"),
                    "reason": "Missing business purpose or key columns",
                }
            )

    weak_business = []
    for dp in business_items:
        calculation = dp.get("calculation") or ""
        aggregation = dp.get("aggregation") or ""
        if not calculation or not aggregation:
            weak_business.append(
                {
                    "datapoint_id": dp.get("datapoint_id"),
                    "name": dp.get("name"),
                    "reason": "Missing calculation or aggregation",
                }
            )

    duplicate_metrics = {}
    for dp in business_items:
        related_tables = dp.get("related_tables") or []
        table_key = related_tables[0] if related_tables else "unknown"
        calculation = dp.get("calculation") or ""
        key = (table_key, calculation)
        duplicate_metrics.setdefault(key, []).append(dp.get("datapoint_id"))

    duplicate_metric_groups = [
        {"table": key[0], "calculation": key[1], "datapoint_ids": ids}
        for key, ids in duplicate_metrics.items()
        if len(ids) > 1
    ]

    duplicate_ids = {}
    for dp in datapoints:
        datapoint_id = dp.get("datapoint_id")
        if not datapoint_id:
            continue
        duplicate_ids.setdefault(datapoint_id, 0)
        duplicate_ids[datapoint_id] += 1

    duplicate_id_list = [
        {"datapoint_id": dp_id, "count": count}
        for dp_id, count in duplicate_ids.items()
        if count > 1
    ]

    return {
        "total_datapoints": len(datapoints),
        "schema_datapoints": len(schema_items),
        "business_datapoints": len(business_items),
        "weak_schema": weak_schema[:limit],
        "weak_business": weak_business[:limit],
        "duplicate_metrics": duplicate_metric_groups[:limit],
        "duplicate_ids": duplicate_id_list[:limit],
    }

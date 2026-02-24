"""Tests for semantic conflict detection and resolution."""

from __future__ import annotations

from pydantic import TypeAdapter

from backend.knowledge.conflicts import DataPointConflictError, resolve_datapoint_conflicts
from backend.models.datapoint import DataPoint

datapoint_adapter = TypeAdapter(DataPoint)


def _business_payload(
    datapoint_id: str,
    *,
    source_tier: str,
    version: str,
) -> dict:
    return {
        "datapoint_id": datapoint_id,
        "type": "Business",
        "name": "Total Revenue",
        "owner": "owner@example.com",
        "tags": [],
        "metadata": {
            "grain": "daily_store",
            "exclusions": "Refunds excluded",
            "confidence_notes": "Validated",
            "source_tier": source_tier,
            "lifecycle": {
                "owner": "owner@example.com",
                "reviewer": "qa@example.com",
                "version": version,
                "changed_by": "sync-test",
                "changed_reason": "fixture",
                "changed_at": "2026-02-20T00:00:00+00:00",
            },
        },
        "calculation": "SUM(public.orders.amount)",
        "synonyms": ["revenue"],
        "business_rules": ["Completed transactions only"],
        "related_tables": ["public.orders"],
        "unit": "USD",
        "aggregation": "SUM",
    }


def test_resolve_datapoint_conflicts_errors_by_default():
    managed = datapoint_adapter.validate_python(
        _business_payload("metric_revenue_managed_001", source_tier="managed", version="1.0.0")
    )
    user = datapoint_adapter.validate_python(
        _business_payload("metric_revenue_user_001", source_tier="user", version="1.0.1")
    )

    try:
        resolve_datapoint_conflicts([managed, user], mode="error")
    except DataPointConflictError as exc:
        assert "conflict" in str(exc).lower()
    else:
        raise AssertionError("Expected DataPointConflictError in error mode")


def test_resolve_datapoint_conflicts_prefer_latest_uses_lifecycle_version():
    v1 = datapoint_adapter.validate_python(
        _business_payload("metric_revenue_v1_001", source_tier="managed", version="1.1.0")
    )
    v2 = datapoint_adapter.validate_python(
        _business_payload("metric_revenue_v2_001", source_tier="managed", version="1.2.0")
    )

    result = resolve_datapoint_conflicts([v1, v2], mode="prefer_latest")

    assert len(result.datapoints) == 1
    assert result.datapoints[0].datapoint_id == "metric_revenue_v2_001"
    assert result.conflicts[0].resolved_datapoint_id == "metric_revenue_v2_001"


def test_resolve_datapoint_conflicts_ignores_demo_conflicts():
    demo_a = datapoint_adapter.validate_python(
        _business_payload("metric_revenue_demo_a_001", source_tier="demo", version="1.0.0")
    )
    demo_b = datapoint_adapter.validate_python(
        _business_payload("metric_revenue_demo_b_001", source_tier="demo", version="1.0.1")
    )

    result = resolve_datapoint_conflicts([demo_a, demo_b], mode="error")

    assert len(result.datapoints) == 2
    assert result.conflicts == []

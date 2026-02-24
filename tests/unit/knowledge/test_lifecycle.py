"""Tests for lifecycle metadata helpers."""

from __future__ import annotations

from pydantic import TypeAdapter

from backend.knowledge.lifecycle import apply_lifecycle_metadata
from backend.models.datapoint import DataPoint

datapoint_adapter = TypeAdapter(DataPoint)


def _query_datapoint_payload(datapoint_id: str) -> dict:
    return {
        "datapoint_id": datapoint_id,
        "type": "Query",
        "name": "Lifecycle test query",
        "owner": "owner@example.com",
        "tags": [],
        "metadata": {
            "grain": "row-level",
            "exclusions": "None documented",
            "confidence_notes": "Validated in tests",
        },
        "description": "Lifecycle helper test query.",
        "sql_template": "SELECT 1 AS value",
        "related_tables": ["public.orders"],
    }


def test_apply_lifecycle_metadata_create_defaults():
    datapoint = datapoint_adapter.validate_python(_query_datapoint_payload("query_lifecycle_001"))

    lifecycle = apply_lifecycle_metadata(datapoint, action="create")

    assert lifecycle["owner"] == "owner@example.com"
    assert lifecycle["version"] == "1.0.0"
    assert lifecycle["changed_by"] == "api"
    assert lifecycle["changed_reason"] == "created"
    assert lifecycle["reviewer"] == "pending-review"
    assert "changed_at" in lifecycle


def test_apply_lifecycle_metadata_update_bumps_patch_version():
    previous_payload = _query_datapoint_payload("query_lifecycle_002")
    previous_payload["metadata"]["lifecycle"] = {
        "owner": "owner@example.com",
        "reviewer": "qa@example.com",
        "version": "1.4.9",
        "changed_by": "qa@example.com",
        "changed_reason": "initial publish",
        "changed_at": "2026-02-20T00:00:00+00:00",
    }
    previous_datapoint = datapoint_adapter.validate_python(previous_payload)
    updated_datapoint = datapoint_adapter.validate_python(_query_datapoint_payload("query_lifecycle_002"))

    lifecycle = apply_lifecycle_metadata(
        updated_datapoint,
        action="update",
        previous_datapoint=previous_datapoint,
        changed_by="api",
    )

    assert lifecycle["version"] == "1.4.10"
    assert lifecycle["reviewer"] == "qa@example.com"
    assert lifecycle["changed_by"] == "api"
    assert lifecycle["changed_reason"] == "updated"


def test_apply_lifecycle_metadata_update_ignores_client_audit_fields():
    previous_payload = _query_datapoint_payload("query_lifecycle_003")
    previous_payload["metadata"]["lifecycle"] = {
        "owner": "owner@example.com",
        "reviewer": "qa@example.com",
        "version": "2.0.8",
        "changed_by": "reviewer@example.com",
        "changed_reason": "approved",
        "changed_at": "2026-02-20T00:00:00+00:00",
    }
    previous_datapoint = datapoint_adapter.validate_python(previous_payload)

    updated_payload = _query_datapoint_payload("query_lifecycle_003")
    updated_payload["metadata"]["lifecycle"] = {
        "owner": "owner@example.com",
        "reviewer": "client-reviewer@example.com",
        "version": "2.0.8",
        "changed_by": "client@example.com",
        "changed_reason": "client-sent-stale",
        "changed_at": "2026-02-20T01:00:00+00:00",
    }
    updated_datapoint = datapoint_adapter.validate_python(updated_payload)

    lifecycle = apply_lifecycle_metadata(
        updated_datapoint,
        action="update",
        previous_datapoint=previous_datapoint,
        changed_by="api",
    )

    assert lifecycle["version"] == "2.0.9"
    assert lifecycle["changed_by"] == "api"
    assert lifecycle["changed_reason"] == "updated"

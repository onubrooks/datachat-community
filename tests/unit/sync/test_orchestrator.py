"""Unit tests for SyncOrchestrator."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.sync.orchestrator import SyncOrchestrator


def _schema_datapoint(datapoint_id: str, *, table_name: str | None = None) -> dict:
    resolved_table_name = table_name
    if not resolved_table_name:
        slug = datapoint_id
        if slug.startswith("table_"):
            slug = slug[len("table_") :]
        if "_" in slug:
            slug = slug.rsplit("_", 1)[0]
        resolved_table_name = f"public.{slug}"
    return {
        "datapoint_id": datapoint_id,
        "type": "Schema",
        "name": "Orders",
        "owner": "data@example.com",
        "tags": [],
        "metadata": {
            "grain": "row-level",
            "exclusions": "None documented",
            "confidence_notes": "Validated in unit test fixtures",
        },
        "table_name": resolved_table_name,
        "schema": "public",
        "business_purpose": "Orders table for testing.",
        "key_columns": [
            {
                "name": "order_id",
                "type": "integer",
                "business_meaning": "Order identifier",
                "nullable": False,
                "default_value": None,
            }
        ],
        "relationships": [],
        "common_queries": [],
        "gotchas": [],
        "freshness": "daily",
        "row_count": 100,
    }


def _write_datapoint(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _business_datapoint(
    datapoint_id: str,
    *,
    owner: str = "data@example.com",
    source_tier: str = "managed",
    version: str | None = None,
) -> dict:
    metadata = {
        "grain": "daily_store",
        "exclusions": "Refunded orders are excluded",
        "confidence_notes": "Validated against finance monthly close",
        "freshness": "daily",
        "source_tier": source_tier,
    }
    lifecycle = {
        "owner": owner,
        "reviewer": "reviewer@example.com",
        "changed_by": "sync-test",
        "changed_reason": "fixture",
        "changed_at": "2026-02-20T00:00:00+00:00",
    }
    if version:
        lifecycle["version"] = version
    metadata["lifecycle"] = lifecycle
    return {
        "datapoint_id": datapoint_id,
        "type": "Business",
        "name": "Total Revenue",
        "owner": owner,
        "tags": [],
        "metadata": metadata,
        "calculation": "SUM(public.orders.amount)",
        "synonyms": ["revenue"],
        "business_rules": ["Completed transactions only"],
        "related_tables": ["public.orders"],
        "unit": "USD",
        "aggregation": "SUM",
    }


@pytest.mark.asyncio
async def test_full_sync_rebuilds_everything(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.clear = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.clear = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    _write_datapoint(
        datapoints_dir / "table_orders_001.json", _schema_datapoint("table_orders_001")
    )
    _write_datapoint(
        datapoints_dir / "table_customers_001.json", _schema_datapoint("table_customers_001")
    )

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_all()

    vector_store.clear.assert_awaited_once()
    vector_store.add_datapoints.assert_awaited_once()
    graph.clear.assert_called_once()
    assert job.status == "completed"
    assert job.total_datapoints == 2


@pytest.mark.asyncio
async def test_incremental_sync_updates_specific_ids(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.delete = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.remove_datapoint = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    _write_datapoint(
        datapoints_dir / "table_orders_001.json", _schema_datapoint("table_orders_001")
    )
    _write_datapoint(
        datapoints_dir / "table_customers_001.json", _schema_datapoint("table_customers_001")
    )

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_incremental(["table_orders_001"])

    vector_store.delete.assert_awaited_once_with(["table_orders_001"])
    graph.remove_datapoint.assert_called_once_with("table_orders_001")
    vector_store.add_datapoints.assert_awaited_once()
    assert job.status == "completed"


@pytest.mark.asyncio
async def test_incremental_sync_preserves_valid_ids_when_one_id_is_invalid(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.delete = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.remove_datapoint = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    invalid_payload = _schema_datapoint("table_bad_001")
    invalid_payload["metadata"] = {}
    _write_datapoint(datapoints_dir / "table_bad_001.json", invalid_payload)
    _write_datapoint(datapoints_dir / "table_good_001.json", _schema_datapoint("table_good_001"))

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_incremental(["table_bad_001", "table_good_001"])

    # Only valid IDs should be deleted/replaced.
    vector_store.delete.assert_awaited_once_with(["table_good_001"])
    graph.remove_datapoint.assert_called_once_with("table_good_001")
    vector_store.add_datapoints.assert_awaited_once()
    added = vector_store.add_datapoints.await_args.args[0]
    assert len(added) == 1
    assert added[0].datapoint_id == "table_good_001"
    assert job.status == "failed"
    assert job.error is not None
    assert "table_bad_001" in job.error


@pytest.mark.asyncio
async def test_status_tracking_updates(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.clear = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.clear = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    _write_datapoint(
        datapoints_dir / "table_orders_001.json", _schema_datapoint("table_orders_001")
    )

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_all()

    status = orchestrator.get_status()
    assert status["status"] == "completed"
    assert status["total_datapoints"] == 1
    assert status["processed_datapoints"] == 1
    assert job.finished_at is not None


@pytest.mark.asyncio
async def test_full_sync_applies_database_scope_metadata(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.clear = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.clear = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    _write_datapoint(
        datapoints_dir / "table_orders_001.json", _schema_datapoint("table_orders_001")
    )

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    await orchestrator.sync_all(scope="database", connection_id="conn-fintech")

    synced_datapoints = vector_store.add_datapoints.await_args.args[0]
    assert synced_datapoints[0].metadata["scope"] == "database"
    assert synced_datapoints[0].metadata["connection_id"] == "conn-fintech"


@pytest.mark.asyncio
async def test_full_sync_allows_demo_datapoints_with_advisory_metadata_gaps(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.clear = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.clear = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    demo_payload = _schema_datapoint("table_demo_users_001")
    demo_payload["metadata"] = {"source": "demo-seed"}
    _write_datapoint(datapoints_dir / "demo" / "table_demo_users_001.json", demo_payload)

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_all()

    assert job.status == "completed"
    vector_store.add_datapoints.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_sync_fails_when_contracts_are_invalid(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.clear = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.clear = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    invalid_payload = _schema_datapoint("table_orders_001")
    invalid_payload["metadata"] = {}
    _write_datapoint(datapoints_dir / "table_orders_001.json", invalid_payload)

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_all()

    assert job.status == "failed"
    assert job.error is not None
    assert "contract" in job.error.lower()


@pytest.mark.asyncio
async def test_full_sync_rejects_semantic_conflicts_without_resolution_mode(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.clear = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.clear = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    _write_datapoint(
        datapoints_dir / "managed" / "metric_revenue_managed_001.json",
        _business_datapoint(
            "metric_revenue_managed_001",
            owner="managed@example.com",
            source_tier="managed",
            version="1.0.0",
        ),
    )
    _write_datapoint(
        datapoints_dir / "user" / "metric_revenue_user_001.json",
        _business_datapoint(
            "metric_revenue_user_001",
            owner="user@example.com",
            source_tier="user",
            version="1.0.1",
        ),
    )

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_all()

    assert job.status == "failed"
    assert job.error is not None
    assert "conflict" in job.error.lower()


@pytest.mark.asyncio
async def test_full_sync_resolves_semantic_conflicts_with_prefer_user_mode(tmp_path: Path):
    vector_store = AsyncMock()
    vector_store.clear = AsyncMock()
    vector_store.add_datapoints = AsyncMock()

    graph = MagicMock()
    graph.clear = MagicMock()
    graph.add_datapoint = MagicMock()

    datapoints_dir = tmp_path / "datapoints"
    _write_datapoint(
        datapoints_dir / "managed" / "metric_revenue_managed_001.json",
        _business_datapoint(
            "metric_revenue_managed_001",
            owner="managed@example.com",
            source_tier="managed",
            version="1.0.0",
        ),
    )
    _write_datapoint(
        datapoints_dir / "user" / "metric_revenue_user_001.json",
        _business_datapoint(
            "metric_revenue_user_001",
            owner="user@example.com",
            source_tier="user",
            version="1.0.1",
        ),
    )

    orchestrator = SyncOrchestrator(
        vector_store=vector_store,
        knowledge_graph=graph,
        datapoints_dir=datapoints_dir,
    )

    job = await orchestrator.sync_all(conflict_mode="prefer_user")

    assert job.status == "completed"
    synced = vector_store.add_datapoints.await_args.args[0]
    assert len(synced) == 1
    assert synced[0].datapoint_id == "metric_revenue_user_001"

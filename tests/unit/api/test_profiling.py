"""Unit tests for profiling pending approval routes."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app
from backend.profiling.models import PendingDataPoint


def _valid_business_datapoint() -> dict:
    return {
        "datapoint_id": "metric_total_revenue_001",
        "type": "Business",
        "name": "Total Revenue",
        "owner": "finance@example.com",
        "tags": ["kpi"],
        "metadata": {
            "grain": "daily_store",
            "freshness": "daily",
            "exclusions": "Refunded orders are excluded.",
            "confidence_notes": "Reviewed against finance dashboard.",
        },
        "calculation": "SUM(orders.amount)",
        "synonyms": ["revenue"],
        "business_rules": ["exclude refunds"],
        "related_tables": ["public.orders"],
        "unit": "USD",
        "aggregation": "SUM",
    }


def _invalid_business_datapoint_missing_unit() -> dict:
    payload = _valid_business_datapoint()
    payload.pop("unit", None)
    return payload


class TestPendingApprovalContracts:
    @pytest.fixture
    def client(self):
        return TestClient(app)

    def test_approve_pending_blocks_contract_errors(self, client):
        pending_id = uuid4()
        pending = PendingDataPoint(
            pending_id=pending_id,
            profile_id=uuid4(),
            datapoint=_invalid_business_datapoint_missing_unit(),
            confidence=0.8,
            status="pending",
            created_at=datetime.now(UTC),
        )

        store = AsyncMock()
        store.list_pending.return_value = [pending]
        store.update_pending_status = AsyncMock()
        store.get_profile = AsyncMock(return_value=MagicMock(connection_id=uuid4()))
        vector_store = AsyncMock()
        graph = MagicMock()

        with (
            patch("backend.api.routes.profiling._get_store", return_value=store),
            patch("backend.api.routes.profiling._get_vector_store", return_value=vector_store),
            patch("backend.api.routes.profiling._get_knowledge_graph", return_value=graph),
        ):
            response = client.post(f"/api/v1/datapoints/pending/{pending_id}/approve", json={})

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["message"] == "DataPoint contract validation failed."
        assert any(item["code"] == "missing_units" for item in detail["contract_errors"])
        store.update_pending_status.assert_not_awaited()

    def test_approve_pending_succeeds_for_valid_contract(self, client):
        pending_id = uuid4()
        pending = PendingDataPoint(
            pending_id=pending_id,
            profile_id=uuid4(),
            datapoint=_valid_business_datapoint(),
            confidence=0.9,
            status="pending",
            created_at=datetime.now(UTC),
        )
        approved = pending.model_copy(update={"status": "approved"})

        store = AsyncMock()
        store.list_pending.return_value = [pending]
        store.update_pending_status.return_value = approved
        store.get_profile = AsyncMock(return_value=MagicMock(connection_id=uuid4()))
        vector_store = AsyncMock()
        graph = MagicMock()

        with (
            patch("backend.api.routes.profiling._get_store", return_value=store),
            patch("backend.api.routes.profiling._get_vector_store", return_value=vector_store),
            patch("backend.api.routes.profiling._get_knowledge_graph", return_value=graph),
            patch(
                "backend.api.routes.profiling._remove_existing_datapoints_for_table",
                return_value=[],
            ),
            patch("backend.sync.orchestrator.save_datapoint_to_disk"),
        ):
            response = client.post(f"/api/v1/datapoints/pending/{pending_id}/approve", json={})

        assert response.status_code == 200
        assert response.json()["status"] == "approved"
        store.update_pending_status.assert_awaited_once()

    def test_approve_pending_rejects_empty_edit_payload(self, client):
        pending_id = uuid4()
        pending = PendingDataPoint(
            pending_id=pending_id,
            profile_id=uuid4(),
            datapoint=_valid_business_datapoint(),
            confidence=0.9,
            status="pending",
            created_at=datetime.now(UTC),
        )

        store = AsyncMock()
        store.list_pending.return_value = [pending]
        store.update_pending_status = AsyncMock()
        store.get_profile = AsyncMock(return_value=MagicMock(connection_id=uuid4()))
        vector_store = AsyncMock()
        graph = MagicMock()

        with (
            patch("backend.api.routes.profiling._get_store", return_value=store),
            patch("backend.api.routes.profiling._get_vector_store", return_value=vector_store),
            patch("backend.api.routes.profiling._get_knowledge_graph", return_value=graph),
        ):
            response = client.post(
                f"/api/v1/datapoints/pending/{pending_id}/approve",
                json={"datapoint": {}},
            )

        assert response.status_code == 400
        assert "Invalid DataPoint payload" in response.json()["detail"]
        store.update_pending_status.assert_not_awaited()

    def test_bulk_approve_blocks_when_any_pending_contract_invalid(self, client):
        valid_pending = PendingDataPoint(
            pending_id=uuid4(),
            profile_id=uuid4(),
            datapoint=_valid_business_datapoint(),
            confidence=0.9,
            status="pending",
            created_at=datetime.now(UTC),
        )
        invalid_pending = PendingDataPoint(
            pending_id=uuid4(),
            profile_id=uuid4(),
            datapoint=_invalid_business_datapoint_missing_unit(),
            confidence=0.7,
            status="pending",
            created_at=datetime.now(UTC),
        )

        store = AsyncMock()
        store.list_pending.return_value = [valid_pending, invalid_pending]
        store.bulk_update_pending = AsyncMock(return_value=[])
        store.get_profile = AsyncMock(return_value=MagicMock(connection_id=uuid4()))
        vector_store = AsyncMock()
        graph = MagicMock()

        with (
            patch("backend.api.routes.profiling._get_store", return_value=store),
            patch("backend.api.routes.profiling._get_vector_store", return_value=vector_store),
            patch("backend.api.routes.profiling._get_knowledge_graph", return_value=graph),
        ):
            response = client.post("/api/v1/datapoints/pending/bulk-approve")

        assert response.status_code == 400
        assert "contract validation failed" in response.json()["detail"]["message"].lower()
        store.bulk_update_pending.assert_not_awaited()

    def test_list_pending_for_connection_scope(self, client):
        connection_id = uuid4()
        pending = PendingDataPoint(
            pending_id=uuid4(),
            profile_id=uuid4(),
            datapoint=_valid_business_datapoint(),
            confidence=0.9,
            status="pending",
            created_at=datetime.now(UTC),
        )
        store = AsyncMock()
        store.list_pending.return_value = [pending]
        store.get_profile = AsyncMock(return_value=MagicMock(connection_id=uuid4()))

        with patch("backend.api.routes.profiling._get_store", return_value=store):
            response = client.get(
                f"/api/v1/datapoints/pending?status_filter=pending&connection_id={connection_id}"
            )

        assert response.status_code == 200
        store.list_pending.assert_awaited_once_with(status="pending", connection_id=connection_id)
        body = response.json()
        assert len(body["pending"]) == 1

    def test_bulk_approve_uses_connection_scope(self, client):
        connection_id = uuid4()
        pending = PendingDataPoint(
            pending_id=uuid4(),
            profile_id=uuid4(),
            datapoint=_valid_business_datapoint(),
            confidence=0.9,
            status="pending",
            created_at=datetime.now(UTC),
        )
        approved = pending.model_copy(update={"status": "approved"})

        store = AsyncMock()
        store.list_pending.return_value = [pending]
        store.bulk_update_pending.return_value = [approved]
        store.get_profile = AsyncMock(return_value=MagicMock(connection_id=connection_id))
        vector_store = AsyncMock()
        graph = MagicMock()

        with (
            patch("backend.api.routes.profiling._get_store", return_value=store),
            patch("backend.api.routes.profiling._get_vector_store", return_value=vector_store),
            patch("backend.api.routes.profiling._get_knowledge_graph", return_value=graph),
            patch(
                "backend.api.routes.profiling._remove_existing_datapoints_for_table",
                return_value=[],
            ),
            patch("backend.sync.orchestrator.save_datapoint_to_disk"),
        ):
            response = client.post(
                f"/api/v1/datapoints/pending/bulk-approve?connection_id={connection_id}"
            )

        assert response.status_code == 200
        store.list_pending.assert_awaited_once_with(status="pending", connection_id=connection_id)
        store.bulk_update_pending.assert_awaited_once()
        kwargs = store.bulk_update_pending.await_args.kwargs
        assert kwargs["status"] == "approved"
        assert kwargs["connection_id"] == connection_id
        assert kwargs["pending_ids"] == [pending.pending_id]

    def test_bulk_approve_validates_and_scopes_fallback_rows(self, client):
        connection_id = uuid4()
        profile_id = uuid4()
        initial_pending = PendingDataPoint(
            pending_id=uuid4(),
            profile_id=profile_id,
            datapoint=_valid_business_datapoint(),
            confidence=0.9,
            status="pending",
            created_at=datetime.now(UTC),
        )
        fallback_pending = PendingDataPoint(
            pending_id=uuid4(),
            profile_id=profile_id,
            datapoint=_valid_business_datapoint(),
            confidence=0.8,
            status="approved",
            created_at=datetime.now(UTC),
        )

        store = AsyncMock()
        store.list_pending.return_value = [initial_pending]
        store.bulk_update_pending.return_value = [fallback_pending]
        store.get_profile = AsyncMock(return_value=MagicMock(connection_id=connection_id))
        vector_store = AsyncMock()
        graph = MagicMock()

        with (
            patch("backend.api.routes.profiling._get_store", return_value=store),
            patch("backend.api.routes.profiling._get_vector_store", return_value=vector_store),
            patch("backend.api.routes.profiling._get_knowledge_graph", return_value=graph),
            patch(
                "backend.api.routes.profiling._remove_existing_datapoints_for_table",
                return_value=[],
            ),
            patch("backend.sync.orchestrator.save_datapoint_to_disk"),
        ):
            response = client.post(
                f"/api/v1/datapoints/pending/bulk-approve?connection_id={connection_id}"
            )

        assert response.status_code == 200
        store.bulk_update_pending.assert_awaited_once()
        kwargs = store.bulk_update_pending.await_args.kwargs
        assert kwargs["pending_ids"] == [initial_pending.pending_id]
        vector_store.add_datapoints.assert_awaited_once()
        added = vector_store.add_datapoints.await_args.args[0]
        assert added[0].metadata["connection_id"] == str(connection_id)

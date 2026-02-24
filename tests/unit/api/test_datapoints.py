"""Unit tests for datapoint listing endpoints."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app


class TestDatapointEndpoints:
    """Test datapoint listing behavior from vector-store state."""

    @pytest.fixture
    def client(self):
        return TestClient(app)

    def test_list_datapoints_reads_from_vector_store(self, client):
        mock_store = AsyncMock()
        mock_store.list_datapoints.return_value = [
            {
                "datapoint_id": "table_grocery_stores_001",
                "metadata": {
                    "type": "Schema",
                    "name": "Grocery Stores",
                    "source_tier": "example",
                    "source_path": "/tmp/datapoints/examples/grocery_store/table_grocery_stores_001.json",
                },
            },
            {
                "datapoint_id": "metric_total_revenue_001",
                "metadata": {
                    "type": "Business",
                    "name": "Total Revenue",
                    "source_tier": "managed",
                    "source_path": "/tmp/datapoints/managed/metric_total_revenue_001.json",
                },
            },
        ]

        with patch("backend.api.routes.datapoints._get_vector_store", return_value=mock_store):
            response = client.get("/api/v1/datapoints")

        assert response.status_code == 200
        payload = response.json()
        assert len(payload["datapoints"]) == 2
        # Managed should be ranked ahead of example.
        assert payload["datapoints"][0]["datapoint_id"] == "metric_total_revenue_001"
        by_id = {item["datapoint_id"]: item for item in payload["datapoints"]}
        assert by_id["table_grocery_stores_001"]["source_tier"] == "example"
        assert by_id["metric_total_revenue_001"]["source_tier"] == "managed"
        assert by_id["table_grocery_stores_001"]["source_path"].endswith(
            "table_grocery_stores_001.json"
        )

    def test_list_datapoints_prefers_higher_source_tier_for_duplicate_ids(self, client):
        duplicate_id = "table_grocery_products_001"
        mock_store = AsyncMock()
        mock_store.list_datapoints.return_value = [
            {
                "datapoint_id": duplicate_id,
                "metadata": {
                    "type": "Schema",
                    "name": "Example Products",
                    "source_tier": "example",
                },
            },
            {
                "datapoint_id": duplicate_id,
                "metadata": {
                    "type": "Schema",
                    "name": "Managed Products",
                    "source_tier": "managed",
                },
            },
        ]

        with patch("backend.api.routes.datapoints._get_vector_store", return_value=mock_store):
            response = client.get("/api/v1/datapoints")

        assert response.status_code == 200
        datapoints = response.json()["datapoints"]
        assert len(datapoints) == 1
        assert datapoints[0]["datapoint_id"] == duplicate_id
        assert datapoints[0]["name"] == "Managed Products"
        assert datapoints[0]["source_tier"] == "managed"

    def test_trigger_sync_accepts_database_scope(self, client):
        orchestrator = AsyncMock()
        orchestrator.enqueue_sync_all.return_value = "11111111-1111-1111-1111-111111111111"

        with patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator):
            response = client.post(
                "/api/v1/sync",
                json={"scope": "database", "connection_id": "db-123"},
            )

        assert response.status_code == 200
        orchestrator.enqueue_sync_all.assert_called_once_with(
            scope="database",
            connection_id="db-123",
            conflict_mode="error",
        )

    def test_trigger_sync_passes_conflict_mode(self, client):
        orchestrator = AsyncMock()
        orchestrator.enqueue_sync_all.return_value = "11111111-1111-1111-1111-111111111111"

        with patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator):
            response = client.post(
                "/api/v1/sync",
                json={"scope": "auto", "conflict_mode": "prefer_latest"},
            )

        assert response.status_code == 200
        orchestrator.enqueue_sync_all.assert_called_once_with(
            scope="auto",
            connection_id=None,
            conflict_mode="prefer_latest",
        )

    def test_trigger_sync_rejects_database_scope_without_connection(self, client):
        orchestrator = AsyncMock()

        with patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator):
            response = client.post("/api/v1/sync", json={"scope": "database"})

        assert response.status_code == 400
        assert "connection_id is required" in response.json()["detail"]

    def test_trigger_sync_rejects_connection_for_global_scope(self, client):
        orchestrator = AsyncMock()

        with patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator):
            response = client.post(
                "/api/v1/sync",
                json={"scope": "global", "connection_id": "db-123"},
            )

        assert response.status_code == 400
        assert "only allowed when scope=database" in response.json()["detail"]

    def test_get_datapoint_returns_managed_json(self, client, tmp_path):
        datapoint_payload = {
            "datapoint_id": "query_top_customers_001",
            "type": "Query",
            "name": "Top customers by revenue",
            "owner": "data-team@example.com",
            "tags": ["manual"],
            "metadata": {},
            "description": "Top customers by completed revenue.",
            "sql_template": "SELECT customer_id, SUM(amount) AS revenue FROM public.transactions GROUP BY customer_id LIMIT {limit}",
            "parameters": {
                "limit": {
                    "type": "integer",
                    "required": False,
                    "default": 20,
                    "description": "Max rows.",
                }
            },
            "related_tables": ["public.transactions"],
        }
        datapoint_path = tmp_path / "query_top_customers_001.json"
        datapoint_path.write_text(json.dumps(datapoint_payload), encoding="utf-8")

        with patch("backend.api.routes.datapoints._file_path", return_value=datapoint_path):
            response = client.get("/api/v1/datapoints/query_top_customers_001")

        assert response.status_code == 200
        payload = response.json()
        assert payload["datapoint_id"] == "query_top_customers_001"
        assert payload["type"] == "Query"

    def test_create_datapoint_rejects_contract_gaps(self, client):
        payload = {
            "datapoint_id": "query_contract_gap_001",
            "type": "Query",
            "name": "Contract gap query",
            "owner": "data-team@example.com",
            "tags": ["manual"],
            "metadata": {},
            "description": "Contract gap validation test query.",
            "sql_template": "SELECT 1 AS value",
            "related_tables": ["public.orders"],
        }

        response = client.post("/api/v1/datapoints", json=payload)

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["message"] == "DataPoint contract validation failed."
        assert detail["datapoint_id"] == "query_contract_gap_001"
        codes = {issue["code"] for issue in detail["contract_errors"]}
        assert "missing_grain" in codes
        assert "missing_exclusions" in codes
        assert "missing_confidence_notes" in codes

    def test_create_datapoint_accepts_contract_complete_payload(self, client, tmp_path):
        payload = {
            "datapoint_id": "query_contract_pass_001",
            "type": "Query",
            "name": "Contract pass query",
            "owner": "data-team@example.com",
            "tags": ["manual"],
            "metadata": {
                "grain": "row-level",
                "exclusions": "None documented",
                "confidence_notes": "Validated with staging data",
            },
            "description": "Contract-complete datapoint creation test query.",
            "sql_template": "SELECT 1 AS value",
            "related_tables": ["public.orders"],
        }
        target_path = tmp_path / "query_contract_pass_001.json"
        orchestrator = AsyncMock()

        with (
            patch("backend.api.routes.datapoints._file_path", return_value=target_path),
            patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator),
        ):
            response = client.post("/api/v1/datapoints", json=payload)

        assert response.status_code == 201
        assert response.json()["datapoint_id"] == "query_contract_pass_001"
        orchestrator.enqueue_sync_incremental.assert_called_once_with(
            ["query_contract_pass_001"], conflict_mode="prefer_latest"
        )
        lifecycle = response.json()["metadata"]["lifecycle"]
        assert lifecycle["version"] == "1.0.0"
        assert lifecycle["changed_by"] == "api"
        assert lifecycle["changed_reason"] == "created"
        assert lifecycle["reviewer"] == "pending-review"

    def test_update_datapoint_increments_lifecycle_version(self, client, tmp_path):
        existing_payload = {
            "datapoint_id": "query_contract_update_001",
            "type": "Query",
            "name": "Contract update query",
            "owner": "data-team@example.com",
            "tags": ["manual"],
            "metadata": {
                "grain": "row-level",
                "exclusions": "None documented",
                "confidence_notes": "Validated with staging data",
                "lifecycle": {
                    "owner": "data-team@example.com",
                    "reviewer": "qa@example.com",
                    "version": "1.2.3",
                    "changed_by": "qa@example.com",
                    "changed_reason": "initial publish",
                    "changed_at": "2026-02-20T00:00:00+00:00",
                },
            },
            "description": "Contract-complete datapoint update test query.",
            "sql_template": "SELECT 1 AS value",
            "related_tables": ["public.orders"],
        }
        target_path = tmp_path / "query_contract_update_001.json"
        target_path.write_text(json.dumps(existing_payload), encoding="utf-8")

        update_payload = dict(existing_payload)
        update_payload["description"] = "Updated description."
        update_payload["metadata"] = {
            "grain": "row-level",
            "exclusions": "None documented",
            "confidence_notes": "Validated with staging data",
        }
        orchestrator = AsyncMock()

        with (
            patch("backend.api.routes.datapoints._file_path", return_value=target_path),
            patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator),
        ):
            response = client.put(
                "/api/v1/datapoints/query_contract_update_001",
                json=update_payload,
            )

        assert response.status_code == 200
        lifecycle = response.json()["metadata"]["lifecycle"]
        assert lifecycle["version"] == "1.2.4"
        assert lifecycle["changed_by"] == "api"
        assert lifecycle["changed_reason"] == "updated"
        assert lifecycle["reviewer"] == "qa@example.com"
        orchestrator.enqueue_sync_incremental.assert_called_once_with(
            ["query_contract_update_001"], conflict_mode="prefer_latest"
        )

    def test_update_datapoint_recomputes_lifecycle_from_previous_state(self, client, tmp_path):
        existing_payload = {
            "datapoint_id": "query_contract_update_002",
            "type": "Query",
            "name": "Contract update query",
            "owner": "data-team@example.com",
            "tags": ["manual"],
            "metadata": {
                "grain": "row-level",
                "exclusions": "None documented",
                "confidence_notes": "Validated with staging data",
                "lifecycle": {
                    "owner": "data-team@example.com",
                    "reviewer": "qa@example.com",
                    "version": "3.1.4",
                    "changed_by": "qa@example.com",
                    "changed_reason": "published",
                    "changed_at": "2026-02-20T00:00:00+00:00",
                },
            },
            "description": "Contract-complete datapoint update test query.",
            "sql_template": "SELECT 1 AS value",
            "related_tables": ["public.orders"],
        }
        target_path = tmp_path / "query_contract_update_002.json"
        target_path.write_text(json.dumps(existing_payload), encoding="utf-8")

        update_payload = dict(existing_payload)
        update_payload["description"] = "Updated description."
        update_payload["metadata"] = {
            "grain": "row-level",
            "exclusions": "None documented",
            "confidence_notes": "Validated with staging data",
            "lifecycle": {
                "owner": "data-team@example.com",
                "reviewer": "client-reviewer@example.com",
                "version": "3.1.4",
                "changed_by": "client@example.com",
                "changed_reason": "stale-client-copy",
                "changed_at": "2026-02-20T01:00:00+00:00",
            },
        }
        orchestrator = AsyncMock()

        with (
            patch("backend.api.routes.datapoints._file_path", return_value=target_path),
            patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator),
        ):
            response = client.put(
                "/api/v1/datapoints/query_contract_update_002",
                json=update_payload,
            )

        assert response.status_code == 200
        lifecycle = response.json()["metadata"]["lifecycle"]
        assert lifecycle["version"] == "3.1.5"
        assert lifecycle["changed_by"] == "api"
        assert lifecycle["changed_reason"] == "updated"
        assert lifecycle["reviewer"] == "client-reviewer@example.com"

    def test_update_datapoint_allows_recovery_when_existing_file_is_invalid(
        self, client, tmp_path
    ):
        target_path = tmp_path / "query_contract_recovery_001.json"
        target_path.write_text("{invalid json", encoding="utf-8")
        update_payload = {
            "datapoint_id": "query_contract_recovery_001",
            "type": "Query",
            "name": "Recovery query",
            "owner": "data-team@example.com",
            "tags": ["manual"],
            "metadata": {
                "grain": "row-level",
                "exclusions": "None documented",
                "confidence_notes": "Validated with staging data",
            },
            "description": "Overwrite corrupted datapoint file.",
            "sql_template": "SELECT 1 AS value",
            "related_tables": ["public.orders"],
        }
        orchestrator = AsyncMock()

        with (
            patch("backend.api.routes.datapoints._file_path", return_value=target_path),
            patch("backend.api.routes.datapoints._get_orchestrator", return_value=orchestrator),
        ):
            response = client.put(
                "/api/v1/datapoints/query_contract_recovery_001",
                json=update_payload,
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["datapoint_id"] == "query_contract_recovery_001"
        assert payload["metadata"]["lifecycle"]["version"] == "1.0.0"
        assert payload["metadata"]["lifecycle"]["changed_by"] == "api"
        assert payload["metadata"]["lifecycle"]["changed_reason"] == "updated"

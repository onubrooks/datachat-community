"""Unit tests for run history routes."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from backend.api.main import app


class TestRunRoutes:
    def setup_method(self) -> None:
        self.client = TestClient(app)

    def test_list_runs_returns_persisted_runs(self) -> None:
        run_id = str(uuid4())
        run_store = AsyncMock()
        run_store.list_runs.return_value = [
            {
                "run_id": run_id,
                "run_type": "chat",
                "status": "completed",
                "route": "sql",
                "connection_id": str(uuid4()),
                "conversation_id": None,
                "correlation_id": run_id,
                "failure_class": None,
                "confidence": 0.91,
                "warning_count": 0,
                "error_count": 0,
                "latency_ms": 245.3,
                "summary": {"query": "How many orders?"},
                "started_at": datetime.now(UTC).isoformat(),
                "completed_at": datetime.now(UTC).isoformat(),
                "created_at": datetime.now(UTC).isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
            }
        ]

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get("/api/v1/runs")

        assert response.status_code == 200
        body = response.json()
        assert len(body["runs"]) == 1
        assert body["runs"][0]["run_id"] == run_id
        assert body["runs"][0]["summary"]["query"] == "How many orders?"

    def test_get_run_returns_detail(self) -> None:
        run_id = uuid4()
        run_store = AsyncMock()
        run_store.get_run.return_value = {
            "run_id": str(run_id),
            "run_type": "chat",
            "status": "completed",
            "route": "sql",
            "connection_id": str(uuid4()),
            "conversation_id": None,
            "correlation_id": str(run_id),
            "failure_class": None,
            "confidence": 0.91,
            "warning_count": 1,
            "error_count": 0,
            "latency_ms": 245.3,
            "summary": {"query": "How many orders?"},
            "output": {"answer_source": "sql"},
            "started_at": datetime.now(UTC).isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
            "steps": [
                {
                    "step_id": str(uuid4()),
                    "step_order": 1,
                    "step_name": "query_analyzer",
                    "status": "ok",
                    "latency_ms": 11.2,
                    "summary": {"selected_action": "sql"},
                    "created_at": datetime.now(UTC).isoformat(),
                }
            ],
            "quality_findings": [
                {
                    "finding_id": str(uuid4()),
                    "run_id": str(run_id),
                    "finding_type": "advisory",
                    "severity": "warning",
                    "category": "retrieval",
                    "code": "retrieval_miss",
                    "message": "No datapoints were retrieved for this run.",
                    "details": {"query": "How many orders?"},
                    "created_at": datetime.now(UTC).isoformat(),
                }
            ],
        }

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get(f"/api/v1/runs/{run_id}")

        assert response.status_code == 200
        body = response.json()
        assert body["run_id"] == str(run_id)
        assert body["output"]["answer_source"] == "sql"
        assert body["steps"][0]["step_name"] == "query_analyzer"
        assert body["quality_findings"][0]["code"] == "retrieval_miss"

    def test_get_run_returns_404_when_missing(self) -> None:
        run_store = AsyncMock()
        run_store.get_run.return_value = None

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get(f"/api/v1/runs/{uuid4()}")

        assert response.status_code == 404
        assert response.json()["detail"] == "Run not found"

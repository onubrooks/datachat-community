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

    def test_compare_runs_returns_backend_diff(self) -> None:
        primary_run_id = uuid4()
        comparison_run_id = uuid4()
        run_store = AsyncMock()
        run_store.compare_runs.return_value = {
            "primary_run": {
                "run_id": str(primary_run_id),
                "run_type": "chat",
                "status": "completed",
                "route": "sql",
                "summary": {"query": "How many orders?"},
                "output": {},
                "steps": [],
                "quality_findings": [],
            },
            "comparison_run": {
                "run_id": str(comparison_run_id),
                "run_type": "chat",
                "status": "completed",
                "route": "sql",
                "summary": {"query": "How many orders?"},
                "output": {},
                "steps": [],
                "quality_findings": [],
            },
            "diff": {
                "sql_changed": True,
                "primary_sql": "select 1",
                "comparison_sql": "select count(*) from orders",
                "confidence_delta": 0.12,
                "latency_delta_ms": -14.0,
                "warning_delta": -1,
                "error_delta": 0,
                "status_changed": False,
                "failure_class_changed": False,
                "datapoints_added": [{"datapoint_id": "dp_new"}],
                "datapoints_removed": [],
                "quality_findings_added": [],
                "quality_findings_resolved": [],
            },
        }

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get(
                f"/api/v1/runs/compare?primary_run_id={primary_run_id}&comparison_run_id={comparison_run_id}"
            )

        assert response.status_code == 200
        body = response.json()
        assert body["diff"]["sql_changed"] is True
        assert body["diff"]["datapoints_added"][0]["datapoint_id"] == "dp_new"

    def test_compare_runs_rejects_same_run_id(self) -> None:
        run_id = uuid4()
        run_store = AsyncMock()

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get(
                f"/api/v1/runs/compare?primary_run_id={run_id}&comparison_run_id={run_id}"
            )

        assert response.status_code == 400
        assert response.json()["detail"] == "Comparison run must be different from primary run"

    def test_get_run_returns_404_when_missing(self) -> None:
        run_store = AsyncMock()
        run_store.get_run.return_value = None

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get(f"/api/v1/runs/{uuid4()}")

        assert response.status_code == 404
        assert response.json()["detail"] == "Run not found"

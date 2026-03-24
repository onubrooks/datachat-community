"""Unit tests for monitoring rollup routes."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from backend.api.main import app


class TestMonitoringRoutes:
    def setup_method(self) -> None:
        self.client = TestClient(app)

    def test_monitoring_summary_returns_rollups(self) -> None:
        run_store = AsyncMock()
        run_store.summarize_runs.return_value = {
            "window_hours": 24,
            "total_runs": 12,
            "completed_runs": 10,
            "failed_runs": 2,
            "success_rate": 10 / 12,
            "p50_latency_ms": 210.5,
            "p95_latency_ms": 842.1,
            "clarification_rate": 0.25,
            "retrieval_miss_rate": 0.1,
            "route_breakdown": [
                {"route": "sql", "count": 8, "success_rate": 0.875, "failed": 1},
                {"route": "context", "count": 4, "success_rate": 0.75, "failed": 1},
            ],
            "failure_breakdown": [
                {"failure_class": "validation_error", "count": 1},
                {"failure_class": "execution_error", "count": 1},
            ],
            "quality_breakdown": [
                {"severity": "warning", "category": "retrieval", "code": "retrieval_miss", "count": 3}
            ],
            "recent_failures": [
                {
                    "run_id": "run-1",
                    "route": "sql",
                    "failure_class": "validation_error",
                    "query": "broken query",
                    "created_at": datetime.now(UTC),
                }
            ],
        }

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get("/api/v1/monitoring/summary?window_hours=24")

        assert response.status_code == 200
        body = response.json()
        assert body["total_runs"] == 12
        assert body["completed_runs"] == 10
        assert body["route_breakdown"][0]["route"] == "sql"
        assert body["failure_breakdown"][0]["failure_class"] == "validation_error"
        assert body["quality_breakdown"][0]["code"] == "retrieval_miss"
        run_store.summarize_runs.assert_awaited_once_with(window_hours=24)

    def test_monitoring_trends_returns_buckets(self) -> None:
        run_store = AsyncMock()
        run_store.summarize_run_trends.return_value = {
            "window_hours": 24,
            "bucket_hours": 1,
            "trend": [
                {
                    "bucket_start": datetime.now(UTC),
                    "total_runs": 5,
                    "failed_runs": 1,
                    "success_rate": 0.8,
                    "p50_latency_ms": 312.4,
                    "clarification_rate": 0.2,
                    "retrieval_miss_rate": 0.4,
                }
            ],
        }

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get("/api/v1/monitoring/trends?window_hours=24&bucket_hours=1")

        assert response.status_code == 200
        body = response.json()
        assert body["trend"][0]["total_runs"] == 5
        assert body["trend"][0]["retrieval_miss_rate"] == 0.4
        run_store.summarize_run_trends.assert_awaited_once_with(window_hours=24, bucket_hours=1)

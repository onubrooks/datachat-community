"""Unit tests for quality summary routes."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from backend.api.main import app


class TestQualityRoutes:
    def setup_method(self) -> None:
        self.client = TestClient(app)

    def test_quality_summary_returns_findings(self) -> None:
        run_store = AsyncMock()
        run_store.summarize_quality.return_value = {
            "window_hours": 24,
            "total_findings": 4,
            "runs_with_findings": 3,
            "severity_breakdown": [{"severity": "warning", "count": 3}],
            "category_breakdown": [{"category": "retrieval", "count": 2}],
            "code_breakdown": [
                {
                    "code": "retrieval_miss",
                    "severity": "warning",
                    "category": "retrieval",
                    "count": 2,
                }
            ],
            "recent_findings": [
                {
                    "finding_id": "finding-1",
                    "run_id": "run-1",
                    "finding_type": "advisory",
                    "severity": "warning",
                    "category": "retrieval",
                    "code": "retrieval_miss",
                    "message": "No datapoints were retrieved for this run.",
                    "details": {},
                    "created_at": datetime.now(UTC),
                    "route": "sql",
                    "query": "How many orders?",
                }
            ],
        }

        with patch("backend.api.main.app_state", {"run_store": run_store}):
            response = self.client.get("/api/v1/quality/summary?window_hours=24")

        assert response.status_code == 200
        body = response.json()
        assert body["total_findings"] == 4
        assert body["code_breakdown"][0]["code"] == "retrieval_miss"
        assert body["recent_findings"][0]["route"] == "sql"
        run_store.summarize_quality.assert_awaited_once_with(window_hours=24)

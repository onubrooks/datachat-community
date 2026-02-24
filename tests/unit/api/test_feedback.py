"""Unit tests for feedback endpoints."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from backend.api.main import app


class TestFeedbackEndpoints:
    def setup_method(self) -> None:
        self.client = TestClient(app)

    def test_submit_feedback_persists_to_system_store(self) -> None:
        feedback_id = uuid4()
        created_at = datetime.now(UTC)
        feedback_store = AsyncMock()
        feedback_store.create_feedback = AsyncMock(return_value=(feedback_id, created_at))

        with patch("backend.api.main.app_state", {"feedback_store": feedback_store}):
            response = self.client.post(
                "/api/v1/feedback",
                json={
                    "category": "answer_feedback",
                    "sentiment": "up",
                    "message_id": "msg_1",
                    "answer": "Looks right",
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["feedback_id"] == str(feedback_id)
        assert payload["saved_to"] == "system_database"
        feedback_store.create_feedback.assert_awaited_once()

    def test_submit_feedback_falls_back_to_logs_when_store_unavailable(self) -> None:
        with patch("backend.api.main.app_state", {"feedback_store": None}):
            response = self.client.post(
                "/api/v1/feedback",
                json={
                    "category": "issue_report",
                    "message_id": "msg_2",
                    "message": "Wrong table selected",
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["saved_to"] == "logs_only"
        assert payload["feedback_id"]
        assert payload["created_at"]

    def test_feedback_summary_returns_store_aggregation(self) -> None:
        feedback_store = AsyncMock()
        feedback_store.get_summary = AsyncMock(
            return_value={
                "window_days": 14,
                "totals": [
                    {"category": "answer_feedback", "sentiment": "up", "count": 8},
                    {"category": "issue_report", "sentiment": None, "count": 2},
                ],
            }
        )

        with patch("backend.api.main.app_state", {"feedback_store": feedback_store}):
            response = self.client.get("/api/v1/feedback/summary?days=14")

        assert response.status_code == 200
        payload = response.json()
        assert payload["window_days"] == 14
        assert len(payload["totals"]) == 2
        feedback_store.get_summary.assert_awaited_once_with(days=14)

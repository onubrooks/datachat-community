"""Unit tests for UI conversation persistence endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from backend.api.main import app

client = TestClient(app)


class TestConversationsRoutes:
    """Coverage for /api/v1/conversations endpoints."""

    def test_list_returns_empty_without_store(self) -> None:
        with patch("backend.api.main.app_state", {"conversation_store": None}):
            response = client.get("/api/v1/conversations")

        assert response.status_code == 200
        assert response.json() == []

    def test_upsert_falls_back_without_store(self) -> None:
        messages = [{"role": "user", "content": f"msg-{idx}"} for idx in range(55)]
        payload = {
            "title": "Revenue review",
            "target_database_id": "db_pg",
            "conversation_id": "conv_123",
            "session_summary": "Summary",
            "session_state": {"last_goal": "revenue"},
            "messages": messages,
        }
        with patch("backend.api.main.app_state", {"conversation_store": None}):
            response = client.put("/api/v1/conversations/session_a", json=payload)

        assert response.status_code == 200
        body = response.json()
        assert body["frontend_session_id"] == "session_a"
        assert body["title"] == "Revenue review"
        assert len(body["messages"]) == 50
        assert body["messages"][0]["content"] == "msg-5"
        assert body["messages"][-1]["content"] == "msg-54"
        assert body["created_at"] is not None
        assert body["updated_at"] is not None

    def test_list_and_delete_with_store(self) -> None:
        store = AsyncMock()
        store.list_conversations.return_value = [
            {
                "frontend_session_id": "session_a",
                "title": "Revenue review",
                "target_database_id": "db_pg",
                "conversation_id": "conv_123",
                "session_summary": "Summary",
                "session_state": {"last_goal": "revenue"},
                "messages": [{"role": "user", "content": "Show revenue"}],
                "created_at": "2026-02-20T00:00:00+00:00",
                "updated_at": "2026-02-20T00:05:00+00:00",
            }
        ]
        store.delete_conversation.return_value = True

        with patch("backend.api.main.app_state", {"conversation_store": store}):
            list_response = client.get("/api/v1/conversations?limit=10")
            delete_response = client.delete("/api/v1/conversations/session_a")

        assert list_response.status_code == 200
        assert delete_response.status_code == 200
        assert len(list_response.json()) == 1
        assert delete_response.json() == {"ok": True, "deleted": True}
        store.list_conversations.assert_awaited_once_with(limit=10)
        store.delete_conversation.assert_awaited_once_with("session_a")

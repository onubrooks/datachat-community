"""Unit tests for tools API routes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from backend.api.main import app
from backend.config import get_settings


class TestToolsEndpoint:
    """Test suite for tools endpoints."""

    def setup_method(self):
        self.client = TestClient(app)

    def test_list_tools_exposes_typed_parameter_schema(self):
        response = self.client.get("/api/v1/tools")
        assert response.status_code == 200
        payload = response.json()
        by_name = {item["name"]: item for item in payload}
        assert "get_table_sample" in by_name
        limit_schema = by_name["get_table_sample"]["parameters_schema"]["properties"]["limit"]
        assert limit_schema["type"] == "integer"

    def test_execute_tool_injects_context_metadata(self):
        manager = AsyncMock()
        manager.get_default_connection = AsyncMock(
            return_value=SimpleNamespace(
                database_type="clickhouse",
                database_url=SimpleNamespace(
                    get_secret_value=lambda: "clickhouse://u:p@host:8123/db"
                ),
            )
        )
        pipeline = SimpleNamespace(retriever=object())
        executor_mock = AsyncMock(return_value={"result": {"ok": True}})

        with (
            patch(
                "backend.api.main.app_state",
                {
                    "pipeline": pipeline,
                    "database_manager": manager,
                    "connector": "connector-instance",
                },
            ),
            patch("backend.api.routes.tools.ToolExecutor.execute", new=executor_mock),
        ):
            response = self.client.post(
                "/api/v1/tools/execute",
                json={"name": "list_tables", "arguments": {"schema": "public"}},
            )

        assert response.status_code == 200
        _, _, ctx = executor_mock.await_args.args
        assert ctx.metadata["retriever"] is pipeline.retriever
        assert ctx.metadata["database_type"] == "clickhouse"
        assert ctx.metadata["database_url"] == "clickhouse://u:p@host:8123/db"
        assert ctx.metadata["connector"] == "connector-instance"

    def test_execute_tool_respects_target_database_override(self):
        manager = AsyncMock()
        manager.get_connection = AsyncMock(
            return_value=SimpleNamespace(
                database_type="postgresql",
                database_url=SimpleNamespace(
                    get_secret_value=lambda: "postgresql://u:p@db.example.com:5432/app"
                ),
            )
        )
        manager.get_default_connection = AsyncMock(return_value=None)
        executor_mock = AsyncMock(return_value={"result": {"ok": True}})

        with (
            patch(
                "backend.api.main.app_state",
                {"pipeline": None, "database_manager": manager, "connector": None},
            ),
            patch("backend.api.routes.tools.ToolExecutor.execute", new=executor_mock),
        ):
            response = self.client.post(
                "/api/v1/tools/execute",
                json={
                    "name": "list_tables",
                    "arguments": {},
                    "target_database": "db-123",
                },
            )

        assert response.status_code == 200
        manager.get_connection.assert_awaited_once_with("db-123")
        _, _, ctx = executor_mock.await_args.args
        assert ctx.metadata["target_database"] == "db-123"
        assert ctx.metadata["database_type"] == "postgresql"

    def test_execute_tool_target_database_not_found(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/app")
        get_settings.cache_clear()
        executor_mock = AsyncMock(return_value={"result": {"ok": True}})

        with (
            patch(
                "backend.api.main.app_state",
                {"pipeline": None, "database_manager": None, "connector": None},
            ),
            patch("backend.api.routes.tools.ToolExecutor.execute", new=executor_mock),
        ):
            response = self.client.post(
                "/api/v1/tools/execute",
                json={
                    "name": "list_tables",
                    "arguments": {},
                    "target_database": "db-123",
                },
            )

        assert response.status_code == 404
        assert "Database connection not found: db-123" in response.json()["detail"]
        assert executor_mock.await_count == 0

    def test_execute_tool_fallback_infers_database_type_from_url(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "mysql://u:p@localhost:3306/app")
        get_settings.cache_clear()
        executor_mock = AsyncMock(return_value={"result": {"ok": True}})

        with (
            patch(
                "backend.api.main.app_state",
                {"pipeline": None, "database_manager": None, "connector": None},
            ),
            patch("backend.api.routes.tools.ToolExecutor.execute", new=executor_mock),
        ):
            response = self.client.post(
                "/api/v1/tools/execute",
                json={"name": "list_tables", "arguments": {}},
            )

        assert response.status_code == 200
        _, _, ctx = executor_mock.await_args.args
        assert ctx.metadata["database_type"] == "mysql"
        assert ctx.metadata["database_url"] == "mysql://u:p@localhost:3306/app"

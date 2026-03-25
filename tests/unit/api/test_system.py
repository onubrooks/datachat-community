"""
Unit tests for system initialization endpoints.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app
from backend.initialization.initializer import SetupStep, SystemStatus


class TestSystemEndpoints:
    """Test system status and initialization endpoints."""

    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.fixture
    def not_initialized_status(self):
        return SystemStatus(
            is_initialized=False,
            has_databases=False,
            has_system_database=False,
            has_datapoints=False,
            setup_required=[
                SetupStep(
                    step="database_connection",
                    title="Connect a database",
                    description="Configure the database connection used for queries.",
                    action="configure_database",
                ),
                SetupStep(
                    step="system_database",
                    title="System database (optional)",
                    description="Configure SYSTEM_DATABASE_URL to enable registry/profiling.",
                    action="configure_system_database",
                ),
                SetupStep(
                    step="datapoints",
                    title="Load DataPoints",
                    description="Add DataPoints describing your schema and business logic.",
                    action="load_datapoints",
                ),
            ],
        )

    @pytest.fixture
    def credentials_only_status(self):
        return SystemStatus(
            is_initialized=True,
            has_databases=True,
            has_system_database=False,
            has_datapoints=False,
            setup_required=[
                SetupStep(
                    step="datapoints",
                    title="Load DataPoints (Recommended)",
                    description="Optional enrichment for higher answer quality.",
                    action="load_datapoints",
                ),
            ],
        )

    @pytest.mark.asyncio
    async def test_status_returns_initialization_state(self, client, not_initialized_status):
        with patch(
            "backend.api.routes.system.SystemInitializer.status",
            new=AsyncMock(return_value=not_initialized_status),
        ):
            response = client.get("/api/v1/system/status")
            assert response.status_code == 200
            data = response.json()
            assert data["is_initialized"] is False
            assert data["has_databases"] is False
            assert data["has_system_database"] is False
            assert data["has_datapoints"] is False
            assert len(data["setup_required"]) == 3

    @pytest.mark.asyncio
    async def test_status_returns_setup_steps(self, client, not_initialized_status):
        with patch(
            "backend.api.routes.system.SystemInitializer.status",
            new=AsyncMock(return_value=not_initialized_status),
        ):
            response = client.get("/api/v1/system/status")
            data = response.json()
            steps = {step["step"] for step in data["setup_required"]}
            assert "database_connection" in steps
            assert "system_database" in steps
            assert "datapoints" in steps

    @pytest.mark.asyncio
    async def test_initialize_validates_input(self, client):
        with patch(
            "backend.api.routes.system.SystemInitializer.initialize",
            new=AsyncMock(side_effect=Exception("Invalid database URL")),
        ):
            response = client.post(
                "/api/v1/system/initialize",
                json={"database_url": "not-a-url", "auto_profile": False},
            )
            assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_chat_returns_empty_state_error(self, client, not_initialized_status):
        with patch(
            "backend.api.routes.chat.SystemInitializer.status",
            new=AsyncMock(return_value=not_initialized_status),
        ):
            response = client.post("/api/v1/chat", json={"message": "Test query"})
            assert response.status_code == 400
            data = response.json()
            assert data["error"] == "system_not_initialized"
            assert "setup_steps" in data

    @pytest.mark.asyncio
    async def test_chat_allows_credentials_only_mode(self, client, credentials_only_status):
        with patch(
            "backend.api.routes.chat.SystemInitializer.status",
            new=AsyncMock(return_value=credentials_only_status),
        ):
            mock_pipeline = AsyncMock()
            mock_pipeline.run = AsyncMock(
                return_value={
                    "natural_language_answer": "Found 1 result.",
                    "validated_sql": "SELECT 1",
                    "query_result": {"rows": [{"value": 1}], "columns": ["value"]},
                    "total_latency_ms": 12.0,
                    "agent_timings": {},
                    "llm_calls": 1,
                    "retry_count": 0,
                }
            )
            with patch(
                "backend.api.main.app_state",
                {"pipeline": mock_pipeline, "database_manager": None},
            ):
                response = client.post("/api/v1/chat", json={"message": "test query"})
                assert response.status_code == 200
                assert "Live schema mode" in response.json()["answer"]

    def test_system_entry_event_accepts_valid_payload(self, client):
        response = client.post(
            "/api/v1/system/entry-event",
            json={
                "flow": "phase1_4_quickstart_ui",
                "step": "connect_database",
                "status": "started",
                "source": "ui",
                "metadata": {"connection_count": 1},
            },
        )
        assert response.status_code == 200
        assert response.json()["ok"] is True

    def test_system_entry_event_requires_flow_and_step(self, client):
        response = client.post(
            "/api/v1/system/entry-event",
            json={"status": "started"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_system_reset_clears_runtime_connector_and_pipeline(
        self, client, not_initialized_status, tmp_path
    ):
        mock_connector = AsyncMock()
        mock_vector_store = AsyncMock()
        fake_settings = SimpleNamespace(
            system_database=SimpleNamespace(url=None),
            chroma=SimpleNamespace(persist_dir=str(tmp_path / "chroma")),
        )

        with patch(
            "backend.api.main.app_state",
            {
                "pipeline": object(),
                "vector_store": mock_vector_store,
                "knowledge_graph": None,
                "connector": mock_connector,
                "database_manager": None,
                "profiling_store": None,
                "feedback_store": None,
                "conversation_store": None,
                "sync_orchestrator": None,
                "datapoint_watcher": None,
            },
        ):
            with (
                patch("backend.api.routes.system.get_settings", return_value=fake_settings),
                patch("backend.api.routes.system.clear_config"),
                patch("backend.api.routes.system.apply_config_defaults"),
                patch(
                    "backend.api.routes.system.SystemInitializer.status",
                    new=AsyncMock(return_value=not_initialized_status),
                ),
            ):
                response = client.post("/api/v1/system/reset")

        assert response.status_code == 200
        assert response.json()["is_initialized"] is False
        mock_connector.close.assert_awaited_once()
        mock_vector_store.clear.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_system_reset_truncates_quality_findings_when_system_db_is_configured(
        self, client, not_initialized_status, tmp_path
    ):
        mock_runtime_connector = AsyncMock()
        mock_vector_store = AsyncMock()
        mock_db_connector = AsyncMock()
        fake_settings = SimpleNamespace(
            system_database=SimpleNamespace(url="postgresql://postgres:@localhost:5432/datachat"),
            chroma=SimpleNamespace(persist_dir=str(tmp_path / "chroma")),
        )

        with patch(
            "backend.api.main.app_state",
            {
                "pipeline": object(),
                "vector_store": mock_vector_store,
                "knowledge_graph": None,
                "connector": mock_runtime_connector,
                "database_manager": None,
                "profiling_store": None,
                "feedback_store": None,
                "conversation_store": None,
                "sync_orchestrator": None,
                "datapoint_watcher": None,
            },
        ):
            with (
                patch("backend.api.routes.system.get_settings", return_value=fake_settings),
                patch("backend.api.routes.system.PostgresConnector", return_value=mock_db_connector),
                patch("backend.api.routes.system.clear_config"),
                patch("backend.api.routes.system.apply_config_defaults"),
                patch(
                    "backend.api.routes.system.SystemInitializer.status",
                    new=AsyncMock(return_value=not_initialized_status),
                ),
            ):
                response = client.post("/api/v1/system/reset")

        assert response.status_code == 200
        mock_db_connector.connect.assert_awaited_once()
        mock_db_connector.close.assert_awaited_once()
        executed_sql = mock_db_connector.execute.await_args.args[0]
        assert "ai_quality_findings" in executed_sql
        assert "ai_runs" in executed_sql
        mock_vector_store.clear.assert_awaited_once()

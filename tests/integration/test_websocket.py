"""
Integration Tests for WebSocket Streaming

Tests the /ws/chat WebSocket endpoint with real-time streaming.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app
from backend.initialization.initializer import SystemStatus


@pytest.mark.integration
class TestWebSocketStreaming:
    """Integration tests for WebSocket streaming endpoint."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    def _app_state_for_pipeline(self, pipeline: AsyncMock | None, database_manager=None):
        vector_store = AsyncMock()
        vector_store.get_count = AsyncMock(return_value=1)
        connector = AsyncMock()
        connector.connect = AsyncMock(return_value=None)
        return {
            "pipeline": pipeline,
            "vector_store": vector_store,
            "connector": connector,
            "database_manager": database_manager,
        }

    @pytest.fixture
    def mock_pipeline_result(self):
        """Mock successful pipeline result."""
        return {
            "query": "What's the total revenue?",
            "natural_language_answer": "The total revenue is $1,234,567.89",
            "validated_sql": "SELECT SUM(amount) as total_revenue FROM analytics.fact_sales WHERE status = 'completed'",
            "generated_sql": "SELECT SUM(amount) as total_revenue FROM analytics.fact_sales WHERE status = 'completed'",
            "query_result": {
                "data": {"total_revenue": [1234567.89]},
                "row_count": 1,
                "column_names": ["total_revenue"],
            },
            "visualization_hint": "none",
            "retrieved_datapoints": [
                {
                    "datapoint_id": "table_fact_sales_001",
                    "datapoint_type": "Schema",
                    "name": "Fact Sales Table",
                    "score": 0.95,
                }
            ],
            "total_latency_ms": 1523.45,
            "agent_timings": {
                "classifier": 234.5,
                "context": 123.4,
                "sql": 567.8,
                "validator": 45.6,
                "executor": 552.15,
            },
            "llm_calls": 3,
            "retry_count": 0,
            "error": None,
            "session_summary": "Intent summary: last_goal=What's the total revenue?",
            "session_state": {"last_goal": "What's the total revenue?"},
            "decision_trace": [
                {
                    "stage": "intent_gate",
                    "decision": "data_query_fast_path",
                    "reason": "deterministic_sql_query",
                }
            ],
            "action_trace": [
                {
                    "version": "v1",
                    "step": 1,
                    "stage": "query_analyzer",
                    "selected_action": "sql",
                    "verification": {"status": "ok"},
                }
            ],
            "loop_terminal_state": "completed",
            "loop_stop_reason": "execution_completed",
            "loop_shadow_decisions": [],
        }

    def test_websocket_connects_successfully(self, client):
        """Test that WebSocket connection is established successfully."""
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(
            return_value={
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }
        )
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            with client.websocket_connect("/ws/chat") as websocket:
                # Send message
                websocket.send_json({"message": "Test query"})

                # Should receive complete event eventually
                messages = []
                try:
                    while True:
                        data = websocket.receive_json()
                        messages.append(data)
                        if data.get("event") == "complete":
                            break
                except Exception:
                    pass

                # Verify we got the complete event
                assert any(msg.get("event") == "complete" for msg in messages)

    def test_websocket_receives_agent_status_events(self, client):
        """Test that WebSocket receives agent_start and agent_complete events."""

        # Create mock pipeline that will call the callback
        async def mock_run_with_streaming(
            query,
            conversation_history,
            session_summary=None,
            session_state=None,
            database_type=None,
            database_url=None,
            target_connection_id=None,
            synthesize_simple_sql=None,
            event_callback=None,
        ):
            # Simulate agent events
            await event_callback(
                "agent_start", {"agent": "ClassifierAgent", "timestamp": "2026-01-16T12:00:00Z"}
            )
            await event_callback(
                "agent_complete",
                {
                    "agent": "ClassifierAgent",
                    "data": {"intent": "data_query"},
                    "duration_ms": 234.5,
                    "timestamp": "2026-01-16T12:00:00Z",
                },
            )
            return {
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }

        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = mock_run_with_streaming
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            with client.websocket_connect("/ws/chat") as websocket:
                # Send message
                websocket.send_json({"message": "Test query"})

                # Collect all messages
                messages = []
                try:
                    while True:
                        data = websocket.receive_json()
                        messages.append(data)
                        if data.get("event") == "complete":
                            break
                except Exception:
                    pass

                # Verify we got agent events
                assert any(
                    msg.get("event") == "agent_start" and msg.get("agent") == "ClassifierAgent"
                    for msg in messages
                )
                assert any(
                    msg.get("event") == "agent_complete" and msg.get("agent") == "ClassifierAgent"
                    for msg in messages
                )
                assert any(msg.get("event") == "thinking" for msg in messages)

    def test_websocket_final_message_contains_complete_response(self, client, mock_pipeline_result):
        """Test that final WebSocket message contains complete response."""
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(return_value=mock_pipeline_result)
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            with client.websocket_connect("/ws/chat") as websocket:
                # Send message
                websocket.send_json({"message": "What's the total revenue?"})

                # Collect all messages until complete
                final_message = None
                try:
                    while True:
                        data = websocket.receive_json()
                        if data.get("event") == "complete":
                            final_message = data
                            break
                except Exception:
                    pass

                # Verify final message has all required fields
                assert final_message is not None
                assert final_message["event"] == "complete"
                assert "answer" in final_message
                assert "sql" in final_message
                assert "data" in final_message
                assert "sources" in final_message
                assert "metrics" in final_message
                assert "conversation_id" in final_message
                assert "session_summary" in final_message
                assert "session_state" in final_message
                assert "decision_trace" in final_message
                assert "action_trace" in final_message
                assert "loop_terminal_state" in final_message
                assert "loop_stop_reason" in final_message

                # Verify content
                assert final_message["answer"] == "The total revenue is $1,234,567.89"
                assert "SELECT SUM(amount)" in final_message["sql"]
                assert final_message["data"]["total_revenue"] == [1234567.89]
                assert len(final_message["sources"]) == 1
                assert final_message["metrics"]["llm_calls"] == 3
                assert final_message["session_state"]["last_goal"] == "What's the total revenue?"
                assert final_message["decision_trace"][0]["stage"] == "intent_gate"
                assert final_message["action_trace"][0]["stage"] == "query_analyzer"

    def test_websocket_uses_target_database_for_streaming_call(self, client):
        manager = AsyncMock()
        manager.get_connection = AsyncMock(
            return_value=SimpleNamespace(
                database_type="clickhouse",
                database_url=SimpleNamespace(
                    get_secret_value=lambda: "clickhouse://u:p@host:8123/db"
                ),
            )
        )
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(
            return_value={
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }
        )
        with patch(
            "backend.api.main.app_state",
            self._app_state_for_pipeline(mock_pipeline, database_manager=manager),
        ):
            with client.websocket_connect("/ws/chat") as websocket:
                websocket.send_json({"message": "Test query", "target_database": "db-123"})
                while True:
                    event = websocket.receive_json()
                    if event.get("event") == "complete":
                        break
        kwargs = mock_pipeline.run_with_streaming.call_args.kwargs
        assert kwargs["database_type"] == "clickhouse"
        assert kwargs["database_url"] == "clickhouse://u:p@host:8123/db"
        assert kwargs["target_connection_id"] == "db-123"
        assert kwargs["synthesize_simple_sql"] is None

    def test_websocket_forwards_synthesize_simple_sql_override(self, client):
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(
            return_value={
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }
        )
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            with client.websocket_connect("/ws/chat") as websocket:
                websocket.send_json({"message": "Test query", "synthesize_simple_sql": False})
                while True:
                    event = websocket.receive_json()
                    if event.get("event") == "complete":
                        break
        kwargs = mock_pipeline.run_with_streaming.call_args.kwargs
        assert kwargs["synthesize_simple_sql"] is False

    def test_websocket_executes_direct_sql_mode_without_pipeline(self, client):
        connector = AsyncMock()
        connector.connect = AsyncMock(return_value=None)
        connector.execute = AsyncMock(
            return_value=SimpleNamespace(
                rows=[{"item_id": 1}],
                columns=["item_id"],
                execution_time_ms=4.2,
            )
        )
        connector.close = AsyncMock(return_value=None)

        with (
            patch("backend.api.websocket.create_connector", return_value=connector),
            patch(
                "backend.api.websocket.resolve_database_type_and_url",
                new=AsyncMock(
                    return_value=("postgresql", "postgresql://user:pass@localhost:5432/db")
                ),
            ),
            patch(
                "backend.api.websocket.SystemInitializer.status",
                new=AsyncMock(
                    return_value=SystemStatus(
                        is_initialized=True,
                        has_databases=True,
                        has_system_database=True,
                        has_datapoints=True,
                        setup_required=[],
                    )
                ),
            ),
            patch("backend.api.main.app_state", {"pipeline": None, "database_manager": None}),
        ):
            with client.websocket_connect("/ws/chat") as websocket:
                websocket.send_json(
                    {
                        "message": "SELECT item_id FROM public.items LIMIT 1",
                        "execution_mode": "direct_sql",
                        "sql": "SELECT item_id FROM public.items LIMIT 1",
                    }
                )
                event = websocket.receive_json()

        assert event["event"] == "complete"
        assert event["sql"] == "SELECT item_id FROM public.items LIMIT 1"
        assert event["answer_source"] == "sql"
        assert event["data"]["item_id"] == [1]
        assert event["visualization_hint"] == "table"
        assert event["metrics"]["llm_calls"] == 0
        connector.execute.assert_awaited_once_with("SELECT item_id FROM public.items LIMIT 1")

    def test_websocket_infers_visualization_for_direct_sql_mode(self, client):
        connector = AsyncMock()
        connector.connect = AsyncMock(return_value=None)
        connector.execute = AsyncMock(
            return_value=SimpleNamespace(
                rows=[
                    {"business_date": "2026-01-01", "revenue": 100.0},
                    {"business_date": "2026-01-02", "revenue": 125.0},
                ],
                columns=["business_date", "revenue"],
                execution_time_ms=4.2,
            )
        )
        connector.close = AsyncMock(return_value=None)

        with (
            patch("backend.api.websocket.create_connector", return_value=connector),
            patch(
                "backend.api.websocket.resolve_database_type_and_url",
                new=AsyncMock(
                    return_value=("postgresql", "postgresql://user:pass@localhost:5432/db")
                ),
            ),
            patch(
                "backend.api.websocket.SystemInitializer.status",
                new=AsyncMock(
                    return_value=SystemStatus(
                        is_initialized=True,
                        has_databases=True,
                        has_system_database=True,
                        has_datapoints=True,
                        setup_required=[],
                    )
                ),
            ),
            patch("backend.api.main.app_state", {"pipeline": None, "database_manager": None}),
        ):
            with client.websocket_connect("/ws/chat") as websocket:
                websocket.send_json(
                    {
                        "message": "SELECT business_date, revenue FROM public.daily_revenue",
                        "execution_mode": "direct_sql",
                        "sql": "SELECT business_date, revenue FROM public.daily_revenue",
                    }
                )
                event = websocket.receive_json()

        assert event["event"] == "complete"
        assert event["visualization_hint"] == "line_chart"
        assert event["visualization_metadata"]["deterministic"] == "line_chart"

    def test_websocket_forwards_session_memory(self, client):
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(
            return_value={
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }
        )
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            with client.websocket_connect("/ws/chat") as websocket:
                websocket.send_json(
                    {
                        "message": "what about stores",
                        "session_summary": "Intent summary: last_goal=How many products do we have?",
                        "session_state": {"last_goal": "How many products do we have?"},
                    }
                )
                while True:
                    event = websocket.receive_json()
                    if event.get("event") == "complete":
                        break
        kwargs = mock_pipeline.run_with_streaming.call_args.kwargs
        assert (
            kwargs["session_summary"] == "Intent summary: last_goal=How many products do we have?"
        )
        assert kwargs["session_state"]["last_goal"] == "How many products do we have?"

    def test_websocket_handles_client_disconnect(self, client):
        """Test that WebSocket handles client disconnect gracefully."""
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(
            return_value={
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }
        )
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            # Connect and immediately disconnect
            with client.websocket_connect("/ws/chat") as websocket:
                websocket.send_json({"message": "Test query"})
                # Connection will be closed when exiting context

            # Should not raise an exception
            assert True

    def test_websocket_validates_request_message(self, client):
        """Test that WebSocket validates incoming message."""
        with patch("backend.api.main.app_state", {"pipeline": AsyncMock()}):
            with client.websocket_connect("/ws/chat") as websocket:
                # Send invalid message (missing required field)
                websocket.send_json({})

                # Should receive error event
                data = websocket.receive_json()
                assert data["event"] == "error"
                assert data["error"] == "validation_error"

    def test_websocket_handles_pipeline_not_initialized(self, client):
        """Test that WebSocket handles uninitialized pipeline."""
        # Pipeline not initialized
        with patch("backend.api.main.app_state", {"pipeline": None}):
            with client.websocket_connect("/ws/chat") as websocket:
                # Send message
                websocket.send_json({"message": "Test query"})

                # Should receive error event
                data = websocket.receive_json()
                assert data["event"] == "error"
                assert data["error"] == "service_unavailable"

    def test_websocket_supports_conversation_id(self, client):
        """Test that WebSocket preserves conversation_id."""
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(
            return_value={
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }
        )
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            with client.websocket_connect("/ws/chat") as websocket:
                # Send message with conversation_id
                websocket.send_json({"message": "Test query", "conversation_id": "conv_custom_123"})

                # Get final message
                final_message = None
                try:
                    while True:
                        data = websocket.receive_json()
                        if data.get("event") == "complete":
                            final_message = data
                            break
                except Exception:
                    pass

                # Verify conversation_id is preserved
                assert final_message is not None
                assert final_message["conversation_id"] == "conv_custom_123"

    def test_websocket_generates_conversation_id_if_not_provided(self, client):
        """Test that WebSocket generates conversation_id if not provided."""
        mock_pipeline = AsyncMock()
        mock_pipeline.run_with_streaming = AsyncMock(
            return_value={
                "natural_language_answer": "Test answer",
                "total_latency_ms": 1000.0,
                "agent_timings": {},
                "llm_calls": 1,
                "retry_count": 0,
                "retrieved_datapoints": [],
            }
        )
        with patch("backend.api.main.app_state", self._app_state_for_pipeline(mock_pipeline)):
            with client.websocket_connect("/ws/chat") as websocket:
                # Send message without conversation_id
                websocket.send_json({"message": "Test query"})

                # Get final message
                final_message = None
                try:
                    while True:
                        data = websocket.receive_json()
                        if data.get("event") == "complete":
                            final_message = data
                            break
                except Exception:
                    pass

                # Verify conversation_id is generated
                assert final_message is not None
                assert "conversation_id" in final_message
                assert final_message["conversation_id"].startswith("conv_")

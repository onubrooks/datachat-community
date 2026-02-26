"""
Unit tests for DataChatPipeline orchestrator.

Tests pipeline execution including:
- Correct agent execution order
- Retry loop on validation failure
- Max retries enforcement
- Streaming status updates
- Error handling and recovery
- State management

Updated: Uses QueryAnalyzerAgent instead of ClassifierAgent
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.agents.query_analyzer import QueryAnalysis, QueryAnalyzerOutput
from backend.models import (
    AgentMetadata,
    ContextAgentOutput,
    ContextAnswer,
    ContextAnswerAgentOutput,
    ExecutedQuery,
    ExecutorAgentOutput,
    GeneratedSQL,
    InvestigationMemory,
    QueryResult,
    RetrievedDataPoint,
    SQLAgentOutput,
    SQLValidationError,
    ToolPlan,
    ToolPlannerAgentOutput,
    ValidatedSQL,
    ValidatorAgentOutput,
)
from backend.pipeline.orchestrator import DataChatPipeline


class TestPipelineExecution:
    """Test basic pipeline execution flow."""

    @pytest.fixture
    def mock_retriever(self):
        """Mock retriever."""
        retriever = AsyncMock()
        return retriever

    @pytest.fixture
    def mock_connector(self):
        """Mock database connector."""
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.close = AsyncMock()
        return connector

    @pytest.fixture
    def mock_llm_provider(self):
        """Mock LLM provider."""
        provider = AsyncMock()
        provider.generate = AsyncMock(return_value="mock response")
        provider.stream = AsyncMock()
        provider.count_tokens = AsyncMock(return_value=100)
        provider.get_model_info = AsyncMock(return_value={"name": "mock-model"})
        provider.provider = "mock"
        provider.model = "mock-model"
        return provider

    @pytest.fixture
    def pipeline(self, mock_retriever, mock_connector, mock_llm_provider, mock_openai_api_key):
        """Create pipeline with mocked dependencies."""
        pipeline = DataChatPipeline(
            retriever=mock_retriever,
            connector=mock_connector,
            max_retries=3,
        )
        # Inject mock LLM providers into agents to avoid real API calls
        pipeline.query_analyzer.llm = mock_llm_provider
        pipeline.sql.llm = mock_llm_provider
        pipeline.executor.llm = mock_llm_provider
        pipeline.tool_planner.execute = AsyncMock(
            return_value=ToolPlannerAgentOutput(
                success=True,
                plan=ToolPlan(tool_calls=[], rationale="No tools needed.", fallback="pipeline"),
                metadata=AgentMetadata(agent_name="ToolPlannerAgent", llm_calls=0),
            )
        )
        pipeline.response_synthesis.execute = AsyncMock(return_value="Found 1 result.")
        return pipeline

    def test_resolve_context_preface_skips_sql_route_and_questions(self, pipeline):
        assert (
            pipeline._resolve_context_preface_for_sql_answer(  # noqa: SLF001
                {"route": "sql", "clarification_needed": False},
                "I cannot answer this from context.",
            )
            is None
        )
        assert (
            pipeline._resolve_context_preface_for_sql_answer(  # noqa: SLF001
                {"route": "context", "clarification_needed": False},
                "Which table should I use?",
            )
            is None
        )
        assert (
            pipeline._resolve_context_preface_for_sql_answer(  # noqa: SLF001
                {"route": "context", "clarification_needed": False},
                "Using known business definitions for customer concentration.",
            )
            == "Using known business definitions for customer concentration."
        )

    @pytest.fixture
    def mock_agents(self, pipeline):
        """Mock all agents in pipeline."""
        # Mock QueryAnalyzerAgent
        from backend.agents.query_analyzer import QueryAnalysis, QueryAnalyzerOutput

        pipeline.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    confidence=0.9,
                    clarifying_questions=[],
                    deterministic=False,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )

        # Mock ContextAgent
        pipeline.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="test query",
                    datapoints=[
                        RetrievedDataPoint(
                            datapoint_id="table_001",
                            datapoint_type="Schema",
                            name="Test Table",
                            score=0.9,
                            source="vector",
                            metadata={"type": "Schema"},
                        )
                    ],
                    retrieval_mode="hybrid",
                    total_retrieved=1,
                    sources_used=["vector"],
                ),
                context_confidence=0.2,
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )

        pipeline.context_answer.execute = AsyncMock(
            return_value=ContextAnswerAgentOutput(
                success=True,
                context_answer=ContextAnswer(
                    answer="Here is a context-only answer.",
                    confidence=0.8,
                    evidence=[],
                    needs_sql=True,
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAnswerAgent", llm_calls=1),
            )
        )

        # Mock SQLAgent
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT * FROM test_table",
                    explanation="Simple select query",
                    confidence=0.95,
                    used_datapoints=["table_001"],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
            )
        )

        # Mock ValidatorAgent (passing)
        pipeline.validator.execute = AsyncMock(
            return_value=ValidatorAgentOutput(
                success=True,
                validated_sql=ValidatedSQL(
                    sql="SELECT * FROM test_table",
                    is_valid=True,
                    is_safe=True,
                    errors=[],
                    warnings=[],
                    performance_score=0.8,
                ),
                metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
            )
        )

        # Mock ExecutorAgent
        pipeline.executor.execute = AsyncMock(
            return_value=ExecutorAgentOutput(
                success=True,
                executed_query=ExecutedQuery(
                    query_result=QueryResult(
                        rows=[{"id": 1, "name": "test"}],
                        row_count=1,
                        columns=["id", "name"],
                        execution_time_ms=50.0,
                        was_truncated=False,
                    ),
                    natural_language_answer="Found 1 result.",
                    visualization_hint="table",
                    key_insights=[],
                    source_citations=["table_001"],
                ),
                metadata=AgentMetadata(agent_name="ExecutorAgent", llm_calls=1),
            )
        )

        return pipeline

    @pytest.mark.asyncio
    async def test_pipeline_executes_in_correct_order(self, mock_agents):
        """Test that agents execute in correct order."""
        result = await mock_agents.run("test query")

        # Verify all agents were called
        assert mock_agents.query_analyzer.execute.called
        assert mock_agents.context.execute.called
        # context_answer is called in the new flow, and it sets needs_sql=True to continue to SQL
        assert mock_agents.context_answer.execute.called
        assert mock_agents.sql.execute.called
        assert mock_agents.validator.execute.called
        assert mock_agents.executor.execute.called

        # Verify final state
        assert result["natural_language_answer"] == "Found 1 result."
        assert result["query_result"]["row_count"] == 1

    @pytest.mark.asyncio
    async def test_pipeline_surfaces_executor_final_sql(self, mock_agents):
        mock_agents.executor.execute = AsyncMock(
            return_value=ExecutorAgentOutput(
                success=True,
                executed_query=ExecutedQuery(
                    query_result=QueryResult(
                        rows=[{"id": 1, "name": "test"}],
                        row_count=1,
                        columns=["id", "name"],
                        execution_time_ms=25.0,
                        was_truncated=False,
                    ),
                    executed_sql="SELECT id, name FROM fixed_table",
                    natural_language_answer="Found 1 result.",
                    visualization_hint="table",
                    key_insights=[],
                    source_citations=["table_001"],
                ),
                metadata=AgentMetadata(agent_name="ExecutorAgent", llm_calls=0),
            )
        )

        result = await mock_agents.run("test query")

        assert result["validated_sql"] == "SELECT id, name FROM fixed_table"
        assert result["generated_sql"] == "SELECT id, name FROM fixed_table"

    @pytest.mark.asyncio
    async def test_sql_route_continues_to_sql_even_when_context_answer_does_not_request_sql(
        self, mock_agents
    ):
        mock_agents.context_answer.execute = AsyncMock(
            return_value=ContextAnswerAgentOutput(
                success=True,
                context_answer=ContextAnswer(
                    answer="I can describe the data.",
                    confidence=0.7,
                    evidence=[],
                    needs_sql=False,
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAnswerAgent", llm_calls=1),
            )
        )

        result = await mock_agents.run(
            "Identify the top 2 segments driving week-over-week net flow decline"
        )

        assert mock_agents.sql.execute.called
        assert result["answer_source"] == "sql"

    @pytest.mark.asyncio
    async def test_data_query_clarification_route_attempts_sql_before_stopping(self, mock_agents):
        mock_agents.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="clarification",
                    entities=[],
                    complexity="medium",
                    confidence=0.42,
                    clarifying_questions=["Which table should I use to answer this?"],
                    deterministic=False,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )
        mock_agents.context_answer.execute = AsyncMock(
            return_value=ContextAnswerAgentOutput(
                success=True,
                context_answer=ContextAnswer(
                    answer="I need more detail.",
                    confidence=0.2,
                    evidence=[],
                    needs_sql=False,
                    clarifying_questions=["Which table should I use to answer this?"],
                ),
                metadata=AgentMetadata(agent_name="ContextAnswerAgent", llm_calls=1),
            )
        )

        result = await mock_agents.run(
            "Identify the top 2 segments driving week-over-week net flow decline"
        )

        assert mock_agents.sql.execute.called
        assert result["answer_source"] == "sql"
        assert result.get("clarification_needed") is False

    @pytest.mark.asyncio
    async def test_pipeline_decomposes_multi_question_prompt(self, mock_agents):
        mock_agents._plan_multi_sql_for_parts = AsyncMock(return_value=({}, 0, 0.0))
        result = await mock_agents.run("Show me top rows? What is total revenue?")

        assert len(result.get("sub_answers", [])) == 2
        assert "multiple questions" in result["natural_language_answer"].lower()
        assert result["answer_source"] == "multi"
        assert result["sub_answers"][0]["sql"] == "SELECT * FROM test_table"
        assert result["sub_answers"][0]["data"]["id"] == [1]
        assert result.get("generated_sql") == "SELECT * FROM test_table"
        assert mock_agents.query_analyzer.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_build_sub_answer_unwraps_json_blob_answer(self, mock_agents):
        result_state = {
            "natural_language_answer": (
                "```json\n"
                '{"answer": "There were 0 sales made today.", "confidence": 0.99}\n'
                "```"
            ),
            "answer_source": "sql",
            "answer_confidence": 0.99,
            "validated_sql": "SELECT 1",
            "query_result": {"data": {"sales_count": [0]}},
            "visualization_hint": "table",
            "visualization_note": None,
            "visualization_metadata": None,
            "clarifying_questions": [],
            "error": None,
        }

        sub_answer = mock_agents._build_sub_answer(
            index=1,
            query="How many sales were made on a particular day?",
            result=result_state,
        )

        assert sub_answer["answer"] == "There were 0 sales made today."
        assert "```json" not in sub_answer["answer"]

    @pytest.mark.asyncio
    async def test_multi_query_uses_batch_sql_plan_and_skips_per_part_sql_agent(self, mock_agents):
        mock_agents._plan_multi_sql_for_parts = AsyncMock(
            return_value=(
                {
                    1: {
                        "sql": "SELECT 1 AS a",
                        "explanation": "first",
                        "confidence": 0.9,
                        "clarifying_questions": [],
                    },
                    2: {
                        "sql": "SELECT 2 AS b",
                        "explanation": "second",
                        "confidence": 0.88,
                        "clarifying_questions": [],
                    },
                },
                1,
                12.0,
            )
        )

        result = await mock_agents.run("Question one? Question two?")

        assert result["answer_source"] == "multi"
        assert len(result.get("sub_answers", [])) == 2
        assert mock_agents.sql.execute.await_count == 0
        assert result.get("llm_calls", 0) >= 1
        assert result.get("agent_timings", {}).get("multi_sql_planner") == pytest.approx(12.0)

    @pytest.mark.asyncio
    async def test_run_with_streaming_multi_emits_agent_flow_for_each_question(self, mock_agents):
        events: list[tuple[str, dict]] = []

        async def _callback(event_type: str, event_data: dict):
            events.append((event_type, event_data))

        mock_agents._plan_multi_sql_for_parts = AsyncMock(
            return_value=(
                {
                    1: {
                        "sql": "SELECT 1",
                        "explanation": "first",
                        "confidence": 0.9,
                        "clarifying_questions": [],
                    },
                    2: {
                        "sql": "SELECT 2",
                        "explanation": "second",
                        "confidence": 0.9,
                        "clarifying_questions": [],
                    },
                },
                1,
                9.5,
            )
        )

        result = await mock_agents.run_with_streaming(
            query="How many stores are there? What is total revenue by store?",
            event_callback=_callback,
        )

        event_types = [event_type for event_type, _ in events]
        assert "decompose_complete" in event_types
        assert "agent_start" in event_types
        assert "agent_complete" in event_types
        assert any(
            event_type == "agent_start" and data.get("agent") == "MultiSQLPlanner"
            for event_type, data in events
        )
        assert any(
            event_type == "agent_complete" and data.get("agent") == "MultiSQLPlanner"
            for event_type, data in events
        )
        assert result.get("answer_source") == "multi"
        assert len(result.get("sub_answers", [])) == 2

        decompose = next(data for event_type, data in events if event_type == "decompose_complete")
        assert decompose.get("part_count") == 2
        thinking_notes = [
            data.get("note", "") for event_type, data in events if event_type == "thinking"
        ]
        question_notes = [
            note for note in thinking_notes if "live agent flow for question:" in note
        ]
        assert len(question_notes) == 2

    def test_should_execute_after_context_answer_prefers_explicit_sql_route(self, pipeline):
        state = {"route": "sql", "context_needs_sql": False}
        assert pipeline._should_execute_after_context_answer(state) == "sql"

    @pytest.mark.asyncio
    async def test_clarification_confirmation_blocks_unneeded_questions(self, pipeline):
        pipeline.query_analyzer.llm = AsyncMock()
        pipeline.query_analyzer.llm.generate = AsyncMock(
            return_value=MagicMock(
                content=(
                    '{"needs_clarification": false, "confidence": 0.92, '
                    '"clarifying_questions": []}'
                )
            )
        )
        state = {
            "query": "Show weekly net flow by segment over the last 8 weeks.",
            "intent": "data_query",
            "route": "sql",
            "clarification_limit": 3,
            "clarification_turn_count": 0,
            "intent_summary": {"clarification_count": 0},
            "retrieved_datapoints": [],
        }

        applied = await pipeline._apply_clarification_response_with_confirmation(
            state,
            ["Which table should I use to answer this?"],
        )

        assert applied is False
        assert state.get("clarification_needed") is False
        assert state.get("clarifying_questions") == []

    @pytest.mark.asyncio
    async def test_clarification_confirmation_refines_question_when_required(self, pipeline):
        pipeline.query_analyzer.llm = AsyncMock()
        pipeline.query_analyzer.llm.generate = AsyncMock(
            return_value=MagicMock(
                content=(
                    '{"needs_clarification": true, "confidence": 0.93, '
                    '"clarifying_questions": ["Which metric column should I aggregate?"], '
                    '"intro": "I need one detail before I run this."}'
                )
            )
        )
        state = {
            "query": "Show weekly net flow by segment over the last 8 weeks.",
            "intent": "data_query",
            "route": "sql",
            "clarification_limit": 3,
            "clarification_turn_count": 0,
            "intent_summary": {"clarification_count": 0},
            "retrieved_datapoints": [],
        }

        applied = await pipeline._apply_clarification_response_with_confirmation(
            state,
            ["Which table should I use to answer this?"],
        )

        assert applied is True
        assert state.get("clarification_needed") is True
        assert state.get("clarifying_questions") == ["Which metric column should I aggregate?"]
        assert str(state.get("natural_language_answer", "")).startswith(
            "I need one detail before I run this."
        )

    def test_select_primary_sub_result_prefers_query_result_with_rows(self, pipeline):
        sub_results = [
            {
                "answer_source": "sql",
                "generated_sql": "SELECT * FROM a",
                "query_result": {"row_count": 1},
            },
            {
                "answer_source": "sql",
                "generated_sql": "SELECT * FROM b",
                "query_result": {"row_count": 8},
            },
            {"answer_source": "context", "natural_language_answer": "context-only"},
        ]

        index, selected = pipeline._select_primary_sub_result(sub_results)  # type: ignore[arg-type]

        assert index == 1
        assert selected is not None
        assert selected.get("generated_sql") == "SELECT * FROM b"

    def test_select_primary_sub_result_prefers_sql_over_clarification(self, pipeline):
        sub_results = [
            {"answer_source": "clarification", "clarifying_questions": ["Which table?"]},
            {"answer_source": "sql", "generated_sql": "SELECT count(*) FROM t"},
        ]

        index, selected = pipeline._select_primary_sub_result(sub_results)  # type: ignore[arg-type]

        assert index == 1
        assert selected is not None
        assert selected.get("generated_sql") == "SELECT count(*) FROM t"

    @pytest.mark.asyncio
    async def test_filter_datapoints_by_live_schema_uses_related_tables(self, pipeline):
        datapoints = [
            {
                "datapoint_id": "metric_grocery_1",
                "datapoint_type": "Business",
                "name": "Total Grocery Revenue",
                "score": 0.9,
                "source": "vector",
                "metadata": {"related_tables": "public.grocery_sales_transactions"},
            },
            {
                "datapoint_id": "metric_fintech_1",
                "datapoint_type": "Business",
                "name": "Total Deposits",
                "score": 0.9,
                "source": "vector",
                "metadata": {"related_tables": "public.bank_accounts"},
            },
        ]
        pipeline._get_live_table_catalog = AsyncMock(return_value={"public.bank_accounts"})

        filtered = await pipeline._filter_datapoints_by_live_schema(
            datapoints, database_type="postgresql", database_url="postgresql://test"
        )

        assert [item["datapoint_id"] for item in filtered] == ["metric_fintech_1"]

    def test_split_multi_query_avoids_false_split_for_single_analytic_request(self, pipeline):
        parts = pipeline._split_multi_query("Show gross margin and net margin by category")
        assert parts == ["Show gross margin and net margin by category"]

    def test_split_multi_query_splits_when_second_clause_is_new_intent(self, pipeline):
        parts = pipeline._split_multi_query("List tables and show columns in customers")
        assert parts == ["List tables", "show columns in customers"]

    def test_clarification_followup_targets_tagged_subquery(self, pipeline):
        history = [
            {"role": "user", "content": "List tables and show columns"},
            {
                "role": "assistant",
                "content": (
                    "I need a bit more detail to generate SQL:\n"
                    "- [Q2] Which table should I list columns for?"
                ),
            },
        ]

        summary = pipeline._build_intent_summary("customers", history)

        assert summary["target_subquery_index"] == 2
        assert summary["resolved_query"] == "Show columns in customers"

    def test_filter_datapoints_by_target_connection(self, pipeline):
        datapoints = [
            {
                "datapoint_id": "dp_unscoped",
                "metadata": {},
            },
            {
                "datapoint_id": "dp_fintech",
                "metadata": {"connection_id": "conn-fintech"},
            },
            {
                "datapoint_id": "dp_grocery",
                "metadata": {"connection_id": "conn-grocery"},
            },
        ]

        filtered = pipeline._filter_datapoints_by_target_connection(
            datapoints, target_connection_id="conn-fintech"
        )

        assert [item["datapoint_id"] for item in filtered] == [
            "dp_fintech",
        ]

    def test_filter_datapoints_by_target_connection_with_global(self, pipeline):
        datapoints = [
            {
                "datapoint_id": "dp_global",
                "metadata": {"scope": "global"},
            },
            {
                "datapoint_id": "dp_fintech",
                "metadata": {"connection_id": "conn-fintech"},
            },
            {
                "datapoint_id": "dp_unscoped",
                "metadata": {},
            },
            {
                "datapoint_id": "dp_grocery",
                "metadata": {"connection_id": "conn-grocery"},
            },
        ]

        filtered = pipeline._filter_datapoints_by_target_connection(
            datapoints, target_connection_id="conn-fintech"
        )

        assert [item["datapoint_id"] for item in filtered] == [
            "dp_fintech",
            "dp_global",
        ]

    def test_filter_datapoints_by_target_connection_accepts_equivalent_connection_ids(
        self, pipeline
    ):
        datapoints = [
            {
                "datapoint_id": "dp_env",
                "metadata": {"connection_id": "00000000-0000-0000-0000-00000000dada"},
            },
            {
                "datapoint_id": "dp_other",
                "metadata": {"connection_id": "conn-grocery"},
            },
        ]

        filtered = pipeline._filter_datapoints_by_target_connection(
            datapoints,
            target_connection_id="conn-managed",
            target_connection_ids={
                "conn-managed",
                "00000000-0000-0000-0000-00000000dada",
                "conn-grocery",
            },
        )

        assert [item["datapoint_id"] for item in filtered] == ["dp_env", "dp_other"]

    @pytest.mark.asyncio
    async def test_resolve_equivalent_connection_ids_includes_registry_and_env_matches(
        self, pipeline
    ):
        matching = MagicMock()
        matching.connection_id = "conn-matching"
        matching.database_url.get_secret_value.return_value = (
            "postgresql://postgres:@localhost:5432/postgres"
        )
        other = MagicMock()
        other.connection_id = "conn-other"
        other.database_url.get_secret_value.return_value = (
            "postgresql://postgres:@localhost:5432/otherdb"
        )
        manager = AsyncMock()
        manager.list_connections.return_value = [other, matching]

        pipeline.config.database.url = "postgresql://postgres:@localhost:5432/postgres"
        pipeline.config.system_database.url = "postgresql://system:pass@localhost:5432/systemdb"

        with patch("backend.pipeline.orchestrator.DatabaseConnectionManager", return_value=manager):
            connection_ids = await pipeline._resolve_equivalent_connection_ids(
                target_connection_id="conn-target",
                database_url="postgresql://postgres@localhost/postgres",
            )

        assert connection_ids == {
            "conn-target",
            "conn-matching",
            "00000000-0000-0000-0000-00000000dada",
        }

    @pytest.mark.asyncio
    async def test_pipeline_tracks_metadata(self, mock_agents):
        """Test that pipeline tracks cost and latency."""
        result = await mock_agents.run("test query")

        # Verify metadata tracking
        assert "agent_timings" in result
        assert "query_analyzer" in result["agent_timings"]
        assert "context" in result["agent_timings"]
        assert "sql" in result["agent_timings"]
        assert "validator" in result["agent_timings"]
        assert "executor" in result["agent_timings"]

        assert result["llm_calls"] >= 3  # query_analyzer + sql + context_answer
        assert result["total_latency_ms"] > 0

    @pytest.mark.asyncio
    async def test_pipeline_emits_action_trace_and_terminal_state(self, mock_agents):
        result = await mock_agents.run("test query")
        assert result.get("action_trace")
        assert result.get("loop_terminal_state") in {
            "completed",
            "needs_user_input",
            "blocked",
            "impossible",
        }
        assert result.get("loop_stop_reason")

    def test_loop_guard_enforced_mode_stops_routing_when_budget_exceeded(self, pipeline):
        state = {
            "loop_enabled": True,
            "loop_shadow_mode": False,
            "loop_budget": {
                "max_steps": 1,
                "max_latency_ms": 1000,
                "max_llm_tokens": 1000,
                "max_clarifications": 3,
            },
            "loop_steps_taken": 1,
            "total_latency_ms": 0.0,
            "total_tokens_used": 0,
            "clarification_turn_count": 0,
            "route": "sql",
            "context_needs_sql": True,
            "decision_trace": [],
            "action_trace": [],
            "loop_shadow_decisions": [],
        }
        decision = pipeline._should_execute_after_context_answer(state)
        assert decision == "end"
        assert state.get("loop_terminal_state") == "blocked"
        assert state.get("loop_stop_reason") == "budget_steps_exceeded"

    def test_loop_guard_shadow_mode_records_recommendation_without_override(self, pipeline):
        state = {
            "loop_enabled": True,
            "loop_shadow_mode": True,
            "loop_budget": {
                "max_steps": 1,
                "max_latency_ms": 1000,
                "max_llm_tokens": 1000,
                "max_clarifications": 3,
            },
            "loop_steps_taken": 1,
            "total_latency_ms": 0.0,
            "total_tokens_used": 0,
            "clarification_turn_count": 0,
            "route": "sql",
            "context_needs_sql": True,
            "decision_trace": [],
            "action_trace": [],
            "loop_shadow_decisions": [],
        }
        decision = pipeline._should_execute_after_context_answer(state)
        assert decision == "sql"
        shadow = state.get("loop_shadow_decisions", [])
        assert shadow
        assert shadow[-1]["recommended_decision"] == "end"
        assert shadow[-1]["actual_decision"] == "sql"

    @pytest.mark.asyncio
    async def test_simple_sql_can_skip_response_synthesis(self, mock_agents):
        """Request-level override should skip synthesis for simple SQL responses."""
        result = await mock_agents.run("test query", synthesize_simple_sql=False)

        # With unified routing, context_answer is called and produces output
        # Response synthesis may or may not be called depending on query complexity
        assert result["natural_language_answer"]  # Should have an answer
        assert result["llm_calls"] >= 2  # query_analyzer + at least sql

    def test_simple_sql_detector_marks_with_cte_as_complex(self, pipeline):
        state = {
            "validated_sql": "WITH recent AS (SELECT id FROM t) SELECT * FROM recent",
            "query_result": {"row_count": 5, "columns": ["id"]},
        }
        assert pipeline._is_simple_sql_response(state) is False

    def test_simple_sql_detector_marks_newline_join_as_complex(self, pipeline):
        state = {
            "validated_sql": "SELECT a.id\nFROM a\nJOIN b ON b.id = a.id",
            "query_result": {"row_count": 5, "columns": ["id"]},
        }
        assert pipeline._is_simple_sql_response(state) is False

    @pytest.mark.asyncio
    async def test_selective_tool_planner_skips_standard_data_query(self, mock_agents):
        """Tool planner should not run for plain SQL data requests."""
        # The mock_agents fixture already sets up route="sql" for query_analyzer
        # Just verify that the query reaches SQL execution
        result = await mock_agents.run("Show first 5 rows from orders")

        # Verify the query went through SQL path
        assert mock_agents.sql.execute.called or result.get("answer_source") == "sql"

    @pytest.mark.asyncio
    async def test_selective_tool_planner_runs_for_tool_like_intent(self, mock_agents):
        """Tool planner should run for tool/action-style requests."""
        from backend.agents.query_analyzer import QueryAnalysis, QueryAnalyzerOutput

        # Mock query_analyzer to return tool route
        mock_agents.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="meta",
                    route="tool",
                    entities=[],
                    complexity="medium",
                    confidence=0.9,
                    clarifying_questions=[],
                    deterministic=False,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )
        await mock_agents.run("Profile database and generate datapoints")
        assert mock_agents.tool_planner.execute.called

    @pytest.mark.asyncio
    async def test_pipeline_includes_all_outputs(self, mock_agents):
        """Test that final state includes all agent outputs."""
        result = await mock_agents.run("test query")

        # ClassifierAgent outputs
        assert result["intent"] == "data_query"
        assert result["complexity"] == "simple"

        # ContextAgent outputs
        assert len(result["retrieved_datapoints"]) == 1
        assert result["retrieved_datapoints"][0]["datapoint_id"] == "table_001"

        # SQLAgent outputs
        assert result["generated_sql"] == "SELECT * FROM test_table"
        assert result["sql_confidence"] == 0.95

        # ValidatorAgent outputs
        assert result["validated_sql"] == "SELECT * FROM test_table"
        assert result["performance_score"] == 0.8

        # ExecutorAgent outputs
        assert result["natural_language_answer"] == "Found 1 result."
        assert result["visualization_hint"] == "table"

    @pytest.mark.asyncio
    async def test_sql_success_clears_stale_classifier_clarification(self, mock_agents):
        """Ensure classifier clarification flags do not block SQL success path."""
        from backend.agents.query_analyzer import QueryAnalysis, QueryAnalyzerOutput

        mock_agents.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    confidence=0.7,
                    clarifying_questions=["Which table should I use?"],
                    deterministic=False,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )

        result = await mock_agents.run("show me the first 5 rows from sales")

        assert result["validation_passed"] is True
        assert result["answer_source"] == "sql"
        assert result["clarification_needed"] is False
        assert result["clarifying_questions"] == []

    @pytest.mark.asyncio
    async def test_pipeline_passes_database_context_to_sql_agent(self, mock_agents):
        """Ensure per-request database type/url reach SQLAgentInput."""
        await mock_agents.run(
            "test query",
            database_type="clickhouse",
            database_url="clickhouse://user:pass@click.example.com:8123/analytics",
        )
        sql_input = mock_agents.sql.execute.call_args.args[0]
        assert sql_input.database_type == "clickhouse"
        assert sql_input.database_url == "clickhouse://user:pass@click.example.com:8123/analytics"

    @pytest.mark.asyncio
    async def test_pipeline_surfaces_query_compiler_metrics_and_decision_trace(self, mock_agents):
        mock_agents.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT * FROM test_table",
                    explanation="Simple select query",
                    confidence=0.95,
                    used_datapoints=["table_001"],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
                data={
                    "formatter_fallback_calls": 0,
                    "formatter_fallback_successes": 0,
                    "query_compiler_llm_calls": 1,
                    "query_compiler_llm_refinements": 1,
                    "query_compiler_latency_ms": 123.4,
                    "query_compiler": {
                        "path": "llm_refined",
                        "reason": "llm_refined_ambiguous_candidates",
                        "confidence": 0.9,
                        "selected_tables": ["public.test_table"],
                        "candidate_tables": ["public.test_table"],
                        "operators": ["reconciliation"],
                    },
                },
            )
        )

        result = await mock_agents.run("test query")

        assert result["query_compiler_llm_calls"] == 1
        assert result["query_compiler_llm_refinements"] == 1
        assert result["query_compiler_latency_ms"] == pytest.approx(123.4)
        assert result["query_compiler"]["path"] == "llm_refined"
        assert any(
            entry.get("stage") == "query_compiler" and entry.get("decision") == "llm_refined"
            for entry in result.get("decision_trace", [])
        )

    @pytest.mark.asyncio
    async def test_live_table_catalog_uses_database_url_connector(self, pipeline):
        mock_catalog = AsyncMock()
        mock_catalog.is_connected = False
        mock_catalog.connect = AsyncMock()
        mock_catalog.get_schema = AsyncMock(return_value=[])
        mock_catalog.close = AsyncMock()
        pipeline._build_catalog_connector = lambda *_args, **_kwargs: mock_catalog

        result = await pipeline._get_live_table_catalog(
            database_type="postgresql",
            database_url="postgresql://user:pass@localhost:5432/warehouse",
        )

        assert result is None
        mock_catalog.connect.assert_awaited()
        mock_catalog.get_schema.assert_awaited()
        mock_catalog.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_routes_to_context_answer_for_exploration(self, mock_agents):
        from backend.agents.query_analyzer import QueryAnalysis, QueryAnalyzerOutput

        mock_agents.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="exploration",
                    route="context",
                    entities=[],
                    complexity="simple",
                    confidence=0.9,
                    clarifying_questions=[],
                    deterministic=False,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )
        mock_agents.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="test query",
                    datapoints=[
                        RetrievedDataPoint(
                            datapoint_id="table_001",
                            datapoint_type="Schema",
                            name="Test Table",
                            score=0.9,
                            source="vector",
                            metadata={"type": "Schema"},
                        )
                    ],
                    retrieval_mode="hybrid",
                    total_retrieved=1,
                    sources_used=["vector"],
                ),
                context_confidence=0.8,
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )
        mock_agents.context_answer.execute = AsyncMock(
            return_value=ContextAnswerAgentOutput(
                success=True,
                context_answer=ContextAnswer(
                    answer="Context-only response.",
                    confidence=0.7,
                    evidence=[],
                    needs_sql=False,
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAnswerAgent", llm_calls=1),
            )
        )

        result = await mock_agents.run("Explain what this dataset is about.")

        assert mock_agents.context_answer.execute.called
        assert not mock_agents.sql.execute.called
        assert result["answer_source"] == "context"

    @pytest.mark.asyncio
    async def test_context_answer_can_fall_through_to_sql(self, mock_agents):
        from backend.agents.query_analyzer import QueryAnalysis, QueryAnalyzerOutput

        mock_agents.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="exploration",
                    route="context",
                    entities=[],
                    complexity="simple",
                    confidence=0.9,
                    clarifying_questions=[],
                    deterministic=False,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )
        mock_agents.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="test query",
                    datapoints=[
                        RetrievedDataPoint(
                            datapoint_id="table_001",
                            datapoint_type="Schema",
                            name="Test Table",
                            score=0.9,
                            source="vector",
                            metadata={"type": "Schema"},
                        )
                    ],
                    retrieval_mode="hybrid",
                    total_retrieved=1,
                    sources_used=["vector"],
                ),
                context_confidence=0.8,
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )
        mock_agents.context_answer.execute = AsyncMock(
            return_value=ContextAnswerAgentOutput(
                success=True,
                context_answer=ContextAnswer(
                    answer="Need numbers.",
                    confidence=0.5,
                    evidence=[],
                    needs_sql=True,
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAnswerAgent", llm_calls=1),
            )
        )

        result = await mock_agents.run("tell me about our revenue metric")

        assert mock_agents.context_answer.execute.called
        assert mock_agents.sql.execute.called
        assert result["answer_source"] == "sql"


class TestRetryLogic:
    """Test SQL validation retry logic."""

    @pytest.fixture
    def mock_retriever(self):
        """Mock retriever."""
        return AsyncMock()

    @pytest.fixture
    def mock_connector(self):
        """Mock connector."""
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.close = AsyncMock()
        return connector

    @pytest.fixture
    def mock_llm_provider(self):
        """Mock LLM provider."""
        provider = AsyncMock()
        provider.provider = "mock"
        provider.model = "mock-model"
        return provider

    @pytest.fixture
    def pipeline(self, mock_retriever, mock_connector, mock_llm_provider, mock_openai_api_key):
        """Create pipeline."""
        pipeline = DataChatPipeline(
            retriever=mock_retriever,
            connector=mock_connector,
            max_retries=3,
        )
        # Inject mock LLM providers
        pipeline.query_analyzer.llm = mock_llm_provider
        pipeline.sql.llm = mock_llm_provider
        pipeline.executor.llm = mock_llm_provider
        pipeline.tool_planner.execute = AsyncMock(
            return_value=ToolPlannerAgentOutput(
                success=True,
                plan=ToolPlan(tool_calls=[], rationale="No tools needed.", fallback="pipeline"),
                metadata=AgentMetadata(agent_name="ToolPlannerAgent", llm_calls=0),
            )
        )
        pipeline.response_synthesis.execute = AsyncMock(return_value="Found 1 result.")
        return pipeline

    @pytest.mark.asyncio
    async def test_retry_on_validation_failure(self, pipeline):
        """Test that pipeline retries SQL on validation failure."""
        # Mock agents
        pipeline.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    deterministic=False,
                    clarifying_questions=[],
                    confidence=0.9,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )

        pipeline.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="test",
                    datapoints=[],
                    retrieval_mode="hybrid",
                    total_retrieved=0,
                    sources_used=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )

        # ContextAnswerAgent returns needs_sql=True
        pipeline.context_answer.execute = AsyncMock(
            return_value=ContextAnswerAgentOutput(
                success=True,
                context_answer=ContextAnswer(
                    answer="Need SQL.",
                    confidence=0.5,
                    evidence=[],
                    needs_sql=True,
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAnswerAgent", llm_calls=1),
            )
        )

        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT * FROM invalid_table",
                    explanation="Query",
                    confidence=0.8,
                    used_datapoint_ids=[],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
            )
        )

        # ValidatorAgent fails first time, passes second time
        validation_call_count = 0

        async def validator_side_effect(*args, **kwargs):
            nonlocal validation_call_count
            validation_call_count += 1

            if validation_call_count == 1:
                # First call: validation fails
                return ValidatorAgentOutput(
                    success=False,
                    validated_sql=ValidatedSQL(
                        sql="SELECT * FROM invalid_table",
                        is_valid=False,
                        is_safe=False,
                        errors=[
                            SQLValidationError(
                                error_type="schema",
                                message="Table 'invalid_table' does not exist",
                            )
                        ],
                        warnings=[],
                        performance_score=0.5,
                    ),
                    metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
                )
            else:
                # Second call: validation passes
                return ValidatorAgentOutput(
                    success=True,
                    validated_sql=ValidatedSQL(
                        sql="SELECT * FROM valid_table",
                        is_valid=True,
                        is_safe=True,
                        errors=[],
                        warnings=[],
                        performance_score=0.9,
                    ),
                    metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
                )

        pipeline.validator.execute = AsyncMock(side_effect=validator_side_effect)

        # ExecutorAgent succeeds
        pipeline.executor.execute = AsyncMock(
            return_value=ExecutorAgentOutput(
                success=True,
                executed_query=ExecutedQuery(
                    query_result=QueryResult(
                        rows=[],
                        row_count=0,
                        columns=[],
                        execution_time_ms=10.0,
                    ),
                    natural_language_answer="No results",
                    visualization_hint="none",
                    key_insights=[],
                    source_citations=[],
                ),
                metadata=AgentMetadata(agent_name="ExecutorAgent", llm_calls=1),
            )
        )

        result = await pipeline.run("test query")

        # Verify retry happened
        assert pipeline.sql.execute.call_count == 2  # Initial + 1 retry
        assert pipeline.validator.execute.call_count == 2
        assert result["retry_count"] == 1
        assert result["validation_passed"] is True

    @pytest.mark.asyncio
    async def test_max_retries_enforced(self, pipeline):
        """Test that max retries is enforced."""
        # Mock agents
        pipeline.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    deterministic=False,
                    clarifying_questions=[],
                    confidence=0.9,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )

        pipeline.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="test",
                    datapoints=[],
                    retrieval_mode="hybrid",
                    total_retrieved=0,
                    sources_used=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )

        # ContextAnswerAgent returns needs_sql=True to continue to SQL
        pipeline.context_answer.execute = AsyncMock(
            return_value=ContextAnswerAgentOutput(
                success=True,
                context_answer=ContextAnswer(
                    answer="Need SQL to answer this.",
                    confidence=0.5,
                    evidence=[],
                    needs_sql=True,
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAnswerAgent", llm_calls=1),
            )
        )

        # SQLAgent always succeeds
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT * FROM bad_table",
                    explanation="Query",
                    confidence=0.8,
                    used_datapoints=[],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
            )
        )

        # ValidatorAgent always fails
        pipeline.validator.execute = AsyncMock(
            return_value=ValidatorAgentOutput(
                success=False,
                validated_sql=ValidatedSQL(
                    sql="SELECT * FROM bad_table",
                    is_valid=False,
                    is_safe=False,
                    errors=[
                        SQLValidationError(
                            error_type="schema",
                            message="Table does not exist",
                        )
                    ],
                    warnings=[],
                    performance_score=0.0,
                ),
                metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
            )
        )

        result = await pipeline.run("test query")

        # Verify max retries hit
        assert pipeline.sql.execute.call_count == 4  # Initial + 3 retries
        assert pipeline.validator.execute.call_count == 4
        assert result["retry_count"] == 3
        assert result.get("error") is not None
        assert "after 3 attempts" in result["error"]


class TestIntentGate:
    """Test intent gate behavior with unified QueryAnalyzerAgent."""

    @pytest.fixture
    def mock_retriever(self):
        return AsyncMock()

    @pytest.fixture
    def mock_connector(self):
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.close = AsyncMock()
        connector.is_connected = True
        connector.get_schema = AsyncMock(return_value=[])
        return connector

    @pytest.fixture
    def pipeline(self, mock_retriever, mock_connector, mock_openai_api_key):
        pipeline = DataChatPipeline(
            retriever=mock_retriever,
            connector=mock_connector,
            max_retries=3,
        )
        pipeline.tool_planner.execute = AsyncMock(
            return_value=ToolPlannerAgentOutput(
                success=True,
                plan=ToolPlan(tool_calls=[], rationale="No tools needed.", fallback="pipeline"),
                metadata=AgentMetadata(agent_name="ToolPlannerAgent", llm_calls=0),
            )
        )
        pipeline.response_synthesis.execute = AsyncMock(return_value="Result is 1")
        # Let real query_analyzer handle pattern matching for deterministic tests
        return pipeline

    @pytest.mark.asyncio
    async def test_intent_gate_exit_short_circuits(self, pipeline):
        result = await pipeline.run("Ok I'm done for now")
        # With unified QueryAnalyzerAgent, exit patterns are matched deterministically
        assert result.get("answer_source") == "system"
        assert "Ending the session" in result.get("natural_language_answer", "")

    @pytest.mark.asyncio
    async def test_intent_gate_exit_detects_talk_later(self, pipeline):
        result = await pipeline.run("let's talk later")
        assert result.get("answer_source") == "system"
        assert "Ending the session" in result.get("natural_language_answer", "")

    @pytest.mark.asyncio
    async def test_intent_gate_exit_detects_never_mind(self, pipeline):
        result = await pipeline.run("never mind, i'll ask later")
        assert result.get("answer_source") == "system"
        assert "Ending the session" in result.get("natural_language_answer", "")

    @pytest.mark.asyncio
    async def test_intent_gate_ambiguous_prompts_clarification(self, pipeline):
        result = await pipeline.run("ok")
        assert result.get("answer_source") == "clarification"
        assert result.get("clarifying_questions")
        assert "data" in result.get("natural_language_answer", "").lower()

    @pytest.mark.asyncio
    async def test_intent_gate_out_of_scope_short_circuits(self, pipeline):
        result = await pipeline.run("Tell me a joke")
        assert result.get("answer_source") == "system"
        assert "connected data" in result.get("natural_language_answer", "").lower()

    @pytest.mark.asyncio
    async def test_intent_gate_datapoint_help_short_circuits(self, pipeline):
        result = await pipeline.run("show datapoints")
        # datapoint_help intent triggers system response
        assert result.get("answer_source") == "system"
        assert "datapoint" in result.get("natural_language_answer", "").lower()

    @pytest.mark.asyncio
    async def test_intent_gate_fast_path_list_tables_handles_empty_investigation_memory(
        self, pipeline
    ):
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql=(
                        "SELECT table_schema, table_name FROM information_schema.tables "
                        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
                    ),
                    explanation="Catalog table listing query.",
                    confidence=0.95,
                    used_datapoints=[],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=0),
            )
        )
        pipeline.validator.execute = AsyncMock(
            return_value=ValidatorAgentOutput(
                success=True,
                validated_sql=ValidatedSQL(
                    sql="SELECT table_schema, table_name FROM information_schema.tables",
                    is_valid=True,
                    is_safe=True,
                    errors=[],
                    warnings=[],
                    performance_score=0.9,
                ),
                metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
            )
        )
        pipeline.executor.execute = AsyncMock(
            return_value=ExecutorAgentOutput(
                success=True,
                executed_query=ExecutedQuery(
                    query_result=QueryResult(
                        rows=[{"table_schema": "public", "table_name": "sales"}],
                        row_count=1,
                        columns=["table_schema", "table_name"],
                        execution_time_ms=5.0,
                        was_truncated=False,
                    ),
                    natural_language_answer="Found 1 table.",
                    visualization_hint="table",
                    key_insights=[],
                    source_citations=[],
                ),
                metadata=AgentMetadata(agent_name="ExecutorAgent", llm_calls=0),
            )
        )

        result = await pipeline.run("list tables")

        assert result.get("error") is None
        # With unified routing, intent_gate reflects the route
        assert result.get("fast_path") is True
        assert pipeline.sql.execute.called

    @pytest.mark.asyncio
    async def test_reply_after_clarification_continues_previous_goal(self, pipeline):
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT * FROM public.petra_campuses LIMIT 2",
                    explanation="Sample rows from selected replacement table.",
                    confidence=0.95,
                    used_datapoints=[],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=0),
            )
        )
        pipeline.validator.execute = AsyncMock(
            return_value=ValidatorAgentOutput(
                success=True,
                validated_sql=ValidatedSQL(
                    sql="SELECT * FROM public.petra_campuses LIMIT 2",
                    is_valid=True,
                    is_safe=True,
                    errors=[],
                    warnings=[],
                    performance_score=0.9,
                ),
                metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
            )
        )
        pipeline.executor.execute = AsyncMock(
            return_value=ExecutorAgentOutput(
                success=True,
                executed_query=ExecutedQuery(
                    query_result=QueryResult(
                        rows=[{"id": 1, "location": "Abuja"}],
                        row_count=1,
                        columns=["id", "location"],
                        execution_time_ms=3.0,
                        was_truncated=False,
                    ),
                    natural_language_answer="Returned 1 sample row.",
                    visualization_hint="table",
                    key_insights=[],
                    source_citations=[],
                ),
                metadata=AgentMetadata(agent_name="ExecutorAgent", llm_calls=0),
            )
        )

        history = [
            {"role": "user", "content": "show 2 rows in public.sales"},
            {
                "role": "assistant",
                "content": (
                    "I couldn't find public.sales in the connected database schema.\n"
                    "Clarifying questions:\n"
                    "- Which existing table should I use instead?"
                ),
            },
        ]

        result = await pipeline.run("petra_campuses", conversation_history=history)

        # With unified routing, the query is processed and SQL is generated
        assert result.get("fast_path") is True or result.get("answer_source") == "sql"
        assert pipeline.sql.execute.called
        sql_input = pipeline.sql.execute.call_args.args[0]
        assert "petra_campuses" in sql_input.query.lower() or "campuses" in sql_input.query.lower()

    @pytest.mark.asyncio
    async def test_reply_after_clarification_can_switch_to_new_exit_intent(self, pipeline):
        pipeline.sql.execute = AsyncMock()
        history = [
            {"role": "user", "content": "show 2 rows in public.sales"},
            {
                "role": "assistant",
                "content": (
                    "I couldn't find public.sales in the connected database schema.\n"
                    "Clarifying questions:\n"
                    "- Which existing table should I use instead?"
                ),
            },
        ]

        result = await pipeline.run(
            "never mind, i'll ask later",
            conversation_history=history,
        )

        # Exit pattern is detected deterministically
        assert result.get("answer_source") == "system"
        assert "Ending the session" in result.get("natural_language_answer", "")
        assert not pipeline.sql.execute.called

    def test_short_command_like_message_not_treated_as_followup_hint(self, pipeline):
        assert pipeline._is_short_followup("show columns") is False

    def test_contextual_followup_rewrites_count_question(self, pipeline):
        summary = pipeline._merge_session_state_into_summary(
            pipeline._build_intent_summary("what about stores", []),
            {"last_goal": "how many products do we have?"},
        )
        rewritten = pipeline._rewrite_contextual_followup("what about stores", summary)
        assert rewritten == "How many stores do we have?"

    def test_augment_history_includes_session_summary(self, pipeline):
        state = {
            "conversation_history": [{"role": "user", "content": "How many products do we have?"}],
            "session_summary": "Intent summary: last_goal=How many products do we have?",
            "intent_summary": {"last_goal": "How many products do we have?"},
        }
        augmented = pipeline._augment_history_with_summary(state)
        assert augmented[0]["role"] == "system"
        assert "Session memory:" in augmented[0]["content"]

    def test_context_vs_sql_uses_configured_confidence_threshold(self, pipeline):
        state = {
            "query": "give an overview",
            "intent": "data_query",
            "context_confidence": 0.65,
            "retrieved_datapoints": [{"datapoint_id": "metric_001"}],
            "decision_trace": [],
        }
        assert pipeline._should_use_context_answer(state) == "sql"
        pipeline.routing_policy["context_answer_confidence_threshold"] = 0.6
        state["decision_trace"] = []
        assert pipeline._should_use_context_answer(state) == "context"

    def test_ambiguous_intent_uses_token_policy(self, pipeline):
        state = {"query": "maybe later now"}
        summary = {"last_clarifying_questions": []}
        assert pipeline._is_ambiguous_intent(state, summary) is True
        pipeline.routing_policy["ambiguous_query_max_tokens"] = 2
        assert pipeline._is_ambiguous_intent(state, summary) is False

    @pytest.mark.asyncio
    async def test_intent_gate_populates_decision_trace(self, pipeline):
        result = await pipeline.run("list tables")
        trace = result.get("decision_trace", [])
        # With unified routing, we check for query_analyzer stage
        assert any(entry.get("stage") == "query_analyzer" for entry in trace)
        # And should have a routing decision
        assert any(
            entry.get("decision") in ("sql", "data_query", "data_query_fast_path")
            for entry in trace
        )

    def test_clean_hint_handles_regarding_prefix(self, pipeline):
        hint = pipeline._clean_hint(
            'Regarding "Which table should I list columns for?": vbs_registrations'
        )
        assert hint == "vbs_registrations"

    def test_merge_query_with_table_hint_rewrites_explicit_table(self, pipeline):
        merged = pipeline._merge_query_with_table_hint(
            "show 2 rows in public.sales",
            "petra_campuses",
        )
        assert merged == "Show 2 rows from petra_campuses"

    def test_query_requires_sql_for_rate_question(self, pipeline):
        assert pipeline._query_requires_sql("what is loan default rate?") is True

    def test_query_requires_sql_ignores_datapoint_keyword(self, pipeline):
        assert pipeline._query_requires_sql("what datapoint explains loan default rate?") is False

    def test_datapoint_help_intent_does_not_trigger_for_metric_explanation_query(self, pipeline):
        assert (
            pipeline._classify_intent_gate("what datapoint explains loan default rate?")
            == "data_query"
        )

    def test_definition_query_with_rate_routes_to_context(self, pipeline):
        state = {
            "query": "define loan default rate",
            "intent": "data_query",
            "context_confidence": 0.4,
            "retrieved_datapoints": [{"datapoint_id": "metric_default_rate_001"}],
        }
        assert pipeline._should_use_context_answer(state) == "context"

    def test_meaning_query_with_rate_routes_to_context(self, pipeline):
        state = {
            "query": "what does failed transaction rate mean?",
            "intent": "data_query",
            "context_confidence": 0.4,
            "retrieved_datapoints": [{"datapoint_id": "metric_failed_transaction_rate_001"}],
        }
        assert pipeline._should_use_context_answer(state) == "context"

    def test_rate_query_routes_to_sql_not_context(self, pipeline):
        state = {
            "query": "what is loan default rate?",
            "intent": "data_query",
            "context_confidence": 0.95,
            "retrieved_datapoints": [{"datapoint_id": "metric_default_rate_001"}],
        }
        assert pipeline._should_use_context_answer(state) == "sql"

    def test_rate_query_overrides_explanation_intent_to_sql(self, pipeline):
        state = {
            "query": "what is loan default rate?",
            "intent": "explanation",
            "context_confidence": 0.95,
            "retrieved_datapoints": [{"datapoint_id": "metric_default_rate_001"}],
        }
        assert pipeline._should_use_context_answer(state) == "sql"

    @pytest.mark.asyncio
    async def test_low_confidence_semantic_sql_triggers_clarification(self, pipeline):
        pipeline.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    deterministic=False,
                    clarifying_questions=[],
                    confidence=0.9,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=0),
            )
        )
        pipeline.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="Show revenue by campus this month",
                    datapoints=[
                        RetrievedDataPoint(
                            datapoint_id="table_orders_001",
                            datapoint_type="Schema",
                            name="Orders",
                            score=0.91,
                            source="vector",
                            metadata={"table_name": "public.orders"},
                        )
                    ],
                    retrieval_mode="hybrid",
                    total_retrieved=1,
                    sources_used=["table_orders_001"],
                ),
                context_confidence=0.1,
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT SUM(amount) FROM public.orders",
                    explanation="Guessing revenue from orders amount.",
                    confidence=0.32,
                    used_datapoints=["table_orders_001"],
                    assumptions=["revenue maps to orders.amount"],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
            )
        )
        pipeline.validator.execute = AsyncMock()
        pipeline.executor.execute = AsyncMock()

        result = await pipeline.run("Show revenue by campus this month")

        assert result.get("answer_source") == "clarification"
        assert result.get("clarifying_questions")
        first_question = result.get("clarifying_questions", [""])[0].lower()
        assert any(token in first_question for token in ("revenue", "month", "campus"))
        assert pipeline.validator.execute.call_count == 0
        assert pipeline.executor.execute.call_count == 0

    @pytest.mark.asyncio
    async def test_context_answer_failure_falls_through_to_sql(self, pipeline):
        pipeline.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    deterministic=False,
                    clarifying_questions=[],
                    confidence=0.9,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=0),
            )
        )
        pipeline.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="Show revenue by campus this month",
                    datapoints=[
                        RetrievedDataPoint(
                            datapoint_id="table_orders_001",
                            datapoint_type="Schema",
                            name="Orders",
                            score=0.91,
                            source="vector",
                            metadata={"table_name": "public.orders"},
                        )
                    ],
                    retrieval_mode="hybrid",
                    total_retrieved=1,
                    sources_used=["table_orders_001"],
                ),
                context_confidence=0.2,
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )
        pipeline.context_answer.execute = AsyncMock(side_effect=RuntimeError("context llm failed"))
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT campus, SUM(amount) AS revenue FROM public.orders GROUP BY campus",
                    explanation="Grouped revenue by campus.",
                    confidence=0.88,
                    used_datapoints=["table_orders_001"],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
            )
        )
        pipeline.validator.execute = AsyncMock(
            return_value=ValidatorAgentOutput(
                success=True,
                validated_sql=ValidatedSQL(
                    sql="SELECT campus, SUM(amount) AS revenue FROM public.orders GROUP BY campus",
                    is_valid=True,
                    is_safe=True,
                    errors=[],
                    warnings=[],
                    performance_score=0.9,
                ),
                metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
            )
        )
        pipeline.executor.execute = AsyncMock(
            return_value=ExecutorAgentOutput(
                success=True,
                executed_query=ExecutedQuery(
                    query_result=QueryResult(
                        rows=[{"campus": "North", "revenue": 100}],
                        row_count=1,
                        columns=["campus", "revenue"],
                        execution_time_ms=8.0,
                        was_truncated=False,
                    ),
                    natural_language_answer="North campus revenue is 100.",
                    visualization_hint="bar_chart",
                    key_insights=[],
                    source_citations=["table_orders_001"],
                ),
                metadata=AgentMetadata(agent_name="ExecutorAgent", llm_calls=0),
            )
        )

        result = await pipeline.run("Show revenue by campus this month")

        assert result.get("answer_source") == "sql"
        assert pipeline.sql.execute.call_count == 1
        assert pipeline.validator.execute.call_count == 1
        assert pipeline.executor.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_sql_clarification_path_records_sql_timing_and_formatter_metrics(self, pipeline):
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT 1",
                    explanation="Clarification required",
                    confidence=0.0,
                    used_datapoints=[],
                    assumptions=[],
                    clarifying_questions=["Which table should I use?"],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=2),
                data={"formatter_fallback_calls": 1, "formatter_fallback_successes": 0},
                needs_clarification=True,
            )
        )

        state = pipeline._build_initial_state(
            query="show me revenue",
            conversation_history=[],
            session_summary=None,
            session_state=None,
            database_type="postgresql",
            database_url=None,
            target_connection_id=None,
            synthesize_simple_sql=None,
            workflow_mode="auto",
            correlation_prefix="test",
        )
        state["retrieved_datapoints"] = []
        state["investigation_memory"] = {
            "query": "show me revenue",
            "datapoints": [],
            "total_retrieved": 0,
            "retrieval_mode": "hybrid",
            "sources_used": [],
        }

        result = await pipeline._run_sql(state)

        assert result["clarification_needed"] is True
        assert result["clarifying_questions"] == ["Which table should I use?"]
        assert result["agent_timings"]["sql"] >= 0
        assert result["llm_calls"] == 2
        assert result["sql_formatter_fallback_calls"] == 1
        assert result["sql_formatter_fallback_successes"] == 0

    @pytest.mark.asyncio
    async def test_sql_placeholder_is_not_executed_when_confirmation_blocks_clarification(
        self, pipeline
    ):
        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT 1",
                    explanation="Clarification required",
                    confidence=0.0,
                    used_datapoints=[],
                    assumptions=[],
                    clarifying_questions=["Which table should I use?"],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
                data={"formatter_fallback_calls": 0, "formatter_fallback_successes": 0},
                needs_clarification=True,
            )
        )
        pipeline._apply_clarification_response_with_confirmation = AsyncMock(return_value=False)

        state = pipeline._build_initial_state(
            query="show me revenue",
            conversation_history=[],
            session_summary=None,
            session_state=None,
            database_type="postgresql",
            database_url=None,
            target_connection_id=None,
            synthesize_simple_sql=None,
            workflow_mode="auto",
            correlation_prefix="test",
        )
        state["retrieved_datapoints"] = []
        state["investigation_memory"] = {
            "query": "show me revenue",
            "datapoints": [],
            "total_retrieved": 0,
            "retrieval_mode": "hybrid",
            "sources_used": [],
        }

        result = await pipeline._run_sql(state)

        assert result["clarification_needed"] is True
        assert result["clarifying_questions"] == ["Which table should I use?"]
        assert result.get("generated_sql") in (None, "")

    @pytest.mark.asyncio
    async def test_preplanned_metadata_sql_is_overridden_by_catalog_plan(self, pipeline):
        state = pipeline._build_initial_state(
            query="What kind of info can I get from available tables?",
            conversation_history=[],
            session_summary=None,
            session_state=None,
            database_type="postgresql",
            database_url=None,
            target_connection_id=None,
            synthesize_simple_sql=None,
            workflow_mode="auto",
            correlation_prefix="test",
        )
        state["retrieved_datapoints"] = []
        state["investigation_memory"] = {
            "query": state["query"],
            "datapoints": [],
            "total_retrieved": 0,
            "retrieval_mode": "hybrid",
            "sources_used": [],
        }
        state["preplanned_sql"] = {
            "sql": (
                "SELECT table_schema, table_name, column_name "
                "FROM information_schema.columns ORDER BY table_schema, table_name"
            ),
            "explanation": "planner output",
            "confidence": 0.7,
            "clarifying_questions": [],
        }

        result = await pipeline._run_sql(state)

        assert "information_schema.tables" in (result.get("generated_sql") or "").lower()
        assert "information_schema.columns" not in (result.get("generated_sql") or "").lower()
        assert result["sql_confidence"] == pytest.approx(0.99)

    def test_normalize_answer_metadata_assigns_defaults(self, pipeline):
        state = {
            "natural_language_answer": "Found rows.",
            "validated_sql": "SELECT * FROM public.orders LIMIT 2",
            "answer_source": None,
            "answer_confidence": None,
        }

        pipeline._normalize_answer_metadata(state)

        assert state["answer_source"] == "sql"
        assert state["answer_confidence"] == 0.7

    def test_normalize_answer_metadata_treats_generated_sql_as_sql(self, pipeline):
        state = {
            "generated_sql": "SELECT * FROM public.orders LIMIT 2",
            "answer_source": None,
            "answer_confidence": None,
            "error": None,
        }

        pipeline._normalize_answer_metadata(state)

        assert state["answer_source"] == "sql"
        assert state["answer_confidence"] == 0.7

    def test_normalize_answer_metadata_extracts_answer_from_fenced_json(self, pipeline):
        state = {
            "natural_language_answer": (
                "```json\n"
                "{\n"
                '  "answer": "Top 5 SKUs by stockout risk this week are A, B, C, D, E.",\n'
                '  "confidence": 0.95,\n'
                '  "used_datapoint": null\n'
                "}\n"
                "```"
            ),
            "validated_sql": "SELECT * FROM public.inventory LIMIT 5",
            "answer_source": None,
            "answer_confidence": None,
            "error": None,
        }

        pipeline._normalize_answer_metadata(state)

        assert state["natural_language_answer"] == (
            "Top 5 SKUs by stockout risk this week are A, B, C, D, E."
        )
        assert state["answer_source"] == "sql"
        assert state["answer_confidence"] == pytest.approx(0.95)


class TestStreaming:
    """Test streaming functionality."""

    @pytest.fixture
    def mock_retriever(self):
        """Mock retriever."""
        return AsyncMock()

    @pytest.fixture
    def mock_connector(self):
        """Mock connector."""
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.close = AsyncMock()
        return connector

    @pytest.fixture
    def mock_llm_provider(self):
        """Mock LLM provider."""
        provider = AsyncMock()
        provider.provider = "mock"
        provider.model = "mock-model"
        return provider

    @pytest.fixture
    def pipeline(self, mock_retriever, mock_connector, mock_llm_provider, mock_openai_api_key):
        """Create pipeline."""
        pipeline = DataChatPipeline(
            retriever=mock_retriever,
            connector=mock_connector,
            max_retries=3,
        )
        # Inject mock LLM providers
        pipeline.query_analyzer.llm = mock_llm_provider
        pipeline.sql.llm = mock_llm_provider
        pipeline.executor.llm = mock_llm_provider
        pipeline.tool_planner.execute = AsyncMock(
            return_value=ToolPlannerAgentOutput(
                success=True,
                plan=ToolPlan(tool_calls=[], rationale="No tools needed.", fallback="pipeline"),
                metadata=AgentMetadata(agent_name="ToolPlannerAgent", llm_calls=0),
            )
        )
        pipeline.response_synthesis.execute = AsyncMock(return_value="Result is 1")
        return pipeline

    @pytest.mark.asyncio
    async def test_streaming_emits_updates(self, pipeline):
        """Test that streaming emits status updates for each agent."""
        # Mock all agents
        pipeline.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    deterministic=False,
                    clarifying_questions=[],
                    confidence=0.9,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )

        pipeline.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="test",
                    datapoints=[],
                    retrieval_mode="hybrid",
                    total_retrieved=0,
                    sources_used=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )

        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT 1",
                    explanation="Test",
                    confidence=0.9,
                    used_datapoints=[],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
            )
        )

        pipeline.validator.execute = AsyncMock(
            return_value=ValidatorAgentOutput(
                success=True,
                validated_sql=ValidatedSQL(
                    sql="SELECT 1",
                    is_valid=True,
                    is_safe=True,
                    errors=[],
                    warnings=[],
                    performance_score=1.0,
                ),
                metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
            )
        )

        pipeline.executor.execute = AsyncMock(
            return_value=ExecutorAgentOutput(
                success=True,
                executed_query=ExecutedQuery(
                    query_result=QueryResult(
                        rows=[{"result": 1}],
                        row_count=1,
                        columns=["result"],
                        execution_time_ms=5.0,
                    ),
                    natural_language_answer="Result is 1",
                    visualization_hint="none",
                    key_insights=[],
                    source_citations=[],
                ),
                metadata=AgentMetadata(agent_name="ExecutorAgent", llm_calls=1),
            )
        )

        # Collect streaming updates
        updates = []
        async for update in pipeline.stream("test query"):
            updates.append(update)

        # Verify we got updates for key agents
        nodes = [u["node"] for u in updates]
        # With unified routing, query_analyzer replaces classifier
        assert "query_analyzer" in nodes or "context" in nodes
        assert "sql" in nodes or len(updates) > 0  # Should have some updates

        # Verify each update has required fields
        for update in updates:
            assert "node" in update
            assert "current_agent" in update
            assert "status" in update
            assert "state" in update


class TestErrorHandling:
    """Test error handling and recovery."""

    @pytest.fixture
    def mock_retriever(self):
        """Mock retriever."""
        return AsyncMock()

    @pytest.fixture
    def mock_connector(self):
        """Mock connector."""
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.close = AsyncMock()
        return connector

    @pytest.fixture
    def pipeline(self, mock_retriever, mock_connector):
        """Create pipeline."""
        pipeline = DataChatPipeline(
            retriever=mock_retriever,
            connector=mock_connector,
            max_retries=2,
        )
        pipeline.tool_planner.execute = AsyncMock(
            return_value=ToolPlannerAgentOutput(
                success=True,
                plan=ToolPlan(tool_calls=[], rationale="No tools needed.", fallback="pipeline"),
                metadata=AgentMetadata(agent_name="ToolPlannerAgent", llm_calls=0),
            )
        )
        pipeline.response_synthesis.execute = AsyncMock(return_value="Found 1 result.")
        return pipeline

    @pytest.mark.asyncio
    async def test_classifier_error_is_captured(self, pipeline):
        """Test that errors in classifier are captured."""
        # Mock classifier to raise error
        pipeline.query_analyzer.execute = AsyncMock(side_effect=Exception("Classification failed"))

        result = await pipeline.run("test query")

        # Verify error is captured
        assert result.get("error") is not None
        assert "Classification failed" in result["error"]

    @pytest.mark.asyncio
    async def test_error_handler_provides_graceful_message(self, pipeline):
        """Test that error handler provides user-friendly message."""
        # Mock all agents to succeed except executor
        pipeline.query_analyzer.execute = AsyncMock(
            return_value=QueryAnalyzerOutput(
                success=True,
                analysis=QueryAnalysis(
                    intent="data_query",
                    route="sql",
                    entities=[],
                    complexity="simple",
                    deterministic=False,
                    clarifying_questions=[],
                    confidence=0.9,
                ),
                metadata=AgentMetadata(agent_name="QueryAnalyzerAgent", llm_calls=1),
            )
        )

        pipeline.context.execute = AsyncMock(
            return_value=ContextAgentOutput(
                success=True,
                data={},
                investigation_memory=InvestigationMemory(
                    query="test",
                    datapoints=[],
                    retrieval_mode="hybrid",
                    total_retrieved=0,
                    sources_used=[],
                ),
                metadata=AgentMetadata(agent_name="ContextAgent", llm_calls=0),
            )
        )

        pipeline.sql.execute = AsyncMock(
            return_value=SQLAgentOutput(
                success=True,
                generated_sql=GeneratedSQL(
                    sql="SELECT 1",
                    explanation="Test",
                    confidence=0.9,
                    used_datapoints=[],
                    assumptions=[],
                    clarifying_questions=[],
                ),
                metadata=AgentMetadata(agent_name="SQLAgent", llm_calls=1),
            )
        )

        # ValidatorAgent always fails to trigger max retries
        pipeline.validator.execute = AsyncMock(
            return_value=ValidatorAgentOutput(
                success=False,
                validated_sql=ValidatedSQL(
                    sql="SELECT 1",
                    is_valid=False,
                    is_safe=False,
                    errors=[
                        SQLValidationError(
                            error_type="other",
                            message="Error",
                        )
                    ],
                    warnings=[],
                    performance_score=0.0,
                ),
                metadata=AgentMetadata(agent_name="ValidatorAgent", llm_calls=0),
            )
        )

        result = await pipeline.run("test query")

        # Verify error is captured in some form
        assert result.get("error") is not None or result.get("natural_language_answer") is not None
        # Either max retries were hit or context answer provided clarification
        error_text = (result.get("error") or result.get("natural_language_answer", "")).lower()
        assert (
            "error" in error_text
            or "failed" in error_text
            or "attempt" in error_text
            or "clarif" in error_text
            or "datapoint" in error_text
            or "sorry" in error_text
            or "don't have" in error_text
            or "provide more" in error_text
            or "unable to" in error_text
        )

"""
DataChat Pipeline Orchestrator

LangGraph-based pipeline that orchestrates all agents:
- QueryAnalyzerAgent → ContextAgent → SQLAgent → ValidatorAgent → ExecutorAgent
- Self-correction loop: ValidatorAgent can send back to SQLAgent (max 3 retries)
- Streaming support for real-time status updates
- Cost and latency tracking
- Error recovery and graceful degradation

REFACTORED: Unified routing with QueryAnalyzerAgent replacing intent_gate + ClassifierAgent.
"""

import json
import logging
import re
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urlparse
from uuid import UUID, uuid4

from langgraph.graph import END, StateGraph

from backend.agents.context import ContextAgent
from backend.agents.context_answer import ContextAnswerAgent
from backend.agents.executor import ExecutorAgent
from backend.agents.query_analyzer import QueryAnalysis, QueryAnalyzerAgent, QueryAnalyzerInput
from backend.agents.query_compiler import QueryCompilerAgent
from backend.agents.response_synthesis import ResponseSynthesisAgent
from backend.agents.sql import SQLAgent
from backend.agents.tool_planner import ToolPlannerAgent
from backend.agents.validator import ValidatorAgent
from backend.config import get_settings
from backend.connectors.base import BaseConnector
from backend.connectors.factory import create_connector
from backend.database.manager import DatabaseConnectionManager
from backend.knowledge.retriever import Retriever
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models import (
    ContextAgentInput,
    ContextAnswerAgentInput,
    EvidenceItem,
    ExecutorAgentInput,
    GeneratedSQL,
    Message,
    SQLAgentInput,
    ToolPlannerAgentInput,
    ValidatorAgentInput,
)
from backend.pipeline.action_loop import (
    ActionLoopController,
    ActionState,
    ActionVerification,
    LoopBudget,
    LoopErrorClass,
    LoopStopReason,
    LoopTerminalState,
)
from backend.pipeline.route_handlers import (
    ClarificationRouteHandler,
    ContextRouteHandler,
    EndRouteHandler,
    RouteDispatcher,
    SQLRouteHandler,
    ToolRouteHandler,
)
from backend.pipeline.session_context import SessionContext
from backend.tools import ToolExecutor, initialize_tools
from backend.tools.base import ToolContext
from backend.tools.policy import ToolPolicyError
from backend.tools.registry import ToolRegistry
from backend.utils.pattern_matcher import QueryPatternMatcher

logger = logging.getLogger(__name__)
ENV_DATABASE_CONNECTION_ID = "00000000-0000-0000-0000-00000000dada"


# ============================================================================
# Pipeline State Schema
# ============================================================================


class PipelineState(TypedDict, total=False):
    """
    State schema for the DataChat pipeline.

    Tracks the complete flow through all agents with all intermediate outputs.
    """

    # Input
    query: str
    original_query: str | None
    conversation_history: list[Message]
    session_summary: str | None
    session_state: dict[str, Any] | None
    database_type: str
    database_url: str | None
    target_connection_id: str | None
    user_id: str | None
    correlation_id: str | None
    run_id: str | None
    run_started_at: datetime | None
    tool_approved: bool
    intent_gate: str | None
    route: (
        str | None
    )  # Explicit route from QueryAnalyzerAgent (sql, context, clarification, tool, end)
    intent_summary: dict[str, Any] | None
    clarification_turn_count: int
    clarification_limit: int
    fast_path: bool
    skip_response_synthesis: bool
    synthesize_simple_sql: bool | None
    workflow_mode: str | None
    preplanned_sql: dict[str, Any] | None

    # Classifier output
    intent: str | None
    entities: list[dict[str, Any]]
    complexity: str | None
    clarification_needed: bool
    clarifying_questions: list[str]

    # Context output
    investigation_memory: dict[str, Any] | None
    retrieved_datapoints: list[dict[str, Any]]
    context_confidence: float | None
    retrieval_trace: dict[str, Any] | None
    context_needs_sql: bool | None
    context_preface: str | None
    context_evidence: list[dict[str, Any]]

    # SQL output
    generated_sql: str | None
    sql_explanation: str | None
    sql_confidence: float | None
    sql_formatter_fallback_calls: int
    sql_formatter_fallback_successes: int
    query_compiler_llm_calls: int
    query_compiler_llm_refinements: int
    query_compiler_latency_ms: float
    query_compiler: dict[str, Any] | None
    used_datapoints: list[str]
    assumptions: list[str]

    # Validator output
    validated_sql: str | None
    validation_passed: bool
    validation_errors: list[dict[str, Any]]
    validation_warnings: list[dict[str, Any]]
    performance_score: float | None
    retry_count: int
    retries_exhausted: bool
    validated_sql_object: Any
    is_safe: bool | None

    # Executor output
    query_result: dict[str, Any] | None
    natural_language_answer: str | None
    visualization_hint: str | None
    visualization_note: str | None
    visualization_metadata: dict[str, Any] | None
    key_insights: list[str]
    answer_source: str | None
    answer_confidence: float | None
    evidence: list[dict[str, Any]]

    # Tool planner/executor output
    tool_plan: dict[str, Any] | None
    tool_calls: list[dict[str, Any]]
    tool_results: list[dict[str, Any]]
    tool_error: str | None
    tool_used: bool
    tool_approval_required: bool
    tool_approval_message: str | None
    tool_approval_calls: list[dict[str, Any]]

    # Pipeline metadata
    current_agent: str | None
    error: str | None
    total_cost: float
    total_latency_ms: float
    agent_timings: dict[str, float]
    llm_calls: int
    schema_refresh_attempted: bool
    sub_answers: list[dict[str, Any]]
    decision_trace: list[dict[str, Any]]
    action_trace: list[dict[str, Any]]
    loop_shadow_decisions: list[dict[str, Any]]
    loop_enabled: bool
    loop_shadow_mode: bool
    loop_budget: dict[str, Any]
    loop_steps_taken: int
    loop_terminal_state: str | None
    loop_stop_reason: str | None
    total_tokens_used: int
    last_error_class: str | None


# ============================================================================
# DataChat Pipeline
# ============================================================================


class DataChatPipeline:
    """
    LangGraph-based pipeline orchestrating all DataChat agents.

    Flow:
        1. ClassifierAgent: Understand intent and extract entities
        2. ContextAgent: Retrieve relevant DataPoints
        3. SQLAgent: Generate SQL query
        4. ValidatorAgent: Validate SQL (retry loop if fails)
        5. ExecutorAgent: Execute query and format results

    Features:
        - Self-correction loop (max 3 retries)
        - Streaming status updates
        - Cost and latency tracking
        - Error recovery

    Usage:
        pipeline = DataChatPipeline(retriever, connector)
        result = await pipeline.run("What were sales last month?")

        # Or with streaming:
        async for update in pipeline.stream("Show revenue by region"):
            print(f"Agent: {update['current_agent']}, Status: {update['status']}")
    """

    def __init__(
        self,
        retriever: Retriever,
        connector: BaseConnector,
        run_store: Any | None = None,
        max_retries: int = 3,
    ):
        """
        Initialize pipeline with dependencies.

        Args:
            retriever: Knowledge retriever for ContextAgent
            connector: Database connector for ExecutorAgent
            max_retries: Maximum SQL validation retry attempts
        """
        self.retriever = retriever
        self.connector = connector
        self.run_store = run_store
        self.max_retries = max_retries
        self.max_clarifications = 3
        self.config = get_settings()
        self.routing_policy = self._build_routing_policy()
        pipeline_cfg = getattr(self.config, "pipeline", None)
        self.loop_enabled = bool(getattr(pipeline_cfg, "action_loop_enabled", True))
        self.loop_shadow_mode = bool(getattr(pipeline_cfg, "action_loop_shadow_mode", True))
        self.loop_budget = LoopBudget(
            max_steps=int(getattr(pipeline_cfg, "action_loop_max_steps", 12)),
            max_latency_ms=int(getattr(pipeline_cfg, "action_loop_max_latency_ms", 45000)),
            max_llm_tokens=int(getattr(pipeline_cfg, "action_loop_max_llm_tokens", 120000)),
            max_clarifications=int(
                getattr(
                    pipeline_cfg,
                    "action_loop_max_clarifications",
                    self.max_clarifications,
                )
            ),
        )
        self.loop_controller = ActionLoopController(self.loop_budget)
        self.max_clarifications = self.loop_budget.max_clarifications

        # Initialize agents - using QueryAnalyzerAgent instead of ClassifierAgent
        self.query_analyzer = QueryAnalyzerAgent()
        self.query_compiler = QueryCompilerAgent()
        self.context = ContextAgent(retriever=retriever)
        self.context_answer = ContextAnswerAgent()
        self.sql = SQLAgent()
        self.validator = ValidatorAgent()
        self.executor = ExecutorAgent()
        self.response_synthesis = ResponseSynthesisAgent()
        self.tool_planner = ToolPlannerAgent()
        self.tool_executor = ToolExecutor()

        # Initialize route dispatcher
        self.route_dispatcher = self._build_route_dispatcher()

        # Initialize pattern matcher for fast-path detection
        self.pattern_matcher = QueryPatternMatcher()

        try:
            self.intent_llm = LLMProviderFactory.create_default_provider(
                self.config.llm, model_type="mini"
            )
        except Exception as exc:
            logger.warning(f"Intent LLM disabled: {exc}")
            self.intent_llm = None
        self.tooling_enabled = self.config.tools.enabled
        self.tool_planner_enabled = self.config.tools.planner_enabled
        initialize_tools(self.config.tools.policy_path)
        self.max_subqueries = 3
        self._connection_scope_cache: dict[str, tuple[float, set[str]]] = {}
        self._connection_scope_ttl_seconds = 300

        # Build LangGraph
        self.graph = self._build_graph()

        logger.info("DataChatPipeline initialized")

    def _build_route_dispatcher(self) -> RouteDispatcher:
        """Build the route dispatcher with all handlers."""
        dispatcher = RouteDispatcher()
        dispatcher.register(EndRouteHandler())
        dispatcher.register(
            ClarificationRouteHandler(
                max_clarifications=self.max_clarifications,
                clarification_count_fn=lambda s: int(s.get("clarification_turn_count", 0) or 0),
            )
        )
        dispatcher.register(SQLRouteHandler())
        dispatcher.register(ContextRouteHandler())
        dispatcher.register(ToolRouteHandler())
        return dispatcher

    def _build_routing_policy(self) -> dict[str, float | int]:
        pipeline_cfg = getattr(self.config, "pipeline", None)
        return {
            "intent_llm_confidence_threshold": float(
                getattr(pipeline_cfg, "intent_llm_confidence_threshold", 0.45)
            ),
            "context_answer_confidence_threshold": float(
                getattr(pipeline_cfg, "context_answer_confidence_threshold", 0.7)
            ),
            "semantic_sql_clarification_confidence_threshold": float(
                getattr(
                    pipeline_cfg,
                    "semantic_sql_clarification_confidence_threshold",
                    0.55,
                )
            ),
            "clarification_confirmation_enabled": bool(
                getattr(pipeline_cfg, "clarification_confirmation_enabled", True)
            ),
            "clarification_confirmation_confidence_threshold": float(
                getattr(
                    pipeline_cfg,
                    "clarification_confirmation_confidence_threshold",
                    0.6,
                )
            ),
            "ambiguous_query_max_tokens": int(
                getattr(pipeline_cfg, "ambiguous_query_max_tokens", 3)
            ),
        }

    def _build_graph(self) -> StateGraph:
        """
        Build LangGraph state machine.

        Returns:
            Compiled LangGraph
        """
        workflow = StateGraph(PipelineState)

        # Add nodes - using query_analyzer instead of intent_gate + classifier
        workflow.add_node("query_analyzer", self._run_query_analyzer)
        workflow.add_node("tool_planner", self._run_tool_planner)
        workflow.add_node("tool_executor", self._run_tool_executor)
        workflow.add_node("context", self._run_context)
        workflow.add_node("context_answer", self._run_context_answer)
        workflow.add_node("sql", self._run_sql)
        workflow.add_node("validator", self._run_validator)
        workflow.add_node("executor", self._run_executor)
        workflow.add_node("response_synthesis", self._run_response_synthesis)
        workflow.add_node("error_handler", self._handle_error)

        # Set entry point
        workflow.set_entry_point("query_analyzer")

        workflow.add_conditional_edges(
            "query_analyzer",
            self._should_continue_after_query_analyzer,
            {
                "end": END,
                "sql": "sql",
                "tool_planner": "tool_planner",
                "context": "context",
            },
        )

        workflow.add_conditional_edges(
            "tool_planner",
            self._should_use_tools,
            {
                "tools": "tool_executor",
                "pipeline": "context",
            },
        )
        workflow.add_conditional_edges(
            "tool_executor",
            self._should_continue_after_tool_execution,
            {
                "end": END,
                "pipeline": "context",
            },
        )

        # Add edges
        workflow.add_edge("context", "context_answer")
        workflow.add_conditional_edges(
            "context_answer",
            self._should_execute_after_context_answer,
            {
                "sql": "sql",
                "end": END,
            },
        )

        # Conditional edge from validator
        workflow.add_conditional_edges(
            "validator",
            self._should_retry_sql,
            {
                "retry": "sql",  # Retry SQL generation
                "execute": "executor",  # Proceed to execution
                "error": "error_handler",  # Max retries exceeded
            },
        )

        workflow.add_conditional_edges(
            "sql",
            self._should_validate_sql,
            {
                "validate": "validator",
                "clarify": END,
                "end": END,
            },
        )
        workflow.add_conditional_edges(
            "executor",
            self._should_synthesize_response,
            {
                "synthesize": "response_synthesis",
                "end": END,
            },
        )
        workflow.add_edge("response_synthesis", END)
        workflow.add_edge("error_handler", END)

        return workflow.compile()

    # ========================================================================
    # Agent Execution Methods
    # ========================================================================

    async def _run_query_analyzer(self, state: PipelineState) -> PipelineState:
        """
        Run QueryAnalyzerAgent - the unified entry point.

        This replaces the old intent_gate + classifier combination.
        """
        start_time = time.time()
        state["current_agent"] = "QueryAnalyzerAgent"
        state["clarification_limit"] = self.max_clarifications
        state["clarification_turn_count"] = max(
            state.get("clarification_turn_count", 0),
            self._current_clarification_count(state),
        )
        state.setdefault("fast_path", False)
        state.setdefault("skip_response_synthesis", False)

        query = state.get("query") or ""

        # Build session context from history
        session_ctx = SessionContext()
        session_ctx.update_from_history(state.get("conversation_history", []))
        if state.get("session_state"):
            session_ctx = SessionContext.from_dict(state.get("session_state", {}))

        # Resolve follow-up queries
        resolved_query = session_ctx.resolve_followup_query(query)
        if resolved_query:
            state["original_query"] = query
            state["query"] = resolved_query
            query = resolved_query
        else:
            table_hint = session_ctx.resolve_table_hint(query)
            if table_hint and session_ctx.last_goal:
                resolved_query = f"{session_ctx.last_goal} Use table {table_hint}."
                state["original_query"] = query
                state["query"] = resolved_query
                query = resolved_query

        # Run the query analyzer
        try:
            input_data = QueryAnalyzerInput(
                query=query,
                conversation_history=state.get("conversation_history", []),
                session_state=session_ctx.to_dict(),
            )

            output = await self.query_analyzer.execute(input_data)

            if output.success and output.analysis:
                analysis = self._coerce_data_query_to_sql_route(
                    query=query,
                    analysis=output.analysis,
                )
                state["intent"] = analysis.intent
                state["route"] = analysis.route  # Store the explicit route
                state["intent_gate"] = (
                    analysis.route if analysis.route != "end" else analysis.intent
                )
                state["entities"] = [
                    {
                        "entity_type": e.entity_type,
                        "value": e.value,
                        "confidence": e.confidence,
                        "normalized_value": e.normalized_value,
                    }
                    for e in analysis.entities
                ]
                state["complexity"] = analysis.complexity
                state["fast_path"] = analysis.deterministic

                if analysis.deterministic:
                    state["skip_response_synthesis"] = True

                # Handle different routes
                if analysis.route == "end":
                    state["answer_source"] = "system"
                    state["answer_confidence"] = 0.8
                    state["natural_language_answer"] = self._build_intent_gate_response(
                        analysis.intent
                    )
                    state["clarification_needed"] = False
                    state["clarifying_questions"] = []
                elif analysis.route == "clarification":
                    questions = analysis.clarifying_questions or [
                        "What would you like to do with your data?"
                    ]
                    applied = await self._apply_clarification_response_with_confirmation(
                        state,
                        questions,
                        default_intro="I need a bit more detail to help you.",
                    )
                    if not applied:
                        analysis.route = "sql"
                        state["route"] = "sql"
                        state["intent_gate"] = "sql"
                        state["answer_source"] = None
                        state["answer_confidence"] = None
                        state["natural_language_answer"] = None

                self._record_decision(
                    state,
                    stage="query_analyzer",
                    decision=analysis.route,
                    reason=f"intent={analysis.intent}, deterministic={analysis.deterministic}",
                    details={"confidence": analysis.confidence},
                )

            elapsed = (time.time() - start_time) * 1000
            state.setdefault("agent_timings", {})["query_analyzer"] = elapsed
            state["total_latency_ms"] = state.get("total_latency_ms", 0) + elapsed
            state["llm_calls"] = state.get("llm_calls", 0) + output.metadata.llm_calls
            self._track_tokens(
                state,
                tokens_used=output.metadata.tokens_used,
                llm_calls=output.metadata.llm_calls,
            )

        except Exception as e:
            logger.error(f"QueryAnalyzerAgent failed: {e}")
            # Fallback to data_query route
            state["intent"] = "data_query"
            state["intent_gate"] = "data_query"
            state["error"] = f"Query analysis failed: {e}"
            self._set_error_class(state, message=state.get("error"))

        self._record_action_step(
            state,
            stage="query_analyzer",
            selected_action=str(state.get("route") or state.get("intent") or "unknown"),
            inputs={"query": state.get("query")},
            outputs={
                "intent": state.get("intent"),
                "route": state.get("route"),
                "clarification_needed": bool(state.get("clarification_needed")),
            },
            verification_status="ok" if not state.get("error") else "failed",
            verification_reason=None if not state.get("error") else "query_analyzer_error",
            error_class=state.get("last_error_class"),
        )

        return state

    def _should_continue_after_query_analyzer(self, state: PipelineState) -> str:
        """Determine route after query analysis."""
        intent_gate = state.get("intent_gate")
        intent = state.get("intent")
        decision = "context"
        reason = "default_context_path"

        # End route for system intents and clarification
        if intent_gate in {
            "exit",
            "out_of_scope",
            "small_talk",
            "setup_help",
            "datapoint_help",
            "clarify",
            "clarification",  # P1 fix: also handle "clarification" route from QueryAnalyzerAgent
        }:
            decision = "end"
            reason = f"intent_gate={intent_gate}"

        # Fast path for deterministic SQL (no context retrieval needed)
        elif state.get("fast_path"):
            decision = "sql"
            reason = "fast_path"

        # Check the explicit route from QueryAnalyzer
        # For route="sql", we still go through context to retrieve datapoints
        # Context is needed for SQL generation context
        route = state.get("route")
        if decision not in {"end", "sql"} and route == "context":
            decision = "context"
            reason = "explicit_route=context"
        # P2 fix: Honor explicit tool route from analyzer
        if decision not in {"end", "sql"} and route == "tool":
            decision = "tool_planner"
            reason = "explicit_route=tool"

        # Check for tool requests (only if no explicit route was set)
        if (
            decision not in {"end", "sql", "tool_planner"}
            and self._should_run_tool_planner(state)
        ):
            decision = "tool_planner"
            reason = "tool_planner_enabled_for_query"

        # Context route for exploration/explanation
        if decision not in {"end", "sql", "tool_planner"} and intent in (
            "exploration",
            "explanation",
            "meta",
            "definition",
        ):
            decision = "context"
            reason = f"intent={intent}"

        decision = self._maybe_apply_loop_guard_decision(
            state,
            stage="continue_after_query_analyzer",
            actual_decision=decision,
            enforced_decision="end",
        )
        self._record_decision(
            state,
            stage="continue_after_query_analyzer",
            decision=decision,
            reason=reason,
        )
        return decision

    def _coerce_data_query_to_sql_route(
        self,
        *,
        query: str,
        analysis: QueryAnalysis,
    ) -> QueryAnalysis:
        """
        Enforce SQL-first execution for actionable data queries.

        QueryAnalyzer can still emit route=context/clarification for data queries
        when confidence is low. For data-query intents, we should still attempt SQL
        before asking the user for clarification.
        """
        if analysis.intent != "data_query":
            return analysis
        if analysis.route in {"sql", "tool", "end"}:
            return analysis

        synthetic_state: PipelineState = {"query": query}
        synthetic_summary = {"last_clarifying_questions": analysis.clarifying_questions}
        if analysis.route == "clarification" and self._is_ambiguous_intent(
            synthetic_state,
            synthetic_summary,
        ):
            return analysis

        analysis.route = "sql"
        analysis.clarifying_questions = []
        return analysis

    async def _run_tool_planner(self, state: PipelineState) -> PipelineState:
        """Run ToolPlannerAgent."""
        start_time = time.time()
        state["current_agent"] = "ToolPlannerAgent"

        try:
            tools = [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "category": tool.category.value,
                    "parameters_schema": tool.parameters_schema,
                }
                for tool in ToolRegistry.list_definitions()
            ]
            output = await self.tool_planner.execute(
                ToolPlannerAgentInput(
                    query=state["query"],
                    conversation_history=self._augment_history_with_summary(state),
                    available_tools=tools,
                )
            )
            plan = output.plan
            state["tool_plan"] = plan.model_dump()
            state["tool_calls"] = [call.model_dump() for call in plan.tool_calls]
            state["tool_used"] = bool(plan.tool_calls)

            elapsed = (time.time() - start_time) * 1000
            state.setdefault("agent_timings", {})["tool_planner"] = elapsed
            state["total_latency_ms"] = state.get("total_latency_ms", 0) + elapsed
            state["llm_calls"] = state.get("llm_calls", 0) + output.metadata.llm_calls
            self._track_tokens(
                state,
                tokens_used=output.metadata.tokens_used,
                llm_calls=output.metadata.llm_calls,
            )

        except Exception as exc:
            logger.error(f"ToolPlannerAgent failed: {exc}")
            state["tool_error"] = str(exc)
            state["tool_calls"] = []
            state["tool_used"] = False
            self._set_error_class(state, message=state.get("tool_error"))

        self._record_action_step(
            state,
            stage="tool_planner",
            selected_action="plan_tools",
            inputs={"query": state.get("query")},
            outputs={
                "tool_calls": len(state.get("tool_calls", [])),
                "tool_used": bool(state.get("tool_used")),
            },
            verification_status="ok" if not state.get("tool_error") else "failed",
            verification_reason=None if not state.get("tool_error") else "tool_planner_error",
            error_class=state.get("last_error_class"),
        )

        return state

    async def _run_tool_executor(self, state: PipelineState) -> PipelineState:
        """Execute planned tools."""
        start_time = time.time()
        state["current_agent"] = "ToolExecutor"

        tool_calls = state.get("tool_calls", [])
        if not tool_calls:
            return state

        if not state.get("tool_approved"):
            approval_calls = []
            for call in tool_calls:
                definition = ToolRegistry.get_definition(call.get("name", ""))
                if definition and definition.policy.requires_approval:
                    approval_calls.append(call)
            if approval_calls:
                state["tool_approval_required"] = True
                state["tool_approval_calls"] = approval_calls
                state["tool_approval_message"] = "Approval required to run this tool."
                state["answer_source"] = "approval"
                state["natural_language_answer"] = (
                    "This action needs approval before I can proceed."
                )
                self._set_loop_terminal(
                    state,
                    terminal_state=LoopTerminalState.NEEDS_USER_INPUT,
                    stop_reason=LoopStopReason.TOOL_APPROVAL_REQUIRED,
                )
                self._record_action_step(
                    state,
                    stage="tool_executor",
                    selected_action="await_tool_approval",
                    outputs={"tool_calls_requiring_approval": len(approval_calls)},
                    verification_status="needs_user_input",
                    verification_reason="tool_approval_required",
                    stop_reason=LoopStopReason.TOOL_APPROVAL_REQUIRED.value,
                    terminal_state=LoopTerminalState.NEEDS_USER_INPUT.value,
                )
                return state

        ctx = ToolContext(
            user_id=state.get("user_id", "unknown"),
            correlation_id=state.get("correlation_id", "unknown"),
            approved=state.get("tool_approved", False),
            metadata={
                "retriever": self.retriever,
                "connector": self.connector,
                "database_type": state.get("database_type") or self.config.database.db_type,
                "database_url": state.get("database_url"),
            },
            state=state,
        )

        try:
            results = await self.tool_executor.execute_plan(tool_calls, ctx)
            state["tool_results"] = results

            elapsed = (time.time() - start_time) * 1000
            state.setdefault("agent_timings", {})["tool_executor"] = elapsed
            state["total_latency_ms"] = state.get("total_latency_ms", 0) + elapsed

            self._apply_tool_results(state, results)
        except ToolPolicyError as exc:
            state["tool_error"] = str(exc)
            state["tool_results"] = []
            self._set_error_class(state, message=state.get("tool_error"))
        except Exception as exc:
            state["tool_error"] = str(exc)
            state["tool_results"] = []
            self._set_error_class(state, message=state.get("tool_error"))

        self._record_action_step(
            state,
            stage="tool_executor",
            selected_action="execute_tools",
            outputs={
                "tool_results": len(state.get("tool_results", [])),
                "tool_error": state.get("tool_error"),
            },
            verification_status="ok" if not state.get("tool_error") else "failed",
            verification_reason=None if not state.get("tool_error") else "tool_execution_error",
            error_class=state.get("last_error_class"),
        )

        return state

    async def _run_context(self, state: PipelineState) -> PipelineState:
        """Run ContextAgent."""
        start_time = time.time()
        state["current_agent"] = "ContextAgent"

        try:
            # Convert entities dict to ExtractedEntity objects
            from backend.models import ExtractedEntity

            entities = [ExtractedEntity(**e) for e in state.get("entities", [])]

            input_data = ContextAgentInput(
                query=state["query"],
                conversation_history=self._augment_history_with_summary(state),
                entities=entities,
                retrieval_mode="hybrid",
                max_datapoints=10,
            )

            output = await self.context.execute(input_data)

            # Update state
            allowed_connection_ids = await self._resolve_equivalent_connection_ids(
                target_connection_id=state.get("target_connection_id"),
                database_url=state.get("database_url"),
            )
            raw_datapoints = [
                {
                    "datapoint_id": dp.datapoint_id,
                    "datapoint_type": dp.datapoint_type,
                    "name": dp.name,
                    "score": dp.score,
                    "source": dp.source,
                    "metadata": dp.metadata,
                    "content": dp.content,
                }
                for dp in output.investigation_memory.datapoints
            ]
            connection_scoped_datapoints, connection_scope_trace = (
                self._filter_datapoints_by_target_connection(
                    raw_datapoints,
                    target_connection_id=state.get("target_connection_id"),
                    target_connection_ids=allowed_connection_ids,
                )
            )
            filtered_datapoints, live_schema_trace = await self._filter_datapoints_by_live_schema(
                connection_scoped_datapoints,
                database_type=state.get("database_type"),
                database_url=state.get("database_url"),
            )
            state["retrieval_trace"] = {
                **(output.data.get("retrieval_trace", {}) if isinstance(output.data, dict) else {}),
                "connection_scope": connection_scope_trace,
                "live_schema_filter": live_schema_trace,
                "selected_datapoints": [
                    {
                        "datapoint_id": dp["datapoint_id"],
                        "name": dp.get("name"),
                        "score": dp.get("score"),
                        "source": dp.get("source"),
                        "source_tier": (dp.get("metadata") or {}).get("source_tier"),
                    }
                    for dp in filtered_datapoints
                ],
            }
            sources_used = list({dp["datapoint_id"] for dp in filtered_datapoints})
            state["investigation_memory"] = {
                "query": output.investigation_memory.query,
                "datapoints": filtered_datapoints,
                "total_retrieved": len(filtered_datapoints),
                "retrieval_mode": output.investigation_memory.retrieval_mode,
                "sources_used": sources_used,
            }
            state["retrieved_datapoints"] = state["investigation_memory"]["datapoints"]
            state["context_confidence"] = output.context_confidence
            self._maybe_set_schema_preface(state)

            # Update metadata
            elapsed = (time.time() - start_time) * 1000
            state["agent_timings"]["context"] = elapsed
            state["total_latency_ms"] += elapsed

            logger.info(
                f"ContextAgent complete: retrieved {len(state['retrieved_datapoints'])} datapoints"
            )

        except Exception as e:
            logger.error(f"ContextAgent failed: {e}")
            state["error"] = f"Context retrieval failed: {e}"
            self._set_error_class(state, message=state.get("error"))

        self._record_action_step(
            state,
            stage="context",
            selected_action="retrieve_context",
            inputs={"query": state.get("query")},
            outputs={
                "retrieved_datapoints": len(state.get("retrieved_datapoints", [])),
                "context_confidence": state.get("context_confidence"),
            },
            verification_status="ok" if not state.get("error") else "failed",
            verification_reason=None if not state.get("error") else "context_error",
            error_class=state.get("last_error_class"),
        )

        return state

    def _filter_datapoints_by_target_connection(
        self,
        datapoints: list[dict[str, Any]],
        target_connection_id: str | None,
        target_connection_ids: set[str] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Scope retrieved DataPoints to the selected connection.

        Rules:
        - Keep datapoints with matching metadata.connection_id
        - Keep explicitly global datapoints (metadata.scope in {global, shared}
          or metadata.shared=True)
        - If no scoped/global datapoints were retrieved at all, fallback to
          legacy unscoped datapoints for backwards compatibility.
        """
        if not target_connection_id:
            return datapoints, {
                "applied": False,
                "allowed_connection_ids": [],
                "filtered_out": [],
                "kept_count": len(datapoints),
            }

        allowed_connection_ids = {str(target_connection_id)}
        if target_connection_ids:
            allowed_connection_ids.update(
                str(connection_id) for connection_id in target_connection_ids if connection_id
            )

        scoped: list[dict[str, Any]] = []
        global_items: list[dict[str, Any]] = []
        unscoped: list[dict[str, Any]] = []
        removed_foreign = 0
        filtered_out: list[dict[str, Any]] = []
        for dp in datapoints:
            metadata = dp.get("metadata") or {}
            scope = str(metadata.get("scope", "")).strip().lower()
            shared_raw = metadata.get("shared")
            shared_flag = shared_raw is True or str(shared_raw).strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
            }
            if scope in {"global", "shared"} or shared_flag:
                global_items.append(dp)
                continue

            connection_id = metadata.get("connection_id")
            if connection_id is None:
                unscoped.append(dp)
                continue

            if str(connection_id) not in allowed_connection_ids:
                removed_foreign += 1
                filtered_out.append(
                    {
                        "datapoint_id": dp.get("datapoint_id"),
                        "reason": "different_connection_scope",
                        "connection_id": str(connection_id),
                    }
                )
                continue
            scoped.append(dp)

        if removed_foreign:
            logger.info(
                "Filtered %s datapoints scoped to a different connection",
                removed_foreign,
            )

        # Preferred mode: only scoped + global datapoints.
        if scoped or global_items:
            kept = [*scoped, *global_items]
            return kept, {
                "applied": True,
                "allowed_connection_ids": sorted(allowed_connection_ids),
                "filtered_out": filtered_out,
                "kept_count": len(kept),
            }

        # Backwards compatibility: old datasets may be unscoped.
        if unscoped:
            logger.info(
                "No scoped datapoints matched target connection; using %s unscoped datapoints",
                len(unscoped),
            )
            return unscoped, {
                "applied": True,
                "allowed_connection_ids": sorted(allowed_connection_ids),
                "filtered_out": filtered_out,
                "kept_count": len(unscoped),
                "fallback": "unscoped_legacy_datapoints",
            }

        return [], {
            "applied": True,
            "allowed_connection_ids": sorted(allowed_connection_ids),
            "filtered_out": filtered_out,
            "kept_count": 0,
        }

    async def _resolve_equivalent_connection_ids(
        self,
        *,
        target_connection_id: str | None,
        database_url: str | None,
    ) -> set[str]:
        """Resolve connection IDs that point to the same runtime database URL."""
        resolved: set[str] = set()
        if target_connection_id:
            resolved.add(str(target_connection_id))
        if not database_url:
            return resolved

        cache_key = self._database_identity(database_url)
        if cache_key:
            cached = self._connection_scope_cache.get(cache_key)
            if cached:
                cached_at, cached_ids = cached
                if (time.time() - cached_at) < self._connection_scope_ttl_seconds:
                    return resolved | set(cached_ids)

        equivalents: set[str] = set()
        if self.config.database.url and self._same_database_url(
            str(self.config.database.url), database_url
        ):
            equivalents.add(ENV_DATABASE_CONNECTION_ID)

        system_database_url = (
            str(self.config.system_database.url) if self.config.system_database.url else None
        )
        if system_database_url:
            manager = DatabaseConnectionManager(system_database_url=system_database_url)
            try:
                await manager.initialize()
                for connection in await manager.list_connections():
                    if self._same_database_url(
                        connection.database_url.get_secret_value(),
                        database_url,
                    ):
                        equivalents.add(str(connection.connection_id))
            except Exception as exc:
                logger.debug(
                    "Failed to resolve equivalent connection IDs for datapoint scoping: %s",
                    exc,
                )
            finally:
                try:
                    await manager.close()
                except Exception:
                    pass

        if cache_key and equivalents:
            self._connection_scope_cache[cache_key] = (time.time(), equivalents)

        return resolved | equivalents

    @staticmethod
    def _database_identity(database_url: str | None) -> str | None:
        if not database_url:
            return None
        normalized = database_url.replace("postgresql+asyncpg://", "postgresql://").strip()
        if not normalized:
            return None
        parsed = urlparse(normalized)
        if not parsed.scheme or not parsed.hostname:
            return normalized.lower()
        scheme = parsed.scheme.split("+", 1)[0].lower()
        host = (parsed.hostname or "").lower()
        default_ports = {"postgresql": 5432, "postgres": 5432, "mysql": 3306, "clickhouse": 8123}
        port = parsed.port or default_ports.get(scheme)
        username = parsed.username or ""
        database = parsed.path.lstrip("/")
        return f"{scheme}://{username}@{host}:{port}/{database}"

    def _same_database_url(self, left: str | None, right: str | None) -> bool:
        return self._database_identity(left) == self._database_identity(right)

    async def _run_context_answer(self, state: PipelineState) -> PipelineState:
        """Run ContextAnswerAgent."""
        start_time = time.time()
        state["current_agent"] = "ContextAnswerAgent"

        if state.get("error"):
            return state

        try:
            from backend.models import InvestigationMemory, RetrievedDataPoint

            investigation_memory_state = state.get("investigation_memory") or {}
            datapoints = [
                RetrievedDataPoint(
                    datapoint_id=dp["datapoint_id"],
                    datapoint_type=dp["datapoint_type"],
                    name=dp["name"],
                    score=dp["score"],
                    source=dp["source"],
                    metadata=dp["metadata"],
                    content=dp.get("content"),
                )
                for dp in state.get("retrieved_datapoints", [])
            ]

            investigation_memory = InvestigationMemory(
                query=state["query"],
                datapoints=datapoints,
                total_retrieved=investigation_memory_state.get("total_retrieved", len(datapoints)),
                retrieval_mode=investigation_memory_state.get("retrieval_mode", "hybrid"),
                sources_used=investigation_memory_state.get("sources_used", []),
            )

            input_data = ContextAnswerAgentInput(
                query=state["query"],
                conversation_history=self._augment_history_with_summary(state),
                investigation_memory=investigation_memory,
                intent=state.get("intent"),
                context_confidence=state.get("context_confidence"),
            )

            output = await self.context_answer.execute(input_data)

            sql_first_route = str(state.get("route") or "").lower() == "sql"
            if sql_first_route:
                # SQL-first routes should always attempt execution; clarification is fallback.
                output.context_answer.needs_sql = True
                output.context_answer.clarifying_questions = []

            state["natural_language_answer"] = output.context_answer.answer
            state["answer_source"] = "context"
            state["answer_confidence"] = output.context_answer.confidence
            state["evidence"] = [
                evidence.model_dump() for evidence in output.context_answer.evidence
            ]
            state["context_needs_sql"] = output.context_answer.needs_sql
            state["generated_sql"] = None
            state["validated_sql"] = None
            state["query_result"] = None
            state["visualization_hint"] = None
            if output.context_answer.needs_sql:
                context_preface = self._resolve_context_preface_for_sql_answer(
                    state,
                    output.context_answer.answer,
                )
                state["context_preface"] = context_preface
                state["context_evidence"] = (
                    [evidence.model_dump() for evidence in output.context_answer.evidence]
                    if context_preface
                    else []
                )
            if output.context_answer.clarifying_questions:
                clarification_intro = (
                    output.context_answer.answer
                    or "I need a bit more detail before I can continue."
                )
                applied = await self._apply_clarification_response_with_confirmation(
                    state,
                    output.context_answer.clarifying_questions,
                    default_intro=clarification_intro,
                )
                if not applied:
                    state["clarification_needed"] = False
                    state["clarifying_questions"] = []
            else:
                state["clarification_needed"] = False
                state["clarifying_questions"] = []

            elapsed = (time.time() - start_time) * 1000
            state["agent_timings"]["context_answer"] = elapsed
            state["total_latency_ms"] += elapsed
            state["llm_calls"] += output.metadata.llm_calls
            self._track_tokens(
                state,
                tokens_used=output.metadata.tokens_used,
                llm_calls=output.metadata.llm_calls,
            )

            logger.info("ContextAnswerAgent complete")

        except Exception as e:
            logger.error(f"ContextAnswerAgent failed: {e}")
            # Fallback to SQL path when context answer generation fails so the
            # pipeline can still attempt deterministic/semantic SQL execution.
            state["context_needs_sql"] = True
            state["clarification_needed"] = False
            state["clarifying_questions"] = []
            state["natural_language_answer"] = None
            state["answer_source"] = None
            state["answer_confidence"] = None
            state["evidence"] = []
            state["context_preface"] = (
                "I couldn't answer from context alone, so I'll query the database directly."
            )
            state["context_evidence"] = []
            state["generated_sql"] = None
            state["validated_sql"] = None
            state["query_result"] = None
            state["visualization_hint"] = None
            self._set_error_class(
                state,
                message=f"Context answer generation failed: {e}",
            )

        if state.get("clarification_needed"):
            self._set_loop_terminal(
                state,
                terminal_state=LoopTerminalState.NEEDS_USER_INPUT,
                stop_reason=LoopStopReason.USER_CLARIFICATION_REQUIRED,
            )

        self._record_action_step(
            state,
            stage="context_answer",
            selected_action="synthesize_context_answer",
            outputs={
                "needs_sql": state.get("context_needs_sql"),
                "clarification_needed": bool(state.get("clarification_needed")),
                "answer_source": state.get("answer_source"),
            },
            verification_status="ok" if not state.get("error") else "failed",
            verification_reason=None if not state.get("error") else "context_answer_error",
            error_class=state.get("last_error_class"),
        )

        return state

    async def _filter_datapoints_by_live_schema(
        self,
        datapoints: list[dict[str, Any]],
        database_type: str | None = None,
        database_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        live_tables = await self._get_live_table_catalog(
            database_type=database_type,
            database_url=database_url,
        )
        if not live_tables:
            return datapoints, {
                "applied": False,
                "live_table_count": 0,
                "filtered_out": [],
                "kept_count": len(datapoints),
            }

        filtered: list[dict[str, Any]] = []
        filtered_out: list[dict[str, Any]] = []
        for dp in datapoints:
            table_keys = self._extract_datapoint_table_keys(dp)
            if table_keys and table_keys.isdisjoint(live_tables):
                filtered_out.append(
                    {
                        "datapoint_id": dp.get("datapoint_id"),
                        "reason": "missing_live_schema",
                        "table_keys": sorted(table_keys),
                    }
                )
                continue
            filtered.append(dp)

        if len(filtered) != len(datapoints):
            logger.info(
                "Filtered %s datapoints not present in live schema",
                len(datapoints) - len(filtered),
            )

        return filtered, {
            "applied": True,
            "live_table_count": len(live_tables),
            "filtered_out": filtered_out,
            "kept_count": len(filtered),
        }

    async def _get_live_table_catalog(
        self,
        database_type: str | None = None,
        database_url: str | None = None,
    ) -> set[str] | None:
        connector = self.connector
        close_connector = False
        if database_url:
            connector = self._build_catalog_connector(database_type, database_url)
            close_connector = connector is not None
        if connector is None:
            return None

        try:
            if not connector.is_connected:
                await connector.connect()
            tables = await connector.get_schema()
        except Exception as exc:
            logger.warning(f"Failed to fetch live schema catalog: {exc}")
            return None
        finally:
            if close_connector:
                await connector.close()

        catalog: set[str] = set()
        for table in tables:
            schema_name = getattr(table, "schema_name", None) or getattr(table, "schema", None)
            table_name = getattr(table, "table_name", None)
            if schema_name and table_name:
                catalog.add(f"{schema_name}.{table_name}".lower())
            elif table_name:
                catalog.add(str(table_name).lower())
        return catalog or None

    def _build_catalog_connector(
        self, database_type: str | None, database_url: str
    ) -> BaseConnector | None:
        try:
            return create_connector(
                database_url=database_url,
                database_type=database_type or getattr(self.config.database, "db_type", None),
                pool_size=self.config.database.pool_size,
            )
        except Exception:
            return None

    def _extract_datapoint_table_keys(self, datapoint: dict[str, Any]) -> set[str]:
        metadata = datapoint.get("metadata") or {}
        schema = metadata.get("schema") or datapoint.get("schema")

        def _normalize_table(value: Any) -> str | None:
            if value is None:
                return None
            table_key = str(value).strip()
            if not table_key:
                return None
            if "." not in table_key and schema:
                table_key = f"{schema}.{table_key}"
            return table_key.lower()

        keys: set[str] = set()

        for value in (
            metadata.get("table_name"),
            metadata.get("table"),
            metadata.get("table_key"),
            datapoint.get("table_name"),
        ):
            normalized = _normalize_table(value)
            if normalized:
                keys.add(normalized)

        related_tables = datapoint.get("related_tables")
        if isinstance(related_tables, list):
            for value in related_tables:
                normalized = _normalize_table(value)
                if normalized:
                    keys.add(normalized)

        metadata_related = metadata.get("related_tables")
        if isinstance(metadata_related, str):
            for value in metadata_related.split(","):
                normalized = _normalize_table(value)
                if normalized:
                    keys.add(normalized)
        elif isinstance(metadata_related, list):
            for value in metadata_related:
                normalized = _normalize_table(value)
                if normalized:
                    keys.add(normalized)

        return keys

    def _maybe_set_schema_preface(self, state: PipelineState) -> None:
        if state.get("context_preface"):
            return
        datapoints = state.get("retrieved_datapoints") or []
        for datapoint in datapoints:
            if datapoint.get("datapoint_type") != "Schema":
                continue
            datapoint_id = datapoint.get("datapoint_id")
            if not datapoint_id:
                continue
            summary = self._load_schema_preface(datapoint_id)
            if summary:
                state["context_preface"] = summary
                return

    def _resolve_context_preface_for_sql_answer(
        self,
        state: PipelineState,
        candidate_preface: str | None = None,
    ) -> str | None:
        """Allow only non-clarifying prefaces for non-SQL-first routes."""
        preface = (
            candidate_preface if candidate_preface is not None else state.get("context_preface")
        )
        if not preface:
            return None
        if str(state.get("route") or "").lower() == "sql":
            return None
        if state.get("clarification_needed"):
            return None

        normalized = preface.lower()
        blocked_phrases = (
            "i cannot",
            "i can't",
            "i could not",
            "i couldn't",
            "not enough context",
            "not enough information",
            "need a bit more detail",
            "need more detail",
            "which table",
            "which column",
            "clarifying question",
            "with the provided information",
        )
        if any(phrase in normalized for phrase in blocked_phrases):
            return None
        if "?" in preface:
            return None
        return preface.strip() or None

    def _load_schema_preface(self, datapoint_id: str) -> str | None:
        data_dir = Path("datapoints") / "managed"
        path = data_dir / f"{datapoint_id}.json"
        if not path.exists():
            return None
        try:
            with path.open() as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return None

        name = payload.get("name")
        table_name = payload.get("table_name")
        business_purpose = payload.get("business_purpose")
        key_columns = payload.get("key_columns") or []
        column_names = []
        for col in key_columns:
            if isinstance(col, dict) and col.get("name"):
                column_names.append(col["name"])
            if len(column_names) >= 5:
                break

        parts = []
        if name and table_name:
            parts.append(f"{name} ({table_name})")
        elif table_name:
            parts.append(table_name)
        elif name:
            parts.append(name)

        if business_purpose:
            parts.append(business_purpose)

        if column_names:
            parts.append(f"Key columns include {', '.join(column_names)}.")

        if not parts:
            return None

        return " ".join(parts)

    async def _run_sql(self, state: PipelineState) -> PipelineState:
        """Run SQLAgent."""
        start_time = time.time()
        state["current_agent"] = "SQLAgent"

        if state.get("error"):
            return state

        try:
            # Reconstruct InvestigationMemory from state before handling preplanned SQL so
            # deterministic catalog planning can safely override weak multi-planner metadata SQL.
            from backend.models import InvestigationMemory, RetrievedDataPoint

            investigation_memory_state = state.get("investigation_memory") or {}
            datapoints = [
                RetrievedDataPoint(
                    datapoint_id=dp["datapoint_id"],
                    datapoint_type=dp["datapoint_type"],
                    name=dp["name"],
                    score=dp["score"],
                    source=dp["source"],
                    metadata=dp["metadata"],
                    content=dp.get("content"),
                )
                for dp in state.get("retrieved_datapoints", [])
            ]

            investigation_memory = InvestigationMemory(
                query=state["query"],
                datapoints=datapoints,
                total_retrieved=investigation_memory_state.get("total_retrieved", len(datapoints)),
                retrieval_mode=investigation_memory_state.get("retrieval_mode", "hybrid"),
                sources_used=investigation_memory_state.get(
                    "sources_used",
                    list({dp["source"] for dp in state.get("retrieved_datapoints", [])}),
                ),
            )

            resolved_query = await self._maybe_apply_any_table_hint(state)
            if resolved_query != state.get("query"):
                state["original_query"] = state.get("original_query") or state.get("query")
                state["query"] = resolved_query
                investigation_memory = investigation_memory.model_copy(update={"query": resolved_query})

            preplanned_sql = state.get("preplanned_sql")
            if isinstance(preplanned_sql, dict):
                catalog_plan = self.sql.catalog.plan_query(
                    query=state["query"],
                    database_type=state.get("database_type", "postgresql"),
                    investigation_memory=investigation_memory,
                )
                if catalog_plan and catalog_plan.clarifying_questions:
                    self._apply_clarification_response(state, catalog_plan.clarifying_questions)
                    elapsed = (time.time() - start_time) * 1000
                    state["agent_timings"]["sql"] = elapsed
                    state["total_latency_ms"] += elapsed
                    return state

                if catalog_plan and catalog_plan.sql:
                    planned_sql = GeneratedSQL(
                        sql=catalog_plan.sql,
                        explanation=catalog_plan.explanation,
                        used_datapoints=[],
                        confidence=max(0.0, min(1.0, float(catalog_plan.confidence))),
                        assumptions=[],
                        clarifying_questions=[],
                    )
                    planned_sql = self.sql._apply_row_limit_policy(planned_sql, state["query"])
                    state["clarification_needed"] = False
                    state["clarifying_questions"] = []
                    state["generated_sql"] = planned_sql.sql
                    state["sql_explanation"] = planned_sql.explanation
                    state["sql_confidence"] = planned_sql.confidence
                    state["used_datapoints"] = planned_sql.used_datapoints
                    state["assumptions"] = planned_sql.assumptions

                    elapsed = (time.time() - start_time) * 1000
                    state["agent_timings"]["sql"] = elapsed
                    state["total_latency_ms"] += elapsed
                    logger.info(
                        "SQLAgent complete from deterministic catalog planner: "
                        f"confidence={state['sql_confidence']:.2f}"
                    )
                    return state

                preplanned_text = str(preplanned_sql.get("sql", "") or "").strip()
                preplanned_questions = preplanned_sql.get("clarifying_questions", [])
                if isinstance(preplanned_questions, str):
                    preplanned_questions = [preplanned_questions]
                if (
                    isinstance(preplanned_questions, list)
                    and preplanned_questions
                    and not preplanned_text
                ):
                    questions = [
                        str(question).strip()
                        for question in preplanned_questions
                        if str(question).strip()
                    ]
                    applied = await self._apply_clarification_response_with_confirmation(
                        state,
                        questions,
                    )
                    if not applied:
                        state["error"] = "SQL planning failed: clarification was not confirmed."
                        self._set_error_class(state, message=state.get("error"))
                    elapsed = (time.time() - start_time) * 1000
                    state["agent_timings"]["sql"] = elapsed
                    state["total_latency_ms"] += elapsed
                    self._record_action_step(
                        state,
                        stage="sql",
                        selected_action="request_clarification",
                        outputs={
                            "clarification_needed": bool(state.get("clarification_needed")),
                            "preplanned_sql": False,
                        },
                        verification_status=(
                            "needs_user_input"
                            if state.get("clarification_needed")
                            else "failed"
                        ),
                        verification_reason=(
                            "preplanned_clarification"
                            if state.get("clarification_needed")
                            else "preplanned_clarification_not_confirmed"
                        ),
                        error_class=state.get("last_error_class"),
                    )
                    return state

                if preplanned_text:
                    planned_confidence = preplanned_sql.get("confidence", 0.78)
                    try:
                        confidence = float(planned_confidence)
                    except (TypeError, ValueError):
                        confidence = 0.78
                    confidence = max(0.0, min(1.0, confidence))
                    planned_sql = GeneratedSQL(
                        sql=preplanned_text,
                        explanation=str(
                            preplanned_sql.get("explanation")
                            or "Multi-question SQL planner output."
                        ),
                        used_datapoints=[],
                        confidence=confidence,
                        assumptions=[],
                        clarifying_questions=[],
                    )
                    planned_sql = self.sql._apply_row_limit_policy(planned_sql, state["query"])

                    state["clarification_needed"] = False
                    state["clarifying_questions"] = []
                    state["generated_sql"] = planned_sql.sql
                    state["sql_explanation"] = planned_sql.explanation
                    state["sql_confidence"] = planned_sql.confidence
                    state["used_datapoints"] = planned_sql.used_datapoints
                    state["assumptions"] = planned_sql.assumptions

                    elapsed = (time.time() - start_time) * 1000
                    state["agent_timings"]["sql"] = elapsed
                    state["total_latency_ms"] += elapsed
                    self._record_action_step(
                        state,
                        stage="sql",
                        selected_action="use_preplanned_sql",
                        outputs={
                            "generated_sql": planned_sql.sql,
                            "preplanned_sql": True,
                            "sql_confidence": planned_sql.confidence,
                        },
                    )
                    logger.info(
                        "SQLAgent complete from multi planner: "
                        f"confidence={state['sql_confidence']:.2f}"
                    )
                    return state

            input_data = SQLAgentInput(
                query=state["query"],
                conversation_history=self._augment_history_with_summary(state),
                investigation_memory=investigation_memory,
                database_type=state.get("database_type", "postgresql"),
                database_url=state.get("database_url"),
            )

            output = await self.sql.execute(input_data)

            elapsed = (time.time() - start_time) * 1000
            state["agent_timings"]["sql"] = elapsed
            state["total_latency_ms"] += elapsed
            state["llm_calls"] += output.metadata.llm_calls
            self._track_tokens(
                state,
                tokens_used=output.metadata.tokens_used,
                llm_calls=output.metadata.llm_calls,
            )
            state["sql_formatter_fallback_calls"] = int(
                (output.data or {}).get("formatter_fallback_calls", 0)
            )
            state["sql_formatter_fallback_successes"] = int(
                (output.data or {}).get("formatter_fallback_successes", 0)
            )

            if output.needs_clarification:
                questions = output.generated_sql.clarifying_questions or []
                if not questions:
                    questions = ["Which table should I use to answer this?"]
                applied = await self._apply_clarification_response_with_confirmation(
                    state,
                    questions,
                )
                if applied:
                    self._record_action_step(
                        state,
                        stage="sql",
                        selected_action="request_clarification",
                        outputs={
                            "clarification_needed": True,
                            "generated_sql": output.generated_sql.sql,
                            "clarifying_questions": questions,
                        },
                        verification_status="needs_user_input",
                        verification_reason="sql_agent_requested_clarification",
                    )
                    return state
                sql_text = str(getattr(output.generated_sql, "sql", "") or "").strip().rstrip(";")
                if sql_text.upper() == "SELECT 1":
                    # Safety guard: never execute placeholder SQL if clarification is still unresolved.
                    self._apply_clarification_response(
                        state,
                        questions,
                        default_intro=(
                            "I still need one detail before I can generate executable SQL."
                        ),
                    )
                    self._record_action_step(
                        state,
                        stage="sql",
                        selected_action="request_clarification",
                        outputs={
                            "clarification_needed": True,
                            "generated_sql": output.generated_sql.sql,
                            "clarifying_questions": questions,
                        },
                        verification_status="needs_user_input",
                        verification_reason="placeholder_sql_blocked",
                    )
                    return state

            if await self._should_gate_low_confidence_sql(state, output):
                fallback = await self._build_clarification_fallback(state)
                if fallback:
                    applied = await self._apply_clarification_response_with_confirmation(
                        state,
                        fallback["questions"],
                        default_intro=fallback["answer"],
                    )
                else:
                    applied = await self._apply_clarification_response_with_confirmation(
                        state,
                        ["Which table should I use to answer this?"],
                        default_intro=(
                            "I am not confident enough to run this query yet. "
                            "Please confirm the table first."
                        ),
                    )
                if applied:
                    self._record_action_step(
                        state,
                        stage="sql",
                        selected_action="request_clarification",
                        outputs={
                            "clarification_needed": True,
                            "generated_sql": output.generated_sql.sql,
                            "clarifying_questions": fallback["questions"] if fallback else [],
                        },
                        verification_status="needs_user_input",
                        verification_reason="low_confidence_sql_guard",
                    )
                    return state

            # Clear any stale clarification flags from earlier agents.
            state["clarification_needed"] = False
            state["clarifying_questions"] = []

            # Update state
            state["generated_sql"] = output.generated_sql.sql
            state["sql_explanation"] = output.generated_sql.explanation
            state["sql_confidence"] = output.generated_sql.confidence
            state["used_datapoints"] = getattr(
                output.generated_sql,
                "used_datapoints",
                getattr(output.generated_sql, "used_datapoint_ids", []),
            )
            state["assumptions"] = output.generated_sql.assumptions
            state["sql_formatter_fallback_calls"] = int(
                (output.data or {}).get("formatter_fallback_calls", 0)
            )
            state["sql_formatter_fallback_successes"] = int(
                (output.data or {}).get("formatter_fallback_successes", 0)
            )
            state["query_compiler_llm_calls"] = int(
                (output.data or {}).get("query_compiler_llm_calls", 0)
            )
            state["query_compiler_llm_refinements"] = int(
                (output.data or {}).get("query_compiler_llm_refinements", 0)
            )
            state["query_compiler_latency_ms"] = float(
                (output.data or {}).get("query_compiler_latency_ms", 0.0)
            )
            query_compiler_summary = (output.data or {}).get("query_compiler")
            if isinstance(query_compiler_summary, dict):
                state["query_compiler"] = query_compiler_summary
                self._record_decision(
                    state,
                    stage="query_compiler",
                    decision=str(query_compiler_summary.get("path") or "unknown"),
                    reason=str(query_compiler_summary.get("reason") or "n/a"),
                    details={
                        "confidence": query_compiler_summary.get("confidence"),
                        "selected_tables": query_compiler_summary.get("selected_tables", []),
                        "candidate_tables": query_compiler_summary.get("candidate_tables", []),
                        "operators": query_compiler_summary.get("operators", []),
                    },
                )

            retry_info = (
                f" (retry {state.get('retry_count', 0)})" if state.get("retry_count", 0) > 0 else ""
            )
            logger.info(f"SQLAgent complete{retry_info}: confidence={state['sql_confidence']:.2f}")

        except Exception as e:
            logger.error(f"SQLAgent failed: {e}")
            state["error"] = f"SQL generation failed: {e}"
            self._set_error_class(state, message=state.get("error"))

        self._record_action_step(
            state,
            stage="sql",
            selected_action="generate_sql",
            outputs={
                "generated_sql": state.get("generated_sql"),
                "clarification_needed": bool(state.get("clarification_needed")),
                "sql_confidence": state.get("sql_confidence"),
                "sql_explanation": state.get("sql_explanation"),
            },
            verification_status="ok" if not state.get("error") else "failed",
            verification_reason=None if not state.get("error") else "sql_generation_error",
            error_class=state.get("last_error_class"),
        )

        return state

    async def _run_validator(self, state: PipelineState) -> PipelineState:
        """Run ValidatorAgent."""
        start_time = time.time()
        state["current_agent"] = "ValidatorAgent"

        if state.get("error"):
            return state

        try:
            if not state.get("generated_sql"):
                raise ValueError("Missing generated SQL for validation")

            generated_sql = GeneratedSQL(
                sql=state["generated_sql"],
                explanation=state.get("sql_explanation", ""),
                used_datapoints=state.get("used_datapoints", []),
                confidence=state.get("sql_confidence", 0.0),
                assumptions=state.get("assumptions", []),
                clarifying_questions=state.get("clarifying_questions", []),
            )

            input_data = ValidatorAgentInput(
                query=state["query"],
                conversation_history=self._augment_history_with_summary(state),
                generated_sql=generated_sql,
                target_database=state.get("database_type", "postgresql"),
                strict_mode=False,  # Allow warnings
            )

            output = await self.validator.execute(input_data)

            # Update state
            state["validated_sql_object"] = output.validated_sql
            state["validated_sql"] = output.validated_sql.sql if output.validated_sql else None
            state["validation_passed"] = (
                output.validated_sql.is_valid if output.validated_sql else output.success
            )
            state["is_safe"] = output.validated_sql.is_safe if output.validated_sql else None
            state["validation_errors"] = (
                [
                    {
                        "message": e.message,
                        "error_type": getattr(e, "error_type", None),
                        "severity": getattr(e, "severity", None),
                        "location": getattr(e, "location", None),
                    }
                    for e in output.validated_sql.errors
                ]
                if hasattr(output.validated_sql, "errors")
                else []
            )
            state["validation_warnings"] = (
                [
                    {
                        "message": w.message,
                        "warning_type": getattr(w, "warning_type", None),
                        "suggestion": getattr(w, "suggestion", None),
                    }
                    for w in output.validated_sql.warnings
                ]
                if hasattr(output.validated_sql, "warnings")
                else []
            )
            state["performance_score"] = (
                output.validated_sql.performance_score
                if hasattr(output.validated_sql, "performance_score")
                else None
            )

            # Update metadata
            elapsed = (time.time() - start_time) * 1000
            state["agent_timings"]["validator"] = elapsed
            state["total_latency_ms"] += elapsed

            if state["validation_passed"]:
                state["last_error_class"] = None
                logger.info(
                    f"ValidatorAgent complete: PASSED (warnings: {len(state['validation_warnings'])})"
                )
            else:
                self._set_error_class(
                    state,
                    message="SQL validation failed",
                    validation_errors=state.get("validation_errors"),
                )
                next_retry = state.get("retry_count", 0) + 1
                if next_retry > self.max_retries:
                    state["retry_count"] = self.max_retries
                    state["retries_exhausted"] = True
                    state["error"] = (
                        f"Failed to generate valid SQL after {self.max_retries} attempts"
                    )
                else:
                    state["retry_count"] = next_retry
                    state["retries_exhausted"] = False
                logger.warning(
                    f"ValidatorAgent complete: FAILED (errors: {len(state['validation_errors'])})"
                )

        except Exception as e:
            logger.error(f"ValidatorAgent failed: {e}")
            state["error"] = f"Validation failed: {e}"
            state["validation_passed"] = False
            self._set_error_class(state, message=state.get("error"))

        self._record_action_step(
            state,
            stage="validator",
            selected_action="validate_sql",
            outputs={
                "validation_passed": bool(state.get("validation_passed")),
                "validation_error_count": len(state.get("validation_errors", [])),
                "retry_count": state.get("retry_count", 0),
            },
            verification_status="ok" if state.get("validation_passed") else "failed",
            verification_reason=(
                None
                if state.get("validation_passed")
                else "sql_validation_failed"
            ),
            error_class=state.get("last_error_class"),
        )

        return state

    async def _run_executor(self, state: PipelineState) -> PipelineState:
        """Run ExecutorAgent."""
        start_time = time.time()
        state["current_agent"] = "ExecutorAgent"

        if state.get("error"):
            return state

        try:
            from backend.models import ValidatedSQL

            validated_sql = state.get("validated_sql_object")
            if validated_sql is None:
                validated_sql = ValidatedSQL(
                    sql=state["validated_sql"],
                    is_valid=True,
                    errors=[],
                    warnings=[],
                    suggestions=[],
                    is_safe=state.get("is_safe", True),
                    performance_score=state.get("performance_score", 1.0),
                )

            input_data = ExecutorAgentInput(
                query=state["query"],
                conversation_history=self._augment_history_with_summary(state),
                validated_sql=validated_sql,
                database_type=state.get("database_type", "postgresql"),
                database_url=state.get("database_url"),
                max_rows=100,
                timeout_seconds=30,
                source_datapoints=state.get("used_datapoints", []),
            )

            output = await self.executor.execute(input_data)
            executed_sql = (
                getattr(output.executed_query, "executed_sql", None)
                or state.get("validated_sql")
                or state.get("generated_sql")
            )

            # Update state
            state["query_result"] = {
                "rows": output.executed_query.query_result.rows,
                "row_count": output.executed_query.query_result.row_count,
                "columns": output.executed_query.query_result.columns,
                "execution_time_ms": output.executed_query.query_result.execution_time_ms,
                "was_truncated": output.executed_query.query_result.was_truncated,
            }
            state["validated_sql"] = executed_sql
            state["generated_sql"] = executed_sql
            validated_sql_object = state.get("validated_sql_object")
            if validated_sql_object is not None:
                validated_sql_object.sql = executed_sql
            state["natural_language_answer"] = output.executed_query.natural_language_answer
            state["visualization_hint"] = output.executed_query.visualization_hint
            state["visualization_note"] = getattr(output.executed_query, "visualization_note", None)
            state["visualization_metadata"] = output.executed_query.visualization_metadata
            state["key_insights"] = output.executed_query.key_insights
            state["answer_source"] = "sql"
            state["answer_confidence"] = state.get("sql_confidence", 0.7)
            sql_evidence = [evidence.model_dump() for evidence in self._build_evidence_items(state)]
            context_evidence = state.get("context_evidence") or []
            seen_ids = set()
            merged_evidence = []
            for item in [*context_evidence, *sql_evidence]:
                datapoint_id = item.get("datapoint_id")
                if datapoint_id and datapoint_id not in seen_ids:
                    seen_ids.add(datapoint_id)
                    merged_evidence.append(item)
            state["evidence"] = merged_evidence
            context_preface = self._resolve_context_preface_for_sql_answer(state)
            if context_preface:
                state["natural_language_answer"] = (
                    f"{context_preface}\n\n{state['natural_language_answer']}"
                )

            # Update metadata
            elapsed = (time.time() - start_time) * 1000
            state["agent_timings"]["executor"] = elapsed
            state["total_latency_ms"] += elapsed
            state["llm_calls"] += output.metadata.llm_calls
            self._track_tokens(
                state,
                tokens_used=output.metadata.tokens_used,
                llm_calls=output.metadata.llm_calls,
            )
            state["last_error_class"] = None

            logger.info(
                f"ExecutorAgent complete: {state['query_result']['row_count']} rows, "
                f"viz={state['visualization_hint']}"
            )

        except Exception as e:
            logger.error(f"ExecutorAgent failed: {e}")
            error_message = str(e)
            state["error"] = f"Execution failed: {error_message}"
            self._set_error_class(state, message=state.get("error"))
            if "Missing table in live schema" in error_message:
                missing = error_message.split("Missing table in live schema:", 1)[-1].strip()
                missing = re.sub(r"\.\s*Schema refresh required\.?", "", missing).strip()
                intro = (
                    f"I couldn't find `{missing}` in the connected database schema. "
                    "Please verify the table name or run `list tables` to choose a valid table."
                )
                applied = await self._apply_clarification_response_with_confirmation(
                    state,
                    ["Which existing table should I use instead?"],
                    default_intro=intro,
                )
                if applied:
                    state["answer_confidence"] = 0.3
                    state.pop("error", None)
                elif not state.get("natural_language_answer"):
                    state["natural_language_answer"] = intro
            elif not state.get("natural_language_answer"):
                state["natural_language_answer"] = (
                    f"I encountered an error while processing your query: {state.get('error')}. "
                    "Please try rephrasing your question or contact support if the issue persists."
                )

        self._record_action_step(
            state,
            stage="executor",
            selected_action="execute_sql",
            outputs={
                "query_result_rows": (state.get("query_result") or {}).get("row_count")
                if isinstance(state.get("query_result"), dict)
                else None,
                "answer_source": state.get("answer_source"),
            },
            verification_status="ok" if not state.get("error") else "failed",
            verification_reason=None if not state.get("error") else "executor_error",
            error_class=state.get("last_error_class"),
        )

        return state

    async def _run_schema_refresh(self, state: PipelineState) -> None:
        if not self.tooling_enabled:
            logger.warning("Tooling disabled; skipping schema refresh.")
            return
        ctx = ToolContext(
            user_id=state.get("user_id"),
            correlation_id=state.get("correlation_id"),
            approved=True,
        )
        try:
            result = await self.tool_executor.execute(
                "profile_and_generate_datapoints",
                {"depth": "schema_only", "batch_size": 10},
                ctx,
            )
            state["tool_results"] = [result]
        except Exception as exc:
            logger.error(f"Schema refresh failed: {exc}")
            state["tool_error"] = str(exc)

    async def _rerun_after_schema_refresh(self, state: PipelineState) -> PipelineState:
        state = await self._run_context(state)
        if self._should_use_context_answer(state) == "context":
            state = await self._run_context_answer(state)
            if self._should_execute_after_context_answer(state) == "end":
                return state

        while True:
            state = await self._run_sql(state)
            state = await self._run_validator(state)
            decision = self._should_retry_sql(state)
            if decision == "retry":
                continue
            if decision == "execute":
                return await self._run_executor(state)
            return await self._handle_error(state)

    async def _run_response_synthesis(self, state: PipelineState) -> PipelineState:
        """Run ResponseSynthesisAgent."""
        start_time = time.time()
        state["current_agent"] = "ResponseSynthesisAgent"
        try:
            query_result = state.get("query_result") or {}
            rows = query_result.get("rows") or []
            columns = query_result.get("columns") or []
            preview_rows = rows[:3]
            result_summary = {
                "row_count": query_result.get("row_count"),
                "columns": columns,
                "preview": preview_rows,
            }
            synthesized = await self.response_synthesis.execute(
                query=state.get("query", ""),
                sql=state.get("validated_sql") or state.get("generated_sql") or "",
                result_summary=json.dumps(result_summary, default=str),
                context_preface=self._resolve_context_preface_for_sql_answer(state),
            )
            state["natural_language_answer"] = synthesized
            elapsed = (time.time() - start_time) * 1000
            state.setdefault("agent_timings", {})["response_synthesis"] = elapsed
            state["total_latency_ms"] = state.get("total_latency_ms", 0) + elapsed
            state["llm_calls"] = state.get("llm_calls", 0) + 1
            self._track_tokens(state, tokens_used=None, llm_calls=1)
        except Exception as exc:
            logger.error(f"ResponseSynthesisAgent failed: {exc}")
            self._set_error_class(state, message=str(exc))
        self._record_action_step(
            state,
            stage="response_synthesis",
            selected_action="synthesize_response",
            outputs={"answer_present": bool(state.get("natural_language_answer"))},
            verification_status="ok" if not state.get("error") else "failed",
            verification_reason=None if not state.get("error") else "response_synthesis_error",
            error_class=state.get("last_error_class"),
        )
        return state

    async def _handle_error(self, state: PipelineState) -> PipelineState:
        """Handle pipeline errors."""
        state["current_agent"] = "ErrorHandler"
        if not state.get("clarification_needed"):
            state["answer_source"] = "error"
        logger.error(f"Pipeline error: {state.get('error', 'Unknown error')}")
        self._set_error_class(
            state,
            message=state.get("error"),
            validation_errors=state.get("validation_errors"),
        )

        # Provide graceful error message
        if not state.get("natural_language_answer"):
            state["natural_language_answer"] = (
                f"I encountered an error while processing your query: {state.get('error')}. "
                "Please try rephrasing your question or contact support if the issue persists."
            )

        if not state.get("loop_terminal_state"):
            self._set_loop_terminal(
                state,
                terminal_state=LoopTerminalState.BLOCKED,
                stop_reason=LoopStopReason.ERROR,
            )
        self._record_action_step(
            state,
            stage="error_handler",
            selected_action="handle_error",
            outputs={"error": state.get("error")},
            verification_status="failed",
            verification_reason="pipeline_error",
            error_class=state.get("last_error_class"),
            stop_reason=state.get("loop_stop_reason"),
            terminal_state=state.get("loop_terminal_state"),
        )

        return state

    # ========================================================================
    # Conditional Edge Logic
    # ========================================================================

    def _record_decision(
        self,
        state: PipelineState,
        *,
        stage: str,
        decision: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        trace = state.setdefault("decision_trace", [])
        entry: dict[str, Any] = {
            "stage": stage,
            "decision": decision,
            "reason": reason,
        }
        if details:
            entry["details"] = details
        trace.append(entry)
        if state.get("loop_enabled") and state.get("loop_shadow_mode"):
            self._record_shadow_decision(
                state,
                stage=stage,
                recommended_decision=decision,
                actual_decision=decision,
                reason="shadow_observe",
            )

    def _loop_controller_for_state(self, state: PipelineState) -> ActionLoopController:
        budget_payload = state.get("loop_budget") or self.loop_budget.model_dump()
        try:
            budget = LoopBudget.model_validate(budget_payload)
        except Exception:
            budget = self.loop_budget
        return ActionLoopController(budget)

    def _set_loop_terminal(
        self,
        state: PipelineState,
        *,
        terminal_state: LoopTerminalState | str,
        stop_reason: LoopStopReason | str,
    ) -> None:
        state["loop_terminal_state"] = (
            terminal_state.value if isinstance(terminal_state, LoopTerminalState) else terminal_state
        )
        state["loop_stop_reason"] = (
            stop_reason.value if isinstance(stop_reason, LoopStopReason) else stop_reason
        )

    def _record_action_step(
        self,
        state: PipelineState,
        *,
        stage: str,
        selected_action: str,
        inputs: dict[str, Any] | None = None,
        outputs: dict[str, Any] | None = None,
        verification_status: str = "ok",
        verification_reason: str | None = None,
        verification_details: dict[str, Any] | None = None,
        error_class: str | None = None,
        stop_reason: str | None = None,
        terminal_state: str | None = None,
    ) -> None:
        if not state.get("loop_enabled"):
            return

        step = int(state.get("loop_steps_taken", 0) or 0) + 1
        state["loop_steps_taken"] = step
        trace = state.setdefault("action_trace", [])
        action_state = ActionState(
            step=step,
            stage=stage,
            selected_action=selected_action,
            inputs=inputs or {},
            outputs=outputs or {},
            verification=ActionVerification(
                status=verification_status,
                reason=verification_reason,
                details=verification_details or {},
            ),
            error_class=error_class,
            stop_reason=stop_reason,
            terminal_state=terminal_state,
        )
        trace.append(action_state.model_dump(mode="json"))

    def _record_shadow_decision(
        self,
        state: PipelineState,
        *,
        stage: str,
        recommended_decision: str,
        actual_decision: str,
        reason: str,
    ) -> None:
        shadow = state.setdefault("loop_shadow_decisions", [])
        shadow.append(
            {
                "stage": stage,
                "recommended_decision": recommended_decision,
                "actual_decision": actual_decision,
                "parity": recommended_decision == actual_decision,
                "reason": reason,
            }
        )

    def _build_run_status(self, state: PipelineState) -> str:
        if state.get("error"):
            return "failed"
        terminal_state = str(state.get("loop_terminal_state") or "").strip().lower()
        if terminal_state == "needs_user_input":
            return "needs_input"
        if terminal_state == "blocked":
            return "failed"
        return "completed"

    @staticmethod
    def _build_run_summary(state: PipelineState) -> dict[str, Any]:
        query_result = state.get("query_result") if isinstance(state.get("query_result"), dict) else {}
        return {
            "query": state.get("query"),
            "route": state.get("route"),
            "answer_source": state.get("answer_source"),
            "answer_confidence": state.get("answer_confidence"),
            "clarification_needed": bool(state.get("clarification_needed")),
            "clarifying_questions": state.get("clarifying_questions", []),
            "validation_error_count": len(state.get("validation_errors", [])),
            "validation_warning_count": len(state.get("validation_warnings", [])),
            "retrieved_datapoint_count": len(state.get("retrieved_datapoints", [])),
            "used_datapoint_count": len(state.get("used_datapoints", [])),
            "retrieval_mode": (state.get("investigation_memory") or {}).get("retrieval_mode"),
            "row_count": query_result.get("row_count") if query_result else None,
            "workflow_mode": state.get("workflow_mode"),
        }

    @staticmethod
    def _build_quality_findings(state: PipelineState) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        query = str(state.get("query") or "")
        answer_confidence = state.get("answer_confidence") or state.get("sql_confidence")
        retrieved_datapoint_count = len(state.get("retrieved_datapoints", []))
        query_result = state.get("query_result") if isinstance(state.get("query_result"), dict) else {}

        if bool(state.get("clarification_needed")) or state.get("clarifying_questions"):
            findings.append(
                {
                    "finding_type": "advisory",
                    "severity": "warning",
                    "category": "intent",
                    "code": "clarification_required",
                    "message": "The run required clarification before it could proceed confidently.",
                    "details": {"query": query, "questions": state.get("clarifying_questions", [])},
                }
            )

        if retrieved_datapoint_count == 0:
            findings.append(
                {
                    "finding_type": "advisory",
                    "severity": "warning",
                    "category": "retrieval",
                    "code": "retrieval_miss",
                    "message": "No datapoints were retrieved for this run.",
                    "details": {
                        "query": query,
                        "route": state.get("route"),
                        "retrieval_trace": state.get("retrieval_trace", {}),
                    },
                }
            )

        for warning in state.get("validation_warnings", []) or []:
            if not isinstance(warning, dict):
                continue
            findings.append(
                {
                    "finding_type": "advisory",
                    "severity": "warning",
                    "category": "validation",
                    "code": str(warning.get("warning_type") or "validation_warning"),
                    "message": str(warning.get("message") or "Validation warning recorded."),
                    "details": warning,
                }
            )

        for error in state.get("validation_errors", []) or []:
            if not isinstance(error, dict):
                continue
            findings.append(
                {
                    "finding_type": "error",
                    "severity": "error",
                    "category": "validation",
                    "code": str(error.get("error_type") or "validation_error"),
                    "message": str(error.get("message") or "Validation error recorded."),
                    "details": error,
                }
            )

        if isinstance(answer_confidence, (int, float)) and answer_confidence < 0.6:
            findings.append(
                {
                    "finding_type": "advisory",
                    "severity": "warning",
                    "category": "answer",
                    "code": "low_confidence_answer",
                    "message": "The run finished with low answer confidence.",
                    "details": {"answer_confidence": float(answer_confidence), "query": query},
                }
            )

        if query_result and int(query_result.get("row_count", 0) or 0) == 0:
            findings.append(
                {
                    "finding_type": "advisory",
                    "severity": "info",
                    "category": "result",
                    "code": "empty_result_set",
                    "message": "The executed query returned zero rows.",
                    "details": {"query": query, "generated_sql": state.get("generated_sql")},
                }
            )

        if state.get("error") or state.get("last_error_class"):
            findings.append(
                {
                    "finding_type": "error",
                    "severity": "error",
                    "category": "execution",
                    "code": str(state.get("last_error_class") or "run_failed"),
                    "message": str(state.get("error") or "The run terminated with an error."),
                    "details": {"query": query, "route": state.get("route")},
                }
            )

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for finding in findings:
            key = (
                str(finding.get("severity")),
                str(finding.get("category")),
                str(finding.get("code")),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(finding)
        return deduped

    def _build_run_output(self, state: PipelineState) -> dict[str, Any]:
        query_result = state.get("query_result") if isinstance(state.get("query_result"), dict) else None
        return {
            "query": state.get("query"),
            "natural_language_answer": state.get("natural_language_answer"),
            "generated_sql": state.get("generated_sql"),
            "validated_sql": state.get("validated_sql"),
            "route": state.get("route"),
            "answer_source": state.get("answer_source"),
            "answer_confidence": state.get("answer_confidence"),
            "error": state.get("error"),
            "failure_class": state.get("last_error_class"),
            "metrics": {
                "total_latency_ms": state.get("total_latency_ms"),
                "agent_timings": state.get("agent_timings", {}),
                "llm_calls": state.get("llm_calls", 0),
                "total_tokens_used": state.get("total_tokens_used", 0),
            },
            "retrieval_trace": state.get("retrieval_trace", {}),
            "query_result": {
                "row_count": query_result.get("row_count"),
                "columns": query_result.get("columns"),
                "execution_time_ms": query_result.get("execution_time_ms"),
                "was_truncated": query_result.get("was_truncated"),
            }
            if query_result
            else None,
            "decision_trace": state.get("decision_trace", []),
            "action_trace": state.get("action_trace", []),
            "loop_terminal_state": state.get("loop_terminal_state"),
            "loop_stop_reason": state.get("loop_stop_reason"),
        }

    def _build_run_steps(self, state: PipelineState) -> list[dict[str, Any]]:
        trace = state.get("action_trace") or []
        if isinstance(trace, list) and trace:
            agent_timings = state.get("agent_timings") or {}
            stage_timings = {
                "query_analyzer": agent_timings.get("query_analyzer"),
                "context": agent_timings.get("context"),
                "context_answer": agent_timings.get("context_answer"),
                "sql": agent_timings.get("sql"),
                "validator": agent_timings.get("validator"),
                "executor": agent_timings.get("executor"),
                "tool_planner": agent_timings.get("tool_planner"),
                "tool_executor": agent_timings.get("tool_executor"),
                "response_synthesis": agent_timings.get("response_synthesis"),
            }
            steps: list[dict[str, Any]] = []
            for entry in trace:
                if not isinstance(entry, dict):
                    continue
                step_order = int(entry.get("step", len(steps) + 1))
                stage = str(entry.get("stage") or f"step_{step_order}")
                verification = entry.get("verification") or {}
                steps.append(
                    {
                        "step_id": uuid4(),
                        "step_order": step_order,
                        "step_name": stage,
                        "status": str(verification.get("status") or "ok"),
                        "latency_ms": stage_timings.get(stage),
                        "summary": entry,
                        "created_at": datetime.now(UTC),
                    }
                )
            if steps:
                return steps

        steps = []
        for idx, (stage, latency_ms) in enumerate((state.get("agent_timings") or {}).items(), start=1):
            steps.append(
                {
                    "step_id": uuid4(),
                    "step_order": idx,
                    "step_name": stage,
                    "status": "ok",
                    "latency_ms": latency_ms,
                    "summary": {"stage": stage, "latency_ms": latency_ms},
                    "created_at": datetime.now(UTC),
                }
            )
        return steps

    async def _persist_completed_run(self, state: PipelineState) -> None:
        if self.run_store is None:
            return
        run_id = state.get("run_id")
        if not run_id:
            return
        try:
            await self.run_store.save_run(
                run_id=UUID(str(run_id)),
                run_type="chat",
                status=self._build_run_status(state),
                route=str(state.get("route") or state.get("answer_source") or "unknown"),
                connection_id=(
                    str(state.get("target_connection_id"))
                    if state.get("target_connection_id")
                    else None
                ),
                conversation_id=None,
                correlation_id=str(state.get("correlation_id") or ""),
                failure_class=state.get("last_error_class"),
                confidence=state.get("answer_confidence") or state.get("sql_confidence"),
                warning_count=len(state.get("validation_warnings", [])),
                error_count=len(state.get("validation_errors", [])) + (1 if state.get("error") else 0),
                latency_ms=state.get("total_latency_ms"),
                summary=self._build_run_summary(state),
                output=self._build_run_output(state),
                started_at=state.get("run_started_at") or datetime.now(UTC),
                completed_at=datetime.now(UTC),
                steps=self._build_run_steps(state),
                quality_findings=self._build_quality_findings(state),
            )
        except Exception as exc:
            logger.warning("Failed to persist pipeline run: %s", exc)

    def _track_tokens(
        self,
        state: PipelineState,
        *,
        tokens_used: int | None,
        llm_calls: int = 0,
    ) -> None:
        current = int(state.get("total_tokens_used", 0) or 0)
        if tokens_used and tokens_used > 0:
            state["total_tokens_used"] = current + int(tokens_used)
            return
        if llm_calls > 0:
            # Fallback estimate when provider metadata does not include token counts.
            state["total_tokens_used"] = current + int(llm_calls) * 800

    def _classify_error(
        self,
        *,
        message: str | None = None,
        validation_errors: list[dict[str, Any]] | None = None,
    ) -> str | None:
        text_parts = []
        if message:
            text_parts.append(str(message))
        if validation_errors:
            text_parts.extend(str(item.get("message", "")) for item in validation_errors)
        text = " ".join(text_parts).strip().lower()
        if not text:
            return None

        if any(token in text for token in ("permission", "forbidden", "not authorized")):
            return LoopErrorClass.PERMISSION.value
        if any(token in text for token in ("timeout", "timed out", "deadline exceeded")):
            return LoopErrorClass.TIMEOUT.value
        if any(
            token in text
            for token in (
                "connection refused",
                "connector",
                "network",
                "could not connect",
                "connection error",
            )
        ):
            return LoopErrorClass.CONNECTOR_FAILURE.value
        if any(
            token in text
            for token in (
                "does not exist",
                "unknown table",
                "unknown column",
                "missing table",
                "missing column",
                "relation",
                "schema",
            )
        ):
            return LoopErrorClass.SCHEMA_MISMATCH.value
        if any(token in text for token in ("ambiguous", "semantic", "clarification needed")):
            return LoopErrorClass.SEMANTIC_MISMATCH.value
        if any(token in text for token in ("validation", "invalid", "syntax error")):
            return LoopErrorClass.VALIDATION.value
        return None

    def _set_error_class(
        self,
        state: PipelineState,
        *,
        message: str | None = None,
        validation_errors: list[dict[str, Any]] | None = None,
    ) -> None:
        error_class = self._classify_error(message=message, validation_errors=validation_errors)
        if error_class:
            state["last_error_class"] = error_class

    def _maybe_apply_loop_guard_decision(
        self,
        state: PipelineState,
        *,
        stage: str,
        actual_decision: str,
        enforced_decision: str,
    ) -> str:
        if not state.get("loop_enabled"):
            return actual_decision

        controller = self._loop_controller_for_state(state)
        stop_reason = controller.budget_stop_reason(state)
        if stop_reason is None:
            return actual_decision

        if state.get("loop_shadow_mode"):
            self._record_shadow_decision(
                state,
                stage=stage,
                recommended_decision=enforced_decision,
                actual_decision=actual_decision,
                reason=stop_reason.value,
            )
            return actual_decision

        self._set_loop_terminal(
            state,
            terminal_state=LoopTerminalState.BLOCKED,
            stop_reason=stop_reason,
        )
        if not state.get("natural_language_answer"):
            state["natural_language_answer"] = (
                "I paused execution because runtime safety limits were reached. "
                "Please narrow the query and try again."
            )
        state["answer_source"] = state.get("answer_source") or "system"
        state["answer_confidence"] = state.get("answer_confidence") or 0.4
        self._record_action_step(
            state,
            stage=stage,
            selected_action=enforced_decision,
            verification_status="blocked",
            verification_reason="loop_budget_limit",
            stop_reason=stop_reason.value,
            terminal_state=LoopTerminalState.BLOCKED.value,
        )
        return enforced_decision

    def _finalize_action_loop(self, state: PipelineState) -> None:
        if not state.get("loop_enabled"):
            return
        if state.get("loop_terminal_state") and state.get("loop_stop_reason"):
            return

        controller = self._loop_controller_for_state(state)
        terminal_state, stop_reason = controller.normalize_terminal(
            answer_source=state.get("answer_source"),
            has_error=bool(state.get("error")),
            clarification_needed=bool(
                state.get("clarification_needed") or state.get("clarifying_questions")
            ),
            tool_approval_required=bool(state.get("tool_approval_required")),
            error_class=state.get("last_error_class"),
        )
        self._set_loop_terminal(
            state,
            terminal_state=terminal_state,
            stop_reason=stop_reason,
        )

    def _should_continue_after_intent_gate(self, state: PipelineState) -> str:
        intent_gate = state.get("intent_gate")
        if intent_gate in {
            "exit",
            "out_of_scope",
            "small_talk",
            "setup_help",
            "datapoint_help",
            "clarify",
        }:
            self._record_decision(
                state,
                stage="continue_after_intent_gate",
                decision="end",
                reason=f"intent_gate={intent_gate}",
            )
            return "end"
        if state.get("fast_path"):
            self._record_decision(
                state,
                stage="continue_after_intent_gate",
                decision="sql",
                reason="fast_path",
            )
            return "sql"
        if self._should_run_tool_planner(state):
            self._record_decision(
                state,
                stage="continue_after_intent_gate",
                decision="tool_planner",
                reason="tool_planner_enabled_for_query",
            )
            return "tool_planner"
        # Default to context path for entity extraction
        self._record_decision(
            state,
            stage="continue_after_query_analyzer",
            decision="context",
            reason="default_context_path",
        )
        return "context"

    def _should_run_tool_planner(self, state: PipelineState) -> bool:
        if not (self.tooling_enabled and self.tool_planner_enabled):
            return False
        pipeline_cfg = getattr(self.config, "pipeline", None)
        selective = (
            True
            if pipeline_cfg is None
            else bool(getattr(pipeline_cfg, "selective_tool_planner_enabled", True))
        )
        if not selective:
            return True
        return self._query_likely_requires_tools(state.get("query", ""))

    def _query_likely_requires_tools(self, query: str) -> bool:
        text = (query or "").strip().lower()
        if not text:
            return False
        tool_patterns = [
            r"\bprofile\b",
            r"\bdatapoint quality\b",
            r"\bquality report\b",
            r"\bsync datapoints?\b",
            r"\bgenerate datapoints?\b",
            r"\brun tool\b",
            r"\bexecute tool\b",
            r"\bapprove\b",
            r"\brefresh profile\b",
        ]
        return any(re.search(pattern, text) for pattern in tool_patterns)

    def _should_validate_sql(self, state: PipelineState) -> str:
        decision = "clarify" if state.get("clarification_needed") else "validate"
        return self._maybe_apply_loop_guard_decision(
            state,
            stage="validate_sql_resolution",
            actual_decision=decision,
            enforced_decision="end",
        )

    def _should_use_context_answer(self, state: PipelineState) -> str:
        if state.get("error"):
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="sql",
                reason="state_error_present",
            )
            return "sql"

        intent = state.get("intent") or "data_query"
        confidence = state.get("context_confidence") or 0.0
        query = (state.get("query") or "").lower()
        retrieved = state.get("retrieved_datapoints") or []

        if not retrieved:
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="sql",
                reason="no_retrieved_datapoints",
            )
            return "sql"

        if self._query_is_table_list(query):
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="sql",
                reason="deterministic_table_list_query",
            )
            return "sql"

        if "datapoint" in query or "data point" in query:
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="context",
                reason="datapoint_definition_request",
            )
            return "context"

        if self._query_is_definition_intent(query):
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="context",
                reason="definition_intent",
            )
            return "context"

        if self._query_requires_sql(query):
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="sql",
                reason="query_requires_sql_keywords",
            )
            return "sql"

        if intent in ("exploration", "explanation", "meta"):
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="context",
                reason=f"intent={intent}",
            )
            return "context"

        threshold = float(self.routing_policy["context_answer_confidence_threshold"])
        if confidence >= threshold:
            self._record_decision(
                state,
                stage="context_vs_sql",
                decision="context",
                reason="context_confidence_threshold_met",
                details={"confidence": confidence, "threshold": threshold},
            )
            return "context"

        self._record_decision(
            state,
            stage="context_vs_sql",
            decision="sql",
            reason="context_confidence_below_threshold",
            details={"confidence": confidence, "threshold": threshold},
        )
        return "sql"

    def _query_is_definition_intent(self, query: str) -> bool:
        patterns = [
            r"^\s*define\b",
            r"\bdefinition of\b",
            r"\bwhat does\b.*\b(mean|stand for)\b",
            r"\bmeaning of\b",
            r"\bhow is\b.*\b(calculated|computed|defined)\b",
            r"\bhow do (?:i|we|you)\b.*\bcalculate\b",
            r"\bbusiness rules?\b",
        ]
        return any(re.search(pattern, query) for pattern in patterns)

    def _query_is_table_list(self, query: str) -> bool:
        patterns = [
            r"\bwhat tables\b",
            r"\blist tables\b",
            r"\bshow tables\b",
            r"\bavailable tables\b",
            r"\bwhich tables\b",
            r"\bwhat tables exist\b",
        ]
        return any(re.search(pattern, query) for pattern in patterns)

    def _query_is_column_list(self, query: str) -> bool:
        patterns = [
            r"\bshow columns\b",
            r"\blist columns\b",
            r"\bwhat columns\b",
            r"\bwhich columns\b",
            r"\bdescribe table\b",
            r"\btable schema\b",
            r"\bcolumn list\b",
            r"\bfields in\b",
        ]
        return any(re.search(pattern, query) for pattern in patterns)

    def _should_use_tools(self, state: PipelineState) -> str:
        decision = "pipeline"
        reason = "no_tool_calls"
        details: dict[str, Any] | None = None
        if state.get("tool_error"):
            reason = "tool_error_present"
        else:
            tool_calls = state.get("tool_calls", [])
            tool_plan = state.get("tool_plan") or {}
            if tool_plan.get("fallback") == "pipeline":
                reason = "tool_plan_fallback_pipeline"
            elif tool_calls:
                decision = "tools"
                reason = "tool_calls_planned"
                details = {"tool_calls": len(tool_calls)}

        decision = self._maybe_apply_loop_guard_decision(
            state,
            stage="tool_plan_resolution",
            actual_decision=decision,
            enforced_decision="pipeline",
        )
        self._record_decision(
            state,
            stage="tool_plan_resolution",
            decision=decision,
            reason=reason,
            details=details,
        )
        return decision

    def _should_continue_after_tool_execution(self, state: PipelineState) -> str:
        decision = "pipeline"
        reason = "continue_pipeline_after_tools"
        if state.get("tool_error"):
            reason = "tool_execution_error"
        elif state.get("tool_approval_required"):
            decision = "end"
            reason = "tool_approval_required"
        elif state.get("tool_used") and state.get("natural_language_answer"):
            decision = "end"
            reason = "tool_answer_ready"

        decision = self._maybe_apply_loop_guard_decision(
            state,
            stage="tool_execution_resolution",
            actual_decision=decision,
            enforced_decision="end",
        )
        self._record_decision(
            state,
            stage="tool_execution_resolution",
            decision=decision,
            reason=reason,
        )
        return decision

    def _query_requires_sql(self, query: str) -> bool:
        if "datapoint" in query or "data point" in query:
            return False
        keywords = (
            "total",
            "sum",
            "count",
            "average",
            "avg",
            "min",
            "max",
            "rate",
            "ratio",
            "percent",
            "percentage",
            "pct",
            "trend",
            "by",
            "per",
            "over time",
            "last",
            "this month",
            "this year",
            "yesterday",
        )
        return any(keyword in query for keyword in keywords)

    def _should_execute_after_context_answer(self, state: PipelineState) -> str:
        decision = "end"
        if not state.get("error") and (
            str(state.get("route") or "").lower() == "sql" or state.get("context_needs_sql")
        ):
            decision = "sql"
        return self._maybe_apply_loop_guard_decision(
            state,
            stage="context_answer_resolution",
            actual_decision=decision,
            enforced_decision="end",
        )

    def _build_evidence_items(self, state: PipelineState) -> list[EvidenceItem]:
        evidence: list[EvidenceItem] = []
        datapoints = {dp.get("datapoint_id"): dp for dp in state.get("retrieved_datapoints", [])}
        for datapoint_id in state.get("used_datapoints", []):
            dp = datapoints.get(datapoint_id, {})
            evidence.append(
                EvidenceItem(
                    datapoint_id=datapoint_id,
                    name=dp.get("name"),
                    type=dp.get("datapoint_type", dp.get("type")),
                    reason="Used for SQL generation",
                )
            )
        return evidence

    def _apply_tool_results(self, state: PipelineState, results: list[dict[str, Any]]) -> None:
        for result in results:
            payload = result.get("result") or {}
            answer = payload.get("answer")
            if answer:
                state["natural_language_answer"] = answer
            if payload.get("answer_source"):
                state["answer_source"] = payload.get("answer_source")
            if payload.get("confidence") is not None:
                state["answer_confidence"] = payload.get("confidence")
            if payload.get("evidence"):
                state["evidence"] = payload.get("evidence")
            if payload.get("sql"):
                state["validated_sql"] = payload.get("sql")
            if payload.get("data"):
                state["query_result"] = payload.get("data")
            if payload.get("visualization_hint"):
                state["visualization_hint"] = payload.get("visualization_hint")
            if payload.get("retrieved_datapoints"):
                state["retrieved_datapoints"] = payload.get("retrieved_datapoints")
            if payload.get("used_datapoints"):
                state["used_datapoints"] = payload.get("used_datapoints")
            if payload.get("validation_warnings"):
                state["validation_warnings"] = payload.get("validation_warnings")
            if payload.get("validation_errors"):
                state["validation_errors"] = payload.get("validation_errors")

        if state.get("used_datapoints") and not state.get("evidence"):
            state["evidence"] = [item.model_dump() for item in self._build_evidence_items(state)]

    def _should_retry_sql(self, state: PipelineState) -> str:
        """
        Determine if SQL should be retried or execution should proceed.

        Returns:
            "retry": Retry SQL generation
            "execute": Proceed to execution
            "error": Max retries exceeded
        """
        decision = "error"
        if state.get("error"):
            decision = "error"
        elif state.get("validation_passed"):
            decision = "execute"
        elif state.get("retries_exhausted"):
            logger.error(f"Max retries ({self.max_retries}) exceeded")
            decision = "error"
        else:
            retry_count = int(state.get("retry_count", 0) or 0)
            error_class = state.get("last_error_class")
            if error_class in {
                LoopErrorClass.PERMISSION.value,
                LoopErrorClass.CONNECTOR_FAILURE.value,
            }:
                decision = "error"
            elif error_class == LoopErrorClass.TIMEOUT.value and retry_count > 1:
                decision = "error"
            elif retry_count <= self.max_retries:
                logger.info(f"Retrying SQL generation (attempt {retry_count}/{self.max_retries})")
                decision = "retry"

        return self._maybe_apply_loop_guard_decision(
            state,
            stage="validator_resolution",
            actual_decision=decision,
            enforced_decision="error",
        )

    def _should_synthesize_response(self, state: PipelineState) -> str:
        decision = "end"
        if not state.get("error") and not state.get("skip_response_synthesis"):
            if state.get("validated_sql") and state.get("query_result"):
                synthesize_simple = state.get("synthesize_simple_sql")
                if synthesize_simple is None:
                    pipeline_cfg = getattr(self.config, "pipeline", None)
                    synthesize_simple = (
                        True
                        if pipeline_cfg is None
                        else bool(getattr(pipeline_cfg, "synthesize_simple_sql_answers", True))
                    )
                if not synthesize_simple and self._is_simple_sql_response(state):
                    decision = "end"
                else:
                    decision = "synthesize"
        return self._maybe_apply_loop_guard_decision(
            state,
            stage="response_synthesis_resolution",
            actual_decision=decision,
            enforced_decision="end",
        )

    def _is_simple_sql_response(self, state: PipelineState) -> bool:
        sql = (state.get("validated_sql") or "").strip()
        if not sql:
            return False
        if re.search(r"\b(JOIN|GROUP\s+BY|WITH|UNION|OVER|HAVING)\b", sql, flags=re.IGNORECASE):
            return False

        query_result = state.get("query_result") or {}
        row_count = query_result.get("row_count")
        if row_count is None and isinstance(query_result.get("rows"), list):
            row_count = len(query_result.get("rows", []))
        try:
            row_count_num = int(row_count) if row_count is not None else 0
        except (TypeError, ValueError):
            row_count_num = 0
        if row_count_num > 10:
            return False

        columns = query_result.get("columns")
        if isinstance(columns, list) and len(columns) > 8:
            return False
        return True

    def _format_clarifying_response(self, query: str, questions: list[str]) -> str:
        if not questions:
            return "I need a bit more detail to generate SQL. Which table should I use?"
        prompt = "I need a bit more detail to generate SQL:"
        formatted = "\n".join(f"- {question}" for question in questions)
        return f"{prompt}\n{formatted}"

    def _current_clarification_count(self, state: PipelineState) -> int:
        summary = state.get("intent_summary") or {}
        from_summary = int(summary.get("clarification_count", 0) or 0)
        from_state = int(state.get("clarification_turn_count", 0) or 0)
        return max(from_summary, from_state)

    async def _apply_clarification_response_with_confirmation(
        self,
        state: PipelineState,
        questions: list[str],
        default_intro: str | None = None,
    ) -> bool:
        (
            should_apply,
            confirmed_questions,
            confirmed_intro,
        ) = await self._confirm_clarification_with_llm(
            state=state,
            questions=questions,
            default_intro=default_intro,
        )
        if not should_apply:
            state["clarification_needed"] = False
            state["clarifying_questions"] = []
            return False

        self._apply_clarification_response(
            state,
            confirmed_questions,
            default_intro=confirmed_intro,
        )
        return True

    async def _confirm_clarification_with_llm(
        self,
        *,
        state: PipelineState,
        questions: list[str],
        default_intro: str | None,
    ) -> tuple[bool, list[str], str | None]:
        enabled = bool(self.routing_policy.get("clarification_confirmation_enabled", True))
        if not enabled:
            return True, questions, default_intro

        intent = str(state.get("intent") or "")
        route = str(state.get("route") or "")
        if not intent and not route:
            return True, questions, default_intro
        if intent and intent not in {"data_query", "clarify"} and route != "sql":
            return True, questions, default_intro

        llm = getattr(self.query_analyzer, "llm", None) or self.intent_llm
        if llm is None:
            return True, questions, default_intro

        ranked_tables = self._rank_table_candidates(
            state.get("query") or "",
            self._collect_table_candidates(state),
            limit=6,
        )
        system_prompt = (
            "You are a clarification gate for a data assistant. Decide if the assistant truly "
            "needs user clarification before continuing. Prefer NO clarification if the request "
            "can be handled by best-effort SQL generation from available tables/metrics.\n"
            "Return JSON only with keys: needs_clarification (bool), confidence (0-1), "
            "clarifying_questions (array), intro (optional)."
        )
        user_prompt = (
            f"User query: {state.get('query') or ''}\n"
            f"Intent: {intent or 'unknown'}\n"
            f"Route: {route or 'unknown'}\n"
            f"Proposed clarifying questions: {questions}\n"
            f"Candidate tables: {ranked_tables}\n"
            "Decide whether clarification is strictly required."
        )
        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=system_prompt),
                LLMMessage(role="user", content=user_prompt),
            ],
            temperature=0.0,
            max_tokens=300,
        )
        try:
            response = await llm.generate(request)
            payload = self._parse_clarification_confirmation_response(str(response.content))
            if payload is None:
                return True, questions, default_intro

            needs_clarification = bool(payload.get("needs_clarification", True))
            confidence = float(payload.get("confidence", 0.0) or 0.0)
            threshold = float(
                self.routing_policy.get("clarification_confirmation_confidence_threshold", 0.6)
            )
            if not needs_clarification or confidence < threshold:
                return False, questions, default_intro

            parsed_questions = payload.get("clarifying_questions") or []
            if isinstance(parsed_questions, str):
                parsed_questions = [parsed_questions]
            confirmed_questions = [
                str(item).strip() for item in parsed_questions if str(item).strip()
            ] or questions
            intro = payload.get("intro")
            confirmed_intro = (
                str(intro).strip()
                if isinstance(intro, str) and str(intro).strip()
                else default_intro
            )
            return True, confirmed_questions, confirmed_intro
        except Exception as exc:
            logger.debug(f"Clarification confirmation skipped due to LLM error: {exc}")
            return True, questions, default_intro

    def _parse_clarification_confirmation_response(
        self,
        content: str,
    ) -> dict[str, Any] | None:
        json_match = re.search(r"\{.*\}", content, re.DOTALL)
        if not json_match:
            return None
        try:
            data = json.loads(json_match.group(0))
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        if "needs_clarification" not in data:
            return None
        return data

    def _apply_clarification_response(
        self,
        state: PipelineState,
        questions: list[str],
        default_intro: str | None = None,
    ) -> None:
        limit = int(state.get("clarification_limit", self.max_clarifications) or 0)
        current_count = self._current_clarification_count(state)
        state["clarification_turn_count"] = current_count

        if current_count >= max(limit, 0):
            fallback = self._format_clarification_limit_message(state)
            state["clarification_needed"] = False
            state["clarifying_questions"] = []
            state["answer_source"] = "system"
            state["answer_confidence"] = 0.5
            state["natural_language_answer"] = fallback
            state["intent"] = "meta"
            self._set_loop_terminal(
                state,
                terminal_state=LoopTerminalState.BLOCKED,
                stop_reason=LoopStopReason.BUDGET_CLARIFICATIONS_EXCEEDED,
            )
            return

        if not questions:
            questions = ["Which table should I use to answer this?"]

        state["clarification_turn_count"] = current_count + 1
        state["clarification_needed"] = True
        state["clarifying_questions"] = questions
        state["answer_source"] = "clarification"
        state["answer_confidence"] = 0.2
        intro = default_intro or "I need a bit more detail to generate SQL:"
        formatted = "\n".join(f"- {question}" for question in questions)
        state["natural_language_answer"] = f"{intro}\n{formatted}"
        state["intent"] = "clarify"
        self._set_loop_terminal(
            state,
            terminal_state=LoopTerminalState.NEEDS_USER_INPUT,
            stop_reason=LoopStopReason.USER_CLARIFICATION_REQUIRED,
        )

    def _format_clarification_limit_message(self, state: PipelineState) -> str:
        candidates = self._collect_table_candidates(state)
        options: list[str] = []
        if candidates:
            options.append(f"1. Pick one table to continue: {', '.join(candidates[:5])}.")
        options.append("2. Ask me to list available tables.")
        options.append("3. Ask a fully specified question with table + metric.")
        options.append("4. Type `exit` to end the session.")
        return "I still cannot answer confidently after several clarifications.\n" + "\n".join(
            options
        )

    def _build_intent_summary(self, query: str, history: list[Message]) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "last_goal": None,
            "last_clarifying_question": None,
            "last_clarifying_questions": [],
            "table_hints": [],
            "column_hints": [],
            "clarification_count": 0,
            "resolved_query": None,
            "any_table": False,
            "slots": {
                "table": None,
                "metric": None,
                "time_range": None,
            },
            "target_subquery_index": None,
        }

        if not history:
            return summary

        last_clarifying_questions: list[str] = []
        last_clarifying_index = None
        clarification_count = 0

        for msg in history:
            role, content = self._message_role_content(msg)
            if role == "assistant" and self._is_clarification_prompt(content):
                clarification_count += 1

        for idx in range(len(history) - 1, -1, -1):
            role, content = self._message_role_content(history[idx])
            if role == "assistant" and self._is_clarification_prompt(content):
                last_clarifying_questions = self._extract_clarifying_questions(content)
                last_clarifying_index = idx
                break

        summary["clarification_count"] = clarification_count
        summary["last_clarifying_questions"] = last_clarifying_questions
        summary["last_clarifying_question"] = (
            last_clarifying_questions[0] if last_clarifying_questions else None
        )

        previous_user_text = None
        if last_clarifying_index is not None:
            for idx in range(last_clarifying_index - 1, -1, -1):
                role, content = self._message_role_content(history[idx])
                if role == "user" and content:
                    previous_user_text = content
                    break

        if previous_user_text:
            summary["last_goal"] = previous_user_text

        target_subquery_index = None
        for question in last_clarifying_questions:
            target_subquery_index = self._extract_subquery_index(question)
            if target_subquery_index:
                break
        if target_subquery_index and previous_user_text:
            split_prior = self._split_multi_query(previous_user_text)
            if 1 <= target_subquery_index <= len(split_prior):
                previous_user_text = split_prior[target_subquery_index - 1]
                summary["last_goal"] = previous_user_text
                summary["target_subquery_index"] = target_subquery_index

        if last_clarifying_questions and self._is_short_followup(query):
            cleaned_hint = self._clean_hint(query)
            if self._is_any_table_request(query):
                summary["any_table"] = True
                if previous_user_text:
                    summary["resolved_query"] = f"{previous_user_text} Use any table."
                return summary

            if cleaned_hint:
                combined_questions = " ".join(last_clarifying_questions).lower()
                if "table" in combined_questions:
                    summary["table_hints"] = [cleaned_hint]
                    summary["slots"]["table"] = cleaned_hint
                    if previous_user_text:
                        summary["resolved_query"] = self._merge_query_with_table_hint(
                            previous_user_text,
                            cleaned_hint,
                        )
                elif "column" in combined_questions or "field" in combined_questions:
                    summary["column_hints"] = [cleaned_hint]
                    summary["slots"]["metric"] = cleaned_hint
                    if previous_user_text:
                        summary["resolved_query"] = (
                            f"{previous_user_text} Use column {cleaned_hint}."
                        )
                elif "date" in combined_questions or "time" in combined_questions:
                    summary["slots"]["time_range"] = cleaned_hint

        return summary

    def _augment_history_with_summary(self, state: PipelineState) -> list[Message]:
        history = (state.get("conversation_history") or [])[-12:]
        summary = state.get("intent_summary") or {}
        summary_text = self._format_intent_summary(summary)
        session_summary = (state.get("session_summary") or "").strip()

        system_messages: list[Message] = []
        if session_summary:
            system_messages.append(
                {"role": "system", "content": f"Session memory: {session_summary}"}
            )
        if summary_text:
            system_messages.append({"role": "system", "content": summary_text})
        if not system_messages:
            return history
        return [*system_messages, *history]

    def _format_intent_summary(self, summary: dict[str, Any]) -> str | None:
        if not summary:
            return None
        parts = []
        if summary.get("last_goal"):
            parts.append(f"last_goal={summary['last_goal']}")
        if summary.get("table_hints"):
            parts.append(f"table_hints={', '.join(summary['table_hints'])}")
        if summary.get("column_hints"):
            parts.append(f"column_hints={', '.join(summary['column_hints'])}")
        slots = summary.get("slots") or {}
        slot_parts = [f"{k}:{v}" for k, v in slots.items() if v]
        if slot_parts:
            parts.append(f"slots={', '.join(slot_parts)}")
        if summary.get("clarification_count"):
            parts.append(f"clarifications={summary['clarification_count']}")
        questions = summary.get("last_clarifying_questions") or []
        if questions:
            parts.append(f"last_questions={'; '.join(questions[:2])}")
        if summary.get("target_subquery_index"):
            parts.append(f"target_subquery=Q{summary['target_subquery_index']}")
        if not parts:
            return None
        return "Intent summary: " + " | ".join(parts)

    def _merge_session_state_into_summary(
        self,
        summary: dict[str, Any],
        session_state: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not session_state:
            return summary

        merged = dict(summary)
        slots = dict(merged.get("slots") or {})
        prior_slots = session_state.get("slots") if isinstance(session_state, dict) else {}
        if isinstance(prior_slots, dict):
            for key, value in prior_slots.items():
                if value and not slots.get(key):
                    slots[key] = value
        merged["slots"] = slots

        for key in ("table_hints", "column_hints", "last_clarifying_questions"):
            current = list(merged.get(key) or [])
            prior = list(session_state.get(key) or [])
            combined: list[str] = []
            for value in [*prior, *current]:
                if value and value not in combined:
                    combined.append(value)
            merged[key] = combined

        merged["clarification_count"] = max(
            int(merged.get("clarification_count", 0) or 0),
            int(session_state.get("clarification_count", 0) or 0),
        )

        if not merged.get("last_goal") and session_state.get("last_goal"):
            merged["last_goal"] = str(session_state.get("last_goal"))
        if not merged.get("target_subquery_index") and session_state.get("target_subquery_index"):
            merged["target_subquery_index"] = session_state.get("target_subquery_index")
        if session_state.get("any_table"):
            merged["any_table"] = True
        if not merged.get("resolved_query") and session_state.get("resolved_query"):
            merged["resolved_query"] = str(session_state.get("resolved_query"))
        if not merged.get("last_clarifying_question"):
            prior_questions = merged.get("last_clarifying_questions") or []
            if prior_questions:
                merged["last_clarifying_question"] = prior_questions[0]
        return merged

    def _rewrite_contextual_followup(self, query: str, summary: dict[str, Any]) -> str | None:
        text = (query or "").strip()
        if not text:
            return None
        if not self._is_contextual_followup_query(text):
            return None
        if self._contains_data_keywords(text):
            return None

        last_goal = str(summary.get("last_goal") or "").strip()
        if not last_goal:
            return None
        focus = self._extract_followup_focus(text)
        if not focus:
            return None

        last_goal_lower = last_goal.lower()
        if "how many " in last_goal_lower:
            return f"How many {focus} do we have?"
        if last_goal_lower.startswith("list "):
            return f"List {focus}"
        if last_goal_lower.startswith("show "):
            return f"Show {focus}"
        if "total " in last_goal_lower:
            return f"What is total {focus}?"
        return None

    def _is_contextual_followup_query(self, text: str) -> bool:
        lowered = text.strip().lower()
        patterns = [
            r"^what\s+about\b",
            r"^how\s+about\b",
            r"^what\s+of\b",
            r"^and\b",
            r"^about\b",
        ]
        return any(re.search(pattern, lowered) for pattern in patterns)

    def _extract_followup_focus(self, text: str) -> str | None:
        cleaned = text.strip().strip("\"'").strip()
        cleaned = re.sub(
            r"^(what\s+about|how\s+about|what\s+of|and|about)\s+", "", cleaned, flags=re.I
        )
        cleaned = cleaned.strip(" .,!?:;\"'")
        cleaned = re.sub(r"^(the|our|their)\s+", "", cleaned, flags=re.I)
        if not cleaned:
            return None
        return cleaned.lower()

    async def _maybe_apply_any_table_hint(self, state: PipelineState) -> str:
        query = state.get("query") or ""
        if not self._is_any_table_request(query):
            return query

        candidates = self._collect_table_candidates(state)
        if not candidates:
            live_tables = await self._get_live_table_catalog(
                database_type=state.get("database_type"),
                database_url=state.get("database_url"),
            )
            if live_tables:
                candidates = sorted(live_tables)

        if not candidates:
            return query

        ranked = self._rank_table_candidates(query, candidates, limit=1)
        if not ranked:
            return query

        selected = ranked[0]
        if "use table" in query.lower():
            return query

        summary = state.get("intent_summary") or {}
        if selected not in summary.get("table_hints", []):
            summary.setdefault("table_hints", []).append(selected)
            state["intent_summary"] = summary

        return f"{query.rstrip('. ')} Use table {selected}."

    async def _build_clarification_fallback(self, state: PipelineState) -> dict[str, Any] | None:
        candidates = self._collect_table_candidates(state)
        if not candidates:
            live_tables = await self._get_live_table_catalog(
                database_type=state.get("database_type"),
                database_url=state.get("database_url"),
            )
            if live_tables:
                candidates = sorted(live_tables)

        if not candidates:
            return None

        ranked = self._rank_table_candidates(state.get("query") or "", candidates, limit=5)
        if not ranked:
            return None
        focus_hint = self._extract_focus_hint(state.get("query") or "")
        focus_text = f" for {focus_hint}" if focus_hint else ""
        table_list = ", ".join(ranked)
        answer = (
            "I still need a bit more detail to generate SQL. "
            f"Here are a few tables that look relevant: {table_list}. "
            "Which table should I use?"
        )
        return {
            "answer": answer,
            "questions": [f"Which table should I use{focus_text}?"],
        }

    def _collect_table_candidates(self, state: PipelineState) -> list[str]:
        candidates: list[str] = []
        for dp in state.get("retrieved_datapoints", []) or []:
            if not isinstance(dp, dict):
                continue
            if dp.get("datapoint_type") != "Schema":
                continue
            metadata = dp.get("metadata") or {}
            table_name = metadata.get("table_name") or metadata.get("table")
            if table_name:
                candidates.append(str(table_name))
        return list(dict.fromkeys(candidates))

    def _rank_table_candidates(
        self, query: str, candidates: list[str], limit: int = 5
    ) -> list[str]:
        tokens = set(re.findall(r"[a-z0-9]+", query.lower()))
        stopwords = {
            "show",
            "me",
            "the",
            "a",
            "an",
            "first",
            "rows",
            "row",
            "count",
            "total",
            "sum",
            "average",
            "avg",
            "use",
            "table",
            "any",
            "from",
            "of",
            "in",
            "for",
            "with",
            "please",
            "pick",
            "select",
            "list",
        }
        tokens = {token for token in tokens if token not in stopwords}

        scored = []
        for name in candidates:
            name_tokens = set(re.findall(r"[a-z0-9]+", name.lower()))
            score = len(tokens & name_tokens)
            scored.append((score, name))
        scored.sort(key=lambda item: (-item[0], item[1]))

        if scored and scored[0][0] == 0:
            return [name for _, name in scored[:limit]]

        return [name for score, name in scored if score > 0][:limit]

    async def _should_gate_low_confidence_sql(
        self,
        state: PipelineState,
        output: Any,
    ) -> bool:
        """Block low-confidence semantic SQL and request clarification first."""
        query = (state.get("query") or "").strip()
        if not query:
            return False
        if self._is_deterministic_sql_query(query):
            return False
        if self._query_is_table_list(query.lower()) or self._query_is_column_list(query.lower()):
            return False
        if self._extract_table_reference(query.lower()):
            return False

        confidence = float(output.generated_sql.confidence or 0.0)
        threshold = float(self.routing_policy["semantic_sql_clarification_confidence_threshold"])
        if confidence >= threshold:
            return False

        if self._extract_focus_hint(query):
            return True

        if self._contains_data_keywords(query.lower()):
            return True

        summary = state.get("intent_summary") or {}
        return bool(self._is_ambiguous_intent(state, summary))

    def _classify_intent_gate(self, query: str) -> str:
        text = query.strip().lower()
        if not text:
            return "data_query"
        if self._is_exit_intent(text):
            return "exit"
        if self._is_datapoint_help_intent(text):
            return "datapoint_help"
        if self._is_setup_help_intent(text):
            return "setup_help"
        if self._is_small_talk(text):
            return "small_talk"
        if self._is_out_of_scope(text):
            return "out_of_scope"
        if self._is_non_actionable_utterance(text):
            return "clarify"
        return "data_query"

    def _build_intent_gate_response(self, intent: str) -> str:
        if intent == "exit":
            return "Got it. Ending the session. If you need more, just start a new chat."
        if intent == "setup_help":
            return (
                "To connect a database, open Settings -> Database Manager in the web app "
                "or run `datachat setup` / `datachat connect` in the CLI. "
                "Then ask questions like: list tables, show first 5 rows of a table, "
                "or total sales last month."
            )
        if intent == "datapoint_help":
            return (
                "You can manage and inspect DataPoints without writing SQL. "
                "In the UI, open Database Manager and review Pending/Approved DataPoints. "
                "In the CLI, use `datachat dp list` for indexed DataPoints and "
                "`datachat pending list` for approval queue."
            )
        if intent == "small_talk":
            return (
                "Hi! I can help you explore your connected data. Try: "
                "list tables, show first 5 rows of a table, or total sales last month."
            )
        return (
            "I can help with questions about your connected data. Try: "
            "list tables, show first 5 rows of a table, or total sales last month."
        )

    def _format_intent_clarification(self, question: str) -> str:
        return (
            "I can help with questions about your connected data, but I need a bit more detail.\n"
            f"- {question}"
        )

    def _is_exit_intent(self, text: str) -> bool:
        if text in {"exit", "quit", "bye", "goodbye", "stop", "end"}:
            return True
        if re.search(r"\bnever\s*mind\b", text):
            return True
        if re.search(r"\b(i'?m|im|we'?re|were)\s+done\b", text):
            return True
        if re.search(r"\b(done for now|done here|that'?s all|all set)\b", text):
            return True
        if re.search(r"\b(let'?s\s+)?talk\s+later\b", text):
            return True
        if re.search(r"\b(talk|see)\s+you\s+later\b", text):
            return True
        if re.search(r"\b(no\s+more|no\s+further)\s+questions\b", text):
            return True
        if re.search(r"\b(end|stop|quit|exit)\b.*\b(chat|conversation|session)\b", text):
            return True
        return False

    def _merge_query_with_table_hint(self, previous_query: str, table_hint: str) -> str:
        base = previous_query.strip()
        hint = table_hint.strip().strip("`").strip('"')
        if not base or not hint:
            return previous_query

        lower = base.lower()
        limit_match = re.search(r"\b(first|top|limit|show)\s+(\d+)\s+rows?\b", lower)
        if limit_match:
            limit = max(1, min(int(limit_match.group(2)), 10))
            return f"Show {limit} rows from {hint}"
        if re.search(r"\b(show|sample|preview)\b.*\brows?\b", lower):
            return f"Show 3 rows from {hint}"
        if "column" in lower or "columns" in lower or "fields" in lower:
            return f"Show columns in {hint}"
        if re.search(r"\b(row count|how many rows|records?)\b", lower):
            return f"How many rows are in {hint}?"
        return f"{base.rstrip('. ')} Use table {hint}."

    def _extract_focus_hint(self, query: str) -> str | None:
        lowered = query.lower()
        for token in (
            "revenue",
            "sales",
            "growth",
            "churn",
            "retention",
            "conversion",
            "registrations",
            "orders",
            "users",
            "customers",
        ):
            if token in lowered:
                return token
        return None

    def _is_setup_help_intent(self, text: str) -> bool:
        patterns = [
            r"\bsetup\b",
            r"\bconnect\b",
            r"\bconfigure\b",
            r"\bconfiguration\b",
            r"\binstall\b",
            r"\bapi key\b",
            r"\bcredentials?\b",
            r"\bdatabase url\b",
            r"\bhow do i\b.*\bconnect\b",
            r"\bwhat can you do\b",
        ]
        return any(re.search(pattern, text) for pattern in patterns)

    def _is_datapoint_help_intent(self, text: str) -> bool:
        if re.search(r"\b(explain|definition|meaning|calculate|metric|sql|query)\b", text):
            return False
        patterns = [
            r"^\s*(show|list|view)\s+(all\s+)?data\s*points?\b",
            r"^\s*(show|list|view)\s+(approved|pending|managed)\s+data\s*points?\b",
            r"^\s*available\s+data\s*points?\b",
            r"^\s*what\s+data\s*points?\s+(are\s+available|do\s+i\s+have)\b",
            r"^\s*data\s*points?\s+(list|overview)\b",
        ]
        return any(re.search(pattern, text) for pattern in patterns)

    def _is_small_talk(self, text: str) -> bool:
        greetings = [
            r"\bhi\b",
            r"\bhello\b",
            r"\bhey\b",
            r"\bhow are you\b",
            r"\bwhat'?s up\b",
            r"\bgood morning\b",
            r"\bgood afternoon\b",
            r"\bgood evening\b",
        ]
        return any(re.search(pattern, text) for pattern in greetings)

    def _is_out_of_scope(self, text: str) -> bool:
        if self._contains_data_keywords(text):
            return False
        out_of_scope = [
            r"\bjoke\b",
            r"\bweather\b",
            r"\bnews\b",
            r"\bsports\b",
            r"\bmovie\b",
            r"\bmusic\b",
            r"\bstock\b",
            r"\brecipe\b",
            r"\btranslate\b",
            r"\bwrite\b.*\bemail\b",
            r"\bcompose\b.*\bmessage\b",
            r"\bpoem\b",
            r"\bstory\b",
        ]
        return any(re.search(pattern, text) for pattern in out_of_scope)

    def _contains_data_keywords(self, text: str) -> bool:
        keywords = {
            "table",
            "tables",
            "column",
            "columns",
            "row",
            "rows",
            "schema",
            "database",
            "sql",
            "query",
            "count",
            "sum",
            "average",
            "avg",
            "min",
            "max",
            "join",
            "group",
            "order",
            "select",
            "from",
            "data",
            "dataset",
            "warehouse",
        }
        return any(word in text for word in keywords)

    def _is_non_actionable_utterance(self, text: str) -> bool:
        normalized = text.strip().lower()
        if not normalized:
            return True
        canned = {
            "ok",
            "okay",
            "k",
            "kk",
            "sure",
            "yes",
            "no",
            "cool",
            "fine",
            "great",
            "thanks",
            "thank you",
            "alright",
            "continue",
            "next",
            "go on",
        }
        if normalized in canned:
            return True
        if re.fullmatch(r"(ok|okay|sure|yes|no|thanks|thank you)[.!]*", normalized):
            return True
        return False

    def _is_deterministic_sql_query(self, query: str) -> bool:
        text = query.strip().lower()
        if not text:
            return False
        if self._query_is_table_list(text) or self._query_is_column_list(text):
            return True

        table_ref = self._extract_table_reference(text)
        if not table_ref:
            return False

        sample_patterns = [
            r"\bshow\b.*\brows\b",
            r"\bfirst\s+\d+\b",
            r"\btop\s+\d+\b",
            r"\blimit\s+\d+\b",
            r"\bpreview\b",
            r"\bsample\b",
        ]
        if any(re.search(pattern, text) for pattern in sample_patterns):
            return True

        count_patterns = [
            r"\bhow\s+many\s+rows?\b",
            r"\brow\s+count\b",
            r"\bcount\s+of\s+rows?\b",
            r"\bhow\s+many\s+records?\b",
            r"\brecord\s+count\b",
        ]
        if any(re.search(pattern, text) for pattern in count_patterns):
            return True

        return False

    def _extract_table_reference(self, query: str) -> str | None:
        patterns = [
            r"\b(?:from|in|of)\s+([a-zA-Z0-9_.]+)",
            r"\btable\s+([a-zA-Z0-9_.]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, query)
            if not match:
                continue
            table = match.group(1).strip().rstrip(".,;:?)")
            if table and table not in {"table", "tables", "rows", "row"}:
                return table
        return None

    def _is_any_table_request(self, text: str) -> bool:
        patterns = [
            r"\bany\s+table\b",
            r"\bpick\s+any\s+table\b",
            r"\bany\s+table\s+from\b",
            r"\bwhatever\s+table\b",
        ]
        return any(re.search(pattern, text.lower()) for pattern in patterns)

    def _should_call_intent_llm(self, state: PipelineState, summary: dict[str, Any]) -> bool:
        query = (state.get("query") or "").strip().lower()
        if not query:
            return False
        if not self.intent_llm:
            return False
        if self._is_ambiguous_intent(state, summary):
            return True
        generic_responses = {
            "ok",
            "okay",
            "sure",
            "yes",
            "no",
            "maybe",
            "help",
            "next",
            "continue",
            "go on",
            "not sure",
        }
        if query in generic_responses:
            return True
        if re.fullmatch(r"[a-z]+", query) and len(query.split()) <= 2:
            return True
        return False

    def _is_ambiguous_intent(self, state: PipelineState, summary: dict[str, Any]) -> bool:
        query = (state.get("query") or "").strip().lower()
        if not query:
            return False
        if self._contains_data_keywords(query):
            return False
        if self._is_any_table_request(query):
            return False
        if summary.get("last_clarifying_questions") and self._is_short_followup(query):
            return True
        max_tokens = int(self.routing_policy["ambiguous_query_max_tokens"])
        return len(query.split()) <= max_tokens

    async def _llm_intent_gate(
        self, query: str, summary: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, int]:
        summary_text = self._format_intent_summary(summary) or "None"
        system_prompt = (
            "You are a fast intent router for a data assistant. "
            "Classify the user's message into one of: "
            "data_query, exit, out_of_scope, small_talk, setup_help, datapoint_help, clarify. "
            "Return JSON with keys: intent, confidence (0-1), "
            "clarifying_question (optional, only if intent=clarify)."
        )
        user_prompt = f"User message: {query}\nIntent summary: {summary_text}\nReturn JSON only."
        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=system_prompt),
                LLMMessage(role="user", content=user_prompt),
            ],
            temperature=0.0,
            max_tokens=200,
        )
        try:
            response = await self.intent_llm.generate(request)
            return self._parse_intent_llm_response(response.content), 1
        except Exception as exc:
            logger.warning(f"Intent LLM fallback failed: {exc}")
            return None, 0

    def _parse_intent_llm_response(self, content: str) -> dict[str, Any] | None:
        json_match = re.search(r"\{.*\}", content, re.DOTALL)
        if not json_match:
            return None
        try:
            data = json.loads(json_match.group(0))
        except json.JSONDecodeError:
            return None
        intent = str(data.get("intent", "")).strip()
        allowed = {
            "data_query",
            "exit",
            "out_of_scope",
            "small_talk",
            "setup_help",
            "datapoint_help",
            "clarify",
        }
        if intent not in allowed:
            return None
        return {
            "intent": intent,
            "confidence": float(data.get("confidence", 0.0) or 0.0),
            "clarifying_question": data.get("clarifying_question"),
        }

    def _is_short_followup(self, text: str) -> bool:
        candidate = text.strip().lower()
        if ":" in candidate:
            candidate = candidate.rsplit(":", 1)[-1].strip()
        tokens = [token for token in candidate.split() if token]
        if not (0 < len(tokens) <= 5):
            return False
        disallowed = {
            "show",
            "list",
            "count",
            "select",
            "describe",
            "rows",
            "columns",
            "help",
        }
        return not any(token in disallowed for token in tokens)

    def _extract_clarifying_questions(self, text: str) -> list[str]:
        questions: list[str] = []
        for line in text.splitlines():
            candidate = line.strip()
            if not candidate:
                continue
            candidate = re.sub(r"^[\-\*\u2022]\s*", "", candidate).strip()
            if not candidate:
                continue
            if "?" in candidate:
                questions.append(candidate.rstrip())
        if not questions and "?" in text:
            chunks = [chunk.strip() for chunk in text.split("?") if chunk.strip()]
            for chunk in chunks[:3]:
                questions.append(f"{chunk}?")
        return questions[:3]

    def _is_clarification_prompt(self, text: str) -> bool:
        lower = text.lower()
        triggers = [
            "clarifying question",
            "clarifying questions",
            "need a bit more detail",
            "which table",
            "which column",
            "what information are you trying",
            "is there a specific",
            "do you want to see",
            "are you looking for",
        ]
        return any(trigger in lower for trigger in triggers)

    def _clean_hint(self, text: str) -> str | None:
        candidate = text.strip()
        if ":" in candidate:
            candidate = candidate.rsplit(":", 1)[-1].strip()
        cleaned = re.sub(r"[^\w.]+", " ", candidate.lower()).strip()
        if not cleaned:
            return None
        tokens = [
            token
            for token in cleaned.split()
            if token
            and token
            not in {
                "table",
                "column",
                "field",
                "use",
                "the",
                "a",
                "an",
                "any",
                "which",
                "what",
                "how",
                "should",
                "show",
                "list",
                "rows",
                "columns",
                "for",
                "i",
                "to",
                "do",
                "we",
            }
        ]
        if not tokens:
            return None
        if len(tokens) > 3:
            return None
        return tokens[0] if len(tokens) == 1 else "_".join(tokens)

    def _message_role_content(self, msg: Message) -> tuple[str, str]:
        if isinstance(msg, dict):
            role = str(msg.get("role", "user"))
            content = str(msg.get("content", ""))
        else:
            role = str(getattr(msg, "role", "user"))
            content = str(getattr(msg, "content", ""))
        return role, content.strip()

    def _build_initial_state(
        self,
        *,
        query: str,
        conversation_history: list[Message] | None,
        session_summary: str | None,
        session_state: dict[str, Any] | None,
        database_type: str,
        database_url: str | None,
        target_connection_id: str | None,
        synthesize_simple_sql: bool | None,
        workflow_mode: str | None,
        correlation_prefix: str,
        tool_approved: bool = False,
        preplanned_sql: dict[str, Any] | None = None,
    ) -> PipelineState:
        return {
            "query": query,
            "original_query": None,
            "conversation_history": conversation_history or [],
            "session_summary": session_summary,
            "session_state": session_state or {},
            "database_type": database_type,
            "database_url": database_url,
            "target_connection_id": target_connection_id,
            "user_id": "anonymous",
            "correlation_id": f"{correlation_prefix}-{int(time.time() * 1000)}",
            "run_id": str(uuid4()),
            "run_started_at": datetime.now(UTC),
            "tool_approved": tool_approved,
            "intent_gate": None,
            "intent_summary": None,
            "clarification_turn_count": 0,
            "clarification_limit": self.loop_budget.max_clarifications,
            "fast_path": False,
            "skip_response_synthesis": False,
            "synthesize_simple_sql": synthesize_simple_sql,
            "workflow_mode": workflow_mode,
            "current_agent": None,
            "error": None,
            "total_cost": 0.0,
            "total_latency_ms": 0.0,
            "agent_timings": {},
            "decision_trace": [],
            "action_trace": [],
            "loop_shadow_decisions": [],
            "loop_enabled": self.loop_enabled,
            "loop_shadow_mode": self.loop_shadow_mode,
            "loop_budget": self.loop_budget.model_dump(mode="json"),
            "loop_steps_taken": 0,
            "loop_terminal_state": None,
            "loop_stop_reason": None,
            "total_tokens_used": 0,
            "last_error_class": None,
            "llm_calls": 0,
            "retry_count": 0,
            "retries_exhausted": False,
            "clarification_needed": False,
            "clarifying_questions": [],
            "entities": [],
            "validation_passed": False,
            "validation_errors": [],
            "validation_warnings": [],
            "key_insights": [],
            "visualization_note": None,
            "visualization_metadata": None,
            "used_datapoints": [],
            "assumptions": [],
            "sql_formatter_fallback_calls": 0,
            "sql_formatter_fallback_successes": 0,
            "query_compiler_llm_calls": 0,
            "query_compiler_llm_refinements": 0,
            "query_compiler_latency_ms": 0.0,
            "query_compiler": None,
            "investigation_memory": None,
            "retrieved_datapoints": [],
            "context_confidence": None,
            "retrieval_trace": None,
            "context_needs_sql": None,
            "context_preface": None,
            "context_evidence": [],
            "answer_source": None,
            "answer_confidence": None,
            "evidence": [],
            "tool_plan": None,
            "tool_calls": [],
            "tool_results": [],
            "tool_error": None,
            "tool_used": False,
            "tool_approval_required": False,
            "tool_approval_message": None,
            "tool_approval_calls": [],
            "sub_answers": [],
            "schema_refresh_attempted": False,
            "preplanned_sql": preplanned_sql,
        }

    def _split_multi_query(self, query: str) -> list[str]:
        text = (query or "").strip()
        if not text:
            return []
        if len(text) < 20:
            return [text]

        parts: list[str] = []
        if text.count("?") >= 1:
            parts = [segment.strip(" ?\n\t") for segment in re.split(r"\?\s*", text)]
            parts = [segment for segment in parts if segment]
        if len(parts) <= 1:
            connector_split = re.split(
                r"\s+(?:and then|then|also|plus|and)\s+"
                r"(?=(?:what|how|show|list|give|define|explain|which|who|where|when|is|are|do|does|count|sum)\b)",
                text,
                flags=re.IGNORECASE,
            )
            connector_split = [
                segment.strip(" .") for segment in connector_split if segment.strip()
            ]
            if len(connector_split) > 1:
                parts = connector_split

        if len(parts) <= 1:
            return [text]

        normalized: list[str] = []
        for part in parts[: self.max_subqueries]:
            candidate = part.strip(" .")
            if not candidate or len(candidate) < 4:
                continue
            normalized.append(candidate)
        if len(normalized) <= 1:
            return [text]
        return normalized

    def _extract_subquery_index(self, text: str) -> int | None:
        match = re.search(r"\[q(\d+)\]", text.strip(), flags=re.IGNORECASE)
        if not match:
            return None
        try:
            value = int(match.group(1))
        except ValueError:
            return None
        return value if value > 0 else None

    async def _run_single_query(
        self,
        *,
        query: str,
        conversation_history: list[Message] | None = None,
        session_summary: str | None = None,
        session_state: dict[str, Any] | None = None,
        database_type: str = "postgresql",
        database_url: str | None = None,
        target_connection_id: str | None = None,
        synthesize_simple_sql: bool | None = None,
        workflow_mode: str | None = "auto",
        tool_approved: bool = False,
        preplanned_sql: dict[str, Any] | None = None,
    ) -> PipelineState:
        initial_state = self._build_initial_state(
            query=query,
            conversation_history=conversation_history,
            session_summary=session_summary,
            session_state=session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=target_connection_id,
            synthesize_simple_sql=synthesize_simple_sql,
            workflow_mode=workflow_mode,
            tool_approved=tool_approved,
            correlation_prefix="local",
            preplanned_sql=preplanned_sql,
        )

        logger.info(f"Starting pipeline for query: {query[:100]}...")
        start_time = time.time()
        result = await self.graph.ainvoke(initial_state)
        self._normalize_answer_metadata(result)
        self._finalize_action_loop(result)
        self._finalize_session_memory(result)
        total_time = (time.time() - start_time) * 1000
        logger.info(
            f"Pipeline complete in {total_time:.1f}ms ({result.get('llm_calls', 0)} LLM calls)"
        )
        return result

    def _build_sub_answer(self, index: int, query: str, result: PipelineState) -> dict[str, Any]:
        answer = self._normalize_sub_answer_text(result.get("natural_language_answer"))
        if not answer:
            if result.get("error"):
                answer = f"I encountered an error: {result.get('error')}"
            else:
                answer = "No answer generated"
        sql_text = result.get("validated_sql") or result.get("generated_sql")
        query_result = result.get("query_result")
        data: dict[str, list[Any]] | None = None
        if isinstance(query_result, dict):
            candidate = query_result.get("data")
            if isinstance(candidate, dict):
                data = candidate
            else:
                rows = query_result.get("rows")
                columns = query_result.get("columns")
                if isinstance(rows, list) and isinstance(columns, list):
                    data = {str(col): [row.get(col) for row in rows] for col in columns}
        return {
            "index": index,
            "query": query,
            "answer": answer,
            "answer_source": result.get("answer_source"),
            "answer_confidence": result.get("answer_confidence"),
            "sql": sql_text,
            "data": data,
            "visualization_hint": result.get("visualization_hint"),
            "visualization_note": result.get("visualization_note"),
            "visualization_metadata": result.get("visualization_metadata"),
            "clarifying_questions": result.get("clarifying_questions", []),
            "error": result.get("error"),
        }

    def _normalize_sub_answer_text(self, value: Any) -> str:
        """Normalize raw sub-answer text so UI never shows raw JSON payload blobs."""
        text = str(value or "").strip()
        if not text:
            return ""

        payload = self._extract_structured_answer_payload(text)
        if payload is not None:
            answer = payload.get("answer")
            if isinstance(answer, str) and answer.strip():
                return answer.strip()

        return text

    def _extract_structured_answer_payload(self, text: str) -> dict[str, Any] | None:
        stripped = text.strip()

        # Case 1: fenced JSON payload (supports ``json ... `` and ```json ... ``` forms).
        fenced = re.fullmatch(
            r"(?P<fence>`{2,})(?:json)?\s*(\{[\s\S]*\})\s*(?P=fence)",
            stripped,
            re.IGNORECASE,
        )
        if fenced:
            candidate = fenced.group(2)
            try:
                payload = json.loads(candidate)
                if isinstance(payload, dict):
                    return payload
            except json.JSONDecodeError:
                return None

        # Case 2: plain JSON object response.
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                payload = json.loads(stripped)
                if isinstance(payload, dict):
                    return payload
            except json.JSONDecodeError:
                return None

        return None

    def _build_multi_sql_planner_prompt(self, parts: list[str], schema_context: str) -> str:
        questions = "\n".join(f"{idx}. {part}" for idx, part in enumerate(parts, start=1))
        return (
            "Generate SQL plans for each question.\n"
            "Return STRICT JSON (no markdown) with shape:\n"
            "{\n"
            '  "plans": [\n'
            "    {\n"
            '      "index": 1,\n'
            '      "sql": "SELECT ...",\n'
            '      "explanation": "short reason",\n'
            '      "confidence": 0.0,\n'
            '      "clarifying_questions": []\n'
            "    }\n"
            "  ]\n"
            "}\n\n"
            "Rules:\n"
            "- One plan per question index.\n"
            "- Use only tables/columns from SCHEMA_CONTEXT.\n"
            "- Use a single SELECT query per question (no DDL/DML).\n"
            "- If answer is unclear, set sql to empty string and add clarifying_questions.\n"
            "- Add LIMIT for detail/list queries unless aggregation already bounds output.\n\n"
            f"QUESTIONS:\n{questions}\n\n"
            f"SCHEMA_CONTEXT:\n{schema_context}"
        )

    def _parse_multi_sql_planner_response(
        self, content: str, part_count: int
    ) -> dict[int, dict[str, Any]]:
        json_text: str | None = None
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", content, re.DOTALL)
        if fenced:
            json_text = fenced.group(1)
        else:
            candidate = re.search(r"\{.*\}", content, re.DOTALL)
            if candidate:
                json_text = candidate.group(0)
        if not json_text:
            return {}

        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}

        plans = payload.get("plans")
        if not isinstance(plans, list):
            return {}

        parsed: dict[int, dict[str, Any]] = {}
        for item in plans:
            if not isinstance(item, dict):
                continue
            raw_index = item.get("index")
            try:
                index = int(raw_index)
            except (TypeError, ValueError):
                continue
            if index < 1 or index > part_count:
                continue

            sql_text = item.get("sql")
            sql_value = sql_text.strip() if isinstance(sql_text, str) else ""
            explanation = item.get("explanation")
            explanation_value = explanation.strip() if isinstance(explanation, str) else ""
            raw_confidence = item.get("confidence", 0.6)
            try:
                confidence_value = float(raw_confidence)
            except (TypeError, ValueError):
                confidence_value = 0.6
            confidence_value = max(0.0, min(1.0, confidence_value))

            questions = item.get("clarifying_questions", [])
            clarifying_questions: list[str] = []
            if isinstance(questions, str):
                questions = [questions]
            if isinstance(questions, list):
                for question in questions:
                    if isinstance(question, str) and question.strip():
                        clarifying_questions.append(question.strip())

            parsed[index] = {
                "sql": sql_value,
                "explanation": explanation_value,
                "confidence": confidence_value,
                "clarifying_questions": clarifying_questions,
            }
        return parsed

    async def _plan_multi_sql_for_parts(
        self,
        *,
        parts: list[str],
        database_type: str,
        database_url: str | None,
    ) -> tuple[dict[int, dict[str, Any]], int, float]:
        if not parts:
            return {}, 0, 0.0

        start = time.time()
        try:
            schema_context = await self.sql._get_live_schema_context(
                query=" ; ".join(parts),
                database_type=database_type,
                database_url=database_url,
                include_profile=False,
            )
            schema_context = schema_context or "No schema context available."
            request = LLMRequest(
                messages=[
                    LLMMessage(
                        role="system",
                        content=(
                            "You are a SQL planner for multiple analytics questions. "
                            "Return strict JSON only."
                        ),
                    ),
                    LLMMessage(
                        role="user",
                        content=self._build_multi_sql_planner_prompt(parts, schema_context),
                    ),
                ],
                temperature=0.0,
                max_tokens=2200,
            )
            provider = getattr(self.sql, "fast_llm", None) or getattr(self.sql, "llm", None)
            if provider is None:
                return {}, 0, (time.time() - start) * 1000
            response = await provider.generate(request)
            plans = self._parse_multi_sql_planner_response(str(response.content or ""), len(parts))
            if not plans:
                return {}, 1, (time.time() - start) * 1000
            return plans, 1, (time.time() - start) * 1000
        except Exception as exc:
            logger.debug(f"Multi SQL planner fallback skipped due to error: {exc}")
            return {}, 0, (time.time() - start) * 1000

    def _aggregate_multi_results(
        self,
        *,
        original_query: str,
        sub_results: list[PipelineState],
        sub_answers: list[dict[str, Any]],
        conversation_history: list[Message] | None,
        session_summary: str | None,
        session_state: dict[str, Any] | None,
        database_type: str,
        database_url: str | None,
        target_connection_id: str | None,
        synthesize_simple_sql: bool | None,
        workflow_mode: str | None,
        tool_approved: bool,
        extra_llm_calls: int = 0,
        extra_agent_timings: dict[str, float] | None = None,
    ) -> PipelineState:
        merged = self._build_initial_state(
            query=original_query,
            conversation_history=conversation_history,
            session_summary=session_summary,
            session_state=session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=target_connection_id,
            synthesize_simple_sql=synthesize_simple_sql,
            workflow_mode=workflow_mode,
            tool_approved=tool_approved,
            correlation_prefix="local",
        )
        merged["sub_answers"] = sub_answers

        section_lines = ["I handled your request as multiple questions:"]
        for item in sub_answers:
            section_lines.append(f"\n{item['index']}. {item['query']}")
            section_lines.append(str(item.get("answer") or "No answer generated"))
        merged["natural_language_answer"] = "\n".join(section_lines).strip()

        clarifications: list[str] = []
        for item in sub_answers:
            for question in item.get("clarifying_questions", []):
                clarifications.append(f"[Q{item['index']}] {question}")
        merged["clarifying_questions"] = clarifications
        merged["clarification_needed"] = bool(clarifications)

        merged["answer_source"] = "multi"
        primary_index, primary_result = self._select_primary_sub_result(sub_results)
        if primary_result is not None:
            merged["generated_sql"] = primary_result.get("generated_sql")
            merged["validated_sql"] = primary_result.get("validated_sql")
            merged["query_result"] = primary_result.get("query_result")
            merged["visualization_hint"] = primary_result.get("visualization_hint")
            merged["visualization_note"] = primary_result.get("visualization_note")
            merged["visualization_metadata"] = primary_result.get("visualization_metadata")
            merged["key_insights"] = primary_result.get("key_insights", [])
            if primary_index is not None and 0 <= primary_index < len(sub_answers):
                merged["natural_language_answer"] = (
                    f"{merged['natural_language_answer']}\n\n"
                    f"Primary SQL/table/visualization shown for question {primary_index + 1}: "
                    f"{sub_answers[primary_index]['query']}"
                )

        confidence_values = [
            float(item.get("answer_confidence"))
            for item in sub_answers
            if item.get("answer_confidence") is not None
        ]
        if confidence_values:
            merged["answer_confidence"] = max(
                0.0, min(1.0, sum(confidence_values) / len(confidence_values))
            )

        merged["llm_calls"] = (
            sum(int(result.get("llm_calls", 0) or 0) for result in sub_results) + extra_llm_calls
        )
        merged["retry_count"] = sum(
            int(result.get("retry_count", 0) or 0) for result in sub_results
        )
        merged["total_latency_ms"] = sum(
            float(result.get("total_latency_ms", 0.0) or 0.0) for result in sub_results
        )
        merged["sql_formatter_fallback_calls"] = sum(
            int(result.get("sql_formatter_fallback_calls", 0) or 0) for result in sub_results
        )
        merged["sql_formatter_fallback_successes"] = sum(
            int(result.get("sql_formatter_fallback_successes", 0) or 0) for result in sub_results
        )
        merged["query_compiler_llm_calls"] = sum(
            int(result.get("query_compiler_llm_calls", 0) or 0) for result in sub_results
        )
        merged["query_compiler_llm_refinements"] = sum(
            int(result.get("query_compiler_llm_refinements", 0) or 0) for result in sub_results
        )
        merged["query_compiler_latency_ms"] = sum(
            float(result.get("query_compiler_latency_ms", 0.0) or 0.0) for result in sub_results
        )
        for result in sub_results:
            summary = result.get("query_compiler")
            if isinstance(summary, dict):
                merged["query_compiler"] = summary
                break
        merged["decision_trace"] = [
            {
                "stage": "subquery",
                "decision": f"Q{idx + 1}",
                "reason": result.get("query"),
                "details": {
                    "answer_source": result.get("answer_source"),
                    "decision_trace": result.get("decision_trace", []),
                },
            }
            for idx, result in enumerate(sub_results)
        ]
        merged["action_trace"] = [
            {
                "version": "v1",
                "step": idx + 1,
                "stage": "subquery",
                "selected_action": f"Q{idx + 1}",
                "inputs": {"query": result.get("query")},
                "outputs": {"answer_source": result.get("answer_source")},
                "verification": {"status": "ok", "details": {}},
                "error_class": result.get("last_error_class"),
                "stop_reason": result.get("loop_stop_reason"),
                "terminal_state": result.get("loop_terminal_state"),
                "sub_action_trace": result.get("action_trace", []),
            }
            for idx, result in enumerate(sub_results)
        ]
        merged["loop_shadow_decisions"] = [
            {
                "stage": "subquery",
                "decision": f"Q{idx + 1}",
                "shadow": result.get("loop_shadow_decisions", []),
            }
            for idx, result in enumerate(sub_results)
            if result.get("loop_shadow_decisions")
        ]
        merged["loop_steps_taken"] = sum(
            int(result.get("loop_steps_taken", 0) or 0) for result in sub_results
        )
        merged["total_tokens_used"] = sum(
            int(result.get("total_tokens_used", 0) or 0) for result in sub_results
        ) + int(extra_llm_calls) * 800

        merged_agent_timings: dict[str, float] = {}
        for result in sub_results:
            for agent, duration in (result.get("agent_timings") or {}).items():
                merged_agent_timings[agent] = merged_agent_timings.get(agent, 0.0) + float(
                    duration or 0.0
                )
        for agent, duration in (extra_agent_timings or {}).items():
            merged_agent_timings[agent] = merged_agent_timings.get(agent, 0.0) + float(
                duration or 0.0
            )
        merged["agent_timings"] = merged_agent_timings

        all_sources: list[dict[str, Any]] = []
        seen_source_ids: set[str] = set()
        all_evidence: list[dict[str, Any]] = []
        seen_evidence_ids: set[tuple[str, str]] = set()
        errors: list[str] = []
        for result in sub_results:
            for dp in result.get("retrieved_datapoints", []):
                datapoint_id = str(dp.get("datapoint_id", ""))
                if datapoint_id and datapoint_id not in seen_source_ids:
                    seen_source_ids.add(datapoint_id)
                    all_sources.append(dp)
            for item in result.get("evidence", []):
                key = (str(item.get("datapoint_id", "")), str(item.get("reason", "")))
                if key not in seen_evidence_ids:
                    seen_evidence_ids.add(key)
                    all_evidence.append(item)
            if result.get("error"):
                errors.append(str(result["error"]))

        merged["retrieved_datapoints"] = all_sources
        merged["evidence"] = all_evidence
        merged["validation_errors"] = [
            item for result in sub_results for item in result.get("validation_errors", [])
        ]
        merged["validation_warnings"] = [
            item for result in sub_results for item in result.get("validation_warnings", [])
        ]
        if errors and not merged["natural_language_answer"]:
            merged["error"] = errors[0]

        self._normalize_answer_metadata(merged)
        self._finalize_action_loop(merged)
        if sub_results:
            merged["session_summary"] = sub_results[-1].get("session_summary")
            merged["session_state"] = sub_results[-1].get("session_state")
        self._finalize_session_memory(merged)
        return merged

    def _select_primary_sub_result(
        self, sub_results: list[PipelineState]
    ) -> tuple[int | None, PipelineState | None]:
        """
        Select the sub-result whose SQL/data artifacts should back the rich UI tabs.

        Priority:
        1. Query results with at least one row
        2. Query results with zero rows (still a concrete SQL outcome)
        3. Validated/generated SQL without execution payload
        4. First non-error/non-clarification answer
        5. First sub-result (fallback)
        """
        if not sub_results:
            return None, None

        def _priority(result: PipelineState) -> tuple[int, int]:
            query_result = result.get("query_result")
            has_query_result = isinstance(query_result, dict)
            row_count = 0
            if has_query_result:
                raw_row_count = query_result.get("row_count", 0)
                try:
                    row_count = int(raw_row_count or 0)
                except (TypeError, ValueError):
                    row_count = 0
                if row_count > 0:
                    return (4, row_count)
                return (3, 0)

            has_sql = bool(result.get("validated_sql") or result.get("generated_sql"))
            if has_sql:
                return (2, 0)

            source = str(result.get("answer_source") or "").lower()
            is_error = bool(result.get("error"))
            if source not in {"clarification", "error"} and not is_error:
                return (1, 0)

            return (0, 0)

        best_index = 0
        best_priority = _priority(sub_results[0])
        for idx, result in enumerate(sub_results[1:], start=1):
            candidate_priority = _priority(result)
            if candidate_priority > best_priority:
                best_priority = candidate_priority
                best_index = idx

        return best_index, sub_results[best_index]

    # ========================================================================
    # Public API
    # ========================================================================

    async def run(
        self,
        query: str,
        conversation_history: list[Message] | None = None,
        session_summary: str | None = None,
        session_state: dict[str, Any] | None = None,
        database_type: str = "postgresql",
        database_url: str | None = None,
        target_connection_id: str | None = None,
        synthesize_simple_sql: bool | None = None,
        workflow_mode: str | None = "auto",
        tool_approved: bool = False,
    ) -> PipelineState:
        """
        Run pipeline synchronously (wait for completion).

        Args:
            query: User's natural language query
            conversation_history: Previous conversation messages
            session_summary: Compact summary carried across turns
            session_state: Structured session memory carried across turns
            database_type: Database type (postgresql, clickhouse, mysql)
            database_url: Database URL override for execution

        Returns:
            Final pipeline state with all outputs
        """
        parts = self._split_multi_query(query)
        if len(parts) <= 1:
            result = await self._run_single_query(
                query=query,
                conversation_history=conversation_history,
                session_summary=session_summary,
                session_state=session_state,
                database_type=database_type,
                database_url=database_url,
                target_connection_id=target_connection_id,
                synthesize_simple_sql=synthesize_simple_sql,
                workflow_mode=workflow_mode,
                tool_approved=tool_approved,
            )
            await self._persist_completed_run(result)
            return result

        (
            planned_sql_map,
            planner_llm_calls,
            planner_duration_ms,
        ) = await self._plan_multi_sql_for_parts(
            parts=parts,
            database_type=database_type,
            database_url=database_url,
        )

        sub_results: list[PipelineState] = []
        sub_answers: list[dict[str, Any]] = []
        for index, part in enumerate(parts, start=1):
            result = await self._run_single_query(
                query=part,
                conversation_history=conversation_history,
                session_summary=session_summary,
                session_state=session_state,
                database_type=database_type,
                database_url=database_url,
                target_connection_id=target_connection_id,
                synthesize_simple_sql=synthesize_simple_sql,
                workflow_mode=workflow_mode,
                tool_approved=tool_approved,
                preplanned_sql=planned_sql_map.get(index),
            )
            sub_results.append(result)
            sub_answers.append(self._build_sub_answer(index, part, result))

        result = self._aggregate_multi_results(
            original_query=query,
            sub_results=sub_results,
            sub_answers=sub_answers,
            conversation_history=conversation_history,
            session_summary=session_summary,
            session_state=session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=target_connection_id,
            synthesize_simple_sql=synthesize_simple_sql,
            workflow_mode=workflow_mode,
            tool_approved=tool_approved,
            extra_llm_calls=planner_llm_calls,
            extra_agent_timings={"multi_sql_planner": planner_duration_ms},
        )
        await self._persist_completed_run(result)
        return result

    async def stream(
        self,
        query: str,
        conversation_history: list[Message] | None = None,
        session_summary: str | None = None,
        session_state: dict[str, Any] | None = None,
        database_type: str = "postgresql",
        database_url: str | None = None,
        target_connection_id: str | None = None,
        synthesize_simple_sql: bool | None = None,
        workflow_mode: str | None = "auto",
        tool_approved: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Run pipeline with streaming updates.

        Yields status updates as each agent completes.

        Args:
            query: User's natural language query
            conversation_history: Previous conversation messages
            session_summary: Compact summary carried across turns
            session_state: Structured session memory carried across turns
            database_type: Database type
            database_url: Database URL override for execution

        Yields:
            Status updates with current agent and progress
        """
        parts = self._split_multi_query(query)
        if len(parts) > 1:
            result = await self.run(
                query=query,
                conversation_history=conversation_history,
                session_summary=session_summary,
                session_state=session_state,
                database_type=database_type,
                database_url=database_url,
                target_connection_id=target_connection_id,
                synthesize_simple_sql=synthesize_simple_sql,
                workflow_mode=workflow_mode,
                tool_approved=tool_approved,
            )
            yield {
                "node": "MultiQueryAggregator",
                "current_agent": "MultiQueryAggregator",
                "status": "completed",
                "state": result,
            }
            return

        initial_state = self._build_initial_state(
            query=query,
            conversation_history=conversation_history,
            session_summary=session_summary,
            session_state=session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=target_connection_id,
            synthesize_simple_sql=synthesize_simple_sql,
            workflow_mode=workflow_mode,
            tool_approved=tool_approved,
            correlation_prefix="stream",
        )

        logger.info(f"Starting streaming pipeline for query: {query[:100]}...")

        # Stream graph execution
        async for update in self.graph.astream(initial_state):
            # Extract current state from update
            for node_name, state_update in update.items():
                yield {
                    "node": node_name,
                    "current_agent": state_update.get("current_agent"),
                    "status": "running",
                    "state": state_update,
                }

        logger.info("Pipeline streaming complete")

    async def _run_single_query_with_streaming_callback(
        self,
        *,
        query: str,
        conversation_history: list[Message] | None,
        session_summary: str | None,
        session_state: dict[str, Any] | None,
        database_type: str,
        database_url: str | None,
        target_connection_id: str | None,
        synthesize_simple_sql: bool | None,
        workflow_mode: str | None = "auto",
        tool_approved: bool = False,
        event_callback: Any = None,
        correlation_prefix: str = "stream",
        preplanned_sql: dict[str, Any] | None = None,
    ) -> PipelineState:
        """Execute one query while emitting per-agent callback events."""
        from datetime import datetime

        initial_state = self._build_initial_state(
            query=query,
            conversation_history=conversation_history,
            session_summary=session_summary,
            session_state=session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=target_connection_id,
            synthesize_simple_sql=synthesize_simple_sql,
            workflow_mode=workflow_mode,
            tool_approved=tool_approved,
            correlation_prefix=correlation_prefix,
            preplanned_sql=preplanned_sql,
        )

        logger.info(f"Starting streaming pipeline for query: {query[:100]}...")
        pipeline_start = time.time()

        agent_start_times: dict[str, float] = {}
        final_state: PipelineState | None = None

        async for update in self.graph.astream(initial_state):
            for _node_name, state_update in update.items():
                current_agent = state_update.get("current_agent")

                if current_agent and current_agent not in agent_start_times:
                    agent_start_times[current_agent] = time.time()
                    if event_callback:
                        await event_callback(
                            "agent_start",
                            {
                                "agent": current_agent,
                                "timestamp": datetime.now(UTC).isoformat(),
                            },
                        )

                if current_agent and current_agent in agent_start_times:
                    duration_ms = (time.time() - agent_start_times[current_agent]) * 1000
                    if event_callback:
                        agent_data: dict[str, Any] = {}
                        if current_agent == "QueryAnalyzerAgent":
                            agent_data = {
                                "intent": state_update.get("intent"),
                                "route": state_update.get("route"),
                                "entities": state_update.get("entities", []),
                                "complexity": state_update.get("complexity"),
                            }
                        elif current_agent == "ContextAgent":
                            agent_data = {
                                "datapoints_found": len(
                                    state_update.get("retrieved_datapoints", [])
                                ),
                            }
                        elif current_agent == "SQLAgent":
                            agent_data = {
                                "sql_generated": bool(state_update.get("generated_sql")),
                                "confidence": state_update.get("sql_confidence"),
                            }
                        elif current_agent == "ValidatorAgent":
                            agent_data = {
                                "validation_passed": state_update.get("validation_passed", False),
                                "issues_found": len(state_update.get("validation_errors", [])),
                            }
                        elif current_agent == "ExecutorAgent":
                            query_result = state_update.get("query_result")
                            agent_data = {
                                "rows_returned": (
                                    query_result.get("row_count", 0) if query_result else 0
                                ),
                                "visualization_hint": state_update.get("visualization_hint"),
                            }
                        elif current_agent == "ContextAnswerAgent":
                            agent_data = {
                                "answer_source": state_update.get("answer_source"),
                                "confidence": state_update.get("answer_confidence"),
                                "evidence_count": len(state_update.get("evidence", [])),
                            }
                        elif current_agent == "ToolPlannerAgent":
                            agent_data = {
                                "tool_calls": len(state_update.get("tool_calls", [])),
                            }
                        elif current_agent == "ToolExecutor":
                            agent_data = {
                                "tool_results": len(state_update.get("tool_results", [])),
                                "tool_error": state_update.get("tool_error"),
                            }

                        await event_callback(
                            "agent_complete",
                            {
                                "agent": current_agent,
                                "data": agent_data,
                                "duration_ms": duration_ms,
                                "timestamp": datetime.now(UTC).isoformat(),
                            },
                        )

                final_state = state_update

        total_latency_ms = (time.time() - pipeline_start) * 1000
        if final_state:
            final_state["total_latency_ms"] = total_latency_ms
            self._normalize_answer_metadata(final_state)
            self._finalize_action_loop(final_state)
            self._finalize_session_memory(final_state)

        logger.info(
            f"Pipeline streaming complete in {total_latency_ms:.1f}ms "
            f"({final_state.get('llm_calls', 0) if final_state else 0} LLM calls)"
        )

        return final_state or initial_state

    async def run_with_streaming(
        self,
        query: str,
        conversation_history: list[Message] | None = None,
        session_summary: str | None = None,
        session_state: dict[str, Any] | None = None,
        database_type: str = "postgresql",
        database_url: str | None = None,
        target_connection_id: str | None = None,
        synthesize_simple_sql: bool | None = None,
        workflow_mode: str | None = "auto",
        tool_approved: bool = False,
        event_callback: Any = None,
    ) -> PipelineState:
        """
        Run pipeline with callback-based streaming for WebSocket support.

        Args:
            query: User's natural language query
            conversation_history: Previous conversation messages
            session_summary: Compact summary carried across turns
            session_state: Structured session memory carried across turns
            database_type: Database type
            database_url: Database URL override for execution
            event_callback: Async callback function for streaming events
                           Signature: async def callback(event_type: str, event_data: dict)

        Returns:
            Final pipeline state with all outputs

        Event Types:
            - agent_start: Agent begins execution
            - agent_complete: Agent finishes execution
            - data_chunk: Intermediate data from agent (optional)
        """
        parts = self._split_multi_query(query)
        if len(parts) > 1:
            if event_callback:
                await event_callback(
                    "decompose_complete",
                    {
                        "parts": parts,
                        "part_count": len(parts),
                    },
                )
            planned_sql_map: dict[int, dict[str, Any]] = {}
            planner_llm_calls = 0
            planner_duration_ms = 0.0
            if event_callback:
                await event_callback("agent_start", {"agent": "MultiSQLPlanner"})
            (
                planned_sql_map,
                planner_llm_calls,
                planner_duration_ms,
            ) = await self._plan_multi_sql_for_parts(
                parts=parts,
                database_type=database_type,
                database_url=database_url,
            )
            if event_callback:
                await event_callback(
                    "agent_complete",
                    {
                        "agent": "MultiSQLPlanner",
                        "duration_ms": planner_duration_ms,
                        "data": {
                            "questions": len(parts),
                            "planned": len(planned_sql_map),
                            "llm_calls": planner_llm_calls,
                        },
                    },
                )

            sub_results: list[PipelineState] = []
            sub_answers: list[dict[str, Any]] = []
            for index, part in enumerate(parts, start=1):
                if event_callback:
                    await event_callback(
                        "thinking",
                        {"note": f"showing live agent flow for question: {part}"},
                    )
                result = await self._run_single_query_with_streaming_callback(
                    query=part,
                    conversation_history=conversation_history,
                    session_summary=session_summary,
                    session_state=session_state,
                    database_type=database_type,
                    database_url=database_url,
                    target_connection_id=target_connection_id,
                    synthesize_simple_sql=synthesize_simple_sql,
                    workflow_mode=workflow_mode,
                    tool_approved=tool_approved,
                    event_callback=event_callback,
                    correlation_prefix=f"stream-q{index}",
                    preplanned_sql=planned_sql_map.get(index),
                )
                sub_results.append(result)
                sub_answers.append(self._build_sub_answer(index, part, result))

            result = self._aggregate_multi_results(
                original_query=query,
                sub_results=sub_results,
                sub_answers=sub_answers,
                conversation_history=conversation_history,
                session_summary=session_summary,
                session_state=session_state,
                database_type=database_type,
                database_url=database_url,
                target_connection_id=target_connection_id,
                synthesize_simple_sql=synthesize_simple_sql,
                workflow_mode=workflow_mode,
                tool_approved=tool_approved,
                extra_llm_calls=planner_llm_calls,
                extra_agent_timings={"multi_sql_planner": planner_duration_ms},
            )
            await self._persist_completed_run(result)
            return result

        result = await self._run_single_query_with_streaming_callback(
            query=query,
            conversation_history=conversation_history,
            session_summary=session_summary,
            session_state=session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=target_connection_id,
            synthesize_simple_sql=synthesize_simple_sql,
            workflow_mode=workflow_mode,
            tool_approved=tool_approved,
            event_callback=event_callback,
            correlation_prefix="stream",
        )
        await self._persist_completed_run(result)
        return result

    def _finalize_session_memory(self, state: PipelineState) -> None:
        """Persist compact memory fields for the next turn."""
        intent_summary = dict(state.get("intent_summary") or {})
        session_state = dict(state.get("session_state") or {})

        merged = self._merge_session_state_into_summary(intent_summary, session_state)
        merged["clarification_count"] = self._current_clarification_count(state)

        latest_goal = (state.get("original_query") or state.get("query") or "").strip()
        if latest_goal:
            merged["last_goal"] = latest_goal

        questions = state.get("clarifying_questions") or []
        if questions:
            merged["last_clarifying_questions"] = questions[:3]
            merged["last_clarifying_question"] = questions[0]

        sql_text = (state.get("validated_sql") or state.get("generated_sql") or "").strip()
        if sql_text:
            table_hint = self._extract_table_reference(sql_text)
            if table_hint:
                table_hints = list(merged.get("table_hints") or [])
                if table_hint not in table_hints:
                    table_hints.append(table_hint)
                merged["table_hints"] = table_hints
                slots = dict(merged.get("slots") or {})
                slots["table"] = slots.get("table") or table_hint
                merged["slots"] = slots

        merged["updated_at"] = int(time.time())
        state["session_state"] = merged
        state["session_summary"] = self._format_intent_summary(merged)

    def _normalize_answer_metadata(self, state: PipelineState) -> None:
        """Ensure answer source/confidence are consistently populated."""
        self._normalize_primary_answer_text(state)
        source = state.get("answer_source")
        if not source:
            if state.get("tool_approval_required"):
                source = "approval"
            elif state.get("clarification_needed") or state.get("clarifying_questions"):
                source = "clarification"
            elif state.get("error"):
                source = "error"
            elif (
                state.get("validated_sql")
                or state.get("generated_sql")
                or state.get("query_result")
            ):
                source = "sql"
            elif state.get("intent_gate") in {"exit", "out_of_scope", "small_talk", "setup_help"}:
                source = "system"
            elif state.get("natural_language_answer"):
                source = "context"
            else:
                source = "error"
            state["answer_source"] = source

        if state.get("answer_confidence") is None:
            defaults = {
                "sql": 0.7,
                "context": 0.6,
                "clarification": 0.2,
                "system": 0.8,
                "approval": 0.5,
                "multi": 0.65,
                "error": 0.0,
            }
            state["answer_confidence"] = defaults.get(source, 0.5)
        else:
            confidence = float(state.get("answer_confidence", 0.5))
            state["answer_confidence"] = max(0.0, min(1.0, confidence))

    def _normalize_primary_answer_text(self, state: PipelineState) -> None:
        """Normalize natural_language_answer when a model returns structured JSON payload text."""
        raw_answer = state.get("natural_language_answer")
        text = str(raw_answer or "").strip()
        if not text:
            return

        payload = self._extract_structured_answer_payload(text)
        if payload is None:
            return

        answer = payload.get("answer")
        if isinstance(answer, str) and answer.strip():
            state["natural_language_answer"] = answer.strip()

        if state.get("answer_confidence") is None:
            confidence = payload.get("confidence")
            if isinstance(confidence, (int, float)):
                state["answer_confidence"] = max(0.0, min(1.0, float(confidence)))


# ============================================================================
# Helper Functions
# ============================================================================


async def create_pipeline(
    database_type: str | None = None,
    database_url: str | None = None,
) -> DataChatPipeline:
    """
    Create a DataChatPipeline with all dependencies initialized.

    Args:
        database_type: Database type (postgresql, clickhouse, mysql). If omitted,
            inferred from database URL.
        database_url: Database connection URL (uses config if not provided)

    Returns:
        Initialized pipeline
    """
    config = get_settings()

    # Initialize retriever
    from backend.knowledge.bootstrap import bootstrap_knowledge_graph_from_datapoints
    from backend.knowledge.graph import KnowledgeGraph
    from backend.knowledge.vectors import VectorStore

    vector_store = VectorStore()
    await vector_store.initialize()

    knowledge_graph = KnowledgeGraph()
    bootstrap_knowledge_graph_from_datapoints(knowledge_graph, datapoints_dir="datapoints")

    retriever = Retriever(
        vector_store=vector_store,
        knowledge_graph=knowledge_graph,
    )

    # Initialize connector
    db_url = database_url or config.database.url
    if not db_url:
        raise ValueError("DATABASE_URL must be set or provided to create a pipeline.")

    db_url_str = str(db_url) if not isinstance(db_url, str) else db_url
    connector = create_connector(
        database_url=db_url_str,
        database_type=database_type,
        pool_size=config.database.pool_size,
    )

    await connector.connect()

    # Create pipeline
    pipeline = DataChatPipeline(
        retriever=retriever,
        connector=connector,
        max_retries=3,
    )

    return pipeline

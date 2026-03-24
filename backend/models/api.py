"""
API Request/Response Models

Pydantic models for FastAPI endpoints.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from backend.models.agent import EvidenceItem, SQLValidationError, ValidationWarning


class Message(BaseModel):
    """Chat message in conversation history."""

    role: str = Field(..., description="Message role: 'user' or 'assistant'")
    content: str = Field(..., description="Message content")


class ChatRequest(BaseModel):
    """Request model for chat endpoint."""

    message: str = Field(..., min_length=1, description="User's natural language query")
    execution_mode: Literal["natural_language", "direct_sql"] = Field(
        default="natural_language",
        description="Execution mode for the request.",
    )
    sql: str | None = Field(
        default=None,
        description="Raw SQL query to execute directly when execution_mode is direct_sql.",
    )
    conversation_id: str | None = Field(None, description="Optional conversation ID for context")
    target_database: str | None = Field(
        None, description="Optional database connection ID to target"
    )
    conversation_history: list[Message] = Field(
        default_factory=list,
        description="Previous messages in the conversation",
    )
    session_summary: str | None = Field(
        default=None,
        description="Optional compact memory summary from prior turns.",
    )
    session_state: dict[str, Any] | None = Field(
        default=None,
        description="Optional structured memory state from prior turns.",
    )
    synthesize_simple_sql: bool | None = Field(
        default=None,
        description=(
            "Override for response synthesis on simple SQL answers (None = use server default)."
        ),
    )
    workflow_mode: Literal["auto", "finance_variance_v1"] | None = Field(
        default="auto",
        description=(
            "Optional workflow packaging mode. "
            "'auto' infers by query/source signals; "
            "'finance_variance_v1' forces finance brief packaging when possible."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "What's the total revenue?",
                "execution_mode": "natural_language",
                "sql": None,
                "conversation_id": "conv_123",
                "target_database": "3a1f2d3e-4b5c-6d7e-8f90-1234567890ab",
                "conversation_history": [],
                "session_summary": "Intent summary: last_goal=How many products do we have?",
                "session_state": {"last_goal": "How many products do we have?"},
                "synthesize_simple_sql": None,
                "workflow_mode": "auto",
            }
        }
    }


class DataSource(BaseModel):
    """Information about a data source used to answer the query."""

    datapoint_id: str = Field(..., description="DataPoint ID")
    type: str = Field(..., description="DataPoint type (Schema, Business, Process)")
    name: str = Field(..., description="Human-readable name")
    relevance_score: float = Field(..., ge=0.0, le=1.0, description="Relevance score (0-1)")


class ChatMetrics(BaseModel):
    """Performance metrics for the chat request."""

    total_latency_ms: float = Field(..., description="Total request latency in ms")
    agent_timings: dict[str, float] = Field(..., description="Per-agent execution times in ms")
    llm_calls: int = Field(..., description="Total number of LLM API calls")
    retry_count: int = Field(default=0, description="Number of SQL retries")
    sql_formatter_fallback_calls: int = Field(
        default=0,
        description="Number of SQL formatter fallback attempts for malformed SQL JSON.",
    )
    sql_formatter_fallback_successes: int = Field(
        default=0,
        description="Number of successful SQL formatter fallback recoveries.",
    )
    query_compiler_llm_calls: int = Field(
        default=0,
        description="Number of query-compiler mini-LLM refinement calls.",
    )
    query_compiler_llm_refinements: int = Field(
        default=0,
        description="Number of query-compiler plans refined by mini-LLM.",
    )
    query_compiler_latency_ms: float = Field(
        default=0.0,
        description="Total time spent in query compiler stage (ms).",
    )


class SubAnswer(BaseModel):
    """One sub-answer produced from a decomposed multi-question prompt."""

    index: int = Field(..., description="1-based index of the sub-question")
    query: str = Field(..., description="Resolved sub-question text")
    answer: str = Field(..., description="Natural language answer for this sub-question")
    answer_source: str | None = Field(default=None, description="Source for this sub-answer")
    answer_confidence: float | None = Field(
        default=None, description="Confidence score for this sub-answer"
    )
    sql: str | None = Field(default=None, description="SQL generated for this sub-answer")
    data: dict[str, list] | None = Field(
        default=None,
        description="Columnar query result data for this sub-answer when available.",
    )
    visualization_hint: str | None = Field(
        default=None, description="Suggested visualization type for this sub-answer"
    )
    visualization_metadata: dict[str, Any] | None = Field(
        default=None,
        description="Visualization decision metadata for this sub-answer.",
    )
    clarifying_questions: list[str] = Field(
        default_factory=list,
        description="Clarifying questions for this sub-answer",
    )
    error: str | None = Field(default=None, description="Error tied to this sub-answer")


class WorkflowMetric(BaseModel):
    """Key metric extracted for a workflow-style answer package."""

    label: str = Field(..., description="Metric label")
    value: str = Field(..., description="Human-readable metric value")


class WorkflowDriver(BaseModel):
    """One ranked driver behind the answer."""

    dimension: str = Field(..., description="Dimension used for grouping")
    value: str = Field(..., description="Dimension value")
    contribution: str = Field(..., description="Driver contribution summary")


class WorkflowSource(BaseModel):
    """Source summary for workflow package provenance."""

    datapoint_id: str = Field(..., description="DataPoint identifier")
    name: str = Field(..., description="DataPoint name")
    source_type: str = Field(..., description="DataPoint type")


class WorkflowArtifacts(BaseModel):
    """Decision-ready output package for workflow-oriented responses."""

    package_version: str = Field(default="1.0", description="Workflow package schema version")
    domain: str = Field(default="finance", description="Workflow domain label")
    summary: str = Field(..., description="Concise business summary")
    metrics: list[WorkflowMetric] = Field(default_factory=list, description="Key metrics")
    drivers: list[WorkflowDriver] = Field(default_factory=list, description="Top drivers")
    caveats: list[str] = Field(default_factory=list, description="Assumptions and caveats")
    sources: list[WorkflowSource] = Field(default_factory=list, description="Source provenance")
    follow_ups: list[str] = Field(
        default_factory=list,
        description="Suggested follow-up prompts to continue analysis",
    )


class ChatResponse(BaseModel):
    """Response model for chat endpoint."""

    run_id: str | None = Field(default=None, description="Persisted run identifier")
    answer: str = Field(..., description="Natural language answer to the query")
    clarifying_questions: list[str] = Field(
        default_factory=list,
        description="Clarifying questions when more detail is required",
    )
    sql: str | None = Field(None, description="Generated SQL query (if applicable)")
    data: dict[str, list] | None = Field(None, description="Query results in columnar format")
    visualization_hint: str | None = Field(None, description="Suggested visualization type")
    visualization_metadata: dict[str, Any] | None = Field(
        default=None,
        description="Optional visualization decision metadata and resolution reason.",
    )
    sources: list[DataSource] = Field(
        default_factory=list, description="Data sources used to answer"
    )
    answer_source: str | None = Field(default=None, description="Answer source (context|sql|error)")
    answer_confidence: float | None = Field(
        default=None, description="Confidence score for the answer"
    )
    evidence: list[EvidenceItem] = Field(
        default_factory=list, description="Evidence items supporting the answer"
    )
    validation_errors: list[SQLValidationError] = Field(
        default_factory=list, description="SQL validation errors (if any)"
    )
    validation_warnings: list[ValidationWarning] = Field(
        default_factory=list, description="SQL validation warnings (if any)"
    )
    tool_approval_required: bool = Field(
        default=False, description="Whether tool execution needs approval"
    )
    tool_approval_message: str | None = Field(default=None, description="Approval request message")
    tool_approval_calls: list[dict] = Field(
        default_factory=list, description="Tool calls requiring approval"
    )
    metrics: ChatMetrics | None = Field(None, description="Performance metrics")
    conversation_id: str | None = Field(None, description="Conversation ID for follow-up")
    session_summary: str | None = Field(
        default=None,
        description="Compact memory summary to send on the next turn.",
    )
    session_state: dict[str, Any] | None = Field(
        default=None,
        description="Structured memory state to send on the next turn.",
    )
    sub_answers: list[SubAnswer] = Field(
        default_factory=list,
        description="Per-question answers when a prompt is decomposed into multiple questions.",
    )
    decision_trace: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Deterministic routing/decision trace for observability and evals.",
    )
    action_trace: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Replayable DYN-001 plan/act/verify loop step trace.",
    )
    loop_terminal_state: str | None = Field(
        default=None,
        description="Action-loop terminal state (completed|needs_user_input|blocked|impossible).",
    )
    loop_stop_reason: str | None = Field(
        default=None,
        description="Reason code describing why the action-loop terminated.",
    )
    loop_shadow_decisions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Shadow-mode recommended-vs-actual decision records.",
    )
    workflow_artifacts: WorkflowArtifacts | None = Field(
        default=None,
        description="Optional decision-ready workflow package (finance-focused v1).",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "answer": "The total revenue is $1,234,567.89",
                "sql": "SELECT SUM(amount) as total_revenue FROM analytics.fact_sales WHERE status = 'completed'",
                "data": {"total_revenue": [1234567.89]},
                "visualization_hint": "none",
                "visualization_metadata": {
                    "requested": None,
                    "deterministic": "none",
                    "llm_suggested": None,
                    "final": "none",
                    "resolution_reason": "deterministic_default",
                },
                "sources": [
                    {
                        "datapoint_id": "table_fact_sales_001",
                        "type": "Schema",
                        "name": "Fact Sales Table",
                        "relevance_score": 0.95,
                    }
                ],
                "answer_source": "sql",
                "answer_confidence": 0.92,
                "evidence": [
                    {
                        "datapoint_id": "table_fact_sales_001",
                        "name": "Fact Sales Table",
                        "type": "Schema",
                        "reason": "Used for SQL generation",
                    }
                ],
                "metrics": {
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
                },
                "conversation_id": "conv_123",
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
            }
        }
    }


class HealthResponse(BaseModel):
    """Response model for health check endpoints."""

    status: str = Field(..., description="Service status: 'healthy' or 'unhealthy'")
    version: str = Field(..., description="API version")
    timestamp: str = Field(..., description="Current timestamp (ISO 8601)")


class ReadinessResponse(BaseModel):
    """Response model for readiness check endpoint."""

    status: str = Field(..., description="Readiness status: 'ready' or 'not_ready'")
    version: str = Field(..., description="API version")
    timestamp: str = Field(..., description="Current timestamp (ISO 8601)")
    checks: dict[str, bool] = Field(
        ..., description="Individual readiness checks (db, vector_store, etc.)"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "status": "ready",
                "version": "0.1.0",
                "timestamp": "2026-01-16T12:00:00Z",
                "checks": {
                    "database": True,
                    "vector_store": True,
                    "pipeline": True,
                },
            }
        }
    }


class SetupStep(BaseModel):
    """Setup step required to initialize the system."""

    step: str = Field(..., description="Setup step identifier")
    title: str = Field(..., description="Short step title")
    description: str = Field(..., description="Description of the setup step")
    action: str = Field(..., description="Suggested action key for clients")


class SystemStatusResponse(BaseModel):
    """Initialization status response."""

    is_initialized: bool = Field(
        ...,
        description=(
            "Whether the system can answer queries (target DB connected). "
            "DataPoints are optional enrichment."
        ),
    )
    has_databases: bool = Field(..., description="Whether a database connection is available")
    has_system_database: bool = Field(
        ..., description="Whether a system database is available for registry/profiling"
    )
    has_datapoints: bool = Field(..., description="Whether DataPoints are loaded")
    setup_required: list[SetupStep] = Field(
        default_factory=list,
        description="Remaining setup/recommended steps",
    )


class SystemInitializeRequest(BaseModel):
    """Initialization request payload."""

    database_url: str | None = Field(None, description="Database URL to use for initialization")
    system_database_url: str | None = Field(
        None, description="System database URL for registry/profiling/demo"
    )
    auto_profile: bool = Field(
        default=False,
        description="Whether to auto-profile the database (not implemented yet)",
    )


class SystemInitializeResponse(BaseModel):
    """Initialization response payload."""

    message: str = Field(..., description="Initialization status message")
    is_initialized: bool = Field(
        ...,
        description=(
            "Whether the system can answer queries (target DB connected). "
            "DataPoints are optional enrichment."
        ),
    )
    has_databases: bool = Field(..., description="Whether a database connection is available")
    has_system_database: bool = Field(
        ..., description="Whether a system database is available for registry/profiling"
    )
    has_datapoints: bool = Field(..., description="Whether DataPoints are loaded")
    setup_required: list[SetupStep] = Field(
        default_factory=list,
        description="Remaining setup/recommended steps",
    )


class EntryEventRequest(BaseModel):
    """Entry-layer telemetry event payload."""

    flow: str = Field(..., min_length=1, description="Flow identifier")
    step: str = Field(..., min_length=1, description="Step identifier")
    status: str = Field(
        ...,
        description="Step status (started, completed, failed, skipped)",
    )
    source: str = Field(default="ui", description="Event source (ui, cli, api)")
    metadata: dict[str, Any] | None = Field(
        default=None,
        description="Optional structured metadata for this event",
    )


class EntryEventResponse(BaseModel):
    """Entry-layer telemetry response."""

    ok: bool = Field(default=True, description="Whether event ingestion succeeded")


class ConversationSnapshotPayload(BaseModel):
    """Persisted UI conversation snapshot."""

    frontend_session_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    target_database_id: str | None = Field(default=None)
    conversation_id: str | None = Field(default=None)
    session_summary: str | None = Field(default=None)
    session_state: dict[str, Any] = Field(default_factory=dict)
    messages: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime | None = Field(default=None)
    updated_at: datetime | None = Field(default=None)


class ConversationUpsertRequest(BaseModel):
    """Upsert payload for persisted UI conversation snapshots."""

    title: str = Field(..., min_length=1)
    target_database_id: str | None = Field(default=None)
    conversation_id: str | None = Field(default=None)
    session_summary: str | None = Field(default=None)
    session_state: dict[str, Any] = Field(default_factory=dict)
    messages: list[dict[str, Any]] = Field(default_factory=list)
    updated_at: datetime | None = Field(default=None)


class ConversationDeleteResponse(BaseModel):
    """Delete result for a persisted conversation snapshot."""

    ok: bool = True
    deleted: bool = False


class FeedbackSubmitRequest(BaseModel):
    """Feedback payload for answer quality, issues, and improvement ideas."""

    category: Literal["answer_feedback", "issue_report", "improvement_suggestion"] = Field(
        ...,
        description="Feedback category.",
    )
    sentiment: Literal["up", "down"] | None = Field(
        default=None,
        description="Optional binary sentiment for answer_feedback.",
    )
    message: str | None = Field(
        default=None,
        description="Optional free-text issue or suggestion details.",
    )
    conversation_id: str | None = Field(default=None)
    message_id: str | None = Field(default=None)
    target_database_id: str | None = Field(default=None)
    answer_source: str | None = Field(default=None)
    answer_confidence: float | None = Field(default=None)
    query: str | None = Field(default=None)
    answer: str | None = Field(default=None)
    sql: str | None = Field(default=None)
    sources: list[dict[str, Any]] | None = Field(default=None)
    metadata: dict[str, Any] | None = Field(default=None)


class FeedbackSubmitResponse(BaseModel):
    """Response for feedback submission."""

    ok: bool = True
    feedback_id: str
    saved_to: Literal["system_database", "logs_only"] = "system_database"
    created_at: str


class FeedbackSummaryResponse(BaseModel):
    """Aggregated feedback counts for dashboarding/ops."""

    window_days: int
    totals: list[dict[str, Any]] = Field(default_factory=list)


class ToolExecuteRequest(BaseModel):
    """Tool execution request payload."""

    name: str = Field(..., description="Tool name")
    arguments: dict = Field(default_factory=dict, description="Tool arguments")
    target_database: str | None = Field(
        default=None,
        description="Optional database connection ID to use for this tool call",
    )
    approved: bool = Field(default=False, description="Whether tool execution is approved")
    user_id: str | None = Field(default=None, description="Optional user ID")
    correlation_id: str | None = Field(
        default=None, description="Optional correlation ID for audit logging"
    )


class ToolExecuteResponse(BaseModel):
    """Tool execution response payload."""

    tool: str = Field(..., description="Tool name")
    success: bool = Field(..., description="Whether execution succeeded")
    result: dict | None = Field(None, description="Tool result payload")
    error: str | None = Field(None, description="Error message if execution failed")


class ToolInfo(BaseModel):
    """Tool definition summary."""

    name: str
    description: str
    category: str
    requires_approval: bool
    enabled: bool
    parameters_schema: dict


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Human-readable error message")
    agent: str | None = Field(None, description="Agent that caused the error")
    recoverable: bool = Field(default=False, description="Whether the error is recoverable")

    model_config = {
        "json_schema_extra": {
            "example": {
                "error": "agent_error",
                "message": "Failed to generate SQL query",
                "agent": "SQLAgent",
                "recoverable": True,
            }
        }
    }

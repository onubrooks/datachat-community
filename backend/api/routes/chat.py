"""
Chat Routes

FastAPI endpoints for natural language chat interface.
"""

import logging
import re
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse

from backend.api.database_context import resolve_database_type_and_url
from backend.api.visualization import infer_direct_sql_visualization
from backend.api.workflow_packaging import build_workflow_artifacts
from backend.config import get_settings
from backend.connectors.base import (
    ConnectionError as ConnectorConnectionError,
)
from backend.connectors.base import (
    QueryError as ConnectorQueryError,
)
from backend.connectors.factory import create_connector
from backend.initialization.initializer import SystemInitializer
from backend.models.api import (
    ChatMetrics,
    ChatRequest,
    ChatResponse,
    DataSource,
)

logger = logging.getLogger(__name__)

router = APIRouter()
LIVE_SCHEMA_MODE_NOTICE = (
    "Live schema mode: DataPoints are not loaded yet. "
    "Answers are generated from database metadata and query results only."
)
READ_ONLY_SQL_PREFIXES = ("select", "with", "show", "describe", "desc", "explain")
MUTATING_SQL_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "merge",
    "call",
    "copy",
)


@router.post("/chat", response_model=ChatResponse)
async def chat(request: Request, chat_request: ChatRequest) -> ChatResponse:
    """
    Process a natural language query and return structured response.

    Args:
        chat_request: User's message with optional conversation context

    Returns:
        ChatResponse with answer, SQL, data, and metadata

    Raises:
        HTTPException: If pipeline fails or is not initialized
    """
    logger.info(f"Chat request received: {chat_request.message[:100]}...")

    try:
        # Get pipeline from app state
        from backend.api.main import app_state

        settings = get_settings()
        database_type = settings.database.db_type
        database_url = None
        manager = app_state.get("database_manager")
        try:
            resolved_type, resolved_url = await resolve_database_type_and_url(
                target_database=chat_request.target_database,
                manager=manager,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(exc),
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc),
            ) from exc
        if resolved_type:
            database_type = resolved_type
        if resolved_url:
            database_url = resolved_url

        initializer = SystemInitializer(app_state)
        status_state = await initializer.status()
        if not status_state.has_databases:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "error": "system_not_initialized",
                    "message": (
                        "DataChat requires a target database connection. "
                        "Run 'datachat setup' or 'datachat demo' to get started."
                    ),
                    "setup_steps": [
                        {
                            "step": step.step,
                            "title": step.title,
                            "description": step.description,
                            "action": step.action,
                        }
                        for step in status_state.setup_required
                    ],
                },
            )

        conversation_id = chat_request.conversation_id or f"conv_{uuid.uuid4().hex[:12]}"
        request_mode = (chat_request.execution_mode or "natural_language").lower()
        if request_mode == "direct_sql":
            sql_query = (chat_request.sql or chat_request.message or "").strip()
            if not sql_query:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="SQL mode requires a non-empty SQL query.",
                )
            if not _is_read_only_sql(sql_query):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        "SQL Editor accepts read-only SQL only "
                        "(SELECT/WITH/SHOW/DESCRIBE/EXPLAIN)."
                    ),
                )
            if not database_url:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="No active database URL available for SQL execution.",
                )

            started_at = time.perf_counter()
            result = await _run_direct_sql_query(
                sql_query=sql_query,
                database_type=database_type,
                database_url=database_url,
                timeout_seconds=settings.database.pool_timeout,
                pool_size=settings.database.pool_size,
            )
            row_count = len(result["rows"])
            answer = f"Executed SQL query successfully. Returned {row_count} row(s)."
            if not status_state.has_datapoints:
                answer = _maybe_append_live_schema_notice(answer, has_datapoints=False)
            total_latency_ms = (time.perf_counter() - started_at) * 1000
            data = {
                column: [row.get(column) for row in result["rows"]]
                for column in result["columns"]
            }
            visualization_hint, visualization_metadata = infer_direct_sql_visualization(data)
            metrics = ChatMetrics(
                total_latency_ms=total_latency_ms,
                agent_timings={"direct_sql_execution": result["execution_time_ms"]},
                llm_calls=0,
                retry_count=0,
            )
            workflow_artifacts = build_workflow_artifacts(
                query=chat_request.message or sql_query,
                answer=answer,
                answer_source="sql",
                data=data,
                sources=[],
                validation_warnings=[],
                clarifying_questions=[],
                has_datapoints=status_state.has_datapoints,
                workflow_mode=chat_request.workflow_mode,
                sql=sql_query,
                retrieved_datapoints=[],
                used_datapoints=[],
            )
            return ChatResponse(
                answer=answer,
                clarifying_questions=[],
                sql=sql_query,
                data=data,
                visualization_hint=visualization_hint,
                visualization_metadata=visualization_metadata,
                sources=[],
                answer_source="sql",
                answer_confidence=1.0,
                evidence=[],
                validation_errors=[],
                validation_warnings=[],
                tool_approval_required=False,
                tool_approval_message=None,
                tool_approval_calls=[],
                metrics=metrics,
                conversation_id=conversation_id,
                session_summary=chat_request.session_summary,
                session_state=chat_request.session_state,
                sub_answers=[],
                workflow_artifacts=workflow_artifacts,
                decision_trace=[
                    {
                        "stage": "execution_mode",
                        "decision": "direct_sql",
                        "reason": "user_selected_sql_editor_mode",
                    }
                ],
                action_trace=[
                    {
                        "version": "v1",
                        "step": 1,
                        "stage": "execution_mode",
                        "selected_action": "direct_sql",
                        "verification": {"status": "ok"},
                        "terminal_state": "completed",
                        "stop_reason": "execution_completed",
                    }
                ],
                loop_terminal_state="completed",
                loop_stop_reason="execution_completed",
                loop_shadow_decisions=[],
            )

        pipeline = app_state.get("pipeline")
        if pipeline is None:
            raise RuntimeError("Pipeline not initialized")

        # Convert conversation history to pipeline format
        conversation_history = [
            {"role": msg.role, "content": msg.content} for msg in chat_request.conversation_history
        ]

        # Run pipeline
        logger.info("Running pipeline...")
        result = await pipeline.run(
            query=chat_request.message,
            conversation_history=conversation_history,
            session_summary=chat_request.session_summary,
            session_state=chat_request.session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=chat_request.target_database,
            synthesize_simple_sql=chat_request.synthesize_simple_sql,
            workflow_mode=chat_request.workflow_mode,
        )

        # Extract data from pipeline state
        answer = result.get("natural_language_answer")
        if not answer:
            # Fallback if no answer generated
            if result.get("error"):
                answer = f"I encountered an error: {result.get('error')}"
            else:
                answer = "I was unable to process your query. Please try rephrasing."
        answer = _maybe_append_live_schema_notice(answer, status_state.has_datapoints)
        answer_source = _resolve_answer_source(result)
        answer_confidence = _resolve_answer_confidence(result, answer_source)

        # Extract SQL
        sql_query = result.get("validated_sql") or result.get("generated_sql")

        # Extract query results
        query_result = result.get("query_result")
        data = None
        if query_result and isinstance(query_result, dict):
            data = query_result.get("data")
            if data is None:
                rows = query_result.get("rows")
                columns = query_result.get("columns")
                if isinstance(rows, list) and isinstance(columns, list):
                    data = {col: [row.get(col) for row in rows] for col in columns}

        # Extract visualization hint
        visualization_hint = result.get("visualization_hint")
        visualization_metadata = result.get("visualization_metadata")

        # Build sources from retrieved datapoints
        sources = _build_sources(result)

        # Build metrics
        metrics = _build_metrics(result)
        workflow_artifacts = build_workflow_artifacts(
            query=chat_request.message,
            answer=answer,
            answer_source=answer_source,
            data=data,
            sources=sources,
            validation_warnings=result.get("validation_warnings", []),
            clarifying_questions=result.get("clarifying_questions", []),
            has_datapoints=status_state.has_datapoints,
            workflow_mode=chat_request.workflow_mode,
            sql=sql_query,
            retrieved_datapoints=result.get("retrieved_datapoints", []),
            used_datapoints=result.get("used_datapoints", []),
        )

        response = ChatResponse(
            answer=answer,
            clarifying_questions=result.get("clarifying_questions", []),
            sql=sql_query,
            data=data,
            visualization_hint=visualization_hint,
            visualization_metadata=visualization_metadata,
            sources=sources,
            answer_source=answer_source,
            answer_confidence=answer_confidence,
            evidence=_build_evidence(result),
            validation_errors=result.get("validation_errors", []),
            validation_warnings=result.get("validation_warnings", []),
            tool_approval_required=bool(result.get("tool_approval_required")),
            tool_approval_message=result.get("tool_approval_message"),
            tool_approval_calls=result.get("tool_approval_calls", []),
            metrics=metrics,
            conversation_id=conversation_id,
            session_summary=result.get("session_summary"),
            session_state=result.get("session_state"),
            sub_answers=result.get("sub_answers", []),
            workflow_artifacts=workflow_artifacts,
            decision_trace=result.get("decision_trace", []),
            action_trace=result.get("action_trace", []),
            loop_terminal_state=result.get("loop_terminal_state"),
            loop_stop_reason=result.get("loop_stop_reason"),
            loop_shadow_decisions=result.get("loop_shadow_decisions", []),
        )

        logger.info(
            "Chat request completed successfully",
            extra={
                "conversation_id": conversation_id,
                "latency_ms": metrics.total_latency_ms if metrics else None,
                "llm_calls": metrics.llm_calls if metrics else None,
            },
        )

        return response

    except RuntimeError as e:
        logger.error(f"Pipeline not initialized: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Pipeline not initialized. Please try again later.",
        ) from e
    except (ConnectorConnectionError, ConnectorQueryError) as e:
        logger.error(f"Direct SQL execution failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        logger.error(f"Unexpected error in chat endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}",
        ) from e


def _is_read_only_sql(sql_query: str) -> bool:
    compact = sql_query.strip()
    if not compact:
        return False
    statements = [part.strip() for part in compact.split(";") if part.strip()]
    if len(statements) != 1:
        return False
    statement = statements[0].lower()
    if not statement.startswith(READ_ONLY_SQL_PREFIXES):
        return False
    return not any(re.search(rf"\b{keyword}\b", statement) for keyword in MUTATING_SQL_KEYWORDS)


async def _run_direct_sql_query(
    *,
    sql_query: str,
    database_type: str,
    database_url: str,
    timeout_seconds: int,
    pool_size: int,
) -> dict[str, Any]:
    connector = create_connector(
        database_type=database_type,
        database_url=database_url,
        timeout=timeout_seconds,
        pool_size=pool_size,
    )
    try:
        await connector.connect()
        query_result = await connector.execute(sql_query)
        return {
            "rows": query_result.rows,
            "columns": query_result.columns,
            "execution_time_ms": query_result.execution_time_ms,
        }
    finally:
        await connector.close()


def _build_sources(result: dict[str, Any]) -> list[DataSource]:
    """Build DataSource list from pipeline result."""
    sources = []

    # Get retrieved datapoints from context agent
    retrieved_datapoints = result.get("retrieved_datapoints", [])

    for dp in retrieved_datapoints:
        # Handle both dict and object formats
        if isinstance(dp, dict):
            sources.append(
                DataSource(
                    datapoint_id=dp.get("datapoint_id", "unknown"),
                    type=dp.get("datapoint_type", dp.get("type", "unknown")),
                    name=dp.get("name", "Unknown"),
                    relevance_score=dp.get("score", 0.0),
                )
            )
        else:
            # Handle Pydantic model
            sources.append(
                DataSource(
                    datapoint_id=getattr(dp, "datapoint_id", "unknown"),
                    type=getattr(dp, "datapoint_type", "unknown"),
                    name=getattr(dp, "name", "Unknown"),
                    relevance_score=getattr(dp, "score", 0.0),
                )
            )

    return sources


def _build_metrics(result: dict[str, Any]) -> ChatMetrics | None:
    """Build ChatMetrics from pipeline result."""
    try:
        return ChatMetrics(
            total_latency_ms=result.get("total_latency_ms", 0.0),
            agent_timings=result.get("agent_timings", {}),
            llm_calls=result.get("llm_calls", 0),
            retry_count=result.get("retry_count", 0),
            sql_formatter_fallback_calls=result.get("sql_formatter_fallback_calls", 0),
            sql_formatter_fallback_successes=result.get("sql_formatter_fallback_successes", 0),
            query_compiler_llm_calls=result.get("query_compiler_llm_calls", 0),
            query_compiler_llm_refinements=result.get("query_compiler_llm_refinements", 0),
            query_compiler_latency_ms=result.get("query_compiler_latency_ms", 0.0),
        )
    except Exception as e:
        logger.warning(f"Failed to build metrics: {e}")
        return None


def _build_evidence(result: dict[str, Any]) -> list[dict[str, Any]]:
    evidence_items = []
    for item in result.get("evidence", []):
        if isinstance(item, dict):
            evidence_items.append(
                {
                    "datapoint_id": item.get("datapoint_id", "unknown"),
                    "name": item.get("name"),
                    "type": item.get("type"),
                    "reason": item.get("reason"),
                }
            )
    return evidence_items


def _maybe_append_live_schema_notice(answer: str, has_datapoints: bool) -> str:
    if has_datapoints:
        return answer
    if LIVE_SCHEMA_MODE_NOTICE in answer:
        return answer
    return f"{answer}\n\n{LIVE_SCHEMA_MODE_NOTICE}"


def _resolve_answer_source(result: dict[str, Any]) -> str:
    source = result.get("answer_source")
    if source:
        return source
    if result.get("tool_approval_required"):
        return "approval"
    if result.get("clarification_needed") or result.get("clarifying_questions"):
        return "clarification"
    if result.get("error"):
        return "error"
    if result.get("validated_sql") or result.get("generated_sql") or result.get("query_result"):
        return "sql"
    if result.get("natural_language_answer"):
        return "context"
    return "error"


def _resolve_answer_confidence(result: dict[str, Any], source: str) -> float:
    confidence = result.get("answer_confidence")
    if confidence is not None:
        try:
            numeric = float(confidence)
        except (TypeError, ValueError):
            numeric = 0.5
        return max(0.0, min(1.0, numeric))

    defaults = {
        "sql": 0.7,
        "context": 0.6,
        "clarification": 0.2,
        "system": 0.8,
        "approval": 0.5,
        "multi": 0.65,
        "error": 0.0,
    }
    return defaults.get(source, 0.5)

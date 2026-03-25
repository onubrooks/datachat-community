"""
WebSocket Routes

Real-time streaming WebSocket endpoint for chat with agent status updates.
"""

import asyncio
import json
import logging
import re
import time
import uuid
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.encoders import jsonable_encoder

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
from backend.knowledge.retriever import Retriever
from backend.models.api import DataSource
from backend.pipeline.orchestrator import DataChatPipeline

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


def _thinking_note_for_event(event_type: str, event_data: dict[str, Any]) -> str | None:
    """Return safe, user-facing progress notes for live reasoning UI."""
    agent = str(event_data.get("agent") or "").strip()

    if event_type == "decompose_complete":
        part_count = event_data.get("part_count")
        if isinstance(part_count, int) and part_count > 1:
            return f"Breaking this into {part_count} focused sub-questions."
        return None

    if event_type == "agent_start":
        start_notes = {
            "ToolPlannerAgent": "Planning the safest tool path.",
            "ToolExecutor": "Running approved tools.",
            "ClassifierAgent": "Classifying your request intent.",
            "ContextAgent": "Gathering relevant metadata and DataPoints.",
            "ContextAnswerAgent": "Checking if context can answer directly.",
            "SQLAgent": "Translating your request into SQL.",
            "ValidatorAgent": "Validating SQL for safety and correctness.",
            "ExecutorAgent": "Executing SQL on the selected database.",
            "ResponseSynthesisAgent": "Synthesizing the final response.",
        }
        return start_notes.get(agent)

    if event_type == "agent_complete":
        data = event_data.get("data")
        payload = data if isinstance(data, dict) else {}
        complete_notes = {
            "ToolPlannerAgent": "Tool planning complete.",
            "ToolExecutor": (
                f"Tools complete ({payload.get('tool_results', 0)} result(s))."
                if payload
                else "Tool execution complete."
            ),
            "ClassifierAgent": "Intent classified.",
            "ContextAgent": (
                f"Context retrieved ({payload.get('datapoints_found', 0)} source(s))."
                if payload
                else "Context retrieval complete."
            ),
            "ContextAnswerAgent": "Context answer pass complete.",
            "SQLAgent": (
                "SQL candidate generated."
                if payload.get("sql_generated")
                else "SQL generation needs clarification."
            ),
            "ValidatorAgent": (
                "SQL validation passed."
                if payload.get("validation_passed")
                else "SQL validation flagged issues."
            ),
            "ExecutorAgent": (
                f"Execution complete ({payload.get('rows_returned', 0)} row(s))."
                if payload
                else "Execution complete."
            ),
            "ResponseSynthesisAgent": "Final response generated.",
        }
        return complete_notes.get(agent)

    return None


@router.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for real-time chat with streaming updates.

    Event Types:
        - agent_start: Agent begins execution
        - agent_complete: Agent finishes execution
        - data_chunk: Intermediate data from agent
        - answer_chunk: Streaming answer text
        - complete: Final response with all data
        - error: Error occurred during processing

    Message Format:
        Client -> Server:
        {
            "message": "What's the total revenue?",
            "conversation_id": "conv_123",  # optional
            "conversation_history": [...]    # optional
        }

        Server -> Client:
        {
            "event": "agent_start",
            "agent": "ClassifierAgent",
            "timestamp": "2026-01-16T12:00:00Z"
        }
        {
            "event": "agent_complete",
            "agent": "ClassifierAgent",
            "data": {...},
            "duration_ms": 234.5
        }
        {
            "event": "complete",
            "answer": "The total revenue is $1,234,567.89",
            "sql": "SELECT ...",
            "data": {...},
            "sources": [...],
            "metrics": {...},
            "conversation_id": "conv_123"
        }
    """
    from backend.api.main import app_state

    await websocket.accept()
    logger.info("WebSocket connection established")

    try:
        # Receive initial message
        data = await websocket.receive_json()
        logger.info(f"Received WebSocket message: {data.get('message', '')[:100]}...")

        # Validate required fields
        if "message" not in data:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "validation_error",
                    "message": "Missing required field: message",
                }
            )
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        settings = get_settings()
        database_type = settings.database.db_type
        database_url = None
        manager = app_state.get("database_manager")
        target_database = data.get("target_database")
        try:
            resolved_type, resolved_url = await resolve_database_type_and_url(
                target_database=target_database,
                manager=manager,
            )
        except (KeyError, ValueError) as exc:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "invalid_target_database",
                    "message": str(exc),
                }
            )
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        if resolved_type:
            database_type = resolved_type
        if resolved_url:
            database_url = resolved_url

        initializer = SystemInitializer(app_state)
        status_state = await initializer.status()
        if not status_state.has_databases and not database_url:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "system_not_initialized",
                    "message": (
                        "DataChat requires a target database connection. "
                        "Start the onboarding wizard in the UI (Databases -> Start Onboarding Wizard) "
                        "or run `datachat onboarding wizard` in the CLI."
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
                }
            )
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        # Extract request data
        message = data["message"]
        conversation_id = data.get("conversation_id") or f"conv_{uuid.uuid4().hex[:12]}"
        conversation_history = data.get("conversation_history", [])
        session_summary = data.get("session_summary")
        session_state = data.get("session_state")
        synthesize_simple_sql = data.get("synthesize_simple_sql")
        workflow_mode = data.get("workflow_mode") or "auto"
        execution_mode = str(data.get("execution_mode") or "natural_language").lower()

        if execution_mode == "direct_sql":
            sql_query = str(data.get("sql") or message).strip()
            if not sql_query:
                await websocket.send_json(
                    {
                        "event": "error",
                        "error": "validation_error",
                        "message": "SQL mode requires a non-empty SQL query.",
                    }
                )
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return
            if not _is_read_only_sql(sql_query):
                await websocket.send_json(
                    {
                        "event": "error",
                        "error": "validation_error",
                        "message": (
                            "SQL Editor accepts read-only SQL only "
                            "(SELECT/WITH/SHOW/DESCRIBE/EXPLAIN)."
                        ),
                    }
                )
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return
            if not database_url:
                await websocket.send_json(
                    {
                        "event": "error",
                        "error": "service_unavailable",
                        "message": "No active database URL available for SQL execution.",
                    }
                )
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                return

            try:
                started_at = time.perf_counter()
                direct_result = await _run_direct_sql_query(
                    sql_query=sql_query,
                    database_type=database_type,
                    database_url=database_url,
                    timeout_seconds=settings.database.pool_timeout,
                    pool_size=settings.database.pool_size,
                )
                total_latency_ms = (time.perf_counter() - started_at) * 1000
                row_count = len(direct_result["rows"])
                answer = f"Executed SQL query successfully. Returned {row_count} row(s)."
                if not status_state.has_datapoints:
                    answer = f"{answer}\n\n{LIVE_SCHEMA_MODE_NOTICE}"
                data_result = {
                    column: [row.get(column) for row in direct_result["rows"]]
                    for column in direct_result["columns"]
                }
                visualization_hint, visualization_metadata = infer_direct_sql_visualization(
                    data_result
                )
                workflow_artifacts = build_workflow_artifacts(
                    query=message or sql_query,
                    answer=answer,
                    answer_source="sql",
                    data=data_result,
                    sources=[],
                    validation_warnings=[],
                    clarifying_questions=[],
                    has_datapoints=status_state.has_datapoints,
                    workflow_mode=workflow_mode,
                    sql=sql_query,
                    retrieved_datapoints=[],
                    used_datapoints=[],
                )
                payload = {
                    "event": "complete",
                    "answer": answer,
                    "clarifying_questions": [],
                    "sub_answers": [],
                    "sql": sql_query,
                    "data": data_result,
                    "visualization_hint": visualization_hint,
                    "visualization_metadata": visualization_metadata,
                    "sources": [],
                    "answer_source": "sql",
                    "answer_confidence": 1.0,
                    "evidence": [],
                    "validation_errors": [],
                    "validation_warnings": [],
                    "tool_approval_required": False,
                    "tool_approval_message": None,
                    "tool_approval_calls": [],
                    "metrics": {
                        "total_latency_ms": total_latency_ms,
                        "agent_timings": {
                            "direct_sql_execution": direct_result["execution_time_ms"]
                        },
                        "llm_calls": 0,
                        "retry_count": 0,
                        "sql_formatter_fallback_calls": 0,
                        "sql_formatter_fallback_successes": 0,
                        "query_compiler_llm_calls": 0,
                        "query_compiler_llm_refinements": 0,
                        "query_compiler_latency_ms": 0.0,
                    },
                    "conversation_id": conversation_id,
                    "session_summary": session_summary,
                    "session_state": session_state,
                    "workflow_artifacts": workflow_artifacts,
                    "decision_trace": [
                        {
                            "stage": "execution_mode",
                            "decision": "direct_sql",
                            "reason": "user_selected_sql_editor_mode",
                        }
                    ],
                    "action_trace": [
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
                    "loop_terminal_state": "completed",
                    "loop_stop_reason": "execution_completed",
                    "loop_shadow_decisions": [],
                }
                await websocket.send_json(jsonable_encoder(payload))
                return
            except (ConnectorConnectionError, ConnectorQueryError) as exc:
                await websocket.send_json(
                    {
                        "event": "error",
                        "error": "query_error",
                        "message": str(exc),
                    }
                )
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return

        # Get pipeline from app state
        pipeline = app_state.get("pipeline")
        if pipeline is None:
            if database_url:
                try:
                    connector = create_connector(
                        database_type=database_type,
                        database_url=database_url,
                        pool_size=settings.database.pool_size,
                    )
                    await connector.connect()
                    retriever = Retriever(
                        vector_store=app_state["vector_store"],
                        knowledge_graph=app_state["knowledge_graph"],
                    )
                    pipeline = DataChatPipeline(
                        retriever=retriever,
                        connector=connector,
                        run_store=app_state.get("run_store"),
                        max_retries=3,
                    )
                    app_state["connector"] = connector
                    app_state["pipeline"] = pipeline
                    logger.info("Initialized pipeline lazily from managed connection.")
                except Exception as exc:
                    await websocket.send_json(
                        {
                            "event": "error",
                            "error": "service_unavailable",
                            "message": (
                                "Pipeline is unavailable and lazy initialization failed: "
                                f"{exc}"
                            ),
                        }
                    )
                    await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                    return
            else:
                await websocket.send_json(
                    {
                        "event": "error",
                        "error": "service_unavailable",
                        "message": "Pipeline not initialized. Please try again later.",
                    }
                )
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                return

        # Convert conversation history to pipeline format
        history = [
            {"role": msg.get("role", "user"), "content": msg.get("content", "")}
            for msg in conversation_history
        ]

        # Define callback for streaming events
        async def event_callback(event_type: str, event_data: dict[str, Any]) -> None:
            """Send event to WebSocket client."""
            try:
                await websocket.send_json(
                    {
                        "event": event_type,
                        **event_data,
                    }
                )
                note = _thinking_note_for_event(event_type, event_data)
                if note:
                    await websocket.send_json({"event": "thinking", "note": note})
            except Exception as e:
                logger.warning(f"Failed to send event {event_type}: {e}")

        # Run pipeline with streaming
        logger.info("Running pipeline with streaming...")
        result = await pipeline.run_with_streaming(
            query=message,
            conversation_history=history,
            session_summary=session_summary,
            session_state=session_state,
            database_type=database_type,
            database_url=database_url,
            target_connection_id=target_database,
            synthesize_simple_sql=synthesize_simple_sql,
            workflow_mode=workflow_mode,
            event_callback=event_callback,
        )

        # Build final response
        answer = result.get("natural_language_answer")
        if not answer:
            if result.get("error"):
                answer = f"I encountered an error: {result.get('error')}"
            else:
                answer = "I was unable to process your query. Please try rephrasing."
        if not status_state.has_datapoints and LIVE_SCHEMA_MODE_NOTICE not in answer:
            answer = f"{answer}\n\n{LIVE_SCHEMA_MODE_NOTICE}"

        sql_query = result.get("validated_sql") or result.get("generated_sql")
        query_result = result.get("query_result")
        data_result = None
        if query_result and isinstance(query_result, dict):
            data_result = query_result.get("data")
            if data_result is None:
                rows = query_result.get("rows")
                columns = query_result.get("columns")
                if isinstance(rows, list) and isinstance(columns, list):
                    data_result = {col: [row.get(col) for row in rows] for col in columns}

        visualization_hint = result.get("visualization_hint")
        visualization_metadata = result.get("visualization_metadata")

        # Build sources
        sources = []
        workflow_sources: list[DataSource] = []
        retrieved_datapoints = result.get("retrieved_datapoints", [])
        for dp in retrieved_datapoints:
            if isinstance(dp, dict):
                source = DataSource(
                    datapoint_id=dp.get("datapoint_id", "unknown"),
                    type=dp.get("datapoint_type", dp.get("type", "unknown")),
                    name=dp.get("name", "Unknown"),
                    relevance_score=dp.get("score", 0.0),
                )
                sources.append(source.model_dump())
                workflow_sources.append(source)
            else:
                source = DataSource(
                    datapoint_id=getattr(dp, "datapoint_id", "unknown"),
                    type=getattr(dp, "datapoint_type", "unknown"),
                    name=getattr(dp, "name", "Unknown"),
                    relevance_score=getattr(dp, "score", 0.0),
                )
                sources.append(source.model_dump())
                workflow_sources.append(source)

        # Build metrics
        metrics = {
            "total_latency_ms": result.get("total_latency_ms", 0.0),
            "agent_timings": result.get("agent_timings", {}),
            "llm_calls": result.get("llm_calls", 0),
            "retry_count": result.get("retry_count", 0),
            "sql_formatter_fallback_calls": result.get("sql_formatter_fallback_calls", 0),
            "sql_formatter_fallback_successes": result.get("sql_formatter_fallback_successes", 0),
            "query_compiler_llm_calls": result.get("query_compiler_llm_calls", 0),
            "query_compiler_llm_refinements": result.get("query_compiler_llm_refinements", 0),
            "query_compiler_latency_ms": result.get("query_compiler_latency_ms", 0.0),
        }
        answer_source = str(result.get("answer_source") or "error")
        workflow_artifacts = build_workflow_artifacts(
            query=message,
            answer=answer,
            answer_source=answer_source,
            data=data_result,
            sources=workflow_sources,
            validation_warnings=result.get("validation_warnings", []),
            clarifying_questions=result.get("clarifying_questions", []),
            has_datapoints=status_state.has_datapoints,
            workflow_mode=workflow_mode,
            sql=sql_query,
            retrieved_datapoints=result.get("retrieved_datapoints", []),
            used_datapoints=result.get("used_datapoints", []),
        )

        # Send final complete event
        payload = {
            "event": "complete",
            "run_id": result.get("run_id"),
            "answer": answer,
            "clarifying_questions": result.get("clarifying_questions", []),
            "sub_answers": result.get("sub_answers", []),
            "sql": sql_query,
            "data": data_result,
            "visualization_hint": visualization_hint,
            "visualization_metadata": visualization_metadata,
            "sources": sources,
            "answer_source": answer_source,
            "answer_confidence": result.get("answer_confidence"),
            "evidence": result.get("evidence", []),
            "validation_errors": result.get("validation_errors", []),
            "validation_warnings": result.get("validation_warnings", []),
            "tool_approval_required": bool(result.get("tool_approval_required")),
            "tool_approval_message": result.get("tool_approval_message"),
            "tool_approval_calls": result.get("tool_approval_calls", []),
            "metrics": metrics,
            "conversation_id": conversation_id,
            "session_summary": result.get("session_summary"),
            "session_state": result.get("session_state"),
            "workflow_artifacts": workflow_artifacts,
            "decision_trace": result.get("decision_trace", []),
            "action_trace": result.get("action_trace", []),
            "loop_terminal_state": result.get("loop_terminal_state"),
            "loop_stop_reason": result.get("loop_stop_reason"),
            "loop_shadow_decisions": result.get("loop_shadow_decisions", []),
        }
        await websocket.send_json(jsonable_encoder(payload))

        logger.info(
            "WebSocket request completed successfully",
            extra={
                "conversation_id": conversation_id,
                "latency_ms": metrics["total_latency_ms"],
                "llm_calls": metrics["llm_calls"],
            },
        )

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON received: {e}")
        try:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "invalid_json",
                    "message": "Invalid JSON format",
                }
            )
            await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket handler: {e}", exc_info=True)
        try:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "internal_error",
                    "message": f"An unexpected error occurred: {str(e)}",
                }
            )
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        except Exception:
            pass


@router.websocket("/ws/profiling")
async def websocket_profiling(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for profiling/generation job updates.

    Client message:
        {"job_id": "..."} or {"profile_id": "..."}

    Server message:
        {"event": "generation_update", "job": {...}}
    """
    from backend.api.main import app_state

    await websocket.accept()
    logger.info("Profiling WebSocket connection established")

    try:
        data = await websocket.receive_json()
        job_id = data.get("job_id")
        profile_id = data.get("profile_id")

        store = app_state.get("profiling_store")
        if store is None:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "service_unavailable",
                    "message": "Profiling store unavailable.",
                }
            )
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return

        if not job_id and not profile_id:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "validation_error",
                    "message": "Missing job_id or profile_id.",
                }
            )
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        if not job_id and profile_id:
            latest = await store.get_latest_generation_job(profile_id)
            if latest is None:
                await websocket.send_json(
                    {
                        "event": "error",
                        "error": "not_found",
                        "message": "No generation job found for profile.",
                    }
                )
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return
            job_id = latest.job_id

        while True:
            job = await store.get_generation_job(job_id)
            await websocket.send_json(
                {
                    "event": "generation_update",
                    "job": {
                        "job_id": str(job.job_id),
                        "profile_id": str(job.profile_id),
                        "status": job.status,
                        "progress": job.progress.model_dump()
                        if job.progress
                        else None,
                        "error": job.error,
                    },
                }
            )
            if job.status in {"completed", "failed"}:
                break
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        logger.info("Profiling WebSocket client disconnected")
    except Exception as exc:
        logger.error(f"Profiling WebSocket error: {exc}", exc_info=True)
        try:
            await websocket.send_json(
                {
                    "event": "error",
                    "error": "internal_error",
                    "message": str(exc),
                }
            )
        except Exception:
            pass
    finally:
        await websocket.close()
        logger.info("Profiling WebSocket connection closed")

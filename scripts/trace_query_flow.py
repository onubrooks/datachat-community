"""
Trace a single DataChat pipeline run with detailed decisions and LLM traffic.

Usage:
  python scripts/trace_query_flow.py \
    --query "Which stores have the largest gap between inventory movement and recorded sales?" \
    --output reports/query_trace_latest.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.pipeline.orchestrator import create_pipeline


def _truncate_text(value: Any, limit: int = 40000) -> Any:
    if isinstance(value, str):
        if len(value) <= limit:
            return value
        return value[:limit] + "\n...[truncated]..."
    return value


def _serialize_messages(messages: list[Any]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for item in messages:
        role = getattr(item, "role", "unknown")
        content = getattr(item, "content", "")
        serialized.append(
            {
                "role": str(role),
                "content": _truncate_text(str(content)),
            }
        )
    return serialized


def _compiler_plan_summary(plan: Any) -> dict[str, Any] | None:
    if plan is None:
        return None
    to_summary = getattr(plan, "to_summary", None)
    if callable(to_summary):
        try:
            summary = to_summary()
            if isinstance(summary, dict):
                return summary
        except Exception:
            return None
    return None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Trace DataChat query flow.")
    parser.add_argument("--query", required=True, help="Natural language query to trace")
    parser.add_argument(
        "--output",
        default="reports/query_trace_latest.json",
        help="Output trace file path (JSON)",
    )
    args = parser.parse_args()

    pipeline = await create_pipeline()
    llm_trace: list[dict[str, Any]] = []
    sql_trace: list[dict[str, Any]] = []

    sql_agent = pipeline.sql

    # Capture deterministic catalog planning.
    original_plan_query = sql_agent.catalog.plan_query

    def traced_plan_query(*, query: str, database_type: str | None, investigation_memory: Any) -> Any:
        started = time.perf_counter()
        plan = original_plan_query(
            query=query,
            database_type=database_type,
            investigation_memory=investigation_memory,
        )
        sql_trace.append(
            {
                "stage": "catalog_plan_query",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "query": query,
                "result": {
                    "operation": getattr(plan, "operation", None) if plan else None,
                    "confidence": getattr(plan, "confidence", None) if plan else None,
                    "has_sql": bool(getattr(plan, "sql", None)) if plan else False,
                    "clarifying_questions": (
                        list(getattr(plan, "clarifying_questions", []) or []) if plan else []
                    ),
                },
            }
        )
        return plan

    sql_agent.catalog.plan_query = traced_plan_query

    # Capture query compiler output.
    original_compile_query_plan = sql_agent._compile_query_plan

    async def traced_compile_query_plan(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        plan = await original_compile_query_plan(*args, **kwargs)
        sql_trace.append(
            {
                "stage": "query_compiler",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "query": kwargs.get("query"),
                "result": _compiler_plan_summary(plan),
            }
        )
        return plan

    sql_agent._compile_query_plan = traced_compile_query_plan

    # Capture SQL prompt build (includes compiler context).
    original_build_generation_prompt = sql_agent._build_generation_prompt

    async def traced_build_generation_prompt(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        built = await original_build_generation_prompt(*args, **kwargs)
        prompt = built
        plan = None
        if isinstance(built, tuple) and len(built) == 2:
            prompt, plan = built
        sql_trace.append(
            {
                "stage": "sql_prompt",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "return_plan": bool(kwargs.get("return_plan")),
                "prompt_chars": len(prompt) if isinstance(prompt, str) else None,
                "prompt": _truncate_text(prompt),
                "compiler_plan": _compiler_plan_summary(plan),
            }
        )
        return built

    sql_agent._build_generation_prompt = traced_build_generation_prompt

    # Capture parsed SQL results per LLM request.
    original_request_sql_from_llm = sql_agent._request_sql_from_llm

    async def traced_request_sql_from_llm(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        provider = kwargs.get("provider")
        llm_request = kwargs.get("llm_request")
        try:
            generated_sql = await original_request_sql_from_llm(*args, **kwargs)
            sql_trace.append(
                {
                    "stage": "sql_llm_result",
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                    "provider": str(getattr(provider, "provider", "unknown")),
                    "model": str(getattr(provider, "model", "unknown")),
                    "request_model_override": getattr(llm_request, "model", None),
                    "sql": getattr(generated_sql, "sql", None),
                    "confidence": getattr(generated_sql, "confidence", None),
                    "clarifying_questions": list(
                        getattr(generated_sql, "clarifying_questions", []) or []
                    ),
                }
            )
            return generated_sql
        except Exception as exc:
            sql_trace.append(
                {
                    "stage": "sql_llm_result",
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                    "provider": str(getattr(provider, "provider", "unknown")),
                    "model": str(getattr(provider, "model", "unknown")),
                    "error": str(exc),
                }
            )
            raise

    sql_agent._request_sql_from_llm = traced_request_sql_from_llm

    # Wrap all distinct providers to capture exact prompts/responses.
    provider_candidates = [
        ("intent_gate", getattr(pipeline, "intent_llm", None)),
        ("tool_planner", getattr(getattr(pipeline, "tool_planner", None), "llm", None)),
        ("classifier", getattr(getattr(pipeline, "classifier", None), "llm", None)),
        ("context_answer", getattr(getattr(pipeline, "context_answer", None), "llm", None)),
        ("sql_main", getattr(getattr(pipeline, "sql", None), "llm", None)),
        ("sql_fast", getattr(getattr(pipeline, "sql", None), "fast_llm", None)),
        ("sql_formatter", getattr(getattr(pipeline, "sql", None), "formatter_llm", None)),
        ("response_synthesis", getattr(getattr(pipeline, "response_synthesis", None), "llm", None)),
    ]

    wrapped_ids: set[int] = set()
    for label, provider in provider_candidates:
        if provider is None:
            continue
        provider_id = id(provider)
        if provider_id in wrapped_ids:
            continue
        wrapped_ids.add(provider_id)

        original_generate = provider.generate
        provider_name = str(getattr(provider, "provider", "unknown"))
        model_name = str(getattr(provider, "model", "unknown"))

        async def traced_generate(
            request: Any,
            *,
            _orig=original_generate,
            _label=label,
            _provider=provider_name,
            _model=model_name,
        ) -> Any:
            entry: dict[str, Any] = {
                "timestamp": datetime.now(UTC).isoformat(),
                "label": _label,
                "provider": _provider,
                "model": _model,
                "request": {
                    "temperature": getattr(request, "temperature", None),
                    "max_tokens": getattr(request, "max_tokens", None),
                    "model_override": getattr(request, "model", None),
                    "messages": _serialize_messages(list(getattr(request, "messages", []) or [])),
                },
            }
            try:
                response = await _orig(request)
                entry["response"] = {
                    "provider": getattr(response, "provider", None),
                    "model": getattr(response, "model", None),
                    "finish_reason": getattr(response, "finish_reason", None),
                    "content": _truncate_text(getattr(response, "content", "")),
                    "usage": {
                        "prompt_tokens": getattr(getattr(response, "usage", None), "prompt_tokens", None),
                        "completion_tokens": getattr(
                            getattr(response, "usage", None), "completion_tokens", None
                        ),
                        "total_tokens": getattr(getattr(response, "usage", None), "total_tokens", None),
                    },
                }
                llm_trace.append(entry)
                return response
            except Exception as exc:
                entry["error"] = str(exc)
                llm_trace.append(entry)
                raise

        provider.generate = traced_generate

    started_at = datetime.now(UTC).isoformat()
    result = await pipeline.run(query=args.query)
    finished_at = datetime.now(UTC).isoformat()

    trace_payload = {
        "started_at": started_at,
        "finished_at": finished_at,
        "query": args.query,
        "result_summary": {
            "answer": result.get("answer") or result.get("natural_language_answer"),
            "answer_source": result.get("answer_source"),
            "clarification_needed": bool(result.get("clarification_needed")),
            "clarifying_questions": result.get("clarifying_questions", []),
            "sql": result.get("sql") or result.get("validated_sql") or result.get("generated_sql"),
            "row_count": (result.get("query_result") or {}).get("row_count"),
            "result_columns": (result.get("query_result") or {}).get("columns"),
            "llm_calls": result.get("llm_calls"),
            "total_latency_ms": result.get("total_latency_ms"),
            "agent_timings": result.get("agent_timings", {}),
            "decision_trace": result.get("decision_trace", []),
            "sql_formatter_fallback_calls": result.get("sql_formatter_fallback_calls", 0),
            "sql_formatter_fallback_successes": result.get("sql_formatter_fallback_successes", 0),
            "query_compiler": result.get("query_compiler"),
            "query_compiler_llm_calls": result.get("query_compiler_llm_calls", 0),
            "query_compiler_llm_refinements": result.get("query_compiler_llm_refinements", 0),
            "query_compiler_latency_ms": result.get("query_compiler_latency_ms", 0.0),
        },
        "sql_internal_trace": sql_trace,
        "llm_trace": llm_trace,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(trace_payload, indent=2, default=str), encoding="utf-8")
    print(f"Wrote trace to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())

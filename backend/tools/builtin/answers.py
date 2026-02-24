"""Built-in answer tools powered by existing agents."""

from __future__ import annotations

from typing import Any

from backend.agents.context import ContextAgent
from backend.agents.context_answer import ContextAnswerAgent
from backend.agents.executor import ExecutorAgent
from backend.agents.sql import SQLAgent
from backend.agents.validator import ValidatorAgent
from backend.models import (
    ContextAgentInput,
    ContextAnswerAgentInput,
    ExecutorAgentInput,
    SQLAgentInput,
    ValidatorAgentInput,
)
from backend.tools.base import ToolCategory, ToolContext, tool


def _get_retriever(ctx: ToolContext):
    retriever = ctx.metadata.get("retriever") if ctx else None
    if not retriever:
        raise ValueError("Retriever is required for this tool.")
    return retriever


def _normalize_validator_database(database_type: str | None) -> str:
    value = (database_type or "").lower()
    if value in {"postgres", "postgresql"}:
        return "postgresql"
    if value in {"clickhouse", "mysql"}:
        return value
    return "generic"


@tool(
    name="context_answer",
    description="Answer using DataPoints only (no SQL).",
    category=ToolCategory.KNOWLEDGE,
)
async def context_answer(query: str, ctx: ToolContext | None = None) -> dict[str, Any]:
    retriever = _get_retriever(ctx)
    context_agent = ContextAgent(retriever=retriever)
    answer_agent = ContextAnswerAgent()

    context_output = await context_agent.execute(
        ContextAgentInput(query=query, max_datapoints=10)
    )
    answer_output = await answer_agent.execute(
        ContextAnswerAgentInput(
            query=query, investigation_memory=context_output.investigation_memory
        )
    )

    context_answer = answer_output.context_answer
    return {
        "answer": context_answer.answer,
        "confidence": context_answer.confidence,
        "needs_sql": context_answer.needs_sql,
        "evidence": [item.model_dump() for item in context_answer.evidence],
        "retrieved_datapoints": [
            dp.model_dump() for dp in context_output.investigation_memory.datapoints
        ],
        "answer_source": "context",
    }


@tool(
    name="run_sql",
    description="Generate and execute SQL for the user's query.",
    category=ToolCategory.DATABASE,
)
async def run_sql(query: str, ctx: ToolContext | None = None) -> dict[str, Any]:
    retriever = _get_retriever(ctx)
    context_agent = ContextAgent(retriever=retriever)
    sql_agent = SQLAgent()
    validator = ValidatorAgent()
    executor = ExecutorAgent()

    context_output = await context_agent.execute(
        ContextAgentInput(query=query, max_datapoints=10)
    )

    sql_output = await sql_agent.execute(
        SQLAgentInput(
            query=query,
            investigation_memory=context_output.investigation_memory,
            database_type=(ctx.metadata.get("database_type") if ctx else None) or "postgresql",
            database_url=ctx.metadata.get("database_url") if ctx else None,
        )
    )
    database_type = ctx.metadata.get("database_type") if ctx else None
    database_url = ctx.metadata.get("database_url") if ctx else None
    if not database_type:
        database_type = "postgresql"

    validated = await validator.execute(
        ValidatorAgentInput(
            generated_sql=sql_output.generated_sql,
            target_database=_normalize_validator_database(database_type),
        )
    )
    exec_output = await executor.execute(
        ExecutorAgentInput(
            validated_sql=validated.validated_sql,
            database_type=database_type,
            database_url=database_url,
            source_datapoints=sql_output.generated_sql.used_datapoints,
        )
    )

    return {
        "answer": exec_output.executed_query.natural_language_answer,
        "sql": validated.validated_sql.sql,
        "data": exec_output.executed_query.query_result.model_dump(),
        "visualization_hint": exec_output.executed_query.visualization_hint,
        "validation_warnings": validated.validation_warnings,
        "validation_errors": validated.validation_errors,
        "retrieved_datapoints": [
            dp.model_dump() for dp in context_output.investigation_memory.datapoints
        ],
        "used_datapoints": sql_output.generated_sql.used_datapoints,
        "answer_source": "sql",
        "confidence": sql_output.generated_sql.confidence,
    }

"""
ContextAnswerAgent

Synthesizes answers directly from DataPoints without SQL execution.
Uses LLM to compose a grounded answer and evidence list.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any

from backend.agents.base import BaseAgent
from backend.config import get_settings
from backend.database.catalog import CatalogIntelligence
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models.agent import (
    ContextAnswer,
    ContextAnswerAgentInput,
    ContextAnswerAgentOutput,
    EvidenceItem,
    LLMError,
)
from backend.prompts.loader import PromptLoader

logger = logging.getLogger(__name__)


class ContextAnswerAgent(BaseAgent):
    """Generate a context-only answer from DataPoints."""

    LOW_CONFIDENCE_THRESHOLD = 0.55

    def __init__(self, llm_provider=None):
        super().__init__(name="ContextAnswerAgent")

        self.config = get_settings()
        if llm_provider is None:
            self.llm = LLMProviderFactory.create_default_provider(
                self.config.llm, model_type="mini"
            )
        else:
            self.llm = llm_provider
        self.prompts = PromptLoader()
        self.catalog = CatalogIntelligence()

    async def execute(self, input: ContextAnswerAgentInput) -> ContextAnswerAgentOutput:
        logger.info(f"[{self.name}] Generating context-only answer")

        try:
            deterministic = self.catalog.build_context_response(
                query=input.query,
                investigation_memory=input.investigation_memory,
            )
            if deterministic:
                context_answer = ContextAnswer(
                    answer=deterministic.answer,
                    confidence=deterministic.confidence,
                    evidence=[
                        EvidenceItem(datapoint_id=item)
                        for item in deterministic.evidence_datapoint_ids[:3]
                    ],
                    needs_sql=deterministic.needs_sql,
                    clarifying_questions=deterministic.clarifying_questions,
                )
                metadata = self._create_metadata()
                metadata.llm_calls = 0
                return ContextAnswerAgentOutput(
                    success=True,
                    context_answer=context_answer,
                    metadata=metadata,
                    next_agent=None,
                )

            datapoint_count = self._count_managed_datapoints(input.query)
            if datapoint_count is not None:
                context_answer = ContextAnswer(
                    answer=(
                        f"I have {datapoint_count} DataPoints loaded for this workspace."
                    ),
                    confidence=0.9,
                    evidence=[],
                    needs_sql=False,
                    clarifying_questions=[],
                )
                metadata = self._create_metadata()
                metadata.llm_calls = 0
                return ContextAnswerAgentOutput(
                    success=True,
                    context_answer=context_answer,
                    metadata=metadata,
                    next_agent=None,
                )

            context_summary = self._build_context_summary(input.investigation_memory)
            prompt = self.prompts.render(
                "agents/context_answer.md",
                user_query=input.query,
                context_summary=context_summary,
            )

            request = LLMRequest(
                messages=[
                    LLMMessage(role="system", content=self.prompts.load("system/main.md")),
                    LLMMessage(role="user", content=prompt),
                ],
                temperature=0.2,
                max_tokens=1200,
            )
            response = await self.llm.generate(request)

            context_answer = self._parse_response(response.content, input)
            if self._requires_sql(input.query):
                context_answer.needs_sql = True
            context_answer = self._maybe_gate_low_confidence_semantic_answer(
                context_answer,
                input,
            )

            metadata = self._create_metadata()
            metadata.llm_calls = 1

            return ContextAnswerAgentOutput(
                success=True,
                context_answer=context_answer,
                metadata=metadata,
                next_agent=None,
            )

        except Exception as exc:
            logger.error(f"[{self.name}] Failed to generate context answer: {exc}")
            raise LLMError(self.name, f"Context answer generation failed: {exc}") from exc

    def _build_context_summary(self, memory) -> str:
        lines = []
        for dp in memory.datapoints[:20]:
            metadata = dp.metadata if isinstance(dp.metadata, dict) else {}
            lines.append(
                f"- {dp.datapoint_type} | {dp.name} | id={dp.datapoint_id} | score={dp.score:.2f}"
            )
            if dp.datapoint_type == "Schema":
                table = metadata.get("table_name")
                schema = metadata.get("schema") or metadata.get("schema_name")
                full_name = f"{schema}.{table}" if schema and table else table
                if full_name:
                    lines.append(f"  Table: {full_name}")
                purpose = metadata.get("business_purpose")
                if purpose:
                    lines.append(f"  Purpose: {purpose}")
                columns = self._coerce_metadata_list(metadata.get("key_columns") or [])
                if columns:
                    col_names = []
                    for col in columns[:8]:
                        if isinstance(col, dict):
                            col_names.append(col.get("name", "unknown"))
                        elif isinstance(col, str):
                            col_names.append(col)
                    lines.append(f"  Columns: {', '.join(col_names)}")
            elif dp.datapoint_type == "Business":
                calculation = metadata.get("calculation")
                if calculation:
                    lines.append(f"  Calculation: {calculation}")
                synonyms = self._coerce_string_list(metadata.get("synonyms") or [])
                if synonyms:
                    lines.append(f"  Synonyms: {', '.join(synonyms[:5])}")
            elif dp.datapoint_type == "Query":
                description = metadata.get("query_description") or metadata.get("description")
                if description:
                    lines.append(f"  Query Pattern: {description}")
                related_tables = self._coerce_string_list(metadata.get("related_tables") or [])
                if related_tables:
                    lines.append(f"  Related Tables: {', '.join(related_tables[:5])}")
                parameter_names = self._coerce_parameter_names(metadata.get("parameters"))
                if parameter_names:
                    lines.append(f"  Parameters: {', '.join(parameter_names[:8])}")
            elif dp.datapoint_type == "Process":
                schedule = metadata.get("schedule")
                if schedule:
                    lines.append(f"  Schedule: {schedule}")
                target_tables = self._coerce_string_list(metadata.get("target_tables") or [])
                if target_tables:
                    lines.append(f"  Target Tables: {', '.join(target_tables[:5])}")
                dependencies = self._coerce_string_list(metadata.get("dependencies") or [])
                if dependencies:
                    lines.append(f"  Dependencies: {', '.join(dependencies[:5])}")

        return "\n".join(lines) if lines else "No DataPoints available."

    def _coerce_metadata_list(self, value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, list):
                        return parsed
                except json.JSONDecodeError:
                    return []
        return []

    def _coerce_string_list(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item) for item in value if str(item).strip()]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, list):
                        return [str(item) for item in parsed if str(item).strip()]
                except json.JSONDecodeError:
                    pass
            if "," in stripped:
                return [part.strip() for part in stripped.split(",") if part.strip()]
            return [stripped]
        return []

    def _coerce_parameter_names(self, value: Any) -> list[str]:
        if isinstance(value, dict):
            return [str(name) for name in value.keys() if str(name).strip()]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    return []
                if isinstance(parsed, dict):
                    return [str(name) for name in parsed.keys() if str(name).strip()]
            return []
        return []

    def _parse_response(
        self, content: str, input: ContextAnswerAgentInput
    ) -> ContextAnswer:
        payload = self._extract_json(content)
        if payload:
            return ContextAnswer(
                answer=str(payload.get("answer", "")).strip() or "No answer available.",
                confidence=float(payload.get("confidence", 0.5)),
                evidence=self._parse_evidence(payload.get("evidence", [])),
                needs_sql=bool(payload.get("needs_sql", False)),
                clarifying_questions=[
                    str(item) for item in payload.get("clarifying_questions", [])
                ],
            )

        return self._fallback_answer(input)

    def _extract_json(self, content: str) -> dict[str, Any] | None:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}") + 1
            if start == -1 or end <= start:
                return None
            try:
                return json.loads(content[start:end])
            except json.JSONDecodeError:
                return None

    def _parse_evidence(self, evidence: list[dict[str, Any]]) -> list[EvidenceItem]:
        items = []
        for item in evidence:
            if not isinstance(item, dict):
                continue
            items.append(
                EvidenceItem(
                    datapoint_id=str(item.get("datapoint_id", "unknown")),
                    name=item.get("name"),
                    type=item.get("type"),
                    reason=item.get("reason"),
                )
            )
            if len(items) >= 3:
                break
        return items

    def _fallback_answer(self, input: ContextAnswerAgentInput) -> ContextAnswer:
        datapoints = input.investigation_memory.datapoints
        if not datapoints:
            return ContextAnswer(
                answer="I don't have enough context to answer that yet.",
                confidence=0.1,
                evidence=[],
                needs_sql=self._requires_sql(input.query),
                clarifying_questions=["Which table or metric should I use?"],
            )

        top = datapoints[0]
        evidence = [
            EvidenceItem(
                datapoint_id=top.datapoint_id,
                name=top.name,
                type=top.datapoint_type,
                reason="Top retrieved DataPoint",
            )
        ]
        needs_sql = self._requires_sql(input.query)
        summary = self._summarize_datapoint(top)
        answer = summary
        return ContextAnswer(
            answer=answer,
            confidence=0.4,
            evidence=evidence,
            needs_sql=needs_sql,
            clarifying_questions=[],
        )

    def _maybe_gate_low_confidence_semantic_answer(
        self,
        context_answer: ContextAnswer,
        input: ContextAnswerAgentInput,
    ) -> ContextAnswer:
        """Ask for clarification instead of returning low-confidence semantic answers."""
        if context_answer.needs_sql:
            return context_answer
        if context_answer.clarifying_questions:
            return context_answer
        if not self._is_semantic_query(input.query, input.intent):
            return context_answer

        confidence_signal = max(
            context_answer.confidence,
            float(input.context_confidence or 0.0),
        )
        if confidence_signal >= self.LOW_CONFIDENCE_THRESHOLD:
            return context_answer

        candidate_tables = self._candidate_tables(input.investigation_memory)
        question = self._build_targeted_clarification_question(
            input.query,
            candidate_tables,
        )
        answer_intro = "I am not confident enough to answer that from current context."
        return ContextAnswer(
            answer=answer_intro,
            confidence=0.2,
            evidence=context_answer.evidence[:2],
            needs_sql=False,
            clarifying_questions=[question],
        )

    def _summarize_datapoint(self, datapoint) -> str:
        if datapoint.datapoint_type != "Schema":
            return f"{datapoint.name} looks most relevant to your question."

        metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
        table = metadata.get("table_name")
        schema = metadata.get("schema") or metadata.get("schema_name")
        full_name = f"{schema}.{table}" if schema and table else table
        purpose = metadata.get("business_purpose")
        columns = self._coerce_metadata_list(metadata.get("key_columns") or [])
        column_names = []
        for col in columns[:5]:
            if isinstance(col, dict):
                column_names.append(col.get("name", "unknown"))
            elif isinstance(col, str):
                column_names.append(col)

        parts = []
        if full_name:
            parts.append(f"The {full_name} table is relevant.")
        else:
            parts.append(f"{datapoint.name} is a relevant table.")
        if purpose:
            parts.append(purpose)
        if column_names:
            parts.append(f"Key columns include {', '.join(column_names)}.")
        return " ".join(parts)

    def _requires_sql(self, query: str) -> bool:
        query_lower = query.lower()
        keywords = (
            "how many",
            "count",
            "total",
            "sum",
            "average",
            "avg",
            "min",
            "max",
            "row count",
            "rows",
            "number of",
        )
        return any(keyword in query_lower for keyword in keywords)

    def _is_semantic_query(self, query: str, intent: str | None) -> bool:
        query_lower = query.lower().strip()
        if not query_lower:
            return False
        if intent in {"exploration", "explanation", "meta"}:
            return True
        if self._requires_sql(query_lower):
            return False
        semantic_keywords = (
            "revenue",
            "sales",
            "growth",
            "trend",
            "churn",
            "retention",
            "conversion",
            "performance",
            "quality",
            "insight",
            "why",
            "explain",
        )
        return any(token in query_lower for token in semantic_keywords)

    def _candidate_tables(self, memory) -> list[str]:
        tables: list[str] = []
        for datapoint in memory.datapoints:
            if datapoint.datapoint_type != "Schema":
                continue
            metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
            table_name = metadata.get("table_name") or metadata.get("table")
            if table_name:
                tables.append(str(table_name))
        return list(dict.fromkeys(tables))

    def _build_targeted_clarification_question(
        self, query: str, candidate_tables: list[str]
    ) -> str:
        focus = self._extract_focus_hint(query)
        if candidate_tables:
            shortlist = ", ".join(candidate_tables[:5])
            if focus:
                return f"Which table should I use for {focus}? Options: {shortlist}."
            return f"Which table should I use? Options: {shortlist}."
        if focus:
            return f"Which table contains {focus}?"
        return "Which table should I use to answer this?"

    def _extract_focus_hint(self, query: str) -> str | None:
        query_lower = query.lower()
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
            if token in query_lower:
                return token

        cleaned = re.sub(r"[^a-z0-9\s]+", " ", query_lower)
        words = [
            word
            for word in cleaned.split()
            if word
            and word
            not in {
                "what",
                "which",
                "how",
                "is",
                "are",
                "the",
                "a",
                "an",
                "of",
                "for",
                "in",
                "to",
                "me",
                "show",
                "list",
                "tell",
                "about",
            }
        ]
        if not words:
            return None
        return " ".join(words[:3])

    def _count_managed_datapoints(self, query: str) -> int | None:
        query_lower = query.lower()
        if "datapoint" not in query_lower and "data point" not in query_lower:
            return None
        if not self._requires_sql(query_lower):
            return None

        managed_dir = Path("datapoints") / "managed"
        if not managed_dir.exists():
            return 0
        return sum(1 for path in managed_dir.rglob("*.json") if path.is_file())

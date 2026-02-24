"""
QueryCompilerAgent

Pre-compiles query plans before SQL generation. This extracts table selection,
operator detection, and column hints from the SQL generation step, making
the process more measurable and debuggable.

This is extracted from SQLAgent as a separate pipeline step.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from backend.agents.base import BaseAgent
from backend.config import get_settings
from backend.database.catalog import CatalogIntelligence
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models.agent import AgentMetadata
from backend.prompts.loader import PromptLoader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueryCompilerPlan:
    """Compiled semantic query plan used to prime SQL generation."""

    query: str
    operators: list[str] = field(default_factory=list)
    candidate_tables: list[str] = field(default_factory=list)
    selected_tables: list[str] = field(default_factory=list)
    join_hypotheses: list[str] = field(default_factory=list)
    column_hints: list[str] = field(default_factory=list)
    confidence: float = 0.0
    path: str = "none"
    reason: str = ""

    def to_summary(self) -> dict[str, Any]:
        return {
            "operators": self.operators,
            "candidate_tables": self.candidate_tables,
            "selected_tables": self.selected_tables,
            "join_hypotheses": self.join_hypotheses,
            "column_hints": self.column_hints,
            "confidence": round(self.confidence, 3),
            "path": self.path,
            "reason": self.reason,
        }


class QueryCompilerInput:
    """Input for QueryCompilerAgent."""

    def __init__(
        self,
        query: str,
        investigation_memory: Any | None = None,
        entity_hints: list[dict] | None = None,
        schema_context: str | None = None,
        database_type: str = "postgresql",
        database_url: str | None = None,
    ):
        self.query = query
        self.investigation_memory = investigation_memory
        self.entity_hints = entity_hints or []
        self.schema_context = schema_context
        self.database_type = database_type
        self.database_url = database_url


class QueryCompilerOutput:
    """Output from QueryCompilerAgent."""

    def __init__(
        self,
        success: bool,
        plan: QueryCompilerPlan | None = None,
        metadata: AgentMetadata | None = None,
        error: str | None = None,
    ):
        self.success = success
        self.plan = plan
        self.metadata = metadata or AgentMetadata(agent_name="QueryCompilerAgent")
        self.error = error


class QueryCompilerAgent(BaseAgent):
    """
    Pre-compiles query plans before SQL generation.

    Responsibilities:
    - Detect analytic operators (total, count, average, etc.)
    - Resolve candidate tables from schema and DataPoints
    - Select most likely tables for the query
    - Generate column hints
    - Estimate confidence in the plan

    This allows SQL generation to focus on syntax, not table discovery.
    """

    # Analytic operator templates for deterministic matching
    OPERATOR_TEMPLATES = {
        "total": ["total", "sum of", "overall"],
        "count": ["count", "number of", "how many"],
        "average": ["average", "avg", "mean"],
        "min": ["minimum", "min", "lowest"],
        "max": ["maximum", "max", "highest"],
        "rate": ["rate", "ratio", "percentage", "pct"],
        "trend": ["trend", "over time", "change"],
        "list": ["list", "show", "display"],
        "compare": ["compare", "versus", "vs", "difference"],
    }

    def __init__(self, llm_provider=None):
        super().__init__(name="QueryCompilerAgent")

        self.config = get_settings()
        if llm_provider is None:
            self.llm = LLMProviderFactory.create_default_provider(
                self.config.llm, model_type="mini"
            )
        else:
            self.llm = llm_provider

        self.prompts = PromptLoader()
        self.catalog = CatalogIntelligence()

    async def execute(self, input: QueryCompilerInput) -> QueryCompilerOutput:
        """
        Compile a query plan from the input query and context.

        Strategy:
        1. Detect operators deterministically
        2. Resolve tables from investigation memory
        3. If confident, return plan without LLM
        4. Otherwise, use LLM to refine table selection
        """
        start_time = time.time()
        metadata = AgentMetadata(agent_name=self.name)

        logger.info(f"[{self.name}] Compiling query: {input.query[:100]}...")

        try:
            query = input.query.strip()
            query_lower = query.lower()

            # Step 1: Detect operators deterministically
            operators = self._detect_operators(query_lower)

            # Step 2: Resolve candidate tables
            candidate_tables = self._resolve_candidate_tables(
                query=query,
                investigation_memory=input.investigation_memory,
                entity_hints=input.entity_hints,
            )

            # Step 3: Select tables
            selected_tables, confidence, path, reason = self._select_tables(
                query=query,
                candidate_tables=candidate_tables,
                operators=operators,
            )

            # Step 4: Generate column hints
            column_hints = self._generate_column_hints(
                query=query_lower,
                operators=operators,
            )

            # Step 5: If low confidence and LLM enabled, refine with LLM
            llm_threshold = self._get_confidence_threshold()
            use_llm = (
                confidence < llm_threshold and self._is_llm_enabled() and len(candidate_tables) > 1
            )

            if use_llm:
                llm_result = await self._llm_refine_plan(
                    query=query,
                    candidate_tables=candidate_tables,
                    operators=operators,
                    schema_context=input.schema_context,
                )
                if llm_result:
                    selected_tables = llm_result.get("tables", selected_tables)
                    confidence = max(confidence, llm_result.get("confidence", confidence))
                    path = "llm_refined"
                    reason = llm_result.get("reason", reason)
                    metadata.llm_calls = 1

            plan = QueryCompilerPlan(
                query=query,
                operators=operators,
                candidate_tables=candidate_tables,
                selected_tables=selected_tables,
                join_hypotheses=[],
                column_hints=column_hints,
                confidence=confidence,
                path=path,
                reason=reason,
            )

            metadata.duration_ms = (time.time() - start_time) * 1000

            logger.info(
                f"[{self.name}] Plan compiled: tables={selected_tables}, "
                f"operators={operators}, confidence={confidence:.2f}, path={path}"
            )

            return QueryCompilerOutput(
                success=True,
                plan=plan,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"[{self.name}] Compilation failed: {e}")
            return QueryCompilerOutput(
                success=False,
                error=str(e),
                metadata=metadata,
            )

    def _detect_operators(self, query_lower: str) -> list[str]:
        """Detect analytic operators from query text."""
        detected = []
        for operator, keywords in self.OPERATOR_TEMPLATES.items():
            for keyword in keywords:
                if keyword in query_lower:
                    detected.append(operator)
                    break
        return list(set(detected))

    def _resolve_candidate_tables(
        self,
        query: str,
        investigation_memory: Any | None,
        entity_hints: list[dict] | None,
    ) -> list[str]:
        """Resolve candidate tables from investigation memory and entity hints."""
        candidates = set()

        # From entity hints
        if entity_hints:
            for entity in entity_hints:
                if isinstance(entity, dict) and entity.get("entity_type") == "table":
                    value = entity.get("value")
                    if value:
                        candidates.add(value)

        # From investigation memory
        if investigation_memory:
            for dp in getattr(investigation_memory, "datapoints", []) or []:
                if not isinstance(dp, dict):
                    dp = vars(dp) if hasattr(dp, "__dict__") else {}
                dp_type = dp.get("datapoint_type", "")
                if dp_type == "Schema":
                    metadata = dp.get("metadata") or {}
                    table_name = metadata.get("table_name") or metadata.get("table")
                    if table_name:
                        candidates.add(table_name)

        return list(candidates)

    def _select_tables(
        self,
        query: str,
        candidate_tables: list[str],
        operators: list[str],
    ) -> tuple[list[str], float, str, str]:
        """Select the most likely tables for the query."""
        if not candidate_tables:
            return [], 0.0, "none", "no_candidates"

        if len(candidate_tables) == 1:
            return candidate_tables, 0.95, "deterministic", "single_candidate"

        query_lower = query.lower()
        scored = []
        for table in candidate_tables:
            score = 0.0
            table_lower = table.lower().replace("_", " ")

            if table_lower in query_lower:
                score += 0.5

            for part in table_lower.split():
                if part in query_lower and len(part) > 2:
                    score += 0.2

            scored.append((table, score))

        scored.sort(key=lambda x: x[1], reverse=True)

        if scored[0][1] >= 0.3:
            return [scored[0][0]], 0.8, "deterministic", "matched_in_query"

        return [scored[0][0]], 0.5, "fallback", "first_candidate"

    def _generate_column_hints(
        self,
        query: str,
        operators: list[str],
    ) -> list[str]:
        """Generate column hints based on operators."""
        hints = []

        if "total" in operators or "sum" in operators:
            hints.append("amount")
            hints.append("value")
            hints.append("total")
        if "count" in operators:
            hints.append("id")
            hints.append("count")
        if "average" in operators:
            hints.append("average")
            hints.append("mean")
        if "trend" in operators:
            hints.append("date")
            hints.append("time")
            hints.append("period")

        return list(set(hints))

    def _get_confidence_threshold(self) -> float:
        pipeline_cfg = getattr(self.config, "pipeline", None)
        if pipeline_cfg is None:
            return 0.72
        return float(getattr(pipeline_cfg, "query_compiler_confidence_threshold", 0.72))

    def _is_llm_enabled(self) -> bool:
        pipeline_cfg = getattr(self.config, "pipeline", None)
        if pipeline_cfg is None:
            return True
        return bool(getattr(pipeline_cfg, "query_compiler_llm_enabled", True))

    async def _llm_refine_plan(
        self,
        query: str,
        candidate_tables: list[str],
        operators: list[str],
        schema_context: str | None,
    ) -> dict | None:
        """Use LLM to refine table selection."""
        if not candidate_tables:
            return None

        prompt = f"""Given this query and candidate tables, select the most relevant table(s).

Query: {query}

Candidate Tables: {", ".join(candidate_tables[:10])}

Detected Operators: {", ".join(operators) if operators else "None"}

Return JSON with:
- tables: list of 1-2 most relevant table names
- confidence: float 0-1
- reason: brief explanation

JSON only:"""

        try:
            import json

            request = LLMRequest(
                messages=[LLMMessage(role="user", content=prompt)],
                temperature=0.1,
                max_tokens=200,
            )
            response = await self.llm.generate(request)
            self._track_llm_call()

            content = response.content.strip()
            start = content.find("{")
            end = content.rfind("}") + 1
            if start == -1 or end == 0:
                return None

            data = json.loads(content[start:end])
            return {
                "tables": data.get("tables", []),
                "confidence": data.get("confidence", 0.5),
                "reason": data.get("reason", "llm_refinement"),
            }
        except Exception as e:
            logger.warning(f"LLM refinement failed: {e}")
            return None

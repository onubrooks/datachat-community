"""
QueryAnalyzerAgent

Replaces both the intent_gate and ClassifierAgent with a single unified agent
that produces comprehensive routing decisions.

This agent classifies:
- Intent: data_query, definition, exploration, meta, exit, out_of_scope, etc.
- Route: sql, context, clarification, tool, end
- Entities: tables, columns, metrics, time_references
- SQL hints: suggested_tables, suggested_columns, suggested_operators
"""

import json
import logging
import time
from typing import Any, Literal

from backend.agents.base import BaseAgent
from backend.config import get_settings
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models.agent import (
    AgentMetadata,
    ExtractedEntity,
)
from backend.prompts.loader import PromptLoader
from backend.utils.pattern_matcher import QueryPatternMatcher, QueryPatternType

logger = logging.getLogger(__name__)


RouteType = Literal["sql", "context", "clarification", "tool", "end"]
IntentType = Literal[
    "data_query",
    "definition",
    "exploration",
    "meta",
    "exit",
    "out_of_scope",
    "small_talk",
    "setup_help",
    "datapoint_help",
]
ComplexityType = Literal["simple", "medium", "complex"]


class QueryAnalysis:
    """
    Comprehensive query analysis result.

    This is the single source of truth for routing decisions.
    """

    def __init__(
        self,
        intent: IntentType,
        route: RouteType,
        entities: list[ExtractedEntity] | None = None,
        complexity: ComplexityType = "simple",
        confidence: float = 1.0,
        clarifying_questions: list[str] | None = None,
        suggested_tables: list[str] | None = None,
        suggested_columns: list[str] | None = None,
        suggested_operators: list[str] | None = None,
        deterministic: bool = False,
        pattern_type: str | None = None,
        extracted_table: str | None = None,
        extracted_limit: int | None = None,
    ):
        self.intent = intent
        self.route = route
        self.entities = entities or []
        self.complexity = complexity
        self.confidence = confidence
        self.clarifying_questions = clarifying_questions or []
        self.suggested_tables = suggested_tables or []
        self.suggested_columns = suggested_columns or []
        self.suggested_operators = suggested_operators or []
        self.deterministic = deterministic
        self.pattern_type = pattern_type
        self.extracted_table = extracted_table
        self.extracted_limit = extracted_limit

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "route": self.route,
            "entities": [
                {
                    "entity_type": e.entity_type,
                    "value": e.value,
                    "confidence": e.confidence,
                    "normalized_value": e.normalized_value,
                }
                for e in self.entities
            ],
            "complexity": self.complexity,
            "confidence": self.confidence,
            "clarifying_questions": self.clarifying_questions,
            "suggested_tables": self.suggested_tables,
            "suggested_columns": self.suggested_columns,
            "suggested_operators": self.suggested_operators,
            "deterministic": self.deterministic,
            "pattern_type": self.pattern_type,
            "extracted_table": self.extracted_table,
            "extracted_limit": self.extracted_limit,
        }


class QueryAnalyzerInput:
    """Input for QueryAnalyzerAgent."""

    def __init__(
        self,
        query: str,
        conversation_history: list[Any] | None = None,
        session_state: dict[str, Any] | None = None,
    ):
        self.query = query
        self.conversation_history = conversation_history or []
        self.session_state = session_state or {}


class QueryAnalyzerOutput:
    """Output from QueryAnalyzerAgent."""

    def __init__(
        self,
        success: bool,
        analysis: QueryAnalysis | None = None,
        metadata: AgentMetadata | None = None,
        error: str | None = None,
    ):
        self.success = success
        self.analysis = analysis
        self.metadata = metadata or AgentMetadata(agent_name="QueryAnalyzerAgent")
        self.error = error


class QueryAnalyzerAgent(BaseAgent):
    """
    Unified query analysis agent.

    Replaces:
    - intent_gate (rules-based classification)
    - ClassifierAgent (LLM-based classification)

    Produces a single QueryAnalysis that drives all routing decisions.
    """

    def __init__(self, llm_provider=None):
        super().__init__(name="QueryAnalyzerAgent")

        self.config = get_settings()
        if llm_provider is None:
            self.llm = LLMProviderFactory.create_default_provider(
                self.config.llm, model_type="mini"
            )
        else:
            self.llm = llm_provider

        self.prompts = PromptLoader()
        self.pattern_matcher = QueryPatternMatcher()
        self._build_routing_config()

    def _build_routing_config(self) -> None:
        pipeline_cfg = getattr(self.config, "pipeline", None)
        self.intent_llm_confidence_threshold = float(
            getattr(pipeline_cfg, "intent_llm_confidence_threshold", 0.45)
        )
        self.ambiguous_query_max_tokens = int(
            getattr(pipeline_cfg, "ambiguous_query_max_tokens", 3)
        )
        self.deep_classify_min_length = int(
            getattr(pipeline_cfg, "classifier_deep_min_query_length", 28)
        )

    async def execute(self, input: QueryAnalyzerInput) -> QueryAnalyzerOutput:
        """
        Analyze query and produce routing decision.

        Strategy:
        1. Check deterministic patterns first (no LLM needed)
        2. Check exit/out_of_scope/small_talk patterns
        3. If ambiguous or complex, call LLM for analysis
        4. Return unified QueryAnalysis
        """
        start_time = time.time()
        metadata = AgentMetadata(agent_name=self.name)

        logger.info(f"[{self.name}] Analyzing query: {input.query[:100]}...")

        try:
            query = input.query.strip()

            primary_pattern = self.pattern_matcher.get_primary_pattern(query)

            if primary_pattern:
                analysis = self._handle_pattern_route(
                    query=query,
                    pattern=primary_pattern,
                    input=input,
                )
                if analysis:
                    metadata.llm_calls = 0
                    metadata.duration_ms = (time.time() - start_time) * 1000
                    return QueryAnalyzerOutput(
                        success=True,
                        analysis=analysis,
                        metadata=metadata,
                    )

            if self.pattern_matcher.is_deterministic(query):
                analysis = self._build_deterministic_analysis(query)
                metadata.llm_calls = 0
                metadata.duration_ms = (time.time() - start_time) * 1000
                return QueryAnalyzerOutput(
                    success=True,
                    analysis=analysis,
                    metadata=metadata,
                )

            if self.pattern_matcher.is_non_actionable(query):
                analysis = QueryAnalysis(
                    intent="meta",
                    route="clarification",
                    confidence=0.5,
                    clarifying_questions=["What would you like to do with your data?"],
                )
                metadata.llm_calls = 0
                metadata.duration_ms = (time.time() - start_time) * 1000
                return QueryAnalyzerOutput(
                    success=True,
                    analysis=analysis,
                    metadata=metadata,
                )

            if self._is_clarification_followup(query, input.conversation_history):
                analysis = self._build_followup_analysis(query, input)
                metadata.llm_calls = 0
                metadata.duration_ms = (time.time() - start_time) * 1000
                return QueryAnalyzerOutput(
                    success=True,
                    analysis=analysis,
                    metadata=metadata,
                )

            analysis = await self._llm_analyze(query, input)

            if self._should_deep_analyze(analysis, query):
                deep_analysis = await self._deep_analyze(query, input)
                if deep_analysis:
                    analysis = deep_analysis
                    metadata.llm_calls = 2
                else:
                    metadata.llm_calls = 1
            else:
                metadata.llm_calls = 1

            metadata.duration_ms = (time.time() - start_time) * 1000

            logger.info(
                f"[{self.name}] Analysis complete: intent={analysis.intent}, "
                f"route={analysis.route}, confidence={analysis.confidence:.2f}"
            )

            return QueryAnalyzerOutput(
                success=True,
                analysis=analysis,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"[{self.name}] Analysis failed: {e}")
            return QueryAnalyzerOutput(
                success=False,
                error=str(e),
                metadata=metadata,
            )

    def _handle_pattern_route(
        self,
        query: str,
        pattern,
        input: QueryAnalyzerInput,
    ) -> QueryAnalysis | None:
        """Convert pattern match to QueryAnalysis."""
        pattern_type = pattern.pattern_type

        if pattern_type == QueryPatternType.EXIT_INTENT:
            return QueryAnalysis(
                intent="exit",
                route="end",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.OUT_OF_SCOPE:
            return QueryAnalysis(
                intent="out_of_scope",
                route="end",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.SMALL_TALK:
            return QueryAnalysis(
                intent="small_talk",
                route="end",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.SETUP_HELP:
            return QueryAnalysis(
                intent="setup_help",
                route="end",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.DATAPPOINT_HELP:
            return QueryAnalysis(
                intent="datapoint_help",
                route="end",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.TABLE_LIST:
            return QueryAnalysis(
                intent="data_query",
                route="sql",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.COLUMN_LIST:
            table_name = pattern.extracted.get("table_name")
            return QueryAnalysis(
                intent="exploration",
                route="sql",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
                extracted_table=table_name,
                entities=[
                    ExtractedEntity(
                        entity_type="table",
                        value=table_name,
                        confidence=1.0,
                    )
                ]
                if table_name
                else [],
            )

        if pattern_type == QueryPatternType.ROW_COUNT:
            table_name = pattern.extracted.get("table_name")
            return QueryAnalysis(
                intent="data_query",
                route="sql",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
                extracted_table=table_name,
                suggested_operators=["count"],
                entities=[
                    ExtractedEntity(
                        entity_type="table",
                        value=table_name,
                        confidence=1.0,
                    )
                ]
                if table_name
                else [],
            )

        if pattern_type == QueryPatternType.SAMPLE_ROWS:
            table_name = pattern.extracted.get("table_name")
            limit = pattern.extracted.get("limit", 5)
            return QueryAnalysis(
                intent="data_query",
                route="sql",
                confidence=1.0,
                deterministic=True,
                pattern_type=pattern_type.value,
                extracted_table=table_name,
                extracted_limit=limit,
                entities=[
                    ExtractedEntity(
                        entity_type="table",
                        value=table_name,
                        confidence=1.0,
                    )
                ]
                if table_name
                else [],
            )

        if pattern_type == QueryPatternType.DEFINITION:
            return QueryAnalysis(
                intent="definition",
                route="context",
                confidence=0.9,
                deterministic=True,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.AGGREGATION:
            return QueryAnalysis(
                intent="data_query",
                route="sql",
                confidence=0.8,
                deterministic=False,
                pattern_type=pattern_type.value,
            )

        if pattern_type == QueryPatternType.ANY_TABLE:
            return QueryAnalysis(
                intent="data_query",
                route="sql",
                confidence=0.7,
                deterministic=False,
                pattern_type=pattern_type.value,
            )

        return None

    def _build_deterministic_analysis(self, query: str) -> QueryAnalysis:
        """Build analysis for deterministic SQL queries."""
        table_name = self.pattern_matcher.extract_table_reference(query)
        limit = None

        if self.pattern_matcher.match(query):
            for pattern in self.pattern_matcher.match(query):
                if pattern.pattern_type == QueryPatternType.SAMPLE_ROWS:
                    limit = pattern.extracted.get("limit", 5)
                    break

        entities = []
        if table_name:
            entities.append(
                ExtractedEntity(
                    entity_type="table",
                    value=table_name,
                    confidence=1.0,
                )
            )

        return QueryAnalysis(
            intent="data_query",
            route="sql",
            confidence=1.0,
            deterministic=True,
            extracted_table=table_name,
            extracted_limit=limit,
            entities=entities,
        )

    def _is_clarification_followup(self, query: str, history: list[Any]) -> bool:
        """Check if this is a followup to a clarification question."""
        return self.pattern_matcher.is_clarification_followup(query, history)

    def _build_followup_analysis(self, query: str, input: QueryAnalyzerInput) -> QueryAnalysis:
        """Build analysis for clarification followup."""
        table_hint = None

        cleaned_hint = self._clean_hint(query)
        if cleaned_hint:
            table_hint = cleaned_hint

        return QueryAnalysis(
            intent="data_query",
            route="sql",
            confidence=0.9,
            deterministic=True,
            extracted_table=table_hint,
            suggested_tables=[table_hint] if table_hint else [],
            entities=[
                ExtractedEntity(
                    entity_type="table",
                    value=table_hint,
                    confidence=0.9,
                )
            ]
            if table_hint
            else [],
        )

    def _clean_hint(self, text: str) -> str | None:
        import re

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

    async def _llm_analyze(self, query: str, input: QueryAnalyzerInput) -> QueryAnalysis:
        """Use LLM for query analysis."""
        system_prompt = self._get_system_prompt()
        user_prompt = self._build_user_prompt(query, input)

        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=system_prompt),
                LLMMessage(role="user", content=user_prompt),
            ],
            temperature=0.2,
            max_tokens=600,
        )

        response = await self.llm.generate(request)
        self._track_llm_call()

        return self._parse_llm_response(response.content)

    def _get_system_prompt(self) -> str:
        return """You are a query analyzer for a data assistant. Analyze the user's query and return a JSON object with:

1. intent: One of: data_query, definition, exploration, meta, exit, out_of_scope, small_talk, setup_help, datapoint_help
   - data_query: User wants to retrieve/analyze data (counts, sums, lists, aggregations)
   - definition: User wants to understand what something means or how it's calculated
   - exploration: User wants to understand what data is available (schemas, columns)
   - meta: User has questions about the system itself
   - exit: User wants to end the session
   - out_of_scope: Request unrelated to data analysis
   - small_talk: Greetings or casual conversation
   - setup_help: Questions about configuration/setup
   - datapoint_help: Questions about DataPoints

2. route: One of: sql, context, clarification, tool, end
   - sql: Query needs database execution (DEFAULT for data queries - even without explicit table)
   - context: Query can be answered from context/DataPoints
   - clarification: ONLY for truly ambiguous cases where no reasonable table can be inferred
   - tool: Query requires a tool action (profiling, etc.)
   - end: Session should end

IMPORTANT: Prefer route="sql" over route="clarification" for data queries. The system will:
- Deduce tables from schema and DataPoints automatically
- Try multiple table candidates before asking user
- Only ask for clarification if SQL generation truly fails

Use route="clarification" ONLY when:
- Query is completely vague ("show me data" with no context)
- Query mentions mutually exclusive concepts with no way to choose
- Session history provides no relevant context

Do NOT use route="clarification" just because a table isn't explicitly mentioned.
Most data queries should route to "sql" and let the system figure out the table.

3. entities: Array of extracted entities with type (table, column, metric, time_reference, filter, other), value, and confidence

4. complexity: One of: simple, medium, complex

5. confidence: Float 0-1 for analysis confidence

6. clarifying_questions: Array of questions if route is clarification (empty otherwise)

7. suggested_tables: Array of table names mentioned or implied (empty if none)

8. suggested_operators: Array of operations implied (count, sum, avg, list, etc.)

Return ONLY valid JSON. No markdown, no prose."""

    def _build_user_prompt(self, query: str, input: QueryAnalyzerInput) -> str:
        history_str = ""
        if input.conversation_history:
            lines = []
            for msg in input.conversation_history[-3:]:
                role = (
                    msg.get("role", "user")
                    if isinstance(msg, dict)
                    else getattr(msg, "role", "user")
                )
                content = (
                    msg.get("content", "") if isinstance(msg, dict) else getattr(msg, "content", "")
                )
                lines.append(f"{role}: {content}")
            history_str = "\n".join(lines)

        session_str = ""
        if input.session_state:
            session_str = json.dumps(input.session_state, default=str)

        return f"""Analyze this query:

User Query: {query}

Conversation History:
{history_str or "None"}

Session State:
{session_str or "None"}

Return JSON only."""

    def _parse_llm_response(self, content: str) -> QueryAnalysis:
        """Parse LLM response into QueryAnalysis."""
        try:
            json_str = content.strip()
            start_idx = json_str.find("{")
            end_idx = json_str.rfind("}") + 1
            if start_idx == -1 or end_idx == 0:
                raise ValueError("No JSON found in response")

            json_str = json_str[start_idx:end_idx]
            data = json.loads(json_str)

            intent = self._validate_intent(data.get("intent", "data_query"))
            route = self._validate_route(data.get("route", "sql"))

            entities = []
            for e in data.get("entities", []):
                entities.append(
                    ExtractedEntity(
                        entity_type=e.get("entity_type", "other"),
                        value=e.get("value", ""),
                        confidence=e.get("confidence", 1.0),
                        normalized_value=e.get("normalized_value"),
                    )
                )

            return QueryAnalysis(
                intent=intent,
                route=route,
                entities=entities,
                complexity=data.get("complexity", "simple"),
                confidence=data.get("confidence", 0.8),
                clarifying_questions=data.get("clarifying_questions", []),
                suggested_tables=data.get("suggested_tables", []),
                suggested_columns=data.get("suggested_columns", []),
                suggested_operators=data.get("suggested_operators", []),
                deterministic=False,
            )

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Failed to parse LLM response: {e}")
            return QueryAnalysis(
                intent="data_query",
                route="sql",
                confidence=0.5,
                clarifying_questions=["Could you rephrase your question?"],
            )

    def _validate_intent(self, intent: str) -> IntentType:
        valid = {
            "data_query",
            "definition",
            "exploration",
            "meta",
            "exit",
            "out_of_scope",
            "small_talk",
            "setup_help",
            "datapoint_help",
        }
        return intent if intent in valid else "data_query"

    def _validate_route(self, route: str) -> RouteType:
        valid = {"sql", "context", "clarification", "tool", "end"}
        return route if route in valid else "sql"

    def _should_deep_analyze(self, analysis: QueryAnalysis, query: str) -> bool:
        """Determine if we need a second, deeper analysis pass."""
        if analysis.confidence < self.intent_llm_confidence_threshold:
            return True
        if len(query.strip()) < self.deep_classify_min_length:
            return False
        if not analysis.entities and analysis.complexity != "simple":
            return True
        if analysis.clarifying_questions and analysis.complexity != "simple":
            return True
        return False

    async def _deep_analyze(self, query: str, input: QueryAnalyzerInput) -> QueryAnalysis | None:
        """Perform deeper analysis with more context."""
        system_prompt = """You are a detailed query analyzer. Analyze thoroughly and extract all entities, implied tables, and operations. Be comprehensive.

Return JSON with keys: intent, route, entities, complexity, confidence, clarifying_questions, suggested_tables, suggested_columns, suggested_operators."""

        user_prompt = f"""Analyze this query in detail:

Query: {query}

Extract all tables, columns, metrics, time references, and implied operations.
If ambiguous, provide specific clarifying questions.

Return JSON only."""

        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=system_prompt),
                LLMMessage(role="user", content=user_prompt),
            ],
            temperature=0.1,
            max_tokens=800,
        )

        try:
            response = await self.llm.generate(request)
            self._track_llm_call()
            return self._parse_llm_response(response.content)
        except Exception as e:
            logger.warning(f"Deep analysis failed: {e}")
            return None

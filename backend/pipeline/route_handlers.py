"""
Route Handlers

Modular route handlers that process queries based on their determined route.
Each handler is responsible for a specific routing path.

Routes:
- sql: Query needs database execution
- context: Query can be answered from DataPoints/context
- clarification: Query is ambiguous, needs more info
- tool: Query requires a tool action
- end: Session should end
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from backend.agents.query_analyzer import QueryAnalysis

logger = logging.getLogger(__name__)


class RouteHandler(ABC):
    """Base class for route handlers."""

    @property
    @abstractmethod
    def route_name(self) -> str:
        """Return the route name this handler processes."""
        pass

    @abstractmethod
    async def handle(
        self,
        state: dict[str, Any],
        analysis: QueryAnalysis,
    ) -> dict[str, Any]:
        """
        Process the query for this route.

        Args:
            state: Current pipeline state
            analysis: Query analysis from QueryAnalyzerAgent

        Returns:
            Updated pipeline state
        """
        pass


class EndRouteHandler(RouteHandler):
    """
    Handle queries that should end the session or return system messages.

    Intents handled:
    - exit: User wants to end
    - out_of_scope: Unrelated to data
    - small_talk: Greetings
    - setup_help: Configuration questions
    - datapoint_help: DataPoint management
    """

    @property
    def route_name(self) -> str:
        return "end"

    def __init__(self):
        self._exit_messages = {
            "exit": "Got it. Ending the session. If you need more, just start a new chat.",
            "out_of_scope": (
                "I can help with questions about your connected data. "
                "Try: list tables, show first 5 rows of a table, or total sales last month."
            ),
            "small_talk": (
                "Hi! I can help you explore your connected data. Try: "
                "list tables, show first 5 rows of a table, or total sales last month."
            ),
            "setup_help": (
                "To connect a database, open Settings -> Database Manager in the web app "
                "or run `datachat setup` / `datachat connect` in the CLI. "
                "Then ask questions like: list tables, show first 5 rows of a table, "
                "or total sales last month."
            ),
            "datapoint_help": (
                "You can manage and inspect DataPoints without writing SQL. "
                "In the UI, open Database Manager and review Pending/Approved DataPoints. "
                "In the CLI, use `datachat dp list` for indexed DataPoints and "
                "`datachat pending list` for approval queue."
            ),
        }

    async def handle(
        self,
        state: dict[str, Any],
        analysis: QueryAnalysis,
    ) -> dict[str, Any]:
        intent = analysis.intent
        state["intent"] = intent
        state["intent_gate"] = intent
        state["answer_source"] = "system"
        state["answer_confidence"] = 0.8
        state["natural_language_answer"] = self._exit_messages.get(
            intent,
            "I can help with questions about your connected data.",
        )
        state["clarification_needed"] = False
        state["clarifying_questions"] = []

        return state


class ClarificationRouteHandler(RouteHandler):
    """
    Handle queries that need clarification.

    Sets clarification_needed and prepares questions for the user.
    """

    @property
    def route_name(self) -> str:
        return "clarification"

    def __init__(
        self,
        max_clarifications: int = 3,
        clarification_count_fn: Callable[[dict[str, Any]], int] | None = None,
    ):
        self.max_clarifications = max_clarifications
        self._get_clarification_count = clarification_count_fn or self._default_count

    def _default_count(self, state: dict[str, Any]) -> int:
        return int(state.get("clarification_turn_count", 0) or 0)

    async def handle(
        self,
        state: dict[str, Any],
        analysis: QueryAnalysis,
    ) -> dict[str, Any]:
        current_count = self._get_clarification_count(state)
        state["clarification_turn_count"] = current_count

        limit = int(state.get("clarification_limit", self.max_clarifications) or 0)

        if current_count >= max(limit, 0):
            state["clarification_needed"] = False
            state["clarifying_questions"] = []
            state["answer_source"] = "system"
            state["answer_confidence"] = 0.5
            state["natural_language_answer"] = self._build_limit_message(state)
            state["intent"] = "meta"
            return state

        questions = analysis.clarifying_questions or ["What would you like to do with your data?"]

        state["clarification_turn_count"] = current_count + 1
        state["clarification_needed"] = True
        state["clarifying_questions"] = questions
        state["answer_source"] = "clarification"
        state["answer_confidence"] = 0.2
        state["intent"] = "clarify"

        intro = "I need a bit more detail to help you."
        formatted = "\n".join(f"- {question}" for question in questions)
        state["natural_language_answer"] = f"{intro}\n{formatted}"

        return state

    def _build_limit_message(self, state: dict[str, Any]) -> str:
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

    def _collect_table_candidates(self, state: dict[str, Any]) -> list[str]:
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


class SQLRouteHandler(RouteHandler):
    """
    Handle queries that need SQL execution.

    This is the primary data query path. The handler prepares state
    for the SQL agent but doesn't execute it (that's done by the pipeline).
    """

    @property
    def route_name(self) -> str:
        return "sql"

    async def handle(
        self,
        state: dict[str, Any],
        analysis: QueryAnalysis,
    ) -> dict[str, Any]:
        state["intent"] = analysis.intent
        state["intent_gate"] = "data_query"
        state["fast_path"] = analysis.deterministic

        if analysis.deterministic:
            state["skip_response_synthesis"] = True

        if analysis.entities:
            state["entities"] = [
                {
                    "entity_type": e.entity_type,
                    "value": e.value,
                    "confidence": e.confidence,
                    "normalized_value": e.normalized_value,
                }
                for e in analysis.entities
            ]

        if analysis.suggested_tables:
            existing = state.get("entities", [])
            table_entities = [
                {
                    "entity_type": "table",
                    "value": table,
                    "confidence": 0.9,
                    "normalized_value": None,
                }
                for table in analysis.suggested_tables
                if not any(e.get("value") == table for e in existing)
            ]
            state["entities"] = existing + table_entities

        if analysis.extracted_table:
            state.setdefault("entities", [])
            if not any(
                e.get("entity_type") == "table" and e.get("value") == analysis.extracted_table
                for e in state.get("entities", [])
            ):
                state.setdefault("entities", []).append(
                    {
                        "entity_type": "table",
                        "value": analysis.extracted_table,
                        "confidence": 1.0,
                        "normalized_value": None,
                    }
                )

        state["complexity"] = analysis.complexity
        state["clarification_needed"] = False
        state["clarifying_questions"] = []

        return state


class ContextRouteHandler(RouteHandler):
    """
    Handle queries that can be answered from context/DataPoints.

    Intents handled:
    - definition: Explaining what something means
    - exploration: Understanding available data
    - meta: Questions about the system
    """

    @property
    def route_name(self) -> str:
        return "context"

    async def handle(
        self,
        state: dict[str, Any],
        analysis: QueryAnalysis,
    ) -> dict[str, Any]:
        state["intent"] = analysis.intent
        state["answer_source"] = "context"
        state["answer_confidence"] = analysis.confidence
        state["clarification_needed"] = False
        state["clarifying_questions"] = []

        if analysis.entities:
            state["entities"] = [
                {
                    "entity_type": e.entity_type,
                    "value": e.value,
                    "confidence": e.confidence,
                    "normalized_value": e.normalized_value,
                }
                for e in analysis.entities
            ]

        return state


class ToolRouteHandler(RouteHandler):
    """
    Handle queries that require tool execution.

    Examples: profiling database, generating DataPoints, etc.
    """

    @property
    def route_name(self) -> str:
        return "tool"

    async def handle(
        self,
        state: dict[str, Any],
        analysis: QueryAnalysis,
    ) -> dict[str, Any]:
        state["intent"] = analysis.intent
        state["answer_source"] = "tool"
        state["tool_used"] = True
        state["clarification_needed"] = False
        state["clarifying_questions"] = []

        return state


class RouteDispatcher:
    """
    Dispatches queries to appropriate route handlers.

    Usage:
        dispatcher = RouteDispatcher()
        dispatcher.register(EndRouteHandler())
        dispatcher.register(ClarificationRouteHandler())
        ...

        state = await dispatcher.dispatch(state, analysis)
    """

    def __init__(self):
        self._handlers: dict[str, RouteHandler] = {}

    def register(self, handler: RouteHandler) -> None:
        """Register a route handler."""
        self._handlers[handler.route_name] = handler

    async def dispatch(
        self,
        state: dict[str, Any],
        analysis: QueryAnalysis,
    ) -> dict[str, Any]:
        """Dispatch to the appropriate handler based on analysis route."""
        route = analysis.route
        handler = self._handlers.get(route)

        if handler is None:
            logger.warning(f"No handler registered for route: {route}, defaulting to sql")
            handler = self._handlers.get("sql")

        if handler is None:
            state["error"] = f"No handler for route: {route}"
            return state

        return await handler.handle(state, analysis)

    def get_handler(self, route: str) -> RouteHandler | None:
        """Get the handler for a specific route."""
        return self._handlers.get(route)

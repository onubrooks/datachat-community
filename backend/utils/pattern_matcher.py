"""
Query Pattern Matcher

Consolidates all query pattern detection (table list, column list, row count,
sample rows, definition intent, aggregation keywords, etc.) into a single
configurable component.

This replaces scattered keyword detection across the orchestrator.
"""

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class QueryPatternType(StrEnum):
    TABLE_LIST = "table_list"
    COLUMN_LIST = "column_list"
    ROW_COUNT = "row_count"
    SAMPLE_ROWS = "sample_rows"
    DEFINITION = "definition"
    AGGREGATION = "aggregation"
    CLARIFICATION_FOLLOWUP = "clarification_followup"
    EXIT_INTENT = "exit_intent"
    SMALL_TALK = "small_talk"
    OUT_OF_SCOPE = "out_of_scope"
    DATAPPOINT_HELP = "datapoint_help"
    SETUP_HELP = "setup_help"
    ANY_TABLE = "any_table"


@dataclass
class QueryPattern:
    pattern_type: QueryPatternType
    confidence: float = 1.0
    extracted: dict[str, Any] = field(default_factory=dict)


class QueryPatternMatcher:
    """
    Single location for all query pattern detection.

    Usage:
        matcher = QueryPatternMatcher()
        patterns = matcher.match("list tables")
        if matcher.is_deterministic("show first 5 rows from users"):
            # Skip LLM, use deterministic SQL
    """

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        self._table_list_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"\bwhat tables\b",
                r"\blist tables\b",
                r"\bshow tables\b",
                r"\bavailable tables\b",
                r"\bwhich tables\b",
                r"\bwhat tables exist\b",
            ]
        ]
        self._column_list_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"\bshow columns\b",
                r"\blist columns\b",
                r"\bwhat columns\b",
                r"\bwhich columns\b",
                r"\bdescribe table\b",
                r"\btable schema\b",
                r"\bcolumn list\b",
                r"\bfields in\b",
            ]
        ]
        self._row_count_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"\brow count\b",
                r"\bhow many rows\b",
                r"\bnumber of rows\b",
                r"\bcount of rows\b",
                r"\btotal rows\b",
                r"\brow total\b",
                r"\bhow many records\b",
                r"\brecord count\b",
                r"\brecords in\b",
            ]
        ]
        self._sample_rows_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"\bshow\b.*\brows\b",
                r"\b(?:first|top|limit)\s+\d+\s+(?:rows?|records?)\b",
                r"\bpreview\b",
                r"\bsample\s+(?:rows?|records?)\b",
                r"\bexample\b",
                r"\bshow me\b.*\brows\b",
                r"\bdisplay\b.*\brows\b",
            ]
        ]
        self._definition_patterns = [
            (re.compile(p, re.IGNORECASE), 0.9)
            for p in [
                r"^\s*define\b",
                r"\bdefinition of\b",
                r"\bwhat does\b.*\b(mean|stand for)\b",
                r"\bmeaning of\b",
                r"\bhow is\b.*\b(calculated|computed|defined)\b",
                r"\bhow do (?:i|we|you)\b.*\bcalculate\b",
                r"\bbusiness rules?\b",
            ]
        ]
        self._aggregation_keywords = {
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
        }
        self._exit_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"^(exit|quit|bye|goodbye|stop|end)$",
                r"\bnever\s*mind\b",
                r"\b(i'?m|im|we'?re|were)\s+done\b",
                r"\b(done for now|done here|that'?s all|all set)\b",
                r"\b(let'?s\s+)?talk\s+later\b",
                r"\b(talk|see)\s+you\s+later\b",
                r"\b(no\s+more|no\s+further)\s+questions\b",
                r"\b(end|stop|quit|exit)\b.*\b(chat|conversation|session)\b",
            ]
        ]
        self._small_talk_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"\bhi\b",
                r"\bhello\b",
                r"\bhey\b",
                r"\bhow are you\b",
                r"\bwhat'?s up\b",
                r"\bgood morning\b",
                r"\bgood afternoon\b",
                r"\bgood evening\b",
            ]
        ]
        self._out_of_scope_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
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
        ]
        self._datapoint_help_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"^\s*(show|list|view)\s+(all\s+)?data\s*points?\b",
                r"^\s*(show|list|view)\s+(approved|pending|managed)\s+data\s*points?\b",
                r"^\s*available\s+data\s*points?\b",
                r"^\s*what\s+data\s*points?\s+(are\s+available|do\s+i\s+have)\b",
                r"^\s*data\s*points?\s+(list|overview)\b",
            ]
        ]
        self._setup_help_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
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
        ]
        self._any_table_patterns = [
            (re.compile(p, re.IGNORECASE), 1.0)
            for p in [
                r"\bany\s+table\b",
                r"\bpick\s+any\s+table\b",
                r"\bany\s+table\s+from\b",
                r"\bwhatever\s+table\b",
            ]
        ]
        self._data_keywords = {
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
        self._non_actionable_set = {
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

    def match(self, query: str) -> list[QueryPattern]:
        """Find all matching patterns in the query."""
        patterns: list[QueryPattern] = []
        text = query.strip().lower()
        if not text:
            return patterns

        for pattern, confidence in self._table_list_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.TABLE_LIST,
                        confidence=confidence,
                    )
                )
                break

        for pattern, confidence in self._column_list_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.COLUMN_LIST,
                        confidence=confidence,
                        extracted=self._extract_table_reference(text) or {},
                    )
                )
                break

        for pattern, confidence in self._row_count_patterns:
            if pattern.search(text):
                table_ref = self._extract_table_reference(text)
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.ROW_COUNT,
                        confidence=confidence,
                        extracted=table_ref or {},
                    )
                )
                break

        for pattern, confidence in self._sample_rows_patterns:
            if pattern.search(text):
                table_ref = self._extract_table_reference(text)
                limit = self._extract_limit(text)
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.SAMPLE_ROWS,
                        confidence=confidence,
                        extracted={**table_ref, "limit": limit} if table_ref else {"limit": limit},
                    )
                )
                break

        for pattern, confidence in self._definition_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.DEFINITION,
                        confidence=confidence,
                    )
                )
                break

        if self._has_aggregation_keywords(text):
            patterns.append(
                QueryPattern(
                    pattern_type=QueryPatternType.AGGREGATION,
                    confidence=0.8,
                )
            )

        for pattern, confidence in self._exit_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.EXIT_INTENT,
                        confidence=confidence,
                    )
                )
                break

        for pattern, confidence in self._small_talk_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.SMALL_TALK,
                        confidence=confidence,
                    )
                )
                break

        if not self._contains_data_keywords(text):
            for pattern, confidence in self._out_of_scope_patterns:
                if pattern.search(text):
                    patterns.append(
                        QueryPattern(
                            pattern_type=QueryPatternType.OUT_OF_SCOPE,
                            confidence=confidence,
                        )
                    )
                    break

        for pattern, confidence in self._datapoint_help_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.DATAPPOINT_HELP,
                        confidence=confidence,
                    )
                )
                break

        for pattern, confidence in self._setup_help_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.SETUP_HELP,
                        confidence=confidence,
                    )
                )
                break

        for pattern, confidence in self._any_table_patterns:
            if pattern.search(text):
                patterns.append(
                    QueryPattern(
                        pattern_type=QueryPatternType.ANY_TABLE,
                        confidence=confidence,
                    )
                )
                break

        return patterns

    def is_deterministic(self, query: str) -> bool:
        """
        Check if the query can be handled with deterministic SQL (no LLM needed).

        Returns True for:
        - Table listing queries
        - Column listing queries with table reference
        - Row count queries with table reference
        - Sample rows queries with table reference
        """
        text = query.strip().lower()
        if not text:
            return False

        for pattern, _ in self._table_list_patterns:
            if pattern.search(text):
                return True

        table_ref = self._extract_table_reference(text)
        if not table_ref:
            return False

        for pattern, _ in self._sample_rows_patterns:
            if pattern.search(text):
                return True

        for pattern, _ in self._row_count_patterns:
            if pattern.search(text):
                return True

        for pattern, _ in self._column_list_patterns:
            if pattern.search(text):
                return True

        return False

    def get_primary_pattern(self, query: str) -> QueryPattern | None:
        """Get the most relevant pattern for routing decisions."""
        patterns = self.match(query)
        if not patterns:
            return None

        priority_order = [
            QueryPatternType.EXIT_INTENT,
            QueryPatternType.OUT_OF_SCOPE,
            QueryPatternType.SMALL_TALK,
            QueryPatternType.SETUP_HELP,
            QueryPatternType.DATAPPOINT_HELP,
            QueryPatternType.TABLE_LIST,
            QueryPatternType.COLUMN_LIST,
            QueryPatternType.ROW_COUNT,
            QueryPatternType.SAMPLE_ROWS,
            QueryPatternType.DEFINITION,
            QueryPatternType.AGGREGATION,
            QueryPatternType.ANY_TABLE,
        ]

        for pattern_type in priority_order:
            for pattern in patterns:
                if pattern.pattern_type == pattern_type:
                    return pattern

        return patterns[0]

    def extract_table_reference(self, query: str) -> str | None:
        """Extract table name from query."""
        result = self._extract_table_reference(query.lower())
        return result.get("table_name") if result else None

    def _extract_table_reference(self, text: str) -> dict[str, Any] | None:
        patterns = [
            r"\b(?:from|in|of)\s+([a-zA-Z0-9_.]+)",
            r"\btable\s+([a-zA-Z0-9_.]+)",
            r"\bhow\s+many\s+rows?\s+(?:are\s+)?in\s+([a-zA-Z0-9_.]+)",
            r"\brows?\s+in\s+([a-zA-Z0-9_.]+)",
            r"\bcount\s+of\s+rows?\s+in\s+([a-zA-Z0-9_.]+)",
            r"\brecords?\s+in\s+([a-zA-Z0-9_.]+)",
            r"\b(?:first|top|last)\s+\d+\s+rows?\s+(?:from|in|of)\s+([a-zA-Z0-9_.]+)",
            r"\bshow\s+me\s+(?:the\s+)?(?:first|top|last)?\s*\d*\s*rows?\s+(?:from|in|of)\s+([a-zA-Z0-9_.]+)",
            r"\b(?:preview|sample)\s+(?:rows?\s+(?:from|in|of)\s+)?([a-zA-Z0-9_.]+)",
        ]
        for pattern_str in patterns:
            match = re.search(pattern_str, text, re.IGNORECASE)
            if match:
                table = match.group(1).rstrip(".,;:?)")
                if table and table.lower() not in {"table", "tables", "row", "rows"}:
                    return {"table_name": table}
        return None

    def _extract_limit(self, text: str) -> int:
        match = re.search(r"\b(first|top|limit)\s+(\d+)\b", text, re.IGNORECASE)
        if match:
            try:
                value = int(match.group(2))
                return max(1, min(value, 25))
            except ValueError:
                return 5
        return 5

    def _has_aggregation_keywords(self, text: str) -> bool:
        return any(keyword in text for keyword in self._aggregation_keywords)

    def _contains_data_keywords(self, text: str) -> bool:
        return any(word in text for word in self._data_keywords)

    def is_non_actionable(self, query: str) -> bool:
        """Check if query is a non-actionable utterance (needs clarification)."""
        normalized = query.strip().lower()
        if not normalized:
            return True
        if normalized in self._non_actionable_set:
            return True
        if re.fullmatch(r"(ok|okay|sure|yes|no|thanks|thank you)[.!]*", normalized):
            return True
        return False

    def is_short_followup(self, query: str) -> bool:
        """Check if query looks like a short followup answer to clarification."""
        candidate = query.strip().lower()
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

    def is_clarification_followup(self, query: str, history: list[dict] | None) -> bool:
        """Check if this is a followup to a clarification question."""
        if not history:
            return False
        if not self.is_short_followup(query):
            return False

        for msg in reversed(history):
            role = msg.get("role", "") if isinstance(msg, dict) else getattr(msg, "role", "")
            if role == "assistant":
                content = (
                    msg.get("content", "") if isinstance(msg, dict) else getattr(msg, "content", "")
                )
                if self._is_clarification_prompt(content):
                    return True
        return False

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

    def extract_clarifying_questions(self, text: str) -> list[str]:
        """Extract clarifying questions from assistant message."""
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

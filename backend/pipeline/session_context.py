"""
Session Context

Manages conversation state for follow-up queries and clarification handling.
This replaces the scattered intent_summary and session_state logic.
"""

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class SessionContext:
    """
    Manages contextual information for the current session.

    Tracks:
    - Last goal: What the user was trying to do
    - Clarification history: How many clarifications and what was asked
    - Table/column hints: Entities mentioned in follow-ups
    - Resolved query: The rewritten query after follow-up
    """

    last_goal: str | None = None
    clarification_count: int = 0
    table_hints: list[str] = field(default_factory=list)
    column_hints: list[str] = field(default_factory=list)
    resolved_query: str | None = None
    any_table: bool = False
    last_clarifying_questions: list[str] = field(default_factory=list)
    target_subquery_index: int | None = None
    slots: dict[str, str | None] = field(
        default_factory=lambda: {
            "table": None,
            "metric": None,
            "time_range": None,
        }
    )

    def update_from_analysis(self, analysis: Any) -> None:
        """Update context from QueryAnalysis result."""
        if analysis.suggested_tables:
            for table in analysis.suggested_tables:
                if table not in self.table_hints:
                    self.table_hints.append(table)

        if analysis.suggested_columns:
            for column in analysis.suggested_columns:
                if column not in self.column_hints:
                    self.column_hints.append(column)

        if analysis.extracted_table:
            if analysis.extracted_table not in self.table_hints:
                self.table_hints.append(analysis.extracted_table)
            self.slots["table"] = analysis.extracted_table

        if analysis.clarifying_questions:
            self.last_clarifying_questions = analysis.clarifying_questions
            self.clarification_count += 1

        if analysis.intent == "data_query":
            self.last_goal = getattr(analysis, "original_query", None)

    def update_from_history(self, history: list[Any]) -> None:
        """Extract context from conversation history."""
        if not history:
            return

        for msg in history:
            role, content = self._extract_role_content(msg)
            if role == "assistant" and self._is_clarification_prompt(content):
                self.clarification_count += 1
                questions = self._extract_clarifying_questions(content)
                if questions:
                    self.last_clarifying_questions = questions

        for idx in range(len(history) - 1, -1, -1):
            role, content = self._extract_role_content(history[idx])
            if role == "assistant" and self._is_clarification_prompt(content):
                for j in range(idx - 1, -1, -1):
                    prev_role, prev_content = self._extract_role_content(history[j])
                    if prev_role == "user" and prev_content:
                        self.last_goal = prev_content
                        break
                break

    def build_followup_context(self) -> str:
        """Build a context string for follow-up handling."""
        parts = []
        if self.last_goal:
            parts.append(f"last_goal={self.last_goal}")
        if self.table_hints:
            parts.append(f"table_hints={', '.join(self.table_hints[:3])}")
        if self.column_hints:
            parts.append(f"column_hints={', '.join(self.column_hints[:3])}")
        slot_parts = [f"{k}:{v}" for k, v in self.slots.items() if v]
        if slot_parts:
            parts.append(f"slots={', '.join(slot_parts)}")
        if self.clarification_count:
            parts.append(f"clarifications={self.clarification_count}")
        if self.last_clarifying_questions:
            parts.append(f"last_questions={'; '.join(self.last_clarifying_questions[:2])}")

        if not parts:
            return ""
        return "Intent summary: " + " | ".join(parts)

    def resolve_followup_query(self, query: str) -> str | None:
        """
        Resolve a follow-up query using context.

        Returns:
            Resolved query or None if not a follow-up
        """
        if not self._is_followup_query(query):
            return None

        if self._contains_data_keywords(query):
            return None

        focus = self._extract_followup_focus(query)
        if not focus:
            return None

        if not self.last_goal:
            return None

        last_goal_lower = self.last_goal.lower()

        if "how many " in last_goal_lower:
            return f"How many {focus} do we have?"
        if last_goal_lower.startswith("list "):
            return f"List {focus}"
        if last_goal_lower.startswith("show "):
            return f"Show {focus}"
        if "total " in last_goal_lower:
            return f"What is total {focus}?"

        return None

    def resolve_table_hint(self, query: str) -> str | None:
        """
        Extract table hint from a short follow-up.

        Returns:
            Table name hint or None
        """
        if not self._is_short_followup(query):
            return None

        cleaned = self._clean_hint(query)
        if not cleaned:
            return None

        if self.last_clarifying_questions:
            combined = " ".join(self.last_clarifying_questions).lower()
            if "table" in combined:
                return cleaned

        return None

    def merge_with_state(self, state: dict[str, Any]) -> dict[str, Any]:
        """Merge this context into a pipeline state dict."""
        merged = dict(state)

        state_slots = state.get("slots") or {}
        if isinstance(state_slots, dict):
            for key, value in state_slots.items():
                if value and not self.slots.get(key):
                    self.slots[key] = value

        for key in ("table_hints", "column_hints", "last_clarifying_questions"):
            current = list(merged.get(key) or [])
            ctx_values = list(getattr(self, key, []) or [])
            combined: list[str] = []
            for value in [*ctx_values, *current]:
                if value and value not in combined:
                    combined.append(value)
            merged[key] = combined

        merged["clarification_count"] = max(
            self.clarification_count,
            int(merged.get("clarification_count", 0) or 0),
        )

        if not merged.get("last_goal") and self.last_goal:
            merged["last_goal"] = self.last_goal

        if not merged.get("resolved_query") and self.resolved_query:
            merged["resolved_query"] = self.resolved_query

        return merged

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "last_goal": self.last_goal,
            "clarification_count": self.clarification_count,
            "table_hints": self.table_hints,
            "column_hints": self.column_hints,
            "resolved_query": self.resolved_query,
            "any_table": self.any_table,
            "last_clarifying_questions": self.last_clarifying_questions,
            "target_subquery_index": self.target_subquery_index,
            "slots": self.slots,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionContext":
        """Deserialize from dictionary."""
        return cls(
            last_goal=data.get("last_goal"),
            clarification_count=data.get("clarification_count", 0),
            table_hints=data.get("table_hints", []),
            column_hints=data.get("column_hints", []),
            resolved_query=data.get("resolved_query"),
            any_table=data.get("any_table", False),
            last_clarifying_questions=data.get("last_clarifying_questions", []),
            target_subquery_index=data.get("target_subquery_index"),
            slots=data.get("slots", {"table": None, "metric": None, "time_range": None}),
        )

    def _extract_role_content(self, msg: Any) -> tuple[str, str]:
        if isinstance(msg, dict):
            role = str(msg.get("role", "user"))
            content = str(msg.get("content", ""))
        else:
            role = str(getattr(msg, "role", "user"))
            content = str(getattr(msg, "content", ""))
        return role, content.strip()

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

    def _extract_clarifying_questions(self, text: str) -> list[str]:
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

    def _is_followup_query(self, text: str) -> bool:
        lowered = text.strip().lower()
        patterns = [
            r"^what\s+about\b",
            r"^how\s+about\b",
            r"^what\s+of\b",
            r"^and\b",
            r"^about\b",
        ]
        return any(re.search(pattern, lowered) for pattern in patterns)

    def _contains_data_keywords(self, text: str) -> bool:
        keywords = {
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
            "data",
            "dataset",
        }
        return any(word in text for word in keywords)

    def _is_short_followup(self, text: str) -> bool:
        candidate = text.strip().lower()
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

    def _extract_followup_focus(self, text: str) -> str | None:
        cleaned = text.strip().strip("\"'").strip()
        cleaned = re.sub(
            r"^(what\s+about|how\s+about|what\s+of|and|about)\s+",
            "",
            cleaned,
            flags=re.I,
        )
        cleaned = cleaned.strip(" .,!?:;\"'")
        cleaned = re.sub(r"^(the|our|their)\s+", "", cleaned, flags=re.I)
        if not cleaned:
            return None
        return cleaned.lower()

    def _clean_hint(self, text: str) -> str | None:
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

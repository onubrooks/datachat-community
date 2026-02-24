"""Deterministic catalog intelligence for credentials-only workflows."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from backend.database.catalog_templates import (
    get_list_columns_query,
    get_list_tables_query,
    normalize_database_type,
)
from backend.models.agent import InvestigationMemory, RetrievedDataPoint

_GENERIC_STOPWORDS = {
    "the",
    "a",
    "an",
    "is",
    "are",
    "was",
    "were",
    "what",
    "which",
    "show",
    "list",
    "table",
    "tables",
    "in",
    "of",
    "for",
    "to",
    "and",
    "or",
    "by",
    "with",
    "on",
    "from",
    "does",
    "exist",
    "exists",
    "rows",
    "row",
    "count",
    "columns",
    "column",
    "me",
    "first",
    "top",
    "last",
    "give",
    "get",
    "all",
    "just",
}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DISALLOWED_TABLE_HINTS = {
    "which",
    "what",
    "who",
    "where",
    "when",
    "why",
    "how",
    "should",
    "would",
    "could",
    "can",
    "do",
    "does",
    "did",
    "is",
    "are",
    "be",
    "show",
    "list",
    "describe",
    "get",
    "select",
    "rows",
    "columns",
    "table",
    "tables",
    "end",
    "exit",
    "quit",
    "stop",
    "bye",
    "goodbye",
}


@dataclass(frozen=True)
class CatalogQueryPlan:
    """Structured deterministic SQL plan."""

    operation: str
    sql: str | None
    explanation: str
    confidence: float
    clarifying_questions: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CatalogContextResponse:
    """Deterministic context-only response (no SQL execution)."""

    answer: str
    confidence: float
    evidence_datapoint_ids: list[str] = field(default_factory=list)
    needs_sql: bool = False
    clarifying_questions: list[str] = field(default_factory=list)


class CatalogIntelligence:
    """Deterministic schema/shape planner used before LLM SQL generation."""

    def plan_query(
        self,
        *,
        query: str,
        database_type: str | None,
        investigation_memory: InvestigationMemory,
    ) -> CatalogQueryPlan | None:
        text = (query or "").strip().lower()
        if not text:
            return None

        db_type = normalize_database_type(database_type)

        if self.is_list_tables_query(text):
            sql = get_list_tables_query(db_type)
            if not sql:
                return None
            return CatalogQueryPlan(
                operation="list_tables",
                sql=sql,
                explanation="Lists available tables using system catalog metadata.",
                confidence=0.99,
            )

        explicit_table = self.extract_explicit_table_name(query)

        if self.is_list_columns_query(text):
            table_name = explicit_table or self.select_table_candidate(query, investigation_memory)
            if not table_name:
                return CatalogQueryPlan(
                    operation="list_columns",
                    sql=None,
                    explanation="Missing table name for column listing.",
                    confidence=0.2,
                    clarifying_questions=["Which table should I list columns for?"],
                )
            split_table = self.split_table_reference(table_name, db_type=db_type)
            if not split_table:
                return CatalogQueryPlan(
                    operation="list_columns",
                    sql=None,
                    explanation="Table name is not a safe identifier.",
                    confidence=0.2,
                    clarifying_questions=["Which table should I list columns for?"],
                )
            schema_name, normalized_table = split_table
            sql = get_list_columns_query(
                db_type,
                table_name=normalized_table,
                schema_name=schema_name,
            )
            if not sql:
                return None
            return CatalogQueryPlan(
                operation="list_columns",
                sql=sql,
                explanation="Lists column metadata from system catalog tables.",
                confidence=0.98,
            )

        if self.is_sample_rows_query(text):
            table_name = explicit_table or self.select_table_candidate(query, investigation_memory)
            if not table_name:
                return CatalogQueryPlan(
                    operation="sample_rows",
                    sql=None,
                    explanation="Missing table name for row sampling.",
                    confidence=0.2,
                    clarifying_questions=["Which table should I sample rows from?"],
                )
            table_sql = self.format_table_reference(table_name, db_type=db_type)
            if not table_sql:
                return CatalogQueryPlan(
                    operation="sample_rows",
                    sql=None,
                    explanation="Table name is not a safe identifier.",
                    confidence=0.2,
                    clarifying_questions=["Which table should I sample rows from?"],
                )
            limit = self.extract_limit(text)
            return CatalogQueryPlan(
                operation="sample_rows",
                sql=f"SELECT * FROM {table_sql} LIMIT {limit}",
                explanation="Returns a bounded sample of rows from the selected table.",
                confidence=0.96,
            )

        if self.is_row_count_query(text):
            table_name = explicit_table or self.select_table_candidate(query, investigation_memory)
            if not table_name:
                return CatalogQueryPlan(
                    operation="row_count",
                    sql=None,
                    explanation="Missing table name for row count.",
                    confidence=0.2,
                    clarifying_questions=["Which table should I count rows for?"],
                )
            table_sql = self.format_table_reference(table_name, db_type=db_type)
            if not table_sql:
                return CatalogQueryPlan(
                    operation="row_count",
                    sql=None,
                    explanation="Table name is not a safe identifier.",
                    confidence=0.2,
                    clarifying_questions=["Which table should I count rows for?"],
                )
            return CatalogQueryPlan(
                operation="row_count",
                sql=f"SELECT COUNT(*) AS row_count FROM {table_sql}",
                explanation="Counts rows in the selected table.",
                confidence=0.96,
            )

        return None

    def build_context_response(
        self,
        *,
        query: str,
        investigation_memory: InvestigationMemory,
    ) -> CatalogContextResponse | None:
        text = (query or "").strip().lower()
        if not text:
            return None

        schema_tables = self._extract_schema_tables(investigation_memory)
        if not schema_tables:
            return None

        if self.is_list_tables_query(text):
            table_names = sorted(schema_tables.keys())
            preview = ", ".join(table_names[:12])
            suffix = (
                f" (and {len(table_names) - 12} more)"
                if len(table_names) > 12
                else ""
            )
            return CatalogContextResponse(
                answer=(
                    f"I found {len(table_names)} table(s) from schema context: "
                    f"{preview}{suffix}."
                ),
                confidence=0.9,
                evidence_datapoint_ids=self._top_schema_evidence(investigation_memory),
                needs_sql=False,
            )

        if self.is_list_columns_query(text):
            table_name = self.extract_explicit_table_name(query) or self.select_table_candidate(
                query, investigation_memory
            )
            if not table_name:
                return CatalogContextResponse(
                    answer="I can list columns once you specify the table.",
                    confidence=0.3,
                    needs_sql=False,
                    clarifying_questions=["Which table should I list columns for?"],
                )
            normalized = self.normalize_table_reference(table_name)
            if not normalized:
                return CatalogContextResponse(
                    answer="I can list columns once you specify a valid table name.",
                    confidence=0.2,
                    needs_sql=False,
                    clarifying_questions=["Which table should I list columns for?"],
                )
            lookup = normalized.lower()
            row = schema_tables.get(lookup)
            if not row or not row["columns"]:
                return CatalogContextResponse(
                    answer=(
                        f"I can query columns for `{normalized}`, but I need SQL execution "
                        "because column metadata is not loaded in context."
                    ),
                    confidence=0.4,
                    needs_sql=True,
                    evidence_datapoint_ids=self._top_schema_evidence(investigation_memory),
                )
            preview = ", ".join(row["columns"][:20])
            suffix = f" (and {len(row['columns']) - 20} more)" if len(row["columns"]) > 20 else ""
            return CatalogContextResponse(
                answer=f"Columns in `{row['name']}`: {preview}{suffix}.",
                confidence=0.88,
                evidence_datapoint_ids=row["evidence"][:3],
                needs_sql=False,
            )

        if self.is_row_count_query(text) or self.is_sample_rows_query(text):
            table_name = self.extract_explicit_table_name(query) or self.select_table_candidate(
                query, investigation_memory
            )
            if not table_name:
                question = (
                    "Which table should I sample rows from?"
                    if self.is_sample_rows_query(text)
                    else "Which table should I count rows for?"
                )
                return CatalogContextResponse(
                    answer="I need a table name before running that query.",
                    confidence=0.3,
                    needs_sql=False,
                    clarifying_questions=[question],
                )
            return CatalogContextResponse(
                answer="I can run that query now using the selected table.",
                confidence=0.75,
                evidence_datapoint_ids=self._top_schema_evidence(investigation_memory),
                needs_sql=True,
            )

        return None

    def build_ranked_schema_context(
        self,
        *,
        query: str,
        investigation_memory: InvestigationMemory,
        max_tables: int = 10,
        max_columns: int = 12,
    ) -> str | None:
        schema_tables = self._extract_schema_tables(investigation_memory)
        if not schema_tables:
            return None

        ranked = self._rank_table_names(query, list(schema_tables.keys()))
        if not ranked:
            ranked = list(schema_tables.keys())

        lines = ["**Catalog schema context (ranked):**"]
        for table_key in ranked[:max_tables]:
            item = schema_tables[table_key]
            columns = item["columns"][:max_columns]
            if columns:
                lines.append(f"- {item['name']}: {', '.join(columns)}")
            else:
                lines.append(f"- {item['name']}")

        return "\n".join(lines)

    def is_list_tables_query(self, query: str) -> bool:
        patterns = (
            r"\bwhat tables\b",
            r"\blist tables\b",
            r"\bshow tables\b",
            r"\bavailable tables\b",
            r"\bwhich tables\b",
            r"\bwhat tables exist\b",
            r"\btable list\b",
        )
        return any(re.search(pattern, query) for pattern in patterns)

    def is_list_columns_query(self, query: str) -> bool:
        patterns = (
            r"\bshow columns\b",
            r"\blist columns\b",
            r"\bwhat columns\b",
            r"\bwhich columns\b",
            r"\bdescribe table\b",
            r"\btable schema\b",
            r"\bcolumn list\b",
            r"\bfields in\b",
        )
        return any(re.search(pattern, query) for pattern in patterns)

    def is_sample_rows_query(self, query: str) -> bool:
        patterns = (
            r"\bshow\b.*\brows\b",
            r"\b(?:first|top|limit)\s+\d+\s+(?:rows?|records?)\b",
            r"\bpreview\b",
            r"\bsample\s+(?:rows?|records?)\b",
            r"\bexample rows?\b",
        )
        return any(re.search(pattern, query) for pattern in patterns)

    def is_row_count_query(self, query: str) -> bool:
        patterns = (
            r"\brow count\b",
            r"\bhow many rows\b",
            r"\bnumber of rows\b",
            r"\bcount of rows\b",
            r"\btotal rows\b",
            r"\bhow many records\b",
            r"\brecord count\b",
            r"\brows in\b",
        )
        return any(re.search(pattern, query) for pattern in patterns)

    def extract_limit(self, query: str) -> int:
        match = re.search(r"\b(first|top|limit)\s+(\d+)\b", query)
        if not match:
            match = re.search(r"\bshow\s+(\d+)\s+rows?\b", query)
        if not match:
            return 3
        try:
            value = int(match.group(match.lastindex or 1))
        except ValueError:
            return 3
        return max(1, min(value, 10))

    def extract_explicit_table_name(self, query: str) -> str | None:
        trailing_value = self._extract_clarification_value(query)
        if trailing_value:
            return trailing_value

        patterns = (
            r"\bhow\s+many\s+rows?\s+(?:are\s+)?in\s+([a-zA-Z0-9_.`\"]+)",
            r"\bcount\s+of\s+rows?\s+in\s+([a-zA-Z0-9_.`\"]+)",
            r"\b(?:rows?|records?|columns?|fields?)\s+(?:from|in|of)\s+([a-zA-Z0-9_.`\"]+)",
            r"\btable\s+([a-zA-Z0-9_.`\"]+)\b",
            r"\bfor\s+table\s+([a-zA-Z0-9_.`\"]+)",
            r"\bdescribe\s+([a-zA-Z0-9_.`\"]+)",
            r"\bin\s+([a-zA-Z0-9_.`\"]+)\s+table\b",
        )
        for pattern in patterns:
            match = re.search(pattern, query.lower())
            if not match:
                continue
            raw = match.group(1).rstrip(".,;:?)")
            if raw.lower() in _DISALLOWED_TABLE_HINTS:
                continue
            normalized = self.normalize_table_reference(raw)
            if normalized:
                return normalized
        return None

    def _extract_clarification_value(self, query: str) -> str | None:
        if ":" not in query:
            return None
        tail = query.rsplit(":", 1)[-1].strip().strip('"').strip("'")
        if not tail:
            return None
        candidate = tail.split()[0].rstrip(".,;:?)")
        if candidate.lower() in _DISALLOWED_TABLE_HINTS:
            return None
        return self.normalize_table_reference(candidate)

    def select_table_candidate(
        self,
        query: str,
        investigation_memory: InvestigationMemory,
    ) -> str | None:
        schema_tables = self._extract_schema_tables(investigation_memory)
        if not schema_tables:
            return None
        if len(schema_tables) == 1:
            only = next(iter(schema_tables.values()))
            return only["name"]

        ranked = self._rank_table_names(query, list(schema_tables.keys()))
        if not ranked:
            return None

        top_key = ranked[0]
        top_score = self._table_score(query, top_key)
        if top_score <= 0:
            return None
        return schema_tables[top_key]["name"]

    def format_table_reference(self, table_name: str, *, db_type: str) -> str | None:
        normalized = self.normalize_table_reference(table_name)
        if not normalized:
            return None
        if db_type == "bigquery":
            return f"`{normalized}`"
        return normalized

    def split_table_reference(
        self,
        table_name: str,
        *,
        db_type: str,
    ) -> tuple[str | None, str] | None:
        normalized = self.normalize_table_reference(table_name)
        if not normalized:
            return None
        parts = normalized.split(".")
        if len(parts) == 1:
            default_schema = self._default_schema_for_db(db_type)
            return default_schema, parts[0]
        if len(parts) == 2:
            return parts[0], parts[1]
        if db_type == "bigquery":
            # project.dataset.table -> use dataset as schema
            return parts[-2], parts[-1]
        return None

    def normalize_table_reference(self, raw: str) -> str | None:
        cleaned = raw.strip().strip("`").strip('"')
        if not cleaned:
            return None

        parts = [part.strip().strip("`").strip('"') for part in cleaned.split(".")]
        if any(not part for part in parts):
            return None

        normalized_parts: list[str] = []
        for index, part in enumerate(parts):
            if _IDENTIFIER_RE.fullmatch(part):
                normalized_parts.append(part)
                continue
            if index == 0 and "-" in part:
                # BigQuery project IDs may include hyphens.
                if re.fullmatch(r"[A-Za-z0-9_-]+", part):
                    normalized_parts.append(part)
                    continue
            return None

        return ".".join(normalized_parts)

    def _extract_schema_tables(
        self,
        investigation_memory: InvestigationMemory,
    ) -> dict[str, dict[str, list[str] | str]]:
        tables: dict[str, dict[str, list[str] | str]] = {}

        for datapoint in investigation_memory.datapoints:
            if datapoint.datapoint_type != "Schema":
                continue
            metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
            table_name = metadata.get("table_name") or metadata.get("table")
            schema_name = metadata.get("schema") or metadata.get("schema_name")
            if not table_name:
                continue
            full_name = str(table_name)
            if "." not in full_name and schema_name:
                full_name = f"{schema_name}.{full_name}"
            normalized = self.normalize_table_reference(full_name)
            if not normalized:
                continue

            table_key = normalized.lower()
            entry = tables.setdefault(
                table_key,
                {"name": normalized, "columns": [], "evidence": []},
            )
            evidence = entry["evidence"]
            if datapoint.datapoint_id not in evidence:
                evidence.append(datapoint.datapoint_id)

            for column in self._extract_column_names(datapoint):
                if column not in entry["columns"]:
                    entry["columns"].append(column)

        return tables

    def _extract_column_names(self, datapoint: RetrievedDataPoint) -> list[str]:
        metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
        key_columns = metadata.get("key_columns") or metadata.get("columns") or []
        names: list[str] = []
        if isinstance(key_columns, list):
            for item in key_columns:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("column_name")
                    dtype = item.get("type") or item.get("data_type")
                    if name:
                        formatted = (
                            f"{name} ({dtype})" if dtype else str(name)
                        )
                        names.append(formatted)
                elif isinstance(item, str):
                    names.append(item)
        return names

    def _rank_table_names(self, query: str, table_keys: list[str]) -> list[str]:
        scored = [(self._table_score(query, key), key) for key in table_keys]
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return [key for _, key in scored]

    def _table_score(self, query: str, table_key: str) -> int:
        tokens = self._tokenize(query)
        if not tokens:
            return 0
        table_tokens = set(re.findall(r"[a-z0-9_]+", table_key.lower()))
        score = 0
        for token in tokens:
            if token in table_tokens:
                score += 3
            if any(token in piece for piece in table_tokens):
                score += 1
        return score

    def _tokenize(self, query: str) -> set[str]:
        tokens = set(re.findall(r"[a-z0-9_]+", query.lower()))
        return {token for token in tokens if token not in _GENERIC_STOPWORDS and len(token) > 1}

    def _default_schema_for_db(self, db_type: str) -> str | None:
        if db_type in {"postgresql", "redshift"}:
            return "public"
        if db_type == "clickhouse":
            return "default"
        return None

    def _top_schema_evidence(
        self, investigation_memory: InvestigationMemory, limit: int = 3
    ) -> list[str]:
        evidence: list[str] = []
        for datapoint in investigation_memory.datapoints:
            if datapoint.datapoint_type != "Schema":
                continue
            if datapoint.datapoint_id not in evidence:
                evidence.append(datapoint.datapoint_id)
            if len(evidence) >= limit:
                break
        return evidence

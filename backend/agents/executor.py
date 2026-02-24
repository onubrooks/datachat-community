"""
ExecutorAgent: Query execution and result formatting.

This agent executes validated SQL queries and formats results:
- Executes queries with timeout protection
- Generates natural language summaries using GPT-4o-mini
- Suggests visualization types based on data shape
- Handles pagination/truncation for large results
- Includes source citations from pipeline

Uses database connectors for execution and LLM for summarization.
"""

import asyncio
import json
import logging
import re
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from backend.agents.base import BaseAgent
from backend.config import get_settings
from backend.connectors.base import BaseConnector, QueryError
from backend.connectors.factory import create_connector
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models import (
    ExecutedQuery,
    ExecutorAgentInput,
    ExecutorAgentOutput,
    QueryResult,
)
from backend.prompts.loader import PromptLoader

logger = logging.getLogger(__name__)


class ExecutorAgent(BaseAgent):
    """
    Query execution and result formatting agent.

    Executes SQL queries, generates natural language summaries,
    and suggests appropriate visualizations.
    """

    def __init__(self, llm_provider=None):
        """
        Initialize ExecutorAgent with LLM provider.

        Args:
            llm_provider: Optional LLM provider. If None, creates default provider.
        """
        super().__init__(name="ExecutorAgent")

        # Get configuration
        self.config = get_settings()

        # Get LLM provider (use mini model for summarization/cost)
        if llm_provider is None:
            self.llm = LLMProviderFactory.create_default_provider(
                self.config.llm, model_type="mini"
            )
        else:
            self.llm = llm_provider
        self.prompts = PromptLoader()

    async def execute(self, input: ExecutorAgentInput) -> ExecutorAgentOutput:
        """
        Execute SQL query and format results.

        Args:
            input: ExecutorAgentInput with validated SQL and database config

        Returns:
            ExecutorAgentOutput with query results and summary

        Raises:
            QueryError: If query execution fails
            LLMError: If summary generation fails
        """
        logger.info(f"[{self.name}] Executing query on {input.database_type}")

        # Get database connector
        connector = await self._get_connector(input.database_type, input.database_url)
        llm_calls = 0
        sql_to_execute = input.validated_sql.sql

        try:
            # Execute query with timeout
            try:
                query_result = await self._execute_query(
                    connector,
                    sql_to_execute,
                    input.max_rows,
                    input.timeout_seconds,
                )
            except QueryError as exc:
                missing_relation = self._extract_missing_relation(exc)
                if missing_relation:
                    catalog = await self._fetch_schema_catalog(
                        connector, input.database_type
                    )
                    if catalog and not self._relation_in_catalog(
                        missing_relation, catalog
                    ):
                        raise QueryError(
                            f"Missing table in live schema: {missing_relation}. "
                            "Schema refresh required."
                        ) from exc

                schema_context = self._load_schema_context(input.source_datapoints)
                if self._should_probe_schema(exc, input.database_type):
                    db_context = await self._fetch_schema_context(connector, input.database_type)
                    if db_context:
                        if schema_context == "No schema datapoints available.":
                            schema_context = db_context
                        else:
                            schema_context = f"{schema_context}\n\n{db_context}"
                corrected_sql = await self._attempt_sql_correction(
                    input, sql_to_execute, exc, schema_context
                )
                if corrected_sql:
                    llm_calls += 1
                    sql_to_execute = corrected_sql
                    query_result = await self._execute_query(
                        connector,
                        sql_to_execute,
                        input.max_rows,
                        input.timeout_seconds,
                    )
                else:
                    raise

            # Generate natural language summary
            deterministic_summary = self._generate_deterministic_summary(
                input.query, sql_to_execute, query_result
            )
            if deterministic_summary:
                nl_answer, insights = deterministic_summary
            else:
                nl_answer, insights = await self._generate_summary(
                    input.query, sql_to_execute, query_result
                )
                llm_calls += 1

            # Suggest visualization (LLM-assisted with deterministic guardrails)
            viz_hint, viz_metadata, viz_llm_calls = await self._recommend_visualization(
                query_result=query_result,
                original_query=input.query,
                sql=sql_to_execute,
            )
            llm_calls += viz_llm_calls
            viz_note = self._build_visualization_note(viz_metadata)

            # Build executed query
            executed_query = ExecutedQuery(
                query_result=query_result,
                executed_sql=sql_to_execute,
                natural_language_answer=nl_answer,
                visualization_hint=viz_hint,
                visualization_note=viz_note,
                visualization_metadata=viz_metadata,
                key_insights=insights,
                source_citations=input.source_datapoints,
            )

            logger.info(
                f"[{self.name}] Execution complete: {query_result.row_count} rows, "
                f"{query_result.execution_time_ms:.1f}ms"
            )

            # Create metadata with LLM call count
            metadata = self._create_metadata()
            metadata.llm_calls = llm_calls

            return ExecutorAgentOutput(
                success=True,
                executed_query=executed_query,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"[{self.name}] Execution failed: {e}")
            raise
        finally:
            await connector.close()

    async def _get_connector(
        self, database_type: str, database_url: str | None
    ) -> BaseConnector:
        """
        Get database connector for specified type.

        Args:
            database_type: Type of database (postgresql, clickhouse, mysql)

        Returns:
            Database connector instance
        """
        if database_url:
            db_url = database_url
        elif self.config.database.url:
            db_url = str(self.config.database.url)
        else:
            raise ValueError("DATABASE_URL is not configured for query execution.")

        connector = create_connector(
            database_url=db_url,
            database_type=database_type,
            pool_size=self.config.database.pool_size,
        )

        await connector.connect()
        return connector

    async def _execute_query(
        self,
        connector: BaseConnector,
        sql: str,
        max_rows: int,
        timeout_seconds: int,
    ) -> QueryResult:
        """
        Execute SQL query with timeout and result truncation.

        Args:
            connector: Database connector
            sql: SQL query to execute
            max_rows: Maximum rows to return
            timeout_seconds: Query timeout in seconds

        Returns:
            QueryResult with data

        Raises:
            QueryError: If query execution fails
            TimeoutError: If query exceeds timeout
        """
        start_time = time.time()

        try:
            # Execute with timeout
            result = await asyncio.wait_for(
                connector.execute(sql),
                timeout=timeout_seconds,
            )

            execution_time_ms = (time.time() - start_time) * 1000

            # Truncate if needed
            rows = result.rows
            was_truncated = False
            if len(rows) > max_rows:
                rows = rows[:max_rows]
                was_truncated = True

            return QueryResult(
                rows=rows,
                row_count=len(rows),
                columns=result.columns,
                execution_time_ms=execution_time_ms,
                was_truncated=was_truncated,
                max_rows=max_rows if was_truncated else None,
            )

        except TimeoutError as e:
            raise TimeoutError(f"Query exceeded timeout of {timeout_seconds}s") from e
        except Exception as e:
            raise QueryError(f"Query execution failed: {e}") from e

    async def _attempt_sql_correction(
        self,
        input: ExecutorAgentInput,
        sql: str,
        error: Exception,
        schema_context: str,
    ) -> str | None:
        issues = "\n".join(
            [
                f"- EXECUTION_ERROR: {error}",
                f"- DATABASE_ENGINE: {input.database_type}",
            ]
        )
        prompt = self.prompts.render(
            "agents/sql_correction.md",
            original_sql=sql,
            issues=issues,
            schema_context=schema_context,
        )

        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=self.prompts.load("system/main.md")),
                LLMMessage(role="user", content=prompt),
            ],
            temperature=0.0,
            max_tokens=2000,
        )

        try:
            response = await self.llm.generate(request)
            corrected = self._parse_correction_response(response.content)
            if not corrected or corrected.strip().lower() == sql.strip().lower():
                return None
            return corrected
        except Exception as exc:
            logger.error(f"SQL execution correction failed: {exc}")
            return None

    def _should_probe_schema(self, error: Exception, database_type: str) -> bool:
        if database_type != "postgresql":
            return False
        message = str(error).lower()
        return bool(
            re.search(r"relation .* does not exist", message)
            or "does not exist" in message
            or "undefined table" in message
        )

    async def _fetch_schema_context(
        self, connector: BaseConnector, database_type: str
    ) -> str | None:
        if database_type != "postgresql":
            return None

        tables_query = (
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY table_schema, table_name"
        )
        try:
            result = await connector.execute(tables_query)
        except Exception as exc:
            logger.warning(f"Failed to fetch schema context: {exc}")
            return None

        if not result.rows:
            return None

        entries = []
        qualified_tables = []
        for row in result.rows[:200]:
            schema = row.get("table_schema")
            table = row.get("table_name")
            if schema and table:
                qualified = f"{schema}.{table}"
                entries.append(qualified)
                qualified_tables.append(qualified)
            elif table:
                entries.append(str(table))

        if not entries:
            return None

        tables = ", ".join(entries)

        columns_context = ""
        if qualified_tables:
            columns_query = (
                "SELECT table_schema, table_name, column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name, ordinal_position"
            )
            try:
                columns_result = await connector.execute(columns_query)
                if columns_result.rows:
                    columns_by_table: dict[str, list[str]] = {}
                    for row in columns_result.rows:
                        schema = row.get("table_schema")
                        table = row.get("table_name")
                        column = row.get("column_name")
                        dtype = row.get("data_type")
                        if not (schema and table and column):
                            continue
                        key = f"{schema}.{table}"
                        if key not in qualified_tables:
                            continue
                        columns_by_table.setdefault(key, []).append(
                            f"{column} ({dtype})" if dtype else str(column)
                        )
                    if columns_by_table:
                        lines = []
                        for table in sorted(columns_by_table):
                            columns = columns_by_table[table]
                            if columns:
                                lines.append(f"- {table}: {', '.join(columns[:30])}")
                        if lines:
                            columns_context = "\n**Columns:**\n" + "\n".join(lines)
            except Exception as exc:
                logger.warning(f"Failed to fetch column context: {exc}")

        return f"**Tables in database:** {tables}{columns_context}"

    async def _fetch_schema_catalog(
        self, connector: BaseConnector, database_type: str
    ) -> set[str] | None:
        if database_type != "postgresql":
            return None

        tables_query = (
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY table_schema, table_name"
        )
        try:
            result = await connector.execute(tables_query)
        except Exception as exc:
            logger.warning(f"Failed to fetch schema catalog: {exc}")
            return None

        catalog: set[str] = set()
        for row in result.rows:
            schema = row.get("table_schema")
            table = row.get("table_name")
            if schema and table:
                catalog.add(f"{schema}.{table}".lower())
            elif table:
                catalog.add(str(table).lower())
        return catalog or None

    def _extract_missing_relation(self, error: Exception) -> str | None:
        message = str(error)
        match = re.search(r'relation \"([^\"]+)\" does not exist', message, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _relation_in_catalog(self, relation: str, catalog: set[str]) -> bool:
        normalized = relation.lower()
        if normalized in catalog:
            return True
        if "." in normalized:
            _, table = normalized.split(".", 1)
            return table in catalog
        return False

    def _parse_correction_response(self, content: str) -> str | None:
        payload = self._extract_json_payload(content)
        if payload:
            sql = payload.get("sql")
            if isinstance(sql, str) and sql.strip():
                return sql.strip()

        fenced = self._extract_code_block(content)
        if fenced:
            return fenced
        return None

    def _extract_json_payload(self, content: str) -> dict | None:
        try:
            payload = json.loads(content)
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass

        fenced = self._extract_code_block(content, include_json=True)
        if fenced:
            stripped = fenced.strip()
            if stripped.startswith("json"):
                stripped = stripped[4:].strip()
            try:
                payload = json.loads(stripped)
                if isinstance(payload, dict):
                    return payload
            except json.JSONDecodeError:
                pass

        brace_start = content.find("{")
        brace_end = content.rfind("}")
        if brace_start != -1 and brace_end != -1 and brace_end > brace_start:
            snippet = content[brace_start : brace_end + 1]
            try:
                payload = json.loads(snippet)
                if isinstance(payload, dict):
                    return payload
            except json.JSONDecodeError:
                pass

        return None

    def _extract_code_block(self, content: str, include_json: bool = False) -> str | None:
        if "```" not in content:
            return None
        chunks = content.split("```")
        for i in range(1, len(chunks), 2):
            block = chunks[i].strip()
            if block.startswith("sql"):
                block = block[3:].strip()
            elif include_json and block.startswith("json"):
                block = block[4:].strip()
            if block:
                return block
        return None

    def _load_schema_context(self, datapoint_ids: list[str]) -> str:
        if not datapoint_ids:
            return "No schema datapoints available."
        context_parts: list[str] = []
        data_dir = Path("datapoints") / "managed"
        for datapoint_id in datapoint_ids:
            path = data_dir / f"{datapoint_id}.json"
            if not path.exists():
                continue
            try:
                with path.open() as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue

            if payload.get("type") != "Schema":
                continue

            table_name = payload.get("table_name", "unknown")
            schema_name = payload.get("schema") or payload.get("schema_name")
            full_name = (
                f"{schema_name}.{table_name}"
                if schema_name and "." not in table_name
                else table_name
            )
            context_parts.append(f"\n**Table: {full_name}**")
            if payload.get("business_purpose"):
                context_parts.append(f"Purpose: {payload['business_purpose']}")
            columns = payload.get("key_columns") or []
            if columns:
                context_parts.append("Columns:")
                for column in columns:
                    column_name = column.get("name", "unknown")
                    column_type = column.get("type", "unknown")
                    context_parts.append(f"- {column_name} ({column_type})")

        return "\n".join(context_parts) if context_parts else "No schema datapoints available."

    async def _generate_summary(
        self, original_query: str, sql: str, query_result: QueryResult
    ) -> tuple[str, list[str]]:
        """
        Generate natural language summary of query results.

        Args:
            original_query: User's original question
            sql: SQL query that was executed
            query_result: Query results

        Returns:
            Tuple of (natural language answer, key insights)
        """
        try:
            # Build summary prompt
            prompt = self._build_summary_prompt(original_query, sql, query_result)

            # Call LLM
            request = LLMRequest(
                messages=[
                    LLMMessage(
                        role="system",
                        content=self.prompts.load("system/main.md"),
                    ),
                    LLMMessage(role="user", content=prompt),
                ],
                temperature=0.3,
            )
            response = await self.llm.generate(request)

            # Parse response (expecting "Answer: ... Insights: ..." format)
            answer, insights = self._parse_summary_response(response.content, query_result)

            return answer, insights

        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
            # Fallback to basic summary
            return self._generate_basic_summary(query_result), []

    def _generate_deterministic_summary(
        self, original_query: str, sql: str, query_result: QueryResult
    ) -> tuple[str, list[str]] | None:
        if query_result.row_count == 0:
            requested_table = self._extract_information_schema_target_table(sql)
            table_name = self._extract_table_name_from_sql(sql)
            if "information_schema.columns" in sql.lower() and table_name:
                return (
                    f"No columns were found for table `{requested_table or table_name}`.",
                    [],
                )
            if table_name:
                return (f"No results found for `{table_name}`.", [])
            return ("No results found.", [])

        lower_sql = sql.lower()
        if "information_schema.columns" in lower_sql:
            grouped_columns = self._group_catalog_columns_by_table(query_result.rows)
            if grouped_columns and (
                len(grouped_columns) > 1
                or not self._has_information_schema_table_filter(sql)
                or self._is_capability_discovery_query(original_query)
            ):
                overview = self._build_catalog_overview_answer(
                    original_query=original_query,
                    table_columns=grouped_columns,
                )
                if overview:
                    return (overview, [])

            table_name = (
                self._extract_information_schema_target_table(sql)
                or self._extract_table_name_from_sql(sql)
                or "the selected table"
            )
            column_values = []
            for row in query_result.rows:
                value = self._row_get(row, "column_name")
                if value is not None:
                    column_values.append(str(value))
            unique_columns = list(dict.fromkeys(column_values))
            if not unique_columns:
                return (f"No columns were found for table `{table_name}`.", [])
            preview = ", ".join(unique_columns[:10])
            suffix = (
                f" (and {len(unique_columns) - 10} more)"
                if len(unique_columns) > 10
                else ""
            )
            return (
                f"The `{table_name}` table has {len(unique_columns)} column(s): {preview}{suffix}.",
                [],
            )

        if "information_schema.tables" in lower_sql:
            table_names = []
            for row in query_result.rows:
                table = self._row_get(row, "table_name")
                schema = self._row_get(row, "table_schema")
                if table is None:
                    continue
                table_names.append(f"{schema}.{table}" if schema else str(table))
            unique_tables = list(dict.fromkeys(table_names))
            if not unique_tables:
                return ("No tables were found.", [])
            if self._is_capability_discovery_query(original_query):
                capability_summary = self._build_table_list_capability_answer(unique_tables)
                if capability_summary:
                    return (capability_summary, [])
            preview = ", ".join(unique_tables[:10])
            suffix = (
                f" (and {len(unique_tables) - 10} more)"
                if len(unique_tables) > 10
                else ""
            )
            return (
                f"Found {len(unique_tables)} table(s): {preview}{suffix}.",
                [],
            )

        if query_result.row_count == 1 and query_result.columns == ["row_count"]:
            value = self._row_get(query_result.rows[0], "row_count")
            table_name = self._extract_table_name_from_sql(sql)
            if table_name:
                return (f"Table `{table_name}` has {value} row(s).", [])
            return (f"The row count is {value}.", [])

        if re.match(r"^\s*select\s+\*\s+from\s+", lower_sql):
            table_name = self._extract_table_name_from_sql(sql)
            if table_name:
                return (
                    f"Returned {query_result.row_count} row(s) from `{table_name}`.",
                    [],
                )

        return None

    def _row_get(self, row: Any, key: str) -> Any:
        """Fetch row values with case-insensitive key fallback."""
        if not isinstance(row, dict):
            return None
        if key in row:
            return row[key]
        lowered = key.lower()
        for candidate, value in row.items():
            if str(candidate).lower() == lowered:
                return value
        return None

    def _extract_table_name_from_sql(self, sql: str) -> str | None:
        match = re.search(
            r'\bfrom\s+(`[^`]+`|(?:"[^"]+"(?:\."[^"]+")*)|[a-zA-Z0-9_.-]+)',
            sql,
            re.IGNORECASE,
        )
        if not match:
            return None

        identifier = match.group(1).strip()
        if identifier.startswith("`") and identifier.endswith("`"):
            return identifier[1:-1]
        if '"' in identifier:
            parts = [part.strip('"') for part in identifier.split(".")]
            return ".".join(parts)
        return identifier

    def _extract_information_schema_target_table(self, sql: str) -> str | None:
        match = re.search(
            r"\btable_name\s*=\s*'([^']+)'",
            sql,
            re.IGNORECASE,
        )
        if not match:
            return None
        return match.group(1).strip()

    def _has_information_schema_table_filter(self, sql: str) -> bool:
        return bool(re.search(r"\btable_name\s*=\s*'[^']+'", sql, re.IGNORECASE))

    def _is_capability_discovery_query(self, query: str) -> bool:
        text = (query or "").lower()
        patterns = (
            r"\bwhat kind of info\b",
            r"\bwhat can i ask\b",
            r"\bwhat can you answer\b",
            r"\bwhat.*available tables\b",
            r"\bavailable tables\b",
            r"\bhelp.*understand.*tables\b",
            r"\bwhich tables should i use\b",
        )
        return any(re.search(pattern, text) for pattern in patterns)

    def _group_catalog_columns_by_table(self, rows: list[dict[str, Any]]) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            table = self._row_get(row, "table_name")
            if table is None:
                continue
            schema = self._row_get(row, "table_schema")
            table_name = str(table)
            full_name = f"{schema}.{table_name}" if schema else table_name
            column = self._row_get(row, "column_name")
            entry = grouped.setdefault(full_name, [])
            if column is not None:
                column_name = str(column)
                if column_name not in entry:
                    entry.append(column_name)
        return grouped

    def _build_table_list_capability_answer(self, table_names: list[str]) -> str | None:
        if not table_names:
            return None
        business_tables = [name for name in table_names if not self._is_system_table_name(name)]
        target_tables = business_tables or table_names
        themes = self._infer_catalog_domains({name: [] for name in target_tables})
        preview = ", ".join(target_tables[:8])
        suffix = f" (and {len(target_tables) - 8} more)" if len(target_tables) > 8 else ""
        message = (
            f"I found {len(target_tables)} available table(s): {preview}{suffix}. "
            "I can answer questions by joining these tables and aggregating trends, counts, and drivers."
        )
        if themes:
            message += f" Likely analysis areas: {', '.join(themes[:4])}."
        return message

    def _build_catalog_overview_answer(
        self, *, original_query: str, table_columns: dict[str, list[str]]
    ) -> str | None:
        if not table_columns:
            return None

        business_tables = {
            name: columns
            for name, columns in table_columns.items()
            if not self._is_system_table_name(name)
        }
        if not business_tables:
            return (
                "I inspected system catalog metadata only. "
                "Ask me to list available user tables so I can summarize business data domains."
            )

        themes = self._infer_catalog_domains(business_tables)
        highlights: list[str] = []
        for table_name in sorted(business_tables.keys())[:6]:
            columns = business_tables.get(table_name, [])
            if columns:
                preview = ", ".join(columns[:5])
                suffix = f" (+{len(columns) - 5} more)" if len(columns) > 5 else ""
                highlights.append(f"{table_name}: {preview}{suffix}")
            else:
                highlights.append(table_name)

        intro = f"I can query {len(business_tables)} table(s) from your database."
        if self._is_capability_discovery_query(original_query):
            intro = (
                f"From your available tables, I can answer questions across {len(business_tables)} table(s)."
            )
        if themes:
            intro += f" Likely analysis areas: {', '.join(themes[:4])}."
        return f"{intro} Table highlights: {'; '.join(highlights)}."

    def _is_system_table_name(self, table_name: str) -> bool:
        lower = table_name.lower()
        system_prefixes = (
            "information_schema.",
            "pg_catalog.",
            "mysql.",
            "performance_schema.",
            "sys.",
            "system.",
        )
        return lower.startswith(system_prefixes)

    def _infer_catalog_domains(self, table_columns: dict[str, list[str]]) -> list[str]:
        if not table_columns:
            return []

        corpus = " ".join(
            [
                " ".join(table_columns.keys()),
                " ".join(
                    " ".join(columns)
                    for columns in table_columns.values()
                    if isinstance(columns, list)
                ),
            ]
        ).lower()
        tokens = set(re.findall(r"[a-z0-9_]+", corpus))

        themes = [
            (
                "customer segmentation",
                {"customer", "segment", "kyc", "profile", "user", "account_holder"},
            ),
            (
                "transactions and payments",
                {"transaction", "payment", "posted", "declined", "reversed", "merchant", "fee"},
            ),
            (
                "balances and cash flow",
                {"balance", "deposit", "withdrawal", "net", "flow", "ledger", "account"},
            ),
            (
                "lending and risk",
                {"loan", "default", "delinquency", "past_due", "risk", "credit", "exposure"},
            ),
            (
                "inventory and operations",
                {"inventory", "stock", "sku", "product", "store", "reorder", "category"},
            ),
            (
                "time-series reporting",
                {"date", "week", "month", "quarter", "year", "timestamp", "snapshot", "created"},
            ),
        ]

        detected: list[str] = []
        for label, keywords in themes:
            if any(keyword in tokens for keyword in keywords):
                detected.append(label)
        return detected

    def _build_summary_prompt(self, query: str, sql: str, query_result: QueryResult) -> str:
        """Build prompt for result summarization."""
        # Format results for prompt (limit to prevent token overflow)
        rows_sample = query_result.rows[:10]  # Show max 10 rows
        results_str = "\n".join([f"Row {i + 1}: {row}" for i, row in enumerate(rows_sample)])

        if query_result.was_truncated:
            results_str += f"\n... (showing {len(rows_sample)} of {query_result.row_count} rows)"

        return self.prompts.render(
            "agents/executor_summary.md",
            user_query=query,
            sql_query=sql,
            results=results_str,
        )

    def _parse_summary_response(
        self, response: str, query_result: QueryResult
    ) -> tuple[str, list[str]]:
        """Parse LLM summary response."""
        lines = response.strip().split("\n")

        answer = ""
        insights = []

        for line in lines:
            if line.startswith("Answer:"):
                answer = line.replace("Answer:", "").strip()
            elif line.startswith("Insights:"):
                insights_text = line.replace("Insights:", "").strip()
                insights = [i.strip("- ").strip() for i in insights_text.split("\n") if i.strip()]
            elif line.strip().startswith("-") or line.strip().startswith("•"):
                insights.append(line.strip("- •").strip())

        # Fallback if parsing fails
        if not answer:
            answer = self._generate_basic_summary(query_result)

        return answer, insights

    def _generate_basic_summary(self, query_result: QueryResult) -> str:
        """Generate basic summary without LLM."""
        if query_result.row_count == 0:
            return "No results found."

        if query_result.row_count == 1:
            # Single row - describe it
            row = query_result.rows[0]
            if len(row) == 1:
                key, value = list(row.items())[0]
                return f"The {key} is {value}."
            return f"Found 1 result with {len(row)} columns."

        return f"Found {query_result.row_count} results."

    async def _recommend_visualization(
        self,
        *,
        query_result: QueryResult,
        original_query: str | None,
        sql: str,
    ) -> tuple[str, dict[str, Any], int]:
        shape = self._analyze_result_shape(query_result)
        requested_hint = self._requested_visualization_hint(original_query)
        deterministic_hint = self._suggest_visualization_deterministic(
            query_result=query_result,
            original_query=original_query,
            shape=shape,
        )
        allowed_hints = sorted(self._compatible_visualizations(shape))
        allowed_hint_set = set(allowed_hints)
        requested_valid = bool(requested_hint and requested_hint in allowed_hint_set)

        llm_suggested: str | None = None
        llm_reason: str | None = None
        llm_calls = 0
        if self._should_use_visualization_llm(
            shape=shape,
            requested_hint=requested_hint,
            requested_valid=requested_valid,
            deterministic_hint=deterministic_hint,
        ):
            llm_suggested, llm_reason, attempted = await self._suggest_visualization_with_llm(
                query_result=query_result,
                original_query=original_query,
                sql=sql,
                allowed_hints=allowed_hints,
                deterministic_hint=deterministic_hint,
            )
            if attempted:
                llm_calls = 1

        final_hint, resolution_reason = self._resolve_visualization_choice(
            requested_hint=requested_hint,
            deterministic_hint=deterministic_hint,
            llm_suggested=llm_suggested,
            allowed_hints=allowed_hint_set,
        )
        metadata = {
            "requested": requested_hint,
            "deterministic": deterministic_hint,
            "llm_suggested": llm_suggested,
            "llm_reason": llm_reason,
            "final": final_hint,
            "resolution_reason": resolution_reason,
            "allowed": allowed_hints,
            "shape": shape,
        }
        return final_hint, metadata, llm_calls

    def _suggest_visualization(
        self, query_result: QueryResult, original_query: str | None = None
    ) -> str:
        return self._suggest_visualization_deterministic(
            query_result=query_result,
            original_query=original_query,
        )

    def _analyze_result_shape(self, query_result: QueryResult) -> dict[str, Any]:
        sample_rows = query_result.rows[: min(len(query_result.rows), 50)]
        numeric_col_count = sum(
            1
            for col in query_result.columns
            if any(self._is_numeric_value(row.get(col)) for row in sample_rows)
        )
        has_negative_numeric = any(
            isinstance(value, (int, float, Decimal)) and value < 0
            for row in sample_rows
            for value in row.values()
        )
        has_time_dimension = any(
            self._is_temporal_column(col, sample_rows) for col in query_result.columns
        )
        return {
            "num_rows": query_result.row_count,
            "num_cols": len(query_result.columns),
            "numeric_col_count": numeric_col_count,
            "has_negative_numeric": has_negative_numeric,
            "has_time_dimension": has_time_dimension,
            "columns": list(query_result.columns),
        }

    def _suggest_visualization_deterministic(
        self,
        *,
        query_result: QueryResult,
        original_query: str | None = None,
        shape: dict[str, Any] | None = None,
    ) -> str:
        if query_result.row_count == 0:
            return "none"

        if shape is None:
            shape = self._analyze_result_shape(query_result)
        num_cols = int(shape.get("num_cols", 0))
        num_rows = int(shape.get("num_rows", 0))
        numeric_col_count = int(shape.get("numeric_col_count", 0))
        has_negative_numeric = bool(shape.get("has_negative_numeric", False))
        has_time_dimension = bool(shape.get("has_time_dimension", False))

        # Single value
        if num_rows == 1 and num_cols == 1:
            return "none"

        requested_hint = self._requested_visualization_hint(original_query)
        if requested_hint == "none":
            return "none"
        if requested_hint == "table":
            return "table"
        if requested_hint == "line_chart" and num_cols >= 2 and num_rows >= 2 and has_time_dimension:
            return "line_chart"
        if requested_hint == "bar_chart" and num_cols >= 2 and num_rows >= 2 and numeric_col_count >= 1:
            return "bar_chart"
        if (
            requested_hint == "pie_chart"
            and num_cols >= 2
            and num_rows >= 2
            and num_rows <= 12
            and numeric_col_count >= 1
            and not has_negative_numeric
        ):
            return "pie_chart"
        if requested_hint == "scatter" and num_cols >= 2 and num_rows >= 2 and numeric_col_count >= 2:
            return "scatter"
        if requested_hint == "line_chart" and num_cols >= 2 and num_rows >= 2 and numeric_col_count >= 1:
            return "bar_chart"

        # Time series detection (prioritize over other 2-column logic)
        if has_time_dimension and num_cols >= 2 and numeric_col_count >= 1:
            return "line_chart"

        # Two columns - likely category + value
        if num_cols == 2:
            if num_rows <= 10:
                return "bar_chart"
            if num_rows <= 20:
                return "line_chart"
            return "table"

        # Multiple columns - scatter or table
        if num_cols >= 3:
            if (
                (requested_hint == "scatter" or self._looks_like_scatter_intent(original_query))
                and num_rows <= 100
                and numeric_col_count >= 2
            ):
                return "scatter"
            if num_rows <= 25 and numeric_col_count >= 1:
                return "bar_chart"
            return "table"

        return "table"

    def _compatible_visualizations(self, shape: dict[str, Any]) -> set[str]:
        num_rows = int(shape.get("num_rows", 0))
        num_cols = int(shape.get("num_cols", 0))
        numeric_col_count = int(shape.get("numeric_col_count", 0))
        has_negative_numeric = bool(shape.get("has_negative_numeric", False))
        has_time_dimension = bool(shape.get("has_time_dimension", False))

        if num_rows == 0:
            return {"none"}
        if num_rows == 1 and num_cols == 1:
            return {"none", "table"}

        allowed = {"table", "none"}
        if num_cols >= 2 and num_rows >= 2 and numeric_col_count >= 1:
            allowed.add("bar_chart")
        if has_time_dimension and num_cols >= 2 and num_rows >= 2 and numeric_col_count >= 1:
            allowed.add("line_chart")
        if (
            num_cols >= 2
            and num_rows >= 2
            and num_rows <= 12
            and numeric_col_count >= 1
            and not has_negative_numeric
        ):
            allowed.add("pie_chart")
        if num_cols >= 2 and num_rows >= 2 and numeric_col_count >= 2:
            allowed.add("scatter")
        return allowed

    def _should_use_visualization_llm(
        self,
        *,
        shape: dict[str, Any],
        requested_hint: str | None,
        requested_valid: bool,
        deterministic_hint: str,
    ) -> bool:
        if not bool(getattr(self.config.pipeline, "visualization_llm_enabled", True)):
            return False
        num_rows = int(shape.get("num_rows", 0))
        num_cols = int(shape.get("num_cols", 0))
        numeric_col_count = int(shape.get("numeric_col_count", 0))
        has_time_dimension = bool(shape.get("has_time_dimension", False))
        if num_rows == 0 or (num_rows == 1 and num_cols == 1):
            return False
        if requested_hint is not None:
            return not requested_valid
        if has_time_dimension and deterministic_hint == "line_chart":
            return False
        if num_cols == 2 and numeric_col_count >= 1 and 2 <= num_rows <= 12:
            return True
        if num_cols >= 3 and numeric_col_count >= 2 and num_rows <= 100:
            return True
        return False

    async def _suggest_visualization_with_llm(
        self,
        *,
        query_result: QueryResult,
        original_query: str | None,
        sql: str,
        allowed_hints: list[str],
        deterministic_hint: str,
    ) -> tuple[str | None, str | None, bool]:
        row_sample_limit = int(getattr(self.config.pipeline, "visualization_llm_row_sample", 8))
        row_sample = query_result.rows[: max(2, min(row_sample_limit, len(query_result.rows)))]
        prompt = (
            "You are selecting the best chart type for SQL results.\n"
            "Return ONLY JSON with keys: visualization, reason, confidence.\n"
            "visualization must be one of the allowed types.\n\n"
            f"USER_QUERY: {original_query or ''}\n"
            f"SQL: {sql}\n"
            f"ALLOWED: {', '.join(allowed_hints)}\n"
            f"DETERMINISTIC_DEFAULT: {deterministic_hint}\n"
            f"ROW_COUNT: {query_result.row_count}\n"
            f"COLUMNS: {', '.join(query_result.columns)}\n"
            f"ROW_SAMPLE: {json.dumps(row_sample, default=str)}\n"
        )
        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content="Return strict JSON only."),
                LLMMessage(role="user", content=prompt),
            ],
            temperature=0.0,
            max_tokens=180,
        )
        try:
            response = await self.llm.generate(request)
            content = response.content if isinstance(response.content, str) else str(response.content)
            match = re.search(r"\{[\s\S]*\}", content)
            if not match:
                return None, None, True
            payload = json.loads(match.group(0))
            suggested = str(payload.get("visualization", "")).strip().lower()
            reason = str(payload.get("reason", "")).strip() or None
            if suggested in set(allowed_hints):
                return suggested, reason, True
            return None, reason, True
        except Exception:
            return None, None, True

    def _resolve_visualization_choice(
        self,
        *,
        requested_hint: str | None,
        deterministic_hint: str,
        llm_suggested: str | None,
        allowed_hints: set[str],
    ) -> tuple[str, str]:
        if requested_hint == "none":
            return "none", "user_requested_none"
        if requested_hint == "table":
            return "table", "user_requested_table"
        if requested_hint and requested_hint in allowed_hints:
            return requested_hint, "user_requested_valid"
        if requested_hint and requested_hint not in allowed_hints:
            if llm_suggested and llm_suggested in allowed_hints:
                return llm_suggested, "user_request_incompatible_using_llm_override"
            if deterministic_hint in allowed_hints:
                return deterministic_hint, "user_request_incompatible_using_deterministic_fallback"
            return "table", "user_request_incompatible_default_table"
        if llm_suggested and llm_suggested in allowed_hints:
            return llm_suggested, "llm_recommended"
        if deterministic_hint in allowed_hints:
            return deterministic_hint, "deterministic_default"
        return "table", "safe_table_fallback"

    def _build_visualization_note(self, metadata: dict[str, Any]) -> str | None:
        if not metadata:
            return None
        reason = str(metadata.get("resolution_reason") or "")
        if not reason.startswith("user_request_incompatible"):
            return None

        requested = self._format_visualization_label(metadata.get("requested"))
        final = self._format_visualization_label(metadata.get("final"))
        detail = str(metadata.get("llm_reason") or "").strip()
        if not detail:
            detail = "the data shape does not support it"
        return f"Requested {requested} was overridden to {final} because {detail}."

    @staticmethod
    def _format_visualization_label(hint: Any) -> str:
        mapping = {
            "bar_chart": "bar chart",
            "line_chart": "line chart",
            "pie_chart": "pie chart",
            "scatter": "scatter plot",
            "table": "table",
            "none": "no visualization",
        }
        key = str(hint or "").strip().lower()
        return mapping.get(key, key or "chart")

    @staticmethod
    def _looks_like_scatter_intent(query: str | None) -> bool:
        text = str(query or "").lower()
        if not text:
            return False
        return bool(
            re.search(
                r"\b(scatter|correlation|correlate|relationship|vs\.?|versus)\b",
                text,
            )
        )

    def _requested_visualization_hint(self, query: str | None) -> str | None:
        """Infer user visualization preference from query text."""
        text = (query or "").lower()
        if not text:
            return None

        if re.search(r"\b(no chart|without chart|table only|just table)\b", text):
            return "table"
        if re.search(r"\b(no visualization|text only)\b", text):
            return "none"
        if re.search(r"\b(bar chart|bar graph|histogram)\b", text):
            return "bar_chart"
        if re.search(r"\bpie chart|donut\b", text):
            return "pie_chart"
        if re.search(r"\bscatter|correlation\b", text):
            return "scatter"
        if re.search(r"\b(line chart|line graph|trend|time series|over time)\b", text):
            return "line_chart"
        return None

    @staticmethod
    def _is_numeric_value(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        return isinstance(value, (int, float, Decimal))

    def _is_temporal_column(self, column_name: str, rows: list[dict[str, Any]]) -> bool:
        lowered = column_name.lower()
        tokens = [token for token in re.split(r"[^a-z0-9]+", lowered) if token]
        direct_markers = {"date", "time", "timestamp", "datetime"}
        period_markers = {"day", "week", "month", "quarter", "year"}
        period_disqualifiers = {"type", "category", "name", "code"}

        if any(marker in tokens for marker in direct_markers):
            return True
        if any(marker in tokens for marker in period_markers) and not any(
            disqualifier in tokens for disqualifier in period_disqualifiers
        ):
            return True
        if len(tokens) >= 2 and tokens[-1] == "at" and tokens[-2] in {
            "created",
            "updated",
            "deleted",
            "opened",
            "closed",
            "posted",
            "processed",
            "occurred",
            "recorded",
        }:
            return True
        for row in rows[:20]:
            if self._is_temporal_value(row.get(column_name)):
                return True
        return False

    @staticmethod
    def _is_temporal_value(value: Any) -> bool:
        if isinstance(value, (datetime, date)):
            return True
        if not isinstance(value, str):
            return False
        candidate = value.strip()
        if len(candidate) < 8:
            return False
        try:
            normalized = candidate.replace("Z", "+00:00")
            datetime.fromisoformat(normalized)
            return True
        except ValueError:
            return False

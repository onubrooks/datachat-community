import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any

from backend.agents.base import BaseAgent
from backend.config import get_settings
from backend.connectors.base import BaseConnector
from backend.connectors.factory import create_connector
from backend.database.catalog import CatalogIntelligence
from backend.database.catalog_templates import (
    get_catalog_aliases,
    get_catalog_schemas,
    get_list_tables_query,
)
from backend.database.operator_templates import build_operator_guidance, match_operator_templates
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models.agent import (
    AgentMetadata,
    CorrectionAttempt,
    GeneratedSQL,
    LLMError,
    SQLAgentInput,
    SQLAgentOutput,
    SQLGenerationError,
    ValidationIssue,
)
from backend.profiling.cache import load_profile_cache
from backend.prompts.loader import PromptLoader

logger = logging.getLogger(__name__)


class SQLClarificationNeeded(Exception):
    """Raised when SQL generation needs user clarification."""

    def __init__(self, questions: list[str]) -> None:
        super().__init__("SQL generation needs clarification")
        self.questions = questions


@dataclass(frozen=True)
class QueryCompilerPlan:
    """Compiled semantic query plan used to prime SQL generation."""

    query: str
    operators: list[str]
    candidate_tables: list[str]
    selected_tables: list[str]
    join_hypotheses: list[str]
    column_hints: list[str]
    confidence: float
    path: str
    reason: str

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


class SQLAgent(BaseAgent):
    """
    SQL generation agent with self-correction.

    Generates SQL queries from natural language using LLM and context from
    ContextAgent. Includes self-correction to fix syntax errors, missing
    columns, table name issues, etc.

    Usage:
        agent = SQLAgent()

        input = SQLAgentInput(
            query="What were total sales last quarter?",
            investigation_memory=context_output.investigation_memory
        )

        output = await agent(input)
        sql = output.generated_sql.sql
    """

    def __init__(self, llm_provider=None):
        """
        Initialize SQLAgent with LLM provider.

        Args:
            llm_provider: Optional LLM provider. If None, creates default provider.
        """
        super().__init__(name="SQLAgent")

        # Get configuration
        self.config = get_settings()

        # Create LLM provider using factory (respects sql_provider override)
        if llm_provider is None:
            self.llm = LLMProviderFactory.create_agent_provider(
                agent_name="sql",
                config=self.config.llm,
                model_type="main",  # Use main model (GPT-4o) for SQL generation
            )
            self.fast_llm = LLMProviderFactory.create_agent_provider(
                agent_name="sql",
                config=self.config.llm,
                model_type="mini",
            )
            self.formatter_llm = self.fast_llm
        else:
            self.llm = llm_provider
            self.fast_llm = llm_provider
            self.formatter_llm = llm_provider

        provider_name = getattr(self.llm, "provider", "unknown")
        model_name = getattr(self.llm, "model", "unknown")
        logger.info(
            f"SQLAgent initialized with {provider_name} provider",
            extra={"provider": provider_name, "model": model_name},
        )
        self.prompts = PromptLoader()
        self.catalog = CatalogIntelligence()
        self._live_schema_cache: dict[str, str] = {}
        self._live_schema_snapshot_cache: dict[str, dict[str, Any]] = {}
        self._live_schema_tables_cache: dict[str, set[str]] = {}
        self._live_profile_cache: dict[str, dict[str, dict[str, object]]] = {}
        self._live_semantic_cache: dict[str, str] = {}
        self._max_safe_row_limit = 100
        self._default_row_limit = 100

    def _pipeline_flag(self, name: str, default: bool) -> bool:
        pipeline_cfg = getattr(self.config, "pipeline", None)
        if pipeline_cfg is None:
            return default
        return bool(getattr(pipeline_cfg, name, default))

    def _pipeline_int(self, name: str, default: int) -> int:
        pipeline_cfg = getattr(self.config, "pipeline", None)
        if pipeline_cfg is None:
            return default
        value = getattr(pipeline_cfg, name, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _pipeline_float(self, name: str, default: float) -> float:
        pipeline_cfg = getattr(self.config, "pipeline", None)
        if pipeline_cfg is None:
            return default
        value = getattr(pipeline_cfg, name, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _providers_are_equivalent(self, primary: Any, secondary: Any) -> bool:
        """Return True when two providers resolve to the same effective model endpoint."""

        def _stored_attr(obj: Any, name: str) -> Any:
            data = getattr(obj, "__dict__", {})
            if isinstance(data, dict) and name in data:
                return data.get(name)
            return None

        if primary is secondary:
            return True
        if primary is None or secondary is None:
            return False

        primary_provider = (
            _stored_attr(primary, "provider_name")
            or _stored_attr(primary, "provider")
            or primary.__class__.__name__
        )
        secondary_provider = (
            _stored_attr(secondary, "provider_name")
            or _stored_attr(secondary, "provider")
            or secondary.__class__.__name__
        )
        if str(primary_provider).lower() != str(secondary_provider).lower():
            return False

        primary_model = _stored_attr(primary, "model")
        secondary_model = _stored_attr(secondary, "model")
        if (primary_model is None) != (secondary_model is None):
            return False
        if primary_model is not None and str(primary_model).lower() != str(secondary_model).lower():
            return False

        primary_base_url = _stored_attr(primary, "base_url")
        secondary_base_url = _stored_attr(secondary, "base_url")
        if (primary_base_url is None) != (secondary_base_url is None):
            return False
        if primary_base_url is not None and str(primary_base_url).rstrip("/") != str(
            secondary_base_url
        ).rstrip("/"):
            return False

        return True

    async def execute(self, input: SQLAgentInput) -> SQLAgentOutput:
        """
        Execute SQL generation with self-correction.

        Args:
            input: SQLAgentInput with query and investigation memory

        Returns:
            SQLAgentOutput with generated SQL and correction history

        Raises:
            SQLGenerationError: If SQL generation fails after all attempts
        """
        # Validate input type
        self._validate_input(input)

        logger.info(
            f"Generating SQL for query: {input.query[:100]}...",
            extra={
                "query_length": len(input.query),
                "num_datapoints": len(input.investigation_memory.datapoints),
            },
        )

        metadata = AgentMetadata(agent_name=self.name)
        correction_attempts: list[CorrectionAttempt] = []
        runtime_stats = {
            "formatter_fallback_calls": 0,
            "formatter_fallback_successes": 0,
            "query_compiler_llm_calls": 0,
            "query_compiler_llm_refinements": 0,
            "query_compiler_latency_ms": 0.0,
            "query_compiler": None,
        }

        try:
            # Initial SQL generation
            try:
                generated_sql = await self._generate_sql(input, metadata, runtime_stats)
            except SQLClarificationNeeded as exc:
                generated_sql = GeneratedSQL(
                    sql="SELECT 1",
                    explanation="Clarification needed before generating SQL.",
                    used_datapoints=[],
                    confidence=0.0,
                    assumptions=[],
                    clarifying_questions=exc.questions,
                )
                return SQLAgentOutput(
                    success=True,
                    data=runtime_stats,
                    metadata=metadata,
                    next_agent="ValidatorAgent",
                    generated_sql=generated_sql,
                    correction_attempts=[],
                    needs_clarification=True,
                )

            # Self-validation
            issues = self._validate_sql(generated_sql, input)

            # Self-correction loop if issues found
            attempt_number = 1
            while issues and attempt_number <= input.max_correction_attempts:
                logger.warning(
                    f"SQL validation found {len(issues)} issues, attempting correction #{attempt_number}",
                    extra={"issues": [issue.issue_type for issue in issues]},
                )

                # Record correction attempt
                original_sql = generated_sql.sql

                # Attempt correction
                corrected_sql = await self._correct_sql(
                    generated_sql=generated_sql,
                    issues=issues,
                    input=input,
                    metadata=metadata,
                    runtime_stats=runtime_stats,
                )

                # Validate corrected SQL
                new_issues = self._validate_sql(corrected_sql, input)
                success = len(new_issues) == 0

                # Record attempt
                correction_attempts.append(
                    CorrectionAttempt(
                        attempt_number=attempt_number,
                        original_sql=original_sql,
                        issues_found=issues,
                        corrected_sql=corrected_sql.sql,
                        success=success,
                    )
                )

                # Update for next iteration
                generated_sql = corrected_sql
                issues = new_issues
                attempt_number += 1

            # Check if we have unresolved issues
            if issues:
                logger.error(
                    f"Failed to resolve {len(issues)} validation issues after {input.max_correction_attempts} attempts",
                    extra={"issues": [issue.message for issue in issues]},
                )
                # Still return the best attempt we have, but mark needs_clarification
                needs_clarification = True
            else:
                needs_clarification = bool(generated_sql.clarifying_questions)

            logger.info(
                "SQL generation complete",
                extra={
                    "correction_attempts": len(correction_attempts),
                    "needs_clarification": needs_clarification,
                    "confidence": generated_sql.confidence,
                    "prompt_version": self.prompts.get_metadata("agents/sql_generator.md").get(
                        "version"
                    ),
                },
            )

            return SQLAgentOutput(
                success=True,
                data=runtime_stats,
                metadata=metadata,
                next_agent="ValidatorAgent",
                generated_sql=generated_sql,
                correction_attempts=correction_attempts,
                needs_clarification=needs_clarification,
            )

        except Exception as e:
            metadata.error = str(e)
            logger.error(f"SQL generation failed: {e}", exc_info=True)
            raise SQLGenerationError(
                agent=self.name,
                message=f"Failed to generate SQL: {e}",
                recoverable=False,
                context={"query": input.query},
            ) from e

    async def _generate_sql(
        self, input: SQLAgentInput, metadata: AgentMetadata, runtime_stats: dict[str, int]
    ) -> GeneratedSQL:
        """
        Generate SQL from user query and context.

        Args:
            input: SQLAgentInput
            metadata: AgentMetadata to track LLM calls

        Returns:
            GeneratedSQL with query and metadata

        Raises:
            LLMError: If LLM call fails
        """
        resolved_query, _ = self._resolve_followup_query(input)
        if resolved_query != input.query:
            input = input.model_copy(update={"query": resolved_query})

        catalog_plan = self.catalog.plan_query(
            query=input.query,
            database_type=input.database_type,
            investigation_memory=input.investigation_memory,
        )
        if catalog_plan and catalog_plan.clarifying_questions:
            raise SQLClarificationNeeded(catalog_plan.clarifying_questions)

        if catalog_plan and catalog_plan.sql:
            generated = GeneratedSQL(
                sql=catalog_plan.sql,
                explanation=catalog_plan.explanation,
                used_datapoints=[],
                confidence=catalog_plan.confidence,
                assumptions=[],
                clarifying_questions=[],
            )
            return self._apply_row_limit_policy(generated, input.query)

        query_dp_sql = self._try_query_datapoint_template(input)
        if query_dp_sql:
            runtime_stats["query_datapoint_template"] = True
            return self._apply_row_limit_policy(query_dp_sql, input.query)

        finance_loan_default_sql = self._build_finance_loan_default_rate_template(input)
        if finance_loan_default_sql:
            runtime_stats["finance_loan_default_rate_template"] = True
            return self._apply_row_limit_policy(finance_loan_default_sql, input.query)

        finance_flow_sql = self._build_finance_net_flow_template(input)
        if finance_flow_sql:
            runtime_stats["finance_net_flow_template"] = True
            return self._apply_row_limit_policy(finance_flow_sql, input.query)

        prompt, compiler_plan = await self._build_generation_prompt(
            input,
            runtime_stats=runtime_stats,
            return_plan=True,
        )
        if compiler_plan is not None:
            runtime_stats["query_compiler"] = compiler_plan.to_summary()

        # Create LLM request
        llm_request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=self._get_system_prompt()),
                LLMMessage(role="system", content=self._sql_output_contract_message()),
                LLMMessage(role="user", content=prompt),
            ],
            temperature=0.0,  # Deterministic for SQL generation
            max_tokens=2000,
        )

        try:
            use_two_stage = self._pipeline_flag(
                "sql_two_stage_enabled", True
            ) and not self._providers_are_equivalent(self.fast_llm, self.llm)

            if use_two_stage:
                fast_generated = await self._request_sql_from_llm(
                    provider=self.fast_llm,
                    llm_request=llm_request,
                    input=input,
                    runtime_stats=runtime_stats,
                )
                if self._should_accept_fast_sql(fast_generated, input):
                    generated_sql = fast_generated
                else:
                    generated_sql = await self._request_sql_from_llm(
                        provider=self.llm,
                        llm_request=llm_request,
                        input=input,
                        runtime_stats=runtime_stats,
                    )
            else:
                generated_sql = await self._request_sql_from_llm(
                    provider=self.llm,
                    llm_request=llm_request,
                    input=input,
                    runtime_stats=runtime_stats,
                )

            if self._should_force_best_effort_retry(
                generated_sql=generated_sql,
                query=input.query,
                compiler_plan=compiler_plan,
                input=input,
            ):
                forced_sql = await self._retry_best_effort_sql_generation(
                    input=input,
                    compiler_plan=compiler_plan,
                    runtime_stats=runtime_stats,
                )
                if forced_sql is not None and forced_sql.sql.strip():
                    generated_sql = forced_sql

            logger.debug(
                f"Generated SQL: {generated_sql.sql[:200]}...",
                extra={"confidence": generated_sql.confidence},
            )

            return generated_sql
        except SQLClarificationNeeded:
            if self._pipeline_flag("sql_force_best_effort_on_clarify", True):
                forced_sql = await self._retry_best_effort_sql_generation(
                    input=input,
                    compiler_plan=compiler_plan,
                    runtime_stats=runtime_stats,
                )
                if forced_sql is not None and forced_sql.sql.strip():
                    return forced_sql.model_copy(update={"clarifying_questions": []})
            raise
        except Exception as e:
            logger.error(f"LLM call failed: {e}", exc_info=True)
            raise LLMError(
                agent=self.name,
                message=f"LLM generation failed: {e}",
                context={"query": input.query},
            ) from e

    async def _request_sql_from_llm(
        self,
        *,
        provider: Any,
        llm_request: LLMRequest,
        input: SQLAgentInput,
        runtime_stats: dict[str, int],
    ) -> GeneratedSQL:
        response = await provider.generate(llm_request)
        self._track_llm_call(tokens=self._safe_total_tokens(response))
        response_content = self._coerce_response_content(response.content)

        try:
            return self._parse_llm_response(response_content, input)
        except ValueError:
            if self._looks_truncated_response(response_content, response.finish_reason):
                recovered = await self._recover_sql_with_formatter(
                    raw_content=response_content,
                    input=input,
                    runtime_stats=runtime_stats,
                )
                if recovered is not None:
                    return recovered

            retry_request = llm_request.model_copy()
            retry_request.messages.append(
                LLMMessage(
                    role="system",
                    content=(
                        "Previous output was malformed. "
                        "Return ONLY valid JSON with keys: sql, explanation, confidence, "
                        "used_datapoints, assumptions, clarifying_questions."
                    ),
                )
            )
            retry_response = await provider.generate(retry_request)
            self._track_llm_call(tokens=self._safe_total_tokens(retry_response))
            retry_content = self._coerce_response_content(retry_response.content)
            try:
                return self._parse_llm_response(retry_content, input)
            except ValueError as exc:
                recovered = await self._recover_sql_with_formatter(
                    raw_content=retry_content or response_content,
                    input=input,
                    runtime_stats=runtime_stats,
                )
                if recovered is not None:
                    return recovered
                raise SQLClarificationNeeded(self._build_clarifying_questions(input.query)) from exc

    def _should_accept_fast_sql(self, generated_sql: GeneratedSQL, input: SQLAgentInput) -> bool:
        if generated_sql.clarifying_questions:
            return False
        threshold = self._pipeline_float("sql_two_stage_confidence_threshold", 0.78)
        if generated_sql.confidence < threshold:
            return False
        issues = self._validate_sql(generated_sql, input)
        return len(issues) == 0

    def _should_force_best_effort_retry(
        self,
        *,
        generated_sql: GeneratedSQL,
        query: str,
        compiler_plan: QueryCompilerPlan | None,
        input: SQLAgentInput,
    ) -> bool:
        if not self._pipeline_flag("sql_force_best_effort_on_clarify", True):
            return False
        if not generated_sql.clarifying_questions:
            return False
        sql_text = generated_sql.sql.strip().rstrip(";").upper()
        if sql_text and sql_text not in {"SELECT 1"}:
            return False
        lowered_query = (query or "").lower()
        if self._extract_explicit_table_name(lowered_query):
            return False
        plan = compiler_plan
        available_tables = self._collect_available_table_names(input)
        has_tables = bool(
            (plan and (plan.selected_tables or plan.candidate_tables)) or available_tables
        )
        if not has_tables:
            return False
        # Avoid forcing for truly generic/noise prompts.
        if len((query or "").split()) < 4:
            return False
        return True

    async def _retry_best_effort_sql_generation(
        self,
        *,
        input: SQLAgentInput,
        compiler_plan: QueryCompilerPlan | None,
        runtime_stats: dict[str, int],
    ) -> GeneratedSQL | None:
        plan = compiler_plan
        preferred_tables: list[str] = []
        join_hints: list[str] = []
        column_hints: list[str] = []
        if plan:
            preferred_tables = list(
                dict.fromkeys((plan.selected_tables or []) + (plan.candidate_tables or []))
            )[:5]
            join_hints = plan.join_hypotheses[:6] if plan.join_hypotheses else []
            column_hints = plan.column_hints[:10] if plan.column_hints else []
        if not preferred_tables:
            preferred_tables = self._rank_tables_for_best_effort(input.query, input)[:5]
        if not join_hints and len(preferred_tables) >= 2:
            table_columns = self._collect_table_columns_from_investigation(
                input.investigation_memory
            )
            join_hints = self._infer_join_hypotheses(table_columns, preferred_tables)[:6]
        if not column_hints:
            table_columns = self._collect_table_columns_from_investigation(input.investigation_memory)
            token_hints: set[str] = set()
            column_hints = self._suggest_column_hints(
                input.query,
                table_columns,
                preferred_tables,
                token_hints,
            )[:10]
        if not preferred_tables:
            return None
        prompt = (
            "Generate best-effort executable SQL for the question now.\n"
            "Do not ask clarifying questions. Use likely assumptions and return SQL.\n"
            "Return ONLY one JSON object with keys: sql, explanation, confidence, "
            "used_datapoints, assumptions, clarifying_questions.\n"
            "Set clarifying_questions to [] unless SQL is impossible.\n\n"
            f"QUESTION: {input.query}\n"
            f"DATABASE: {input.database_type}\n"
            f"PREFERRED_TABLES: {', '.join(preferred_tables)}\n"
            f"JOIN_HINTS: {', '.join(join_hints) if join_hints else 'None'}\n"
            f"COLUMN_HINTS: {', '.join(column_hints) if column_hints else 'None'}\n"
            "DEFAULT_LIMIT: 100 rows for list outputs."
        )
        llm_request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=self._get_system_prompt()),
                LLMMessage(role="system", content=self._sql_output_contract_message()),
                LLMMessage(role="user", content=prompt),
            ],
            temperature=0.0,
            max_tokens=1200,
        )
        provider = (
            self.fast_llm
            if not self._providers_are_equivalent(self.fast_llm, self.llm)
            else self.llm
        )
        try:
            generated = await self._request_sql_from_llm(
                provider=provider,
                llm_request=llm_request,
                input=input,
                runtime_stats=runtime_stats,
            )
            if not generated.sql.strip():
                return None
            return generated
        except Exception:
            return None

    async def _correct_sql(
        self,
        generated_sql: GeneratedSQL,
        issues: list[ValidationIssue],
        input: SQLAgentInput,
        metadata: AgentMetadata,
        runtime_stats: dict[str, int],
    ) -> GeneratedSQL:
        """
        Self-correct SQL based on validation issues.

        Args:
            generated_sql: Original generated SQL
            issues: Validation issues found
            input: Original input
            metadata: Metadata to track LLM calls

        Returns:
            Corrected GeneratedSQL

        Raises:
            LLMError: If correction fails
        """
        # Build correction prompt
        prompt = self._build_correction_prompt(generated_sql, issues, input)

        # Create LLM request
        llm_request = LLMRequest(
            messages=[
                LLMMessage(role="system", content=self._get_system_prompt()),
                LLMMessage(role="system", content=self._sql_output_contract_message()),
                LLMMessage(role="user", content=prompt),
            ],
            temperature=0.0,
            max_tokens=2000,
        )

        use_two_stage = self._pipeline_flag(
            "sql_two_stage_enabled", True
        ) and not self._providers_are_equivalent(self.fast_llm, self.llm)

        try:
            fast_clarification: SQLClarificationNeeded | None = None
            if use_two_stage:
                fast_corrected: GeneratedSQL | None = None
                try:
                    fast_corrected = await self._request_sql_from_llm(
                        provider=self.fast_llm,
                        llm_request=llm_request,
                        input=input,
                        runtime_stats=runtime_stats,
                    )
                except SQLClarificationNeeded as exc:
                    fast_clarification = exc

                if fast_corrected is not None and self._should_accept_fast_sql(fast_corrected, input):
                    corrected_sql = fast_corrected
                else:
                    try:
                        corrected_sql = await self._request_sql_from_llm(
                            provider=self.llm,
                            llm_request=llm_request,
                            input=input,
                            runtime_stats=runtime_stats,
                        )
                    except SQLClarificationNeeded as exc:
                        if fast_clarification is not None:
                            combined_questions = [
                                question
                                for question in (
                                    list(fast_clarification.questions) + list(exc.questions)
                                )
                                if question
                            ]
                            raise SQLClarificationNeeded(combined_questions[:2]) from exc
                        raise
            else:
                corrected_sql = await self._request_sql_from_llm(
                    provider=self.llm,
                    llm_request=llm_request,
                    input=input,
                    runtime_stats=runtime_stats,
                )

            logger.debug(
                f"Corrected SQL: {corrected_sql.sql[:200]}...",
                extra={"issues_addressed": len(issues)},
            )

            return corrected_sql

        except SQLClarificationNeeded as exc:
            logger.warning(
                "SQL correction returned clarification request; preserving last valid SQL candidate",
                extra={"questions": exc.questions[:2], "issues": len(issues)},
            )
            clarifying_questions = [question for question in exc.questions if question]
            if not clarifying_questions:
                clarifying_questions = self._build_clarifying_questions(input.query)
            return generated_sql.model_copy(
                update={
                    "clarifying_questions": clarifying_questions[:2],
                    "confidence": min(generated_sql.confidence, 0.2),
                }
            )
        except Exception as e:
            logger.error(f"SQL correction failed: {e}", exc_info=True)
            raise LLMError(
                agent=self.name,
                message=f"SQL correction failed: {e}",
                context={"original_sql": generated_sql.sql},
            ) from e

    async def _recover_sql_with_formatter(
        self, *, raw_content: str, input: SQLAgentInput, runtime_stats: dict[str, int]
    ) -> GeneratedSQL | None:
        """Attempt to recover malformed SQL-agent output with a formatter model."""
        if not self._pipeline_flag("sql_formatter_fallback_enabled", True):
            return None
        if not raw_content or not raw_content.strip():
            return None

        provider = getattr(self, "formatter_llm", None) or self.fast_llm or self.llm
        if provider is None:
            return None

        formatter_model = getattr(self.config.llm, "sql_formatter_model", None)
        if not isinstance(formatter_model, str):
            formatter_model = None
        elif not formatter_model.strip():
            formatter_model = None

        formatter_prompt = self._build_sql_formatter_prompt(raw_content, input.query)
        formatter_request = LLMRequest(
            messages=[
                LLMMessage(
                    role="system",
                    content=(
                        "You are a strict JSON formatter for SQL generation outputs. "
                        "Return only valid JSON."
                    ),
                ),
                LLMMessage(role="user", content=formatter_prompt),
            ],
            temperature=0.0,
            max_tokens=600,
            model=formatter_model,
        )

        try:
            runtime_stats["formatter_fallback_calls"] = (
                int(runtime_stats.get("formatter_fallback_calls", 0)) + 1
            )
            formatter_response = await provider.generate(formatter_request)
            self._track_llm_call(tokens=self._safe_total_tokens(formatter_response))
            parsed = self._parse_llm_response(formatter_response.content, input)
            runtime_stats["formatter_fallback_successes"] = (
                int(runtime_stats.get("formatter_fallback_successes", 0)) + 1
            )
            return parsed
        except Exception as exc:
            logger.debug(
                "Formatter fallback failed to recover SQL response",
                extra={"error": str(exc)},
            )
            return None

    def _build_sql_formatter_prompt(self, raw_content: str, query: str) -> str:
        max_chars = self._pipeline_int("sql_prompt_max_context_chars", 12000)
        clipped_content = raw_content.strip()[:max_chars]
        return (
            "Reformat the MODEL_OUTPUT into strict JSON.\n"
            "Output requirements:\n"
            '- Return ONLY one JSON object with keys: "sql", "explanation", '
            '"used_datapoints", "confidence", "assumptions", "clarifying_questions".\n'
            "- sql must contain executable SQL when present.\n"
            "- If SQL is not recoverable from MODEL_OUTPUT, set sql to an empty string and "
            "add one concise clarifying question.\n"
            "- used_datapoints and assumptions must be JSON arrays.\n"
            "- confidence must be a number between 0 and 1.\n\n"
            f"USER_QUERY:\n{query}\n\n"
            f"MODEL_OUTPUT:\n{clipped_content}\n"
        )

    def _validate_sql(
        self, generated_sql: GeneratedSQL, input: SQLAgentInput
    ) -> list[ValidationIssue]:
        """
        Validate generated SQL for common issues.

        Performs basic validation checks:
        - Basic syntax check (SELECT, FROM keywords)
        - Table names match available DataPoints (excluding CTEs and subqueries)
        - Column names referenced in DataPoints
        - No obvious SQL injection patterns

        Args:
            generated_sql: Generated SQL to validate
            input: Original input with context

        Returns:
            List of validation issues (empty if valid)
        """
        issues: list[ValidationIssue] = []
        sql = generated_sql.sql.strip().upper()
        db_type = input.database_type or getattr(self.config.database, "db_type", "postgresql")
        is_show_statement = (
            sql.startswith("SHOW") or sql.startswith("DESCRIBE") or sql.startswith("DESC")
        )

        # Basic syntax checks
        if is_show_statement:
            return issues
        if not sql.startswith("SELECT") and not sql.startswith("WITH"):
            issues.append(
                ValidationIssue(
                    issue_type="syntax",
                    message="SQL must start with SELECT or WITH",
                    suggested_fix="Ensure query begins with SELECT or WITH (for CTEs)",
                )
            )

        if "FROM" not in sql:
            issues.append(
                ValidationIssue(
                    issue_type="syntax",
                    message="SQL missing FROM clause",
                    suggested_fix="Add FROM clause to specify table(s)",
                )
            )

        # Extract CTE names (Common Table Expressions) from WITH clause
        # Pattern: WITH cte_name AS (...), another_cte AS (...)
        cte_names = set()
        cte_pattern = r"WITH\s+([a-zA-Z0-9_]+)\s+AS\s*\("
        cte_matches = re.findall(cte_pattern, sql, re.IGNORECASE)
        for cte_name in cte_matches:
            cte_names.add(cte_name.upper())

        # Also match comma-separated CTEs: , cte_name AS (
        additional_cte_pattern = r",\s*([a-zA-Z0-9_]+)\s+AS\s*\("
        additional_ctes = re.findall(additional_cte_pattern, sql, re.IGNORECASE)
        for cte_name in additional_ctes:
            cte_names.add(cte_name.upper())

        # Extract table names from SQL (FROM and JOIN clauses)
        table_pattern = r"FROM\s+([a-zA-Z0-9_.]+)|JOIN\s+([a-zA-Z0-9_.]+)"
        table_matches = re.findall(table_pattern, sql, re.IGNORECASE)
        referenced_tables = {match[0] or match[1] for match in table_matches}
        referenced_table_lowers = {table.lower() for table in referenced_tables}
        catalog_schemas = self._catalog_schemas_for_db(db_type)
        catalog_aliases = self._catalog_aliases_for_db(db_type)
        catalog_tables = {
            table
            for table in referenced_table_lowers
            if self._is_catalog_table(table, catalog_schemas, catalog_aliases)
        }
        is_catalog_only = referenced_tables and len(catalog_tables) == len(referenced_table_lowers)

        # Get available tables from DataPoints and related metadata (templates/relationships/live cache)
        available_tables = {
            table_name.upper() for table_name in self._collect_available_table_names(input).values()
        }
        available_table_lowers = {table.lower() for table in available_tables}

        live_schema_tables: set[str] = set()
        db_url = input.database_url or (
            str(self.config.database.url) if self.config.database.url else None
        )
        if db_url:
            schema_key = f"{db_type}::{db_url}"
            live_schema_tables = self._live_schema_tables_cache.get(schema_key, set())

        table_validation_candidates = set(available_table_lowers)
        table_validation_candidates.update(live_schema_tables)
        has_table_validation = bool(table_validation_candidates)

        # Check for missing tables (excluding CTEs and special tables)
        for table in referenced_tables:
            table_upper = table.upper()

            # Skip if this is a CTE name
            if table_upper in cte_names:
                continue

            # Skip special tables
            if table_upper in ("DUAL", "LATERAL"):
                continue
            if is_catalog_only:
                continue
            if table.lower().startswith(("information_schema.", "pg_catalog.")):
                continue
            if table.lower() in catalog_aliases:
                continue
            if not has_table_validation:
                continue

            # Check if table exists in DataPoints
            if table.lower() not in table_validation_candidates and "." in table:
                # Check without schema
                table_no_schema = table.split(".")[-1].lower()
                if (
                    table_no_schema not in table_validation_candidates
                    and table_no_schema.upper() not in cte_names
                ):
                    issues.append(
                        ValidationIssue(
                            issue_type="missing_table",
                            message=f"Table '{table}' not found in available DataPoints",
                            suggested_fix=(
                                f"Use one of: {', '.join(sorted(available_tables))}"
                                if available_tables
                                else "Use a table from the live schema snapshot."
                            ),
                        )
                    )
            elif table.lower() not in table_validation_candidates:
                # Simple table name not found
                issues.append(
                    ValidationIssue(
                        issue_type="missing_table",
                        message=f"Table '{table}' not found in available DataPoints",
                        suggested_fix=(
                            f"Use one of: {', '.join(sorted(available_tables))}"
                            if available_tables
                            else "Use a table from the live schema snapshot."
                        ),
                    )
                )

        return issues

    def _build_introspection_query(
        self,
        query: str,
        database_type: str | None = None,
    ) -> str | None:
        text = query.lower().strip()
        if not self.catalog.is_list_tables_query(text):
            return None
        target_db_type = database_type or getattr(self.config.database, "db_type", "postgresql")
        return get_list_tables_query(target_db_type)

    def _build_list_columns_fallback(self, input: SQLAgentInput) -> str | None:
        plan = self.catalog.plan_query(
            query=input.query,
            database_type=input.database_type,
            investigation_memory=input.investigation_memory,
        )
        if plan and plan.operation == "list_columns":
            return plan.sql
        return None

    def _build_row_count_fallback(self, input: SQLAgentInput) -> str | None:
        plan = self.catalog.plan_query(
            query=input.query,
            database_type=input.database_type,
            investigation_memory=input.investigation_memory,
        )
        if plan and plan.operation == "row_count":
            return plan.sql
        return None

    def _requires_row_count(self, query: str) -> bool:
        text = query.lower()
        patterns = [
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
        return any(re.search(pattern, text) for pattern in patterns)

    def _build_sample_rows_fallback(self, input: SQLAgentInput) -> str | None:
        plan = self.catalog.plan_query(
            query=input.query,
            database_type=input.database_type,
            investigation_memory=input.investigation_memory,
        )
        if plan and plan.operation == "sample_rows":
            return plan.sql
        return None

    def _requires_sample_rows(self, query: str) -> bool:
        text = query.lower()
        patterns = [
            r"\bshow\b.*\brows\b",
            r"\b(?:first|top|limit)\s+\d+\s+(?:rows?|records?)\b",
            r"\bpreview\b",
            r"\bsample\s+(?:rows?|records?)\b",
            r"\bexample\b",
            r"\bshow me\b.*\brows\b",
            r"\bdisplay\b.*\brows\b",
        ]
        return any(re.search(pattern, text) for pattern in patterns)

    def _extract_sample_limit(self, query: str) -> int:
        text = query.lower()
        match = re.search(r"\b(first|top|limit)\s+(\d+)\b", text)
        if match:
            try:
                value = int(match.group(2))
                return max(1, min(value, 25))
            except ValueError:
                return 3
        return 3

    def _build_clarifying_questions(self, query: str) -> list[str]:
        questions = ["Which table should I use to answer this?"]
        text = query.lower()
        if any(term in text for term in ("total", "sum", "average", "avg", "count")):
            questions.append(
                "Which column should I aggregate (for example: amount, total_amount, revenue)?"
            )
        if any(term in text for term in ("date", "time", "month", "year", "quarter")):
            questions.append("Is there a specific date or time range I should use?")
        return questions

    def _resolve_followup_query(self, input: SQLAgentInput) -> tuple[str, str | None]:
        history = input.conversation_history or []
        if not history:
            return input.query, None

        last_assistant = None
        last_assistant_index = None
        for idx in range(len(history) - 1, -1, -1):
            msg = history[idx]
            role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", None)
            if role == "assistant":
                last_assistant = msg
                last_assistant_index = idx
                break

        if last_assistant is None:
            return input.query, None

        assistant_text = (
            str(last_assistant.get("content", ""))
            if isinstance(last_assistant, dict)
            else str(getattr(last_assistant, "content", ""))
        ).lower()
        if "which table" not in assistant_text and "clarifying" not in assistant_text:
            return input.query, None

        query_text = input.query.strip()
        if (
            not query_text
            or len(query_text.split()) > 4
            or not self._looks_like_followup_hint(query_text)
        ):
            return input.query, None

        previous_user = None
        if last_assistant_index is not None:
            for idx in range(last_assistant_index - 1, -1, -1):
                msg = history[idx]
                role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", None)
                if role == "user":
                    previous_user = msg
                    break
        if previous_user is None:
            return input.query, None

        previous_text = (
            str(previous_user.get("content", ""))
            if isinstance(previous_user, dict)
            else str(getattr(previous_user, "content", ""))
        ).strip()
        if not previous_text:
            return input.query, None

        table_hint = re.sub(r"[^\w.]+", "", query_text)
        if not table_hint:
            return input.query, None

        resolved_query = self._merge_query_with_table_hint(previous_text, table_hint)
        return resolved_query, table_hint

    def _looks_like_followup_hint(self, text: str) -> bool:
        lowered = text.lower().strip()
        if not lowered:
            return False
        if ":" in lowered:
            lowered = lowered.rsplit(":", 1)[-1].strip()
        disallowed = {
            "show",
            "list",
            "count",
            "select",
            "describe",
            "help",
            "what",
            "which",
            "how",
            "rows",
            "columns",
            "table",
            "tables",
        }
        words = [word for word in re.split(r"\s+", lowered) if word]
        if any(word in disallowed for word in words):
            return False
        return bool(re.fullmatch(r"[a-zA-Z0-9_.`\"'-]+", lowered))

    def _merge_query_with_table_hint(self, previous_query: str, table_hint: str) -> str:
        base = previous_query.strip()
        hint = table_hint.strip().strip("`").strip('"')
        if not base or not hint:
            return previous_query

        lower = base.lower()
        limit_match = re.search(r"\b(first|top|limit|show)\s+(\d+)\s+rows?\b", lower)
        if limit_match:
            limit = max(1, min(int(limit_match.group(2)), 10))
            return f"Show {limit} rows from {hint}"
        if re.search(r"\b(show|sample|preview)\b.*\brows?\b", lower):
            return f"Show 3 rows from {hint}"
        if "column" in lower or "columns" in lower or "fields" in lower:
            return f"Show columns in {hint}"
        if re.search(r"\b(row count|how many rows|records?)\b", lower):
            return f"How many rows are in {hint}?"
        return f"{base.rstrip('. ')} Use table {hint}."

    def _extract_explicit_table_name(self, query: str) -> str | None:
        patterns = [
            r"\bhow\s+many\s+rows?\s+(?:are\s+)?in\s+([a-zA-Z0-9_.]+)",
            r"\brows?\s+in\s+([a-zA-Z0-9_.]+)",
            r"\bcount\s+of\s+rows?\s+in\s+([a-zA-Z0-9_.]+)",
            r"\brecords?\s+in\s+([a-zA-Z0-9_.]+)",
            r"\b(?:first|top|last)\s+\d+\s+rows?\s+(?:from|in|of)\s+([a-zA-Z0-9_.]+)",
            r"\bshow\s+me\s+(?:the\s+)?(?:first|top|last)?\s*\d*\s*rows?\s+(?:from|in|of)\s+([a-zA-Z0-9_.]+)",
            r"\b(?:preview|sample)\s+(?:rows?\s+(?:from|in|of)\s+)?([a-zA-Z0-9_.]+)",
            r"\btable\s+([a-zA-Z0-9_.]+)",
        ]
        lowered = query.lower()
        for pattern in patterns:
            match = re.search(pattern, lowered)
            if match:
                table_name = match.group(1).rstrip(".,;:?)")
                if table_name and table_name not in {
                    "table",
                    "tables",
                    "row",
                    "rows",
                }:
                    return table_name
        return None

    def _select_schema_table(self, input: SQLAgentInput) -> str | None:
        for dp in input.investigation_memory.datapoints:
            if dp.datapoint_type != "Schema":
                continue
            metadata = dp.metadata if isinstance(dp.metadata, dict) else {}
            table_name = metadata.get("table_name") or metadata.get("table")
            if table_name:
                return table_name
        return None

    def _get_system_prompt(self) -> str:
        """
        Get system prompt for SQL generation.

        Returns:
            System prompt string
        """
        return self.prompts.load("system/main.md")

    def _sql_output_contract_message(self) -> str:
        """Return strict output contract for SQL generation responses."""
        return (
            "Return ONLY one valid JSON object. No markdown, no prose outside JSON.\n"
            "Allowed keys: sql, explanation, confidence, used_datapoints, assumptions, "
            "clarifying_questions.\n"
            "If SQL is available, set sql to executable SQL text.\n"
            "When schema/table context is present, prefer a best-effort SQL with assumptions "
            "instead of asking for table/column clarification.\n"
            "Only ask clarifying questions when SQL is truly impossible from available schema.\n"
            "If SQL is not possible, set sql to an empty string and provide at most 2 "
            "clarifying_questions.\n"
            "Keep explanation concise (max 2 sentences). Do not include sql_components or metadata."
        )

    async def _build_generation_prompt(
        self,
        input: SQLAgentInput,
        *,
        runtime_stats: dict[str, Any] | None = None,
        return_plan: bool = False,
    ) -> str | tuple[str, QueryCompilerPlan | None]:
        """
        Build prompt for SQL generation.

        Args:
            input: SQLAgentInput with query and context

        Returns:
            Formatted prompt string
        """
        resolved_query, _ = self._resolve_followup_query(input)
        # Extract schema and business context
        schema_context = self._format_schema_context(input.investigation_memory)
        ranked_catalog_context = self.catalog.build_ranked_schema_context(
            query=resolved_query,
            investigation_memory=input.investigation_memory,
        )
        if ranked_catalog_context:
            if schema_context == "No schema context available":
                schema_context = ranked_catalog_context
            else:
                schema_context = f"{schema_context}\n\n{ranked_catalog_context}"
        include_profile = not input.investigation_memory.datapoints
        live_context = await self._get_live_schema_context(
            query=resolved_query,
            database_type=input.database_type,
            database_url=input.database_url,
            include_profile=include_profile,
        )
        db_type = input.database_type or getattr(self.config.database, "db_type", "postgresql")
        db_url = input.database_url or (
            str(self.config.database.url) if self.config.database.url else None
        )
        if live_context:
            if schema_context == "No schema context available":
                schema_context = live_context
            else:
                schema_context = (
                    f"{schema_context}\n\n**Live schema snapshot (authoritative):**\n{live_context}"
                )

        compiler_plan: QueryCompilerPlan | None = None
        if self._pipeline_flag("query_compiler_enabled", True):
            compiler_plan = await self._compile_query_plan(
                query=resolved_query,
                investigation_memory=input.investigation_memory,
                db_type=db_type,
                db_url=db_url,
                runtime_stats=runtime_stats,
            )
            if compiler_plan:
                compiled_context = self._format_query_compiler_context(compiler_plan)
                if schema_context == "No schema context available":
                    schema_context = compiled_context
                else:
                    schema_context = f"{schema_context}\n\n{compiled_context}"

        if self._pipeline_flag("sql_operator_templates_enabled", True):
            operator_guidance = self._build_operator_guidance_context(
                query=resolved_query,
                investigation_memory=input.investigation_memory,
                db_type=db_type,
                db_url=db_url,
            )
            if operator_guidance:
                if schema_context == "No schema context available":
                    schema_context = operator_guidance
                else:
                    schema_context = f"{schema_context}\n\n{operator_guidance}"

        if self._pipeline_flag("sql_prompt_budget_enabled", True):
            max_chars = self._pipeline_int("sql_prompt_max_context_chars", 12000)
            schema_context = self._truncate_context(schema_context, max_chars)
        business_context = self._format_business_context(input.investigation_memory)
        conversation_context = self._format_conversation_context(input.conversation_history)
        prompt = self.prompts.render(
            "agents/sql_generator.md",
            user_query=resolved_query,
            schema_context=schema_context,
            business_context=business_context,
            conversation_context=conversation_context,
            backend=db_type,
            user_preferences={"default_limit": 100},
        )
        if return_plan:
            return prompt, compiler_plan
        return prompt

    def _truncate_context(self, text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        truncated = text[:max_chars].rstrip()
        return (
            f"{truncated}\n\n"
            "[Context truncated for latency budget. Ask a narrower query for more schema detail.]"
        )

    def _build_operator_guidance_context(
        self,
        *,
        query: str,
        investigation_memory,
        db_type: str,
        db_url: str | None,
    ) -> str:
        table_columns = self._collect_table_columns_from_investigation(investigation_memory)
        if db_url:
            schema_key = f"{db_type}::{db_url}"
            cached_snapshot = self._live_schema_snapshot_cache.get(schema_key, {})
            cached_columns = cached_snapshot.get("columns")
            if isinstance(cached_columns, dict):
                for table, columns in cached_columns.items():
                    if not isinstance(table, str):
                        continue
                    values = table_columns.setdefault(table, [])
                    if not isinstance(columns, list):
                        continue
                    for col in columns:
                        if isinstance(col, tuple) and col:
                            col_name = str(col[0])
                        else:
                            col_name = str(col)
                        if col_name and col_name not in values:
                            values.append(col_name)

        if not table_columns:
            return ""

        max_templates = self._pipeline_int("sql_operator_templates_max", 8)
        return build_operator_guidance(
            query,
            table_columns=table_columns,
            max_templates=max(2, min(max_templates, 12)),
            max_table_hints=3,
        )

    async def _compile_query_plan(
        self,
        *,
        query: str,
        investigation_memory,
        db_type: str,
        db_url: str | None,
        runtime_stats: dict[str, Any] | None,
    ) -> QueryCompilerPlan | None:
        started = time.perf_counter()
        table_columns = self._collect_table_columns_from_investigation(investigation_memory)
        schema_key = f"{db_type}::{db_url}" if db_url else None
        if schema_key:
            cached_snapshot = self._live_schema_snapshot_cache.get(schema_key, {})
            cached_columns = cached_snapshot.get("columns")
            if isinstance(cached_columns, dict):
                for table, columns in cached_columns.items():
                    if not isinstance(table, str):
                        continue
                    values = table_columns.setdefault(table, [])
                    if not isinstance(columns, list):
                        continue
                    for col in columns:
                        col_name = str(col[0] if isinstance(col, tuple) and col else col)
                        if col_name and col_name not in values:
                            values.append(col_name)
        if not table_columns:
            if runtime_stats is not None:
                runtime_stats["query_compiler_latency_ms"] = runtime_stats.get(
                    "query_compiler_latency_ms", 0.0
                ) + ((time.perf_counter() - started) * 1000.0)
            return None

        operator_matches = match_operator_templates(
            query,
            limit=max(2, min(self._pipeline_int("sql_operator_templates_max", 8), 12)),
        )
        operator_keys = [item.template.key for item in operator_matches]
        signal_tokens: set[str] = set()
        for item in operator_matches:
            signal_tokens.update(token.lower() for token in item.template.signal_tokens)

        scores = self._score_table_candidates(query, table_columns, signal_tokens)
        ranked_tables = sorted(
            table_columns.keys(),
            key=lambda table: (scores.get(table, 0), table),
            reverse=True,
        )
        candidate_tables = ranked_tables[:8]
        selected_tables = self._pick_selected_tables(query, candidate_tables, scores)
        join_hypotheses = self._infer_join_hypotheses(table_columns, selected_tables)
        column_hints = self._suggest_column_hints(
            query, table_columns, selected_tables, signal_tokens
        )
        confidence = self._estimate_compiler_confidence(scores, candidate_tables, selected_tables)

        reason = "deterministic"
        path = "deterministic"
        if self._should_refine_compiler_with_llm(
            confidence=confidence,
            candidate_tables=candidate_tables,
            selected_tables=selected_tables,
            query=query,
        ):
            refined = await self._refine_compiler_with_llm(
                query=query,
                candidate_tables=candidate_tables,
                table_columns=table_columns,
                runtime_stats=runtime_stats,
            )
            if refined:
                selected_tables = refined.get("selected_tables", selected_tables) or selected_tables
                candidate_tables = (
                    refined.get("candidate_tables", candidate_tables) or candidate_tables
                )
                join_hypotheses = refined.get("join_hypotheses", join_hypotheses) or join_hypotheses
                column_hints = refined.get("column_hints", column_hints) or column_hints
                confidence = max(confidence, float(refined.get("confidence", confidence)))
                reason = "llm_refined_ambiguous_candidates"
                path = "llm_refined"
                if runtime_stats is not None:
                    runtime_stats["query_compiler_llm_refinements"] = (
                        int(runtime_stats.get("query_compiler_llm_refinements", 0)) + 1
                    )

        if runtime_stats is not None:
            runtime_stats["query_compiler_latency_ms"] = runtime_stats.get(
                "query_compiler_latency_ms", 0.0
            ) + ((time.perf_counter() - started) * 1000.0)
        return QueryCompilerPlan(
            query=query,
            operators=operator_keys,
            candidate_tables=candidate_tables,
            selected_tables=selected_tables,
            join_hypotheses=join_hypotheses,
            column_hints=column_hints,
            confidence=max(0.0, min(1.0, confidence)),
            path=path,
            reason=reason,
        )

    def _score_table_candidates(
        self,
        query: str,
        table_columns: dict[str, list[str]],
        signal_tokens: set[str],
    ) -> dict[str, int]:
        query_tokens = set(self._tokenize_query(query))
        scores: dict[str, int] = {}
        for table, columns in table_columns.items():
            table_tokens = set(table.lower().replace(".", "_").split("_"))
            score = 0
            for token in query_tokens:
                if token in table_tokens:
                    score += 4
                if any(token in col.lower() for col in columns):
                    score += 3
            for token in signal_tokens:
                if any(token in col.lower() for col in columns):
                    score += 2
                if token in table_tokens:
                    score += 1
            if table.lower().startswith("information_schema.") or table.lower().startswith(
                "pg_catalog."
            ):
                score -= 3
            scores[table] = score
        return scores

    def _pick_selected_tables(
        self,
        query: str,
        candidate_tables: list[str],
        scores: dict[str, int],
    ) -> list[str]:
        if not candidate_tables:
            return []
        first = candidate_tables[0]
        first_score = scores.get(first, 0)
        selected = [first] if first_score > 0 else []
        multi_hint = bool(
            re.search(
                r"\b(compare|between|versus|vs|and|gap|difference|reconcile|join)\b",
                query.lower(),
            )
        )
        for table in candidate_tables[1:4]:
            table_score = scores.get(table, 0)
            if table_score <= 0:
                continue
            if table_score >= first_score - 1 and (multi_hint or table_score >= 4):
                selected.append(table)
        # Keep stable ordering and uniqueness.
        deduped: list[str] = []
        for name in selected:
            if name not in deduped:
                deduped.append(name)
        return deduped[:3]

    def _infer_join_hypotheses(
        self,
        table_columns: dict[str, list[str]],
        selected_tables: list[str],
    ) -> list[str]:
        if len(selected_tables) < 2:
            return []
        normalized = {
            table: [col.lower() for col in table_columns.get(table, [])]
            for table in selected_tables
        }
        hints: list[str] = []
        for source in selected_tables:
            source_cols = normalized.get(source, [])
            for target in selected_tables:
                if source == target:
                    continue
                target_cols = normalized.get(target, [])
                for col in source_cols:
                    if not col.endswith("_id"):
                        continue
                    key = col[:-3]
                    target_base = target.split(".")[-1].lower()
                    target_aliases = {target_base, target_base.rstrip("s")}
                    target_key = "id" if "id" in target_cols else f"{key}_id"
                    if key in target_aliases and target_key in target_cols:
                        hint = f"{source}.{col} = {target}.{target_key}"
                        if hint not in hints:
                            hints.append(hint)
                    elif col in target_cols:
                        hint = f"{source}.{col} = {target}.{col}"
                        if hint not in hints:
                            hints.append(hint)
                shared = (
                    {"store_id", "product_id", "customer_id", "account_id"}
                    & set(source_cols)
                    & set(target_cols)
                )
                for col in sorted(shared):
                    hint = f"{source}.{col} = {target}.{col}"
                    if hint not in hints:
                        hints.append(hint)
                if len(hints) >= 8:
                    return hints
        return hints[:8]

    def _suggest_column_hints(
        self,
        query: str,
        table_columns: dict[str, list[str]],
        selected_tables: list[str],
        signal_tokens: set[str],
    ) -> list[str]:
        tokens = set(self._tokenize_query(query)) | set(signal_tokens)
        hints: list[str] = []
        for table in selected_tables:
            for col in table_columns.get(table, []):
                col_lower = col.lower()
                if any(token and token in col_lower for token in tokens):
                    candidate = f"{table}.{col}"
                    if candidate not in hints:
                        hints.append(candidate)
                if len(hints) >= 10:
                    return hints
        return hints

    def _estimate_compiler_confidence(
        self,
        scores: dict[str, int],
        candidate_tables: list[str],
        selected_tables: list[str],
    ) -> float:
        if not candidate_tables or not selected_tables:
            return 0.2
        top = scores.get(candidate_tables[0], 0)
        second = scores.get(candidate_tables[1], 0) if len(candidate_tables) > 1 else 0
        gap = top - second
        if top >= 12 and gap >= 4:
            return 0.9
        if top >= 8 and gap >= 2:
            return 0.8
        if top >= 4:
            return 0.68
        return 0.45

    def _should_refine_compiler_with_llm(
        self,
        *,
        confidence: float,
        candidate_tables: list[str],
        selected_tables: list[str],
        query: str,
    ) -> bool:
        if not self._pipeline_flag("query_compiler_llm_enabled", True):
            return False
        if self._providers_are_equivalent(self.fast_llm, self.llm):
            return False
        if len(candidate_tables) < 2:
            return False
        if len(candidate_tables) > max(
            2, self._pipeline_int("query_compiler_llm_max_candidates", 10)
        ):
            return False
        if self.catalog.is_list_tables_query(query.lower()) or self.catalog.is_list_columns_query(
            query.lower()
        ):
            return False
        threshold = self._pipeline_float("query_compiler_confidence_threshold", 0.72)
        if confidence >= threshold and selected_tables:
            return False
        return True

    async def _refine_compiler_with_llm(
        self,
        *,
        query: str,
        candidate_tables: list[str],
        table_columns: dict[str, list[str]],
        runtime_stats: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        rows = []
        for index, table in enumerate(candidate_tables, start=1):
            columns = ", ".join(table_columns.get(table, [])[:12])
            rows.append(f"{index}. {table} | columns: {columns}")
        prompt = (
            "You are refining a SQL table-selection plan.\n"
            "Pick likely tables and join keys for the question. Return strict JSON with keys:\n"
            "candidate_tables (array), selected_tables (array), join_hypotheses (array), "
            "column_hints (array), confidence (0..1), reason (string).\n"
            "Use table names exactly as given.\n\n"
            f"QUESTION:\n{query}\n\n"
            f"CANDIDATE_TABLES:\n{chr(10).join(rows)}\n"
        )
        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content="Return only valid JSON."),
                LLMMessage(role="user", content=prompt),
            ],
            temperature=0.0,
            max_tokens=500,
        )
        try:
            response = await self.fast_llm.generate(request)
            self._track_llm_call(tokens=self._safe_total_tokens(response))
            if runtime_stats is not None:
                runtime_stats["query_compiler_llm_calls"] = (
                    int(runtime_stats.get("query_compiler_llm_calls", 0)) + 1
                )
            content = self._coerce_response_content(response.content)
            json_match = re.search(r"\{[\s\S]*\}", content)
            if not json_match:
                return None
            payload = json.loads(json_match.group(0))
            selected_tables = [
                str(item)
                for item in payload.get("selected_tables", [])
                if str(item) in candidate_tables
            ]
            candidate = [
                str(item)
                for item in payload.get("candidate_tables", [])
                if str(item) in candidate_tables
            ]
            joins = [str(item) for item in payload.get("join_hypotheses", []) if str(item).strip()]
            columns = [str(item) for item in payload.get("column_hints", []) if str(item).strip()]
            confidence = float(payload.get("confidence", 0.0) or 0.0)
            return {
                "selected_tables": selected_tables[:4],
                "candidate_tables": candidate[:8] or candidate_tables,
                "join_hypotheses": joins[:8],
                "column_hints": columns[:10],
                "confidence": max(0.0, min(1.0, confidence)),
                "reason": str(payload.get("reason", "")),
            }
        except Exception:
            return None

    def _format_query_compiler_context(self, plan: QueryCompilerPlan) -> str:
        lines = ["**Query compiler plan:**"]
        if plan.operators:
            lines.append(f"- Operators: {', '.join(plan.operators[:6])}")
        if plan.selected_tables:
            lines.append(f"- Selected tables: {', '.join(plan.selected_tables[:4])}")
        if plan.candidate_tables:
            lines.append(f"- Candidate tables: {', '.join(plan.candidate_tables[:6])}")
        if plan.join_hypotheses:
            lines.append("- Join hypotheses:")
            lines.extend(f"  - {hint}" for hint in plan.join_hypotheses[:6])
        if plan.column_hints:
            lines.append(f"- Column hints: {', '.join(plan.column_hints[:8])}")
        lines.append(f"- Confidence: {plan.confidence:.2f} ({plan.path})")
        return "\n".join(lines)

    def _collect_table_columns_from_investigation(
        self, investigation_memory
    ) -> dict[str, list[str]]:
        table_columns: dict[str, list[str]] = {}
        datapoints = getattr(investigation_memory, "datapoints", []) or []
        for datapoint in datapoints:
            metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
            table_candidates: list[str] = []
            primary_table = metadata.get("table_name") or metadata.get("table")
            if isinstance(primary_table, str) and primary_table.strip():
                table_candidates.append(primary_table.strip())
            table_candidates.extend(self._coerce_string_list(metadata.get("related_tables")))
            table_candidates.extend(self._coerce_string_list(metadata.get("target_tables")))
            sql_template = metadata.get("sql_template")
            if isinstance(sql_template, str) and sql_template.strip():
                table_candidates.extend(self._extract_table_names_from_sql(sql_template))

            for table_name in table_candidates:
                normalized = str(table_name).strip()
                if normalized:
                    table_columns.setdefault(normalized, [])

            if getattr(datapoint, "datapoint_type", None) != "Schema":
                continue

            if not table_candidates:
                continue
            columns = table_columns.setdefault(table_candidates[0], [])
            key_columns = metadata.get("key_columns") or metadata.get("columns") or []
            if not isinstance(key_columns, list):
                continue
            for item in key_columns:
                if isinstance(item, dict):
                    col_name = item.get("name") or item.get("column_name")
                else:
                    col_name = item
                if not col_name:
                    continue
                col_text = str(col_name)
                if col_text not in columns:
                    columns.append(col_text)
        return table_columns

    def _rank_tables_for_best_effort(self, query: str, input: SQLAgentInput) -> list[str]:
        table_columns = self._collect_table_columns_from_investigation(input.investigation_memory)
        if table_columns:
            signal_tokens: set[str] = set()
            scores = self._score_table_candidates(query, table_columns, signal_tokens)
            ranked = sorted(
                table_columns.keys(),
                key=lambda table: (scores.get(table, 0), table),
                reverse=True,
            )
            if ranked:
                return ranked
        return list(self._collect_available_table_names(input).values())

    async def _get_live_schema_context(
        self,
        query: str,
        database_type: str | None = None,
        database_url: str | None = None,
        include_profile: bool = False,
    ) -> str | None:
        db_type = database_type or getattr(self.config.database, "db_type", "postgresql")

        db_url = database_url or (
            str(self.config.database.url) if self.config.database.url else None
        )
        if not db_url:
            return None

        cache_key = f"{db_type}::{db_url}::{query.lower().strip()}::{include_profile}"
        schema_key = f"{db_type}::{db_url}"
        cached = self._live_schema_cache.get(cache_key)
        if cached:
            return cached

        try:
            connector = create_connector(
                database_url=db_url,
                database_type=db_type,
                pool_size=self.config.database.pool_size,
                timeout=10,
            )
        except Exception:
            return None

        try:
            await connector.connect()
            context, qualified_tables = await self._fetch_live_schema_context(
                connector,
                query,
                schema_key,
                include_profile,
                db_type=db_type,
                db_url=db_url,
            )
            if context:
                self._live_schema_cache[cache_key] = context
            if qualified_tables:
                expanded_tables = set()
                for table in qualified_tables:
                    expanded_tables.add(table.lower())
                    if "." in table:
                        expanded_tables.add(table.split(".")[-1].lower())
                if expanded_tables:
                    self._live_schema_tables_cache[schema_key] = expanded_tables
            return context
        except Exception as exc:
            logger.warning(f"Live schema lookup failed: {exc}")
            return None
        finally:
            await connector.close()

    async def _fetch_live_schema_context(
        self,
        connector: BaseConnector,
        query: str,
        schema_key: str,
        include_profile: bool,
        *,
        db_type: str,
        db_url: str,
    ) -> tuple[str | None, list[str]]:
        qualified_tables, columns_by_table = await self._load_schema_snapshot(
            connector=connector,
            schema_key=schema_key,
            db_type=db_type,
        )
        if not qualified_tables:
            return None, []

        max_tables = 200
        if self._pipeline_flag("sql_prompt_budget_enabled", True):
            max_tables = self._pipeline_int("sql_prompt_max_tables", 80)

        entries = []
        for qualified in qualified_tables[:max_tables]:
            entries.append(qualified)

        if not entries:
            return None, []

        tables = ", ".join(entries)

        columns_context, focus_tables = self._build_columns_context_from_map(
            query=query,
            qualified_tables=qualified_tables,
            columns_by_table=columns_by_table,
        )

        join_context = ""
        profile_context = ""
        cached_profile_context = ""
        semantic_context = ""
        if include_profile and columns_by_table:
            join_context = self._build_join_hints_context(columns_by_table, focus_tables)
        if include_profile and columns_by_table and db_type == "postgresql":
            profile_context = await self._build_lightweight_profile_context(
                connector, schema_key, query, columns_by_table, focus_tables
            )
            cached_profile_context = self._build_cached_profile_context(
                db_type=db_type,
                db_url=db_url,
                focus_tables=focus_tables,
            )
        if include_profile and columns_by_table:
            semantic_context = await self._build_live_semantic_context(
                schema_key=schema_key,
                query=query,
                columns_by_table=columns_by_table,
                focus_tables=focus_tables,
            )

        return (
            f"**Tables in database (compact list):** {tables}"
            f"{columns_context}"
            f"{join_context}"
            f"{profile_context}"
            f"{cached_profile_context}"
            f"{semantic_context}"
        ), qualified_tables

    async def _load_schema_snapshot(
        self,
        *,
        connector: BaseConnector,
        schema_key: str,
        db_type: str,
    ) -> tuple[list[str], dict[str, list[tuple[str, str | None]]]]:
        use_snapshot_cache = self._pipeline_flag("schema_snapshot_cache_enabled", True)
        snapshot_ttl_seconds = self._pipeline_int("schema_snapshot_cache_ttl_seconds", 21600)
        if use_snapshot_cache:
            cached = self._live_schema_snapshot_cache.get(schema_key)
            if cached:
                tables = cached.get("tables")
                columns = cached.get("columns")
                cached_at = cached.get("cached_at")
                is_expired = False
                if snapshot_ttl_seconds > 0 and isinstance(cached_at, int | float):
                    is_expired = (time.time() - float(cached_at)) > snapshot_ttl_seconds
                if not is_expired and isinstance(tables, list) and isinstance(columns, dict):
                    return list(tables), columns
                if is_expired:
                    self._live_schema_snapshot_cache.pop(schema_key, None)

        if db_type == "postgresql":
            tables_query = (
                "SELECT table_schema, table_name "
                "FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name"
            )
            result = await connector.execute(tables_query)
            if not result.rows:
                return [], {}

            qualified_tables: list[str] = []
            for row in result.rows:
                schema = row.get("table_schema")
                table = row.get("table_name")
                if schema and table:
                    qualified_tables.append(f"{schema}.{table}")
                elif table:
                    qualified_tables.append(str(table))

            if not qualified_tables:
                return [], {}

            qualified_set = set(qualified_tables)
            columns_query = (
                "SELECT table_schema, table_name, column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name, ordinal_position"
            )
            columns_result = await connector.execute(columns_query)
            columns_by_table: dict[str, list[tuple[str, str | None]]] = {}
            for row in columns_result.rows:
                schema = row.get("table_schema")
                table = row.get("table_name")
                column = row.get("column_name")
                dtype = row.get("data_type")
                if not (schema and table and column):
                    continue
                key = f"{schema}.{table}"
                if key not in qualified_set:
                    continue
                columns_by_table.setdefault(key, []).append((str(column), dtype))
        else:
            tables_info = await connector.get_schema()
            qualified_tables = []
            columns_by_table = {}
            for table in tables_info:
                schema = getattr(table, "schema_name", None) or getattr(table, "schema", None)
                name = getattr(table, "table_name", None)
                if not name:
                    continue
                key = f"{schema}.{name}" if schema else str(name)
                qualified_tables.append(key)
                cols = []
                for column in getattr(table, "columns", []):
                    col_name = getattr(column, "name", None)
                    if not col_name:
                        continue
                    cols.append((str(col_name), getattr(column, "data_type", None)))
                if cols:
                    columns_by_table[key] = cols

        if use_snapshot_cache:
            self._live_schema_snapshot_cache[schema_key] = {
                "tables": list(qualified_tables),
                "columns": columns_by_table,
                "cached_at": time.time(),
            }

        return qualified_tables, columns_by_table

    def _build_columns_context_from_map(
        self,
        *,
        query: str,
        qualified_tables: list[str],
        columns_by_table: dict[str, list[tuple[str, str | None]]],
    ) -> tuple[str, list[str]]:
        if not qualified_tables or not columns_by_table:
            return "", []

        query_lower = query.lower()
        is_list_tables = bool(
            re.search(r"\b(list|show|what|which)\s+tables\b", query_lower)
            or "tables exist" in query_lower
            or "available tables" in query_lower
        )

        focus_limit = 10
        if self._pipeline_flag("sql_prompt_budget_enabled", True):
            focus_limit = self._pipeline_int("sql_prompt_focus_tables", 8)

        if is_list_tables:
            focus_tables = sorted(columns_by_table.keys())[:focus_limit]
        else:
            focus_tables = self._rank_tables_by_query(query, columns_by_table)[:focus_limit]

        if not focus_tables:
            return "", []

        max_columns = 30
        if self._pipeline_flag("sql_prompt_budget_enabled", True):
            max_columns = self._pipeline_int("sql_prompt_max_columns_per_table", 18)

        lines = []
        for table in focus_tables:
            columns = columns_by_table.get(table, [])
            if columns:
                formatted_columns = []
                for name, dtype in columns[:max_columns]:
                    formatted_columns.append(f"{name} ({dtype})" if dtype else name)
                lines.append(f"- {table}: {', '.join(formatted_columns)}")

        if not lines:
            return "", focus_tables

        header = (
            "**Columns (all tables):**" if is_list_tables else "**Columns (top matched tables):**"
        )
        return f"\n{header}\n" + "\n".join(lines), focus_tables

    def _rank_tables_by_query(
        self, query: str, columns_by_table: dict[str, list[tuple[str, str | None]]]
    ) -> list[str]:
        tokens = self._tokenize_query(query)
        if not tokens:
            return sorted(columns_by_table.keys())

        scores: dict[str, int] = {}
        for table, columns in columns_by_table.items():
            score = 0
            table_tokens = table.lower().replace(".", "_").split("_")
            column_names = [name.lower() for name, _ in columns]
            for token in tokens:
                if token in table_tokens:
                    score += 3
                if any(token in col for col in column_names):
                    score += 2
            if len(tokens) >= 2:
                bigrams = {" ".join(tokens[i : i + 2]) for i in range(len(tokens) - 1)}
                for bigram in bigrams:
                    if bigram.replace(" ", "_") in table.lower():
                        score += 4
            scores[table] = score

        return sorted(
            scores.keys(),
            key=lambda item: (scores[item], item),
            reverse=True,
        )

    def _tokenize_query(self, query: str) -> list[str]:
        text = re.sub(r"[^a-z0-9_\\s]", " ", query.lower())
        raw_tokens = [token for token in text.split() if len(token) > 1]
        stopwords = {
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
            "tables",
            "table",
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
        }
        return [token for token in raw_tokens if token not in stopwords]

    def _build_join_hints_context(
        self,
        columns_by_table: dict[str, list[tuple[str, str | None]]],
        focus_tables: list[str],
    ) -> str:
        if not focus_tables:
            return ""

        table_aliases: dict[str, set[str]] = {}
        table_columns: dict[str, list[str]] = {}
        for table in focus_tables[:8]:
            base = table.split(".")[-1].lower()
            aliases = {base}
            if base.endswith("s") and len(base) > 1:
                aliases.add(base[:-1])
            else:
                aliases.add(f"{base}s")
            table_aliases[table] = aliases
            table_columns[table] = [name.lower() for name, _ in columns_by_table.get(table, [])]

        hints: list[str] = []
        seen = set()
        for source_table, columns in table_columns.items():
            for column in columns:
                if not column.endswith("_id"):
                    continue
                key = column[:-3]
                for target_table, aliases in table_aliases.items():
                    if target_table == source_table:
                        continue
                    if key not in aliases:
                        continue
                    target_columns = table_columns.get(target_table, [])
                    target_column = None
                    if "id" in target_columns:
                        target_column = "id"
                    elif f"{key}_id" in target_columns:
                        target_column = f"{key}_id"
                    hint = f"- {source_table}.{column} -> {target_table}.{target_column or 'id'}"
                    if hint not in seen:
                        seen.add(hint)
                        hints.append(hint)
                if len(hints) >= 8:
                    break
            if len(hints) >= 8:
                break

        if not hints:
            return ""
        return "\n**Join hints (heuristic):**\n" + "\n".join(hints)

    async def _build_lightweight_profile_context(
        self,
        connector: BaseConnector,
        schema_key: str,
        query: str,
        columns_by_table: dict[str, list[tuple[str, str | None]]],
        focus_tables: list[str],
    ) -> str:
        if not focus_tables:
            return ""

        profile_cache = self._live_profile_cache.setdefault(schema_key, {})
        lines: list[str] = []
        for table in focus_tables[:3]:
            if table not in profile_cache:
                profile_cache[table] = await self._fetch_table_profile(
                    connector, table, query, columns_by_table.get(table, [])
                )
            profile = profile_cache.get(table, {})
            if not profile:
                continue
            row_count = profile.get("row_count")
            columns = profile.get("columns", {})
            if not columns:
                continue
            line = f"- {table}"
            if row_count is not None:
                line += f" (~{row_count} rows)"
            lines.append(line)
            for column_name, stats in list(columns.items())[:5]:
                parts = []
                null_frac = stats.get("null_frac")
                if null_frac is not None:
                    parts.append(f"null_frac={null_frac:.2f}")
                n_distinct = stats.get("n_distinct")
                if n_distinct is not None:
                    parts.append(f"n_distinct={n_distinct}")
                common_vals = stats.get("common_vals")
                if common_vals:
                    preview = ", ".join(common_vals[:3])
                    parts.append(f"examples=[{preview}]")
                if parts:
                    lines.append(f"  - {column_name}: " + ", ".join(parts))

        if not lines:
            return ""
        return "\n**Lightweight stats (cached):**\n" + "\n".join(lines)

    async def _fetch_table_profile(
        self,
        connector: BaseConnector,
        table: str,
        query: str,
        columns: list[tuple[str, str | None]],
    ) -> dict[str, object]:
        schema = "public"
        table_name = table
        if "." in table:
            schema, table_name = table.split(".", 1)

        row_count = await self._fetch_table_row_estimate(connector, schema, table_name)
        selected_columns = self._select_profile_columns(query, columns)
        column_stats = await self._fetch_column_stats(
            connector, schema, table_name, selected_columns
        )
        return {
            "row_count": row_count,
            "columns": column_stats,
        }

    def _select_profile_columns(
        self, query: str, columns: list[tuple[str, str | None]]
    ) -> list[str]:
        if not columns:
            return []
        tokens = set(self._tokenize_query(query))
        tokens.update({"amount", "total", "revenue", "price", "cost", "sales"})
        numeric_types = {
            "smallint",
            "integer",
            "bigint",
            "numeric",
            "decimal",
            "real",
            "double precision",
            "float",
        }
        preferred: list[str] = []
        fallback: list[str] = []
        for name, dtype in columns:
            dtype_norm = (dtype or "").lower()
            if dtype_norm and dtype_norm in numeric_types:
                fallback.append(name)
                if any(token in name.lower() for token in tokens):
                    preferred.append(name)

        selected = preferred or fallback
        return selected[:5]

    async def _fetch_table_row_estimate(
        self, connector: BaseConnector, schema: str, table: str
    ) -> int | None:
        query = (
            "SELECT reltuples::BIGINT AS estimate "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = $2"
        )
        try:
            result = await connector.execute(query, params=[schema, table])
        except Exception:
            return None
        if not result.rows:
            return None
        value = result.rows[0].get("estimate")
        return int(value) if value is not None else None

    async def _fetch_column_stats(
        self,
        connector: BaseConnector,
        schema: str,
        table: str,
        columns: list[str],
    ) -> dict[str, dict[str, object]]:
        if not columns:
            return {}
        query = (
            "SELECT attname, null_frac, n_distinct, most_common_vals "
            "FROM pg_stats "
            "WHERE schemaname = $1 AND tablename = $2 AND attname = ANY($3::text[])"
        )
        try:
            result = await connector.execute(query, params=[schema, table, columns])
        except Exception:
            return {}

        stats: dict[str, dict[str, object]] = {}
        for row in result.rows:
            attname = row.get("attname")
            if not attname:
                continue
            common_vals = row.get("most_common_vals") or []
            common_vals = [str(value) for value in common_vals if value is not None]
            stats[str(attname)] = {
                "null_frac": row.get("null_frac"),
                "n_distinct": row.get("n_distinct"),
                "common_vals": common_vals,
            }
        return stats

    def _build_cached_profile_context(
        self,
        *,
        db_type: str,
        db_url: str,
        focus_tables: list[str],
    ) -> str:
        snapshot = load_profile_cache(database_type=db_type, database_url=db_url)
        if not snapshot:
            return ""
        table_entries = snapshot.get("tables")
        if not isinstance(table_entries, list) or not table_entries:
            return ""

        selected = []
        focus_set = {table.lower() for table in focus_tables}
        for entry in table_entries:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name") or "").strip()
            if not name:
                continue
            if focus_set and name.lower() not in focus_set:
                continue
            selected.append(entry)
            if len(selected) >= 3:
                break

        if not selected:
            for entry in table_entries[:3]:
                if isinstance(entry, dict):
                    selected.append(entry)

        if not selected:
            return ""

        lines = []
        for entry in selected:
            table_name = str(entry.get("name") or "unknown")
            status = str(entry.get("status") or "unknown")
            row_count = entry.get("row_count")
            line = f"- {table_name}"
            if row_count is not None:
                line += f" (~{row_count} rows)"
            if status != "completed":
                line += f" [{status}]"
            lines.append(line)
            columns = entry.get("columns")
            if isinstance(columns, list):
                for column in columns[:4]:
                    if not isinstance(column, dict):
                        continue
                    col_name = str(column.get("name") or "")
                    data_type = str(column.get("data_type") or "")
                    if not col_name:
                        continue
                    if data_type:
                        lines.append(f"  - {col_name} ({data_type})")
                    else:
                        lines.append(f"  - {col_name}")
        if not lines:
            return ""
        return "\n**Auto-profile cache snapshot:**\n" + "\n".join(lines)

    async def _build_live_semantic_context(
        self,
        *,
        schema_key: str,
        query: str,
        columns_by_table: dict[str, list[tuple[str, str | None]]],
        focus_tables: list[str],
    ) -> str:
        if not self._pipeline_flag("live_schema_semantic_context_enabled", True):
            return ""
        if not focus_tables:
            return ""

        max_tables = max(2, min(self._pipeline_int("live_schema_semantic_max_tables", 6), 12))
        selected_tables = focus_tables[:max_tables]
        cache_key = f"{schema_key}::{'|'.join(selected_tables)}"
        cached = self._live_semantic_cache.get(cache_key)
        if cached:
            return cached

        # Deterministic baseline works without any extra model call.
        semantic_rows = self._build_semantic_rows_deterministic(columns_by_table, selected_tables)

        # Optionally refine with LLM for better table summaries.
        if (
            self._pipeline_flag("live_schema_semantic_llm_enabled", True)
            and not self._providers_are_equivalent(self.fast_llm, self.llm)
        ):
            llm_rows = await self._refine_semantic_rows_with_llm(
                query=query,
                columns_by_table=columns_by_table,
                selected_tables=selected_tables,
            )
            if llm_rows:
                semantic_rows = llm_rows

        if not semantic_rows:
            return ""

        lines = ["\n**Semantic schema digest (auto-generated):**"]
        for row in semantic_rows:
            table = str(row.get("table") or "").strip()
            if not table:
                continue
            summary = str(row.get("summary") or "").strip()
            dimensions = [str(item) for item in row.get("dimensions", []) if str(item).strip()]
            measures = [str(item) for item in row.get("measures", []) if str(item).strip()]
            time_columns = [str(item) for item in row.get("time_columns", []) if str(item).strip()]
            viz = str(row.get("visualization_hint") or "table").strip()
            if summary:
                lines.append(f"- {table}: {summary}")
            else:
                lines.append(f"- {table}")
            if dimensions:
                lines.append(f"  - Dimensions: {', '.join(dimensions[:5])}")
            if measures:
                lines.append(f"  - Measures: {', '.join(measures[:5])}")
            if time_columns:
                lines.append(f"  - Time columns: {', '.join(time_columns[:3])}")
            lines.append(f"  - Suggested visualization: {viz}")

        context = "\n".join(lines)
        self._live_semantic_cache[cache_key] = context
        return context

    def _build_semantic_rows_deterministic(
        self,
        columns_by_table: dict[str, list[tuple[str, str | None]]],
        selected_tables: list[str],
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for table in selected_tables:
            columns = columns_by_table.get(table, [])
            dimensions: list[str] = []
            measures: list[str] = []
            time_columns: list[str] = []
            for col_name, col_type in columns:
                dtype = (col_type or "").lower()
                if any(token in dtype for token in ("date", "time", "timestamp")):
                    time_columns.append(col_name)
                elif any(token in dtype for token in ("int", "numeric", "decimal", "float", "double")):
                    measures.append(col_name)
                else:
                    dimensions.append(col_name)
                if len(dimensions) > 12 and len(measures) > 12 and len(time_columns) > 8:
                    break

            if time_columns and measures:
                role = "time-series fact table"
                viz = "line"
            elif dimensions and measures:
                role = "categorical fact table"
                viz = "bar"
            elif measures:
                role = "metric table"
                viz = "kpi"
            else:
                role = "reference table"
                viz = "table"

            rows.append(
                {
                    "table": table,
                    "summary": f"Likely {role} for analytical questions.",
                    "dimensions": dimensions[:8],
                    "measures": measures[:8],
                    "time_columns": time_columns[:5],
                    "visualization_hint": viz,
                }
            )
        return rows

    async def _refine_semantic_rows_with_llm(
        self,
        *,
        query: str,
        columns_by_table: dict[str, list[tuple[str, str | None]]],
        selected_tables: list[str],
    ) -> list[dict[str, object]] | None:
        max_columns = max(6, min(self._pipeline_int("live_schema_semantic_max_columns", 12), 20))
        schema_payload: list[dict[str, object]] = []
        for table in selected_tables:
            columns = columns_by_table.get(table, [])
            schema_payload.append(
                {
                    "table": table,
                    "columns": [
                        {"name": name, "type": dtype or "unknown"}
                        for name, dtype in columns[:max_columns]
                    ],
                }
            )
        if not schema_payload:
            return None

        prompt = (
            "You are a data modeling assistant. Using only the provided schema, infer short "
            "semantic summaries and visualization hints for each table.\n"
            "Return strict JSON object with key 'tables' containing list items with keys: "
            "table, summary, dimensions (array), measures (array), time_columns (array), "
            "visualization_hint.\n"
            "Do not invent tables or columns. Keep summaries under 14 words.\n\n"
            f"User question: {query}\n\n"
            f"Schema: {json.dumps(schema_payload)}"
        )
        request = LLMRequest(
            messages=[
                LLMMessage(role="system", content="Return only valid JSON."),
                LLMMessage(role="user", content=prompt),
            ],
            temperature=0.0,
            max_tokens=700,
        )
        try:
            response = await self.fast_llm.generate(request)
            self._track_llm_call(tokens=self._safe_total_tokens(response))
            content = self._coerce_response_content(response.content)
            json_match = re.search(r"\{[\s\S]*\}", content)
            if not json_match:
                return None
            payload = json.loads(json_match.group(0))
            table_rows = payload.get("tables")
            if not isinstance(table_rows, list):
                return None
            allowed_tables = set(selected_tables)
            normalized_rows: list[dict[str, object]] = []
            for row in table_rows:
                if not isinstance(row, dict):
                    continue
                table = str(row.get("table") or "").strip()
                if table not in allowed_tables:
                    continue
                normalized_rows.append(
                    {
                        "table": table,
                        "summary": str(row.get("summary") or "").strip(),
                        "dimensions": [
                            str(item)
                            for item in row.get("dimensions", [])
                            if isinstance(item, str)
                        ],
                        "measures": [
                            str(item)
                            for item in row.get("measures", [])
                            if isinstance(item, str)
                        ],
                        "time_columns": [
                            str(item)
                            for item in row.get("time_columns", [])
                            if isinstance(item, str)
                        ],
                        "visualization_hint": str(row.get("visualization_hint") or "table").strip(),
                    }
                )
            return normalized_rows or None
        except Exception:
            return None

    def _catalog_schemas_for_db(self, db_type: str) -> set[str]:
        return get_catalog_schemas(db_type)

    def _catalog_aliases_for_db(self, db_type: str) -> set[str]:
        return get_catalog_aliases(db_type)

    def _is_catalog_table(
        self, table: str, catalog_schemas: set[str], catalog_aliases: set[str]
    ) -> bool:
        if table in catalog_aliases:
            return True
        if table in catalog_schemas:
            return True
        for schema in catalog_schemas:
            if table.startswith(f"{schema}."):
                return True
            if f".{schema}." in table:
                return True
        return False

    def _build_correction_prompt(
        self, generated_sql: GeneratedSQL, issues: list[ValidationIssue], input: SQLAgentInput
    ) -> str:
        """
        Build prompt for SQL self-correction.

        Args:
            generated_sql: Original generated SQL
            issues: Validation issues found
            input: Original input

        Returns:
            Correction prompt string
        """
        # Extract schema context
        schema_context = self._format_schema_context(input.investigation_memory)

        # Format issues
        issues_text = "\n".join(
            [
                f"- {issue.issue_type.upper()}: {issue.message}"
                + (f" (Suggested fix: {issue.suggested_fix})" if issue.suggested_fix else "")
                for issue in issues
            ]
        )

        return self.prompts.render(
            "agents/sql_correction.md",
            original_sql=generated_sql.sql,
            issues=issues_text,
            schema_context=schema_context,
        )

    def _format_schema_context(self, memory) -> str:
        """
        Format schema DataPoints into readable context.

        Args:
            memory: InvestigationMemory with DataPoints

        Returns:
            Formatted schema context string
        """
        schema_parts = []

        for dp in memory.datapoints:
            if dp.datapoint_type == "Schema":
                # Access metadata as dict
                metadata = dp.metadata if isinstance(dp.metadata, dict) else {}

                table_name = metadata.get("table_name", "unknown")
                table_schema = metadata.get("schema", "")
                business_purpose = metadata.get("business_purpose", "")

                # Only prefix schema if table_name doesn't already include it
                # Avoids double-qualification like "analytics.analytics.fact_sales"
                if table_schema and "." not in table_name:
                    full_table_name = f"{table_schema}.{table_name}"
                else:
                    full_table_name = table_name
                schema_parts.append(f"\n**Table: {full_table_name}**")
                if business_purpose:
                    schema_parts.append(f"Purpose: {business_purpose}")

                # Add columns
                columns = self._coerce_metadata_list(metadata.get("key_columns", []))
                if columns:
                    schema_parts.append("Columns:")
                    for col in columns:
                        if isinstance(col, dict):
                            col_name = col.get("name", "unknown")
                            col_type = col.get("type", "unknown")
                            col_meaning = col.get("business_meaning", "")
                            schema_parts.append(f"  - {col_name} ({col_type}): {col_meaning}")
                        elif isinstance(col, str):
                            schema_parts.append(f"  - {col}")

                # Add relationships
                relationships = self._coerce_metadata_list(metadata.get("relationships", []))
                if relationships:
                    schema_parts.append("Relationships:")
                    for rel in relationships:
                        if isinstance(rel, dict):
                            target = rel.get("target_table", "unknown")
                            join_col = rel.get("join_column", "unknown")
                            cardinality = rel.get("cardinality", "")
                            schema_parts.append(f"  - JOIN {target} ON {join_col} ({cardinality})")
                        elif isinstance(rel, str):
                            schema_parts.append(f"  - {rel}")

                # Add gotchas/common queries
                gotchas = self._coerce_string_list(metadata.get("gotchas", []))
                if gotchas:
                    schema_parts.append(f"Important Notes: {'; '.join(gotchas)}")

        return "\n".join(schema_parts) if schema_parts else "No schema context available"

    def _format_business_context(self, memory) -> str:
        """
        Format business DataPoints into readable context.

        Args:
            memory: InvestigationMemory with DataPoints

        Returns:
            Formatted business context string
        """
        business_parts = []

        for dp in memory.datapoints:
            if dp.datapoint_type == "Business":
                # Access metadata as dict
                metadata = dp.metadata if isinstance(dp.metadata, dict) else {}

                name = dp.name
                calculation = metadata.get("calculation", "")
                synonyms = self._coerce_string_list(metadata.get("synonyms", []))
                business_rules = self._coerce_string_list(metadata.get("business_rules", []))

                business_parts.append(f"\n**Metric: {name}**")
                if calculation:
                    business_parts.append(f"Calculation: {calculation}")
                if synonyms:
                    business_parts.append(f"Also known as: {', '.join(synonyms)}")
                if business_rules:
                    business_parts.append("Business Rules:")
                    for rule in business_rules:
                        business_parts.append(f"  - {rule}")

        return "\n".join(business_parts) if business_parts else "No business rules available"

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

    def _format_conversation_context(self, history: list[dict] | list) -> str:
        if not history:
            return "No conversation context available."

        recent = history[-6:]
        lines = []
        for msg in recent:
            role = "user"
            content = ""
            if isinstance(msg, dict):
                role = str(msg.get("role", role))
                content = str(msg.get("content", ""))
            else:
                role = str(getattr(msg, "role", role))
                content = str(getattr(msg, "content", ""))
            if content:
                lines.append(f"{role}: {content}")

        return "\n".join(lines) if lines else "No conversation context available."

    def _parse_llm_response(self, content: Any, input: SQLAgentInput) -> GeneratedSQL:
        """
        Parse LLM response into GeneratedSQL.

        Args:
            content: Raw LLM response content
            input: Original input

        Returns:
            GeneratedSQL object

        Raises:
            ValueError: If response cannot be parsed
        """
        normalized_content = self._coerce_response_content(content)
        if not normalized_content.strip():
            raise ValueError("Failed to parse LLM response: empty content")

        # Try JSON first.
        json_str = None
        json_match = re.search(
            r"```(?:json)?\s*(\{.*?\})\s*```",
            normalized_content,
            re.DOTALL,
        )
        if json_match:
            json_str = json_match.group(1)
        else:
            json_match = re.search(r"\{.*\}", normalized_content, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)

        if json_str:
            try:
                data = json.loads(json_str)
                sql_value = data.get("sql") or data.get("query")
                if isinstance(sql_value, str):
                    sql_value = sql_value.strip()
                if sql_value:
                    explanation = (
                        data.get("explanation")
                        or data.get("reasoning")
                        or data.get("rationale")
                        or ""
                    )
                    generated = GeneratedSQL(
                        sql=sql_value,
                        explanation=explanation,
                        used_datapoints=data.get("used_datapoints", []),
                        confidence=data.get("confidence", 0.8),
                        assumptions=data.get("assumptions", []),
                        clarifying_questions=data.get("clarifying_questions", []),
                    )
                    return self._apply_row_limit_policy(generated, input.query)
                raw_questions = data.get("clarifying_questions", [])
                if isinstance(raw_questions, str):
                    clarifying_questions = [raw_questions.strip()] if raw_questions.strip() else []
                elif isinstance(raw_questions, list):
                    clarifying_questions = [str(item).strip() for item in raw_questions if str(item).strip()]
                else:
                    clarifying_questions = []
                if clarifying_questions:
                    confidence = data.get("confidence", 0.2)
                    try:
                        parsed_confidence = float(confidence)
                    except (TypeError, ValueError):
                        parsed_confidence = 0.2
                    return GeneratedSQL(
                        sql="SELECT 1",
                        explanation=(
                            data.get("explanation")
                            or "Model requested clarification before generating executable SQL."
                        ),
                        used_datapoints=data.get("used_datapoints", []),
                        confidence=max(0.0, min(1.0, parsed_confidence)),
                        assumptions=data.get("assumptions", []),
                        clarifying_questions=clarifying_questions[:2],
                    )
            except json.JSONDecodeError as e:
                logger.debug(f"Invalid JSON in LLM response: {e}")

        partial_sql = self._extract_partial_json_sql(normalized_content)
        if partial_sql:
            generated = GeneratedSQL(
                sql=partial_sql,
                explanation="Recovered SQL from partial JSON output",
                used_datapoints=[],
                confidence=0.55,
                assumptions=["Recovered from incomplete model output"],
                clarifying_questions=[],
            )
            return self._apply_row_limit_policy(generated, input.query)

        # Fallback: extract SQL from markdown/code/text output.
        sql_text = self._extract_sql_from_response(normalized_content)
        if sql_text:
            generated = GeneratedSQL(
                sql=sql_text,
                explanation="Generated SQL",
                used_datapoints=[],
                confidence=0.6,
                assumptions=[],
                clarifying_questions=[],
            )
            return self._apply_row_limit_policy(generated, input.query)

        logger.warning("Failed to parse LLM response: Response missing 'sql' field")
        logger.debug(f"LLM response content: {normalized_content}")
        raise ValueError("Failed to parse LLM response: Response missing 'sql' field")

    def _build_finance_net_flow_template(self, input: SQLAgentInput) -> GeneratedSQL | None:
        """Return deterministic SQL for weekly deposits/withdrawals/net-flow by segment."""
        query = (input.query or "").lower()
        required_signals = ("deposit", "withdraw", "net flow", "segment", "week")
        if not all(signal in query for signal in required_signals):
            return None

        table_name_map = self._collect_available_table_names(input)
        required_tables = ("bank_transactions", "bank_accounts", "bank_customers")
        resolved: dict[str, str] = {}
        for required in required_tables:
            matched = next(
                (
                    table_name
                    for key, table_name in table_name_map.items()
                    if key == required or key.endswith(f".{required}")
                ),
                None,
            )
            if not matched:
                return None
            resolved[required] = matched

        db_type = input.database_type or getattr(self.config.database, "db_type", "postgresql")
        if db_type != "postgresql":
            return None

        transactions_table = self.catalog.format_table_reference(
            resolved["bank_transactions"], db_type=db_type
        )
        accounts_table = self.catalog.format_table_reference(resolved["bank_accounts"], db_type=db_type)
        customers_table = self.catalog.format_table_reference(
            resolved["bank_customers"], db_type=db_type
        )
        if not transactions_table or not accounts_table or not customers_table:
            return None

        sql = (
            "WITH anchor AS ("
            f" SELECT MAX(t.business_date) AS max_business_date FROM {transactions_table} t"
            " WHERE t.status = 'posted'"
            "), weekly_segment_flow AS ("
            " SELECT"
            "   DATE_TRUNC('week', t.business_date)::date AS week_start,"
            "   c.segment AS segment,"
            "   SUM(CASE WHEN t.direction = 'credit' THEN t.amount ELSE 0 END) AS deposits,"
            "   SUM(CASE WHEN t.direction = 'debit' THEN t.amount ELSE 0 END) AS withdrawals,"
            "   SUM(CASE WHEN t.direction = 'credit' THEN t.amount ELSE -t.amount END) AS net_flow"
            f" FROM {transactions_table} t"
            f" JOIN {accounts_table} a ON a.account_id = t.account_id"
            f" JOIN {customers_table} c ON c.customer_id = a.customer_id"
            " CROSS JOIN anchor"
            " WHERE t.status = 'posted'"
            "   AND t.business_date >= (anchor.max_business_date - INTERVAL '7 weeks')"
            " GROUP BY 1, 2"
            "), with_wow AS ("
            " SELECT"
            "   week_start,"
            "   segment,"
            "   deposits,"
            "   withdrawals,"
            "   net_flow,"
            "   net_flow - LAG(net_flow) OVER (PARTITION BY segment ORDER BY week_start)"
            "     AS wow_net_flow_change"
            " FROM weekly_segment_flow"
            "), segment_decline AS ("
            " SELECT"
            "   segment,"
            "   SUM(CASE WHEN wow_net_flow_change < 0 THEN wow_net_flow_change ELSE 0 END)"
            "     AS total_wow_decline,"
            "   ROW_NUMBER() OVER ("
            "     ORDER BY SUM(CASE WHEN wow_net_flow_change < 0 THEN wow_net_flow_change ELSE 0 END)"
            "   ) AS decline_rank"
            " FROM with_wow"
            " GROUP BY segment"
            ")"
            " SELECT"
            "   w.week_start,"
            "   w.segment,"
            "   w.deposits,"
            "   w.withdrawals,"
            "   w.net_flow,"
            "   w.wow_net_flow_change,"
            "   (COALESCE(d.decline_rank, 999) <= 2) AS top_decline_driver"
            " FROM with_wow w"
            " LEFT JOIN segment_decline d ON d.segment = w.segment"
            " ORDER BY w.week_start DESC, w.segment"
        )
        return GeneratedSQL(
            sql=sql,
            explanation=(
                "Computed weekly deposits, withdrawals, and net flow by segment over the latest 8 weeks, "
                "then flagged top 2 segments with largest cumulative week-over-week net-flow decline."
            ),
            used_datapoints=[],
            confidence=0.78,
            assumptions=[
                "Deposits are inferred as transactions where direction = 'credit'.",
                "Withdrawals are inferred as transactions where direction = 'debit'.",
                "Window anchors to latest posted business_date in bank_transactions.",
            ],
            clarifying_questions=[],
        )

    def _build_finance_loan_default_rate_template(self, input: SQLAgentInput) -> GeneratedSQL | None:
        """Return deterministic SQL for loan default rate aggregated by customer segment."""
        query = (input.query or "").lower()
        has_default_rate_intent = "default rate" in query or (
            "default" in query and "rate" in query
        )
        if not (has_default_rate_intent and "loan" in query and "segment" in query):
            return None

        table_name_map = self._collect_available_table_names(input)

        def _resolve_table(required: str) -> str | None:
            return next(
                (
                    table_name
                    for key, table_name in table_name_map.items()
                    if key == required or key.endswith(f".{required}")
                ),
                None,
            )

        loans_table_name = _resolve_table("bank_loans")
        customers_table_name = _resolve_table("bank_customers")
        if not loans_table_name and not customers_table_name:
            return None

        db_type = input.database_type or getattr(self.config.database, "db_type", "postgresql")
        if db_type != "postgresql":
            return None

        # If one side is missing from retrieval context, fall back to canonical fintech table names.
        if not loans_table_name:
            loans_table_name = "public.bank_loans"
        if not customers_table_name:
            customers_table_name = "public.bank_customers"

        loans_table = self.catalog.format_table_reference(loans_table_name, db_type=db_type)
        customers_table = self.catalog.format_table_reference(customers_table_name, db_type=db_type)
        if not loans_table or not customers_table:
            return None

        sql = (
            "SELECT"
            "   c.segment,"
            "   COUNT(*) AS total_loans,"
            "   COUNT(*) FILTER ("
            "     WHERE l.days_past_due >= 90 OR l.status = 'non_performing'"
            "   ) AS defaulted_loans,"
            "   ROUND("
            "     100.0 * COUNT(*) FILTER ("
            "       WHERE l.days_past_due >= 90 OR l.status = 'non_performing'"
            "     ) / NULLIF(COUNT(*), 0),"
            "     2"
            "   ) AS default_rate_pct,"
            "   ROUND(AVG(l.days_past_due)::numeric, 2) AS avg_days_past_due"
            f" FROM {loans_table} l"
            f" JOIN {customers_table} c ON c.customer_id = l.customer_id"
            " GROUP BY c.segment"
            " ORDER BY default_rate_pct DESC, avg_days_past_due DESC"
        )

        return GeneratedSQL(
            sql=sql,
            explanation="Computed loan default rate by customer segment using 90+ DPD/non-performing default proxy.",
            used_datapoints=[],
            confidence=0.8,
            assumptions=[
                "Default proxy uses days_past_due >= 90 or status = 'non_performing'.",
                "Segment comes from bank_customers joined via customer_id.",
            ],
            clarifying_questions=[],
        )

    def _collect_available_table_names(self, input: SQLAgentInput) -> dict[str, str]:
        table_map: dict[str, str] = {}
        for datapoint in getattr(input.investigation_memory, "datapoints", []) or []:
            metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
            table_candidates: list[str] = []
            table_name = metadata.get("table_name") or metadata.get("table")
            if isinstance(table_name, str) and table_name.strip():
                table_candidates.append(table_name.strip())
            table_candidates.extend(self._coerce_string_list(metadata.get("related_tables")))
            table_candidates.extend(self._coerce_string_list(metadata.get("target_tables")))
            sql_template = metadata.get("sql_template")
            if isinstance(sql_template, str) and sql_template.strip():
                table_candidates.extend(self._extract_table_names_from_sql(sql_template))
            relationships = metadata.get("relationships")
            if isinstance(relationships, str):
                try:
                    relationships = json.loads(relationships)
                except json.JSONDecodeError:
                    relationships = None
            if isinstance(relationships, list):
                for relation in relationships:
                    if not isinstance(relation, dict):
                        continue
                    target_table = relation.get("target_table") or relation.get("table")
                    if isinstance(target_table, str) and target_table.strip():
                        table_candidates.append(target_table.strip())

            for candidate in table_candidates:
                cleaned = str(candidate).strip()
                if not cleaned:
                    continue
                normalized = cleaned.lower()
                table_map.setdefault(normalized, cleaned)

        db_type = input.database_type or getattr(self.config.database, "db_type", "postgresql")
        db_url = input.database_url or (
            str(self.config.database.url) if self.config.database.url else None
        )
        if db_url:
            schema_key = f"{db_type}::{db_url}"
            for table_name in self._live_schema_tables_cache.get(schema_key, set()):
                if isinstance(table_name, str) and table_name.strip():
                    normalized = table_name.strip().lower()
                    table_map.setdefault(normalized, table_name.strip())
        return table_map

    def _extract_table_names_from_sql(self, sql_template: str) -> list[str]:
        matches = re.findall(
            r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_\.]*)",
            sql_template,
            flags=re.IGNORECASE,
        )
        tables: list[str] = []
        for match in matches:
            table = str(match).strip()
            if table and table not in tables:
                tables.append(table)
        return tables

    def _coerce_response_content(self, content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, bytes):
            return content.decode("utf-8", errors="ignore")
        if isinstance(content, dict | list):
            try:
                return json.dumps(content)
            except TypeError:
                return str(content)
        if content is None:
            return ""
        return str(content)

    def _safe_total_tokens(self, response: Any) -> int | None:
        usage = getattr(response, "usage", None)
        total_tokens = getattr(usage, "total_tokens", None)
        try:
            return int(total_tokens) if total_tokens is not None else None
        except (TypeError, ValueError):
            return None

    def _extract_partial_json_sql(self, content: str) -> str | None:
        complete_match = re.search(
            r'"(?:sql|query)"\s*:\s*"((?:\\.|[^"\\])*)"',
            content,
            re.IGNORECASE | re.DOTALL,
        )
        if complete_match:
            candidate = self._decode_json_string_fragment(complete_match.group(1))
            extracted = self._extract_sql_statement(candidate)
            if extracted:
                return extracted

        truncated_match = re.search(
            r'"(?:sql|query)"\s*:\s*"([\s\S]+?)(?:",\s*"(?:explanation|confidence|used_datapoints|assumptions|clarifying_questions)"|\}\s*$)',
            content,
            re.IGNORECASE,
        )
        if truncated_match:
            candidate = self._decode_json_string_fragment(truncated_match.group(1))
            extracted = self._extract_sql_statement(candidate)
            if extracted:
                return extracted

        return None

    def _decode_json_string_fragment(self, value: str) -> str:
        candidate = value.strip()
        if not candidate:
            return ""
        try:
            return json.loads(f'"{candidate}"')
        except json.JSONDecodeError:
            # Best-effort unescape for partially malformed JSON strings.
            return candidate.replace("\\n", " ").replace("\\t", " ").replace('\\"', '"')

    def _looks_truncated_response(self, content: str, finish_reason: str | None) -> bool:
        normalized_reason = str(finish_reason or "").lower()
        if normalized_reason in {"length", "max_tokens", "max_output_tokens"}:
            return True

        stripped = content.rstrip()
        if stripped.startswith("```") and stripped.count("```") % 2 == 1:
            return True
        if stripped.count("{") > stripped.count("}"):
            return True
        if stripped.endswith(":") or stripped.endswith('"sql"'):
            return True
        return False

    def _apply_row_limit_policy(self, generated: GeneratedSQL, query: str) -> GeneratedSQL:
        """Normalize SQL limits for safe interactive responses."""
        sql = generated.sql.strip()
        if not sql:
            return generated

        sql_upper = sql.upper()
        if (
            sql_upper.startswith("SHOW")
            or sql_upper.startswith("DESCRIBE")
            or sql_upper.startswith("DESC")
        ):
            return generated
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            return generated
        if self._is_single_value_query(sql_upper):
            return generated

        requested_limit = self._extract_requested_limit(query)
        if requested_limit is not None:
            target_limit = max(1, min(requested_limit, self._max_safe_row_limit))
            force_limit = True
        else:
            if self._is_aggregate_query(sql_upper):
                return generated
            target_limit = self._default_row_limit
            if self.catalog.is_list_tables_query(query.lower()) or self._is_catalog_metadata_query(
                sql_upper
            ):
                target_limit = self._max_safe_row_limit
            force_limit = False

        rewritten = self._rewrite_sql_limit(sql, target_limit, force_limit=force_limit)
        if rewritten == sql:
            return generated
        return generated.model_copy(update={"sql": rewritten})

    def _extract_requested_limit(self, query: str) -> int | None:
        """Extract explicit row limit requested by the user."""
        text = (query or "").lower()
        if not text:
            return None

        patterns = (
            r"\b(first|top|limit)\s+(\d+)\b",
            r"\bshow\s+(\d+)\s+rows?\b",
            r"\b(\d+)\s+rows?\b",
        )
        for pattern in patterns:
            match = re.search(pattern, text)
            if not match:
                continue
            numbers = [group for group in match.groups() if group and group.isdigit()]
            if not numbers:
                continue
            try:
                return int(numbers[0])
            except ValueError:
                continue
        return None

    def _rewrite_sql_limit(self, sql: str, target_limit: int, *, force_limit: bool) -> str:
        """Rewrite SQL LIMIT to enforce bounded row returns."""
        top_level_limits = self._scan_top_level_limit_clauses(sql)
        numeric_limits = [entry for entry in top_level_limits if entry[2].isdigit()]

        if numeric_limits:
            value_start, value_end, value_token = numeric_limits[-1]
            current_limit = int(value_token)
            new_limit = target_limit if force_limit else min(current_limit, target_limit)
            if new_limit == current_limit:
                return sql
            return f"{sql[:value_start]}{new_limit}{sql[value_end:]}"

        # Avoid appending a second LIMIT when a top-level non-numeric variant already exists
        # (e.g. LIMIT $1). Subquery limits do not count.
        if top_level_limits:
            return sql

        stripped = sql.rstrip()
        if stripped.endswith(";"):
            stripped = stripped[:-1].rstrip()
            return f"{stripped} LIMIT {target_limit};"
        return f"{stripped} LIMIT {target_limit}"

    def _scan_top_level_limit_clauses(self, sql: str) -> list[tuple[int, int, str]]:
        """
        Return top-level LIMIT clause values.

        Returns tuples of: (value_start_index, value_end_index, value_token).
        """
        clauses: list[tuple[int, int, str]] = []
        i = 0
        depth = 0
        length = len(sql)

        while i < length:
            ch = sql[i]
            nxt = sql[i + 1] if i + 1 < length else ""

            # Skip line comments.
            if ch == "-" and nxt == "-":
                i += 2
                while i < length and sql[i] != "\n":
                    i += 1
                continue

            # Skip block comments.
            if ch == "/" and nxt == "*":
                i += 2
                while i + 1 < length and not (sql[i] == "*" and sql[i + 1] == "/"):
                    i += 1
                i = min(i + 2, length)
                continue

            # Skip quoted strings/identifiers.
            if ch in ("'", '"', "`"):
                quote = ch
                i += 1
                while i < length:
                    if sql[i] == quote:
                        # Handle doubled quotes: '' or "" or ``
                        if i + 1 < length and sql[i + 1] == quote:
                            i += 2
                            continue
                        i += 1
                        break
                    i += 1
                continue

            if ch == "(":
                depth += 1
                i += 1
                continue

            if ch == ")":
                depth = max(0, depth - 1)
                i += 1
                continue

            if depth == 0 and self._matches_keyword(sql, i, "LIMIT"):
                value_start = i + len("LIMIT")
                while value_start < length and sql[value_start].isspace():
                    value_start += 1
                value_end = value_start
                while value_end < length and re.match(r"[A-Za-z0-9_$:]", sql[value_end]):
                    value_end += 1
                value_token = sql[value_start:value_end]
                clauses.append((value_start, value_end, value_token))
                i = value_end
                continue

            i += 1

        return clauses

    @staticmethod
    def _matches_keyword(sql: str, index: int, keyword: str) -> bool:
        end = index + len(keyword)
        if sql[index:end].upper() != keyword:
            return False
        before = sql[index - 1] if index > 0 else " "
        after = sql[end] if end < len(sql) else " "
        return not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_")

    def _is_single_value_query(self, sql_upper: str) -> bool:
        """Heuristic: single aggregate queries usually return one row and don't need LIMIT."""
        if "GROUP BY" in sql_upper:
            return False
        aggregate_tokens = ("COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "BOOL_OR(", "BOOL_AND(")
        return any(token in sql_upper for token in aggregate_tokens)

    def _is_aggregate_query(self, sql_upper: str) -> bool:
        """Detect aggregate queries where implicit row limits should not be added."""
        if "GROUP BY" in sql_upper:
            return True
        aggregate_pattern = re.compile(
            r"\b(COUNT|SUM|AVG|MIN|MAX|BOOL_OR|BOOL_AND|ARRAY_AGG|JSON_AGG|STRING_AGG)\s*\("
        )
        return bool(aggregate_pattern.search(sql_upper))

    def _is_catalog_metadata_query(self, sql_upper: str) -> bool:
        """Detect catalog/system metadata SQL where a larger default preview is acceptable."""
        catalog_markers = (
            "INFORMATION_SCHEMA.TABLES",
            "INFORMATION_SCHEMA.COLUMNS",
            "PG_CATALOG.",
            "SYSTEM.TABLES",
            "SVV_TABLES",
            "PG_TABLE_DEF",
        )
        return any(marker in sql_upper for marker in catalog_markers)

    def _extract_sql_from_response(self, content: str) -> str | None:
        """Extract SQL from non-JSON LLM output."""
        code_blocks = re.findall(r"```(?:sql)?\s*(.*?)```", content, re.DOTALL | re.IGNORECASE)
        for block in code_blocks:
            candidate = block.strip()
            extracted = self._extract_sql_statement(candidate)
            if extracted:
                return extracted

        # Handle truncated markdown fence (opening fence without closing fence).
        fence_match = re.search(r"```(?:sql)?\s*([\s\S]+)$", content, re.IGNORECASE)
        if fence_match:
            candidate = fence_match.group(1).strip()
            extracted = self._extract_sql_statement(candidate)
            if extracted:
                return extracted

        # Common key-value style fallback: sql: SELECT ...
        inline_sql_field = re.search(
            r"\bsql\s*[:=]\s*(.+)$",
            content,
            re.IGNORECASE | re.MULTILINE,
        )
        if inline_sql_field:
            candidate = inline_sql_field.group(1).strip()
            extracted = self._extract_sql_statement(candidate)
            if extracted:
                return extracted

        return self._extract_sql_statement(content)

    def _extract_sql_statement(self, text: str) -> str | None:
        """Extract a SQL statement from free text."""
        # Prefer SQL-looking statements at line boundaries to avoid
        # accidentally parsing natural language like "show you ...".
        select_like = re.compile(
            r"(?:^|[\r\n])\s*(SELECT|WITH|EXPLAIN|DESCRIBE|DESC)\b[\s\S]*?(?:;|$)",
            re.IGNORECASE,
        )
        show_like = re.compile(
            r"(?:^|[\r\n])\s*SHOW\s+"
            r"(?:TABLES|FULL\s+TABLES|COLUMNS|DATABASES|SCHEMAS|CREATE\s+TABLE)\b"
            r"[\s\S]*?(?:;|$)",
            re.IGNORECASE,
        )

        for pattern in (select_like, show_like):
            match = pattern.search(text)
            if not match:
                continue
            statement = match.group(0).strip()
            statement = re.sub(r"\s+", " ", statement).strip().rstrip(";")
            if statement and "{" not in statement and "}" not in statement:
                return statement

        return None

    def _validate_input(self, input: SQLAgentInput) -> None:
        """
        Validate input type.

        Args:
            input: Input to validate

        Raises:
            ValidationError: If input is invalid
        """
        if not isinstance(input, SQLAgentInput):
            from backend.models.agent import ValidationError

            raise ValidationError(
                agent=self.name,
                message=f"Expected SQLAgentInput, got {type(input).__name__}",
                context={"input_type": type(input).__name__},
            )

    def _try_query_datapoint_template(
        self,
        input: SQLAgentInput,
    ) -> GeneratedSQL | None:
        """
        Try to match user query against QueryDataPoints for template execution.

        QueryDataPoints provide pre-validated SQL templates that can be
        parameterized and executed directly, bypassing LLM generation.

        Args:
            input: SQLAgentInput with investigation memory

        Returns:
            GeneratedSQL if a matching QueryDataPoint is found, None otherwise
        """
        if not input.investigation_memory or not input.investigation_memory.datapoints:
            return None

        query_lower = input.query.lower()

        best_match: tuple[float, Any, dict[str, Any]] | None = None

        for retrieved_dp in input.investigation_memory.datapoints:
            if retrieved_dp.datapoint_type != "Query":
                continue

            metadata = retrieved_dp.metadata if isinstance(retrieved_dp.metadata, dict) else {}

            sql_template = metadata.get("sql_template")
            if not sql_template:
                continue

            description = metadata.get("query_description", retrieved_dp.name).lower()

            tags = self._coerce_tag_list(metadata.get("tags"))
            score = self._query_template_match_score(
                query_lower=query_lower,
                name_lower=retrieved_dp.name.lower(),
                description_lower=description,
                tags=tags,
            )
            if score < 8:
                continue
            if best_match is None or score > best_match[0]:
                best_match = (score, retrieved_dp, metadata)

        if best_match is None:
            return None

        _score, matched_dp, metadata = best_match
        db_type = input.database_type or getattr(self.config.database, "db_type", "postgresql")
        backend_variants = metadata.get("backend_variants")
        if isinstance(backend_variants, str):
            try:
                backend_variants = json.loads(backend_variants)
            except json.JSONDecodeError:
                backend_variants = None

        sql = metadata.get("sql_template")
        if not isinstance(sql, str) or not sql.strip():
            return None
        if (
            backend_variants
            and isinstance(backend_variants, dict)
            and db_type in backend_variants
        ):
            sql = backend_variants[db_type]

        sql = self._fill_template_defaults(sql, metadata.get("parameters"))

        logger.info(
            f"Using QueryDataPoint template: {matched_dp.datapoint_id}",
            extra={"datapoint": matched_dp.datapoint_id},
        )

        return GeneratedSQL(
            sql=sql,
            explanation=f"Using pre-defined query template: {matched_dp.name}",
            used_datapoints=[matched_dp.datapoint_id],
            confidence=0.95,
            assumptions=["Using default parameter values from QueryDataPoint"],
            clarifying_questions=[],
        )

    def _query_matches_template(
        self,
        query_lower: str,
        name_lower: str,
        description_lower: str,
    ) -> bool:
        """
        Check if query matches a QueryDataPoint template.

        Simple keyword matching for now. Future versions could use
        semantic similarity or intent classification.

        Args:
            query_lower: Lowercased user query
            name_lower: Lowercased QueryDataPoint name
            description_lower: Lowercased QueryDataPoint description

        Returns:
            True if the query appears to match the template
        """
        name_words = set(name_lower.split())
        query_words = set(query_lower.split())

        overlap = name_words & query_words
        if len(overlap) >= 2:
            return True

        if len(name_words) >= 2:
            if all(word in query_lower for word in name_words if len(word) > 3):
                return True

        desc_keywords = [w for w in description_lower.split() if len(w) > 4]
        matching_keywords = sum(1 for k in desc_keywords[:10] if k in query_lower)
        if matching_keywords >= 3:
            return True

        return False

    def _query_template_match_score(
        self,
        *,
        query_lower: str,
        name_lower: str,
        description_lower: str,
        tags: list[str] | None = None,
    ) -> float:
        """Score QueryDataPoint-template relevance for a query."""
        query_tokens = self._tokenize_template_match_terms(query_lower)
        name_tokens = self._tokenize_template_match_terms(name_lower)
        description_tokens = self._tokenize_template_match_terms(description_lower)
        tag_tokens = self._tokenize_template_match_terms(" ".join(tags or []))

        if not query_tokens or not name_tokens:
            return 0.0

        overlap_name = len(query_tokens & name_tokens)
        overlap_description = len(query_tokens & description_tokens)
        overlap_tags = len(query_tokens & tag_tokens)
        coverage_name = overlap_name / max(1, len(name_tokens))

        score = float((overlap_name * 3) + (overlap_description * 2) + (overlap_tags * 2))
        if coverage_name >= 0.5:
            score += 2.0
        if overlap_name >= 3 and overlap_description >= 2:
            score += 1.5

        query_has_wow = "week-over-week" in query_lower or "week over week" in query_lower
        template_text = f"{name_lower} {description_lower} {' '.join(tags or [])}"
        template_has_wow = (
            "week-over-week" in template_text
            or "week over week" in template_text
            or "wow" in template_text
        )
        if query_has_wow and template_has_wow:
            score += 3.0

        if "net flow" in query_lower and "net flow" in template_text:
            score += 2.0
        if "decline" in query_tokens and "decline" in (
            name_tokens | description_tokens | tag_tokens
        ):
            score += 1.5

        return score

    def _tokenize_template_match_terms(self, text: str) -> set[str]:
        """Tokenize terms for robust template matching (handles hyphens/plurals/weekly)."""
        raw_tokens = re.findall(r"[a-z0-9]+", text.lower())
        stopwords = {
            "the",
            "a",
            "an",
            "for",
            "of",
            "to",
            "in",
            "on",
            "by",
            "with",
            "from",
            "and",
            "or",
            "query",
            "report",
            "show",
            "list",
            "what",
            "which",
        }
        tokens: set[str] = set()
        for token in raw_tokens:
            if not token or token in stopwords:
                continue
            tokens.add(token)
            if token.endswith("s") and len(token) > 3:
                tokens.add(token[:-1])
            if token.endswith("ly") and len(token) > 4:
                tokens.add(token[:-2])
            if token == "wow":
                tokens.update({"week", "over"})
        return tokens

    def _coerce_tag_list(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            if "," in value:
                return [part.strip() for part in value.split(",") if part.strip()]
            return [value.strip()] if value.strip() else []
        return []

    def _fill_template_defaults(
        self,
        sql_template: str,
        parameters: dict[str, Any] | str | None,
    ) -> str:
        """
        Fill SQL template placeholders with default values.

        Args:
            sql_template: SQL template with {parameter} placeholders
            parameters: Parameter definitions (dict or JSON string)

        Returns:
            SQL with defaults filled in
        """
        if not parameters:
            return sql_template

        if isinstance(parameters, str):
            try:
                parameters = json.loads(parameters)
            except json.JSONDecodeError:
                return sql_template

        if not isinstance(parameters, dict):
            return sql_template

        sql = sql_template
        for param_name, param_def in parameters.items():
            placeholder = f"{{{param_name}}}"
            if placeholder not in sql:
                continue

            if not isinstance(param_def, dict):
                continue

            default_value = param_def.get("default")
            if default_value is None:
                continue

            param_type = param_def.get("type", "string")
            if param_type in ("string", "timestamp", "enum"):
                formatted_value = f"'{default_value}'"
            elif param_type in ("integer", "float"):
                formatted_value = str(default_value)
            elif param_type == "boolean":
                formatted_value = "TRUE" if default_value else "FALSE"
            else:
                formatted_value = str(default_value)

            sql = sql.replace(placeholder, formatted_value)

        return sql

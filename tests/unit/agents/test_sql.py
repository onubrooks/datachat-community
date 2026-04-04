"""
Unit tests for SQLAgent.

Tests the SQL generation agent that creates SQL queries from natural language
with self-correction capabilities.
"""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from backend.agents.sql import QueryCompilerPlan, SQLAgent, SQLClarificationNeeded
from backend.llm.models import LLMResponse, LLMUsage
from backend.models.agent import (
    AgentMetadata,
    GeneratedSQL,
    InvestigationMemory,
    RetrievedDataPoint,
    SQLAgentInput,
    SQLAgentOutput,
    SQLGenerationError,
    ValidationIssue,
)


@pytest.fixture
def mock_llm_provider():
    """Create mock LLM provider."""
    provider = Mock()
    provider.generate = AsyncMock()
    provider.provider = "openai"
    provider.model = "gpt-4o"
    return provider


@pytest.fixture
def sql_agent(mock_llm_provider):
    """Create SQLAgent with mock LLM provider."""
    # Mock get_settings to avoid API key validation
    mock_settings = Mock()
    mock_settings.llm = Mock()
    mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)

    with (
        patch("backend.agents.sql.get_settings", return_value=mock_settings),
        patch(
            "backend.agents.sql.LLMProviderFactory.create_agent_provider",
            return_value=mock_llm_provider,
        ),
    ):
        agent = SQLAgent()
    return agent


@pytest.fixture
def sample_investigation_memory():
    """Create sample investigation memory with schema context."""
    return InvestigationMemory(
        query="What were total sales last quarter?",
        datapoints=[
            RetrievedDataPoint(
                datapoint_id="table_fact_sales_001",
                datapoint_type="Schema",
                name="Fact Sales Table",
                score=0.95,
                source="hybrid",
                metadata={
                    "table_name": "analytics.fact_sales",
                    "schema": "analytics",
                    "business_purpose": "Central fact table for all sales transactions",
                    "key_columns": [
                        {
                            "name": "amount",
                            "type": "DECIMAL(18,2)",
                            "business_meaning": "Transaction value in USD",
                            "nullable": False,
                        },
                        {
                            "name": "date",
                            "type": "DATE",
                            "business_meaning": "Transaction date",
                            "nullable": False,
                        },
                    ],
                    "relationships": [],
                    "gotchas": ["Always filter by date for performance"],
                },
            ),
            RetrievedDataPoint(
                datapoint_id="metric_revenue_001",
                datapoint_type="Business",
                name="Revenue",
                score=0.88,
                source="vector",
                metadata={
                    "calculation": "SUM(fact_sales.amount) WHERE status = 'completed'",
                    "synonyms": ["sales", "income"],
                    "business_rules": [
                        "Exclude refunds (status != 'refunded')",
                        "Only completed transactions",
                    ],
                },
            ),
        ],
        total_retrieved=2,
        retrieval_mode="hybrid",
        sources_used=["table_fact_sales_001", "metric_revenue_001"],
    )


@pytest.fixture
def sample_sql_agent_input(sample_investigation_memory):
    """Create sample SQLAgentInput."""
    return SQLAgentInput(
        query="What were total sales last quarter?",
        investigation_memory=sample_investigation_memory,
        max_correction_attempts=3,
    )


@pytest.fixture
def sample_valid_llm_response():
    """Create sample valid LLM response with SQL."""
    response_json = {
        "sql": "SELECT SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-07-01' AND date < '2024-10-01'",
        "explanation": "This query calculates total sales for Q3 2024",
        "used_datapoints": ["table_fact_sales_001", "metric_revenue_001"],
        "confidence": 0.95,
        "assumptions": ["'last quarter' refers to Q3 2024"],
        "clarifying_questions": [],
    }

    return LLMResponse(
        content=f"```json\n{json.dumps(response_json)}\n```",
        model="gpt-4o",
        usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
        finish_reason="stop",
        provider="openai",
    )


class TestInitialization:
    """Test SQLAgent initialization."""

    def test_initialization_creates_llm_provider(self, sql_agent):
        """Test agent initializes with LLM provider."""
        assert sql_agent.name == "SQLAgent"
        assert sql_agent.llm is not None
        assert sql_agent.llm.provider == "openai"
        assert sql_agent.llm.model == "gpt-4o"


class TestExecution:
    """Test SQLAgent execution."""

    @pytest.mark.asyncio
    async def test_successful_sql_generation(
        self, sql_agent, sample_sql_agent_input, sample_valid_llm_response
    ):
        """Test successful SQL generation without corrections."""
        # Mock LLM response
        sql_agent.llm.generate.return_value = sample_valid_llm_response

        # Execute
        output = await sql_agent(sample_sql_agent_input)

        # Assertions
        assert isinstance(output, SQLAgentOutput)
        assert output.success is True
        assert output.generated_sql.sql.startswith("SELECT")
        assert "fact_sales" in output.generated_sql.sql.lower()
        assert output.generated_sql.confidence == 0.95
        assert len(output.correction_attempts) == 0
        assert output.needs_clarification is False
        assert output.metadata.llm_calls == 1
        assert output.metadata.tokens_used == 650

    @pytest.mark.asyncio
    async def test_two_stage_sql_accepts_fast_model_when_confident(
        self, sample_sql_agent_input, sample_valid_llm_response
    ):
        fast_provider = Mock()
        fast_provider.generate = AsyncMock(return_value=sample_valid_llm_response)
        fast_provider.provider = "openai"
        fast_provider.model = "gpt-4o-mini"

        main_provider = Mock()
        main_provider.generate = AsyncMock(return_value=sample_valid_llm_response)
        main_provider.provider = "openai"
        main_provider.model = "gpt-4o"

        mock_settings = Mock()
        mock_settings.llm = Mock()
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=True,
            sql_two_stage_confidence_threshold=0.7,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                side_effect=[main_provider, fast_provider],
            ),
        ):
            agent = SQLAgent()

        output = await agent(sample_sql_agent_input)
        assert output.success is True
        assert output.metadata.llm_calls == 1
        assert fast_provider.generate.await_count == 1
        assert main_provider.generate.await_count == 0

    @pytest.mark.asyncio
    async def test_two_stage_sql_escalates_to_main_when_fast_low_confidence(
        self, sample_sql_agent_input, sample_valid_llm_response
    ):
        fast_response = LLMResponse(
            content=(
                "```json\n"
                + json.dumps(
                    {
                        "sql": "SELECT SUM(amount) FROM analytics.fact_sales",
                        "explanation": "Draft SQL",
                        "used_datapoints": ["table_fact_sales_001"],
                        "confidence": 0.4,
                        "assumptions": [],
                        "clarifying_questions": [],
                    }
                )
                + "\n```"
            ),
            model="gpt-4o-mini",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=120, total_tokens=620),
            finish_reason="stop",
            provider="openai",
        )

        fast_provider = Mock()
        fast_provider.generate = AsyncMock(return_value=fast_response)
        fast_provider.provider = "openai"
        fast_provider.model = "gpt-4o-mini"

        main_provider = Mock()
        main_provider.generate = AsyncMock(return_value=sample_valid_llm_response)
        main_provider.provider = "openai"
        main_provider.model = "gpt-4o"

        mock_settings = Mock()
        mock_settings.llm = Mock()
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=True,
            sql_two_stage_confidence_threshold=0.7,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                side_effect=[main_provider, fast_provider],
            ),
        ):
            agent = SQLAgent()

        output = await agent(sample_sql_agent_input)
        assert output.success is True
        assert output.metadata.llm_calls == 2
        assert fast_provider.generate.await_count == 1
        assert main_provider.generate.await_count == 1

    @pytest.mark.asyncio
    async def test_two_stage_skips_when_providers_are_effectively_same(
        self, sample_sql_agent_input, sample_valid_llm_response
    ):
        fast_provider = Mock()
        fast_provider.generate = AsyncMock(return_value=sample_valid_llm_response)
        fast_provider.provider = "openai"
        fast_provider.model = "gpt-4o"

        main_provider = Mock()
        main_provider.generate = AsyncMock(return_value=sample_valid_llm_response)
        main_provider.provider = "openai"
        main_provider.model = "gpt-4o"

        mock_settings = Mock()
        mock_settings.llm = Mock()
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=True,
            sql_two_stage_confidence_threshold=0.7,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                side_effect=[main_provider, fast_provider],
            ),
        ):
            agent = SQLAgent()

        output = await agent(sample_sql_agent_input)
        assert output.success is True
        assert output.metadata.llm_calls == 1
        assert main_provider.generate.await_count == 1
        assert fast_provider.generate.await_count == 0

    @pytest.mark.asyncio
    async def test_tracks_used_datapoints(
        self, sql_agent, sample_sql_agent_input, sample_valid_llm_response
    ):
        """Test tracks which DataPoints were used in generation."""
        sql_agent.llm.generate.return_value = sample_valid_llm_response

        output = await sql_agent(sample_sql_agent_input)

        assert "table_fact_sales_001" in output.generated_sql.used_datapoints
        assert "metric_revenue_001" in output.generated_sql.used_datapoints

    @pytest.mark.asyncio
    async def test_handles_clarifying_questions(self, sql_agent, sample_sql_agent_input):
        """Test handles ambiguous queries with clarifying questions."""
        # Create response with clarifying questions
        response_json = {
            "sql": "SELECT SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-07-01'",
            "explanation": "Partial query - needs date range clarification",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.7,
            "assumptions": [],
            "clarifying_questions": [
                "Which quarter do you mean by 'last quarter'? Q3 2024 or Q2 2024?"
            ],
        }

        llm_response = LLMResponse(
            content=f"```json\n{json.dumps(response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        sql_agent.llm.generate.return_value = llm_response

        output = await sql_agent(sample_sql_agent_input)

        assert output.success is True
        assert output.needs_clarification is True
        assert len(output.generated_sql.clarifying_questions) == 1

    @pytest.mark.asyncio
    async def test_force_best_effort_retry_after_generic_clarification(
        self, sql_agent, sample_sql_agent_input
    ):
        compiler_plan = QueryCompilerPlan(
            query=sample_sql_agent_input.query,
            operators=[],
            candidate_tables=["analytics.fact_sales"],
            selected_tables=["analytics.fact_sales"],
            join_hypotheses=[],
            column_hints=["analytics.fact_sales.amount", "analytics.fact_sales.date"],
            confidence=0.82,
            path="deterministic",
            reason="table_match",
        )

        first = GeneratedSQL(
            sql="   ",
            explanation="Need table clarification.",
            confidence=0.4,
            used_datapoints=[],
            assumptions=[],
            clarifying_questions=["Which table should I use to answer this?"],
        )
        second = GeneratedSQL(
            sql=(
                "SELECT DATE_TRUNC('month', date) AS month, SUM(amount) AS total_sales "
                "FROM analytics.fact_sales GROUP BY 1 ORDER BY 1 LIMIT 10"
            ),
            explanation="Best-effort SQL generated from selected tables.",
            confidence=0.74,
            used_datapoints=[],
            assumptions=["Assumed sales are stored in analytics.fact_sales."],
            clarifying_questions=[],
        )

        with (
            patch.object(
                sql_agent,
                "_build_generation_prompt",
                new=AsyncMock(return_value=("prompt", compiler_plan)),
            ),
            patch.object(
                sql_agent,
                "_request_sql_from_llm",
                new=AsyncMock(side_effect=[first, second]),
            ) as mock_request,
        ):
            output = await sql_agent(sample_sql_agent_input)

        assert output.success is True
        assert output.needs_clarification is False
        assert "FROM analytics.fact_sales" in output.generated_sql.sql
        assert output.generated_sql.clarifying_questions == []
        assert mock_request.await_count == 2

    @pytest.mark.asyncio
    async def test_force_best_effort_retry_after_sql_clarification_exception(
        self, sql_agent, sample_sql_agent_input
    ):
        compiler_plan = QueryCompilerPlan(
            query=sample_sql_agent_input.query,
            operators=[],
            candidate_tables=["analytics.fact_sales"],
            selected_tables=["analytics.fact_sales"],
            join_hypotheses=[],
            column_hints=["analytics.fact_sales.amount", "analytics.fact_sales.date"],
            confidence=0.82,
            path="deterministic",
            reason="table_match",
        )
        forced = GeneratedSQL(
            sql=(
                "SELECT DATE_TRUNC('month', date) AS month, SUM(amount) AS total_sales "
                "FROM analytics.fact_sales GROUP BY 1 ORDER BY 1 LIMIT 10"
            ),
            explanation="Best-effort SQL generated from selected tables.",
            confidence=0.74,
            used_datapoints=[],
            assumptions=["Assumed sales are stored in analytics.fact_sales."],
            clarifying_questions=[],
        )

        with (
            patch.object(
                sql_agent,
                "_build_generation_prompt",
                new=AsyncMock(return_value=("prompt", compiler_plan)),
            ),
            patch.object(
                sql_agent,
                "_request_sql_from_llm",
                new=AsyncMock(
                    side_effect=[
                        SQLClarificationNeeded(["Which table should I use to answer this?"]),
                        forced,
                    ]
                ),
            ) as mock_request,
        ):
            output = await sql_agent(sample_sql_agent_input)

        assert output.success is True
        assert output.needs_clarification is False
        assert "FROM analytics.fact_sales" in output.generated_sql.sql
        assert output.generated_sql.clarifying_questions == []
        assert mock_request.await_count == 2

    @pytest.mark.asyncio
    async def test_force_best_effort_retry_without_compiler_plan_uses_datapoint_tables(
        self, sql_agent
    ):
        memory = InvestigationMemory(
            query="Show top customers by deposit concentration risk.",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="metric_deposit_concentration_001",
                    datapoint_type="Business",
                    name="Deposit Concentration Risk",
                    score=0.91,
                    source="hybrid",
                    metadata={
                        "related_tables": "public.bank_accounts,public.bank_customers",
                        "business_rules": "top customer exposure",
                    },
                )
            ],
            total_retrieved=1,
            retrieval_mode="hybrid",
            sources_used=["metric_deposit_concentration_001"],
        )
        input_data = SQLAgentInput(
            query="Show top customers by deposit concentration risk.",
            investigation_memory=memory,
            database_type="postgresql",
        )

        first = GeneratedSQL(
            sql="SELECT 1",
            explanation="Need clarification.",
            confidence=0.2,
            used_datapoints=[],
            assumptions=[],
            clarifying_questions=["Which table should I use?"],
        )
        second = GeneratedSQL(
            sql=(
                "SELECT bc.customer_code, SUM(ba.current_balance) AS total_balance "
                "FROM public.bank_accounts ba "
                "JOIN public.bank_customers bc ON ba.customer_id = bc.customer_id "
                "GROUP BY bc.customer_code ORDER BY total_balance DESC LIMIT 10"
            ),
            explanation="Best-effort retry resolved likely tables.",
            confidence=0.7,
            used_datapoints=[],
            assumptions=[],
            clarifying_questions=[],
        )

        with (
            patch.object(
                sql_agent,
                "_build_generation_prompt",
                new=AsyncMock(return_value=("prompt", None)),
            ),
            patch.object(
                sql_agent,
                "_request_sql_from_llm",
                new=AsyncMock(side_effect=[first, second]),
            ) as mock_request,
        ):
            output = await sql_agent(input_data)

        assert output.success is True
        assert output.needs_clarification is False
        assert "public.bank_accounts" in output.generated_sql.sql
        assert "public.bank_customers" in output.generated_sql.sql
        assert mock_request.await_count == 2

    def test_collect_table_columns_includes_related_and_sql_template_tables(self, sql_agent):
        memory = InvestigationMemory(
            query="test",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="query_dp",
                    datapoint_type="Query",
                    name="Query DP",
                    score=0.9,
                    source="hybrid",
                    metadata={
                        "related_tables": "public.bank_accounts,public.bank_customers",
                        "sql_template": (
                            "SELECT * FROM public.bank_transactions bt "
                            "JOIN public.bank_accounts ba ON bt.account_id = ba.account_id"
                        ),
                    },
                )
            ],
            total_retrieved=1,
            retrieval_mode="hybrid",
            sources_used=["query_dp"],
        )

        table_columns = sql_agent._collect_table_columns_from_investigation(memory)

        assert "public.bank_accounts" in table_columns
        assert "public.bank_customers" in table_columns
        assert "public.bank_transactions" in table_columns

    @pytest.mark.asyncio
    async def test_uses_deterministic_catalog_for_table_list(
        self, sql_agent, sample_investigation_memory
    ):
        input_data = SQLAgentInput(
            query="list tables",
            investigation_memory=sample_investigation_memory,
            database_type="postgresql",
        )

        output = await sql_agent(input_data)

        assert output.success is True
        assert output.generated_sql.sql.startswith("SELECT table_schema, table_name")
        assert output.metadata.llm_calls == 0
        sql_agent.llm.generate.assert_not_called()

    @pytest.mark.asyncio
    async def test_uses_deterministic_catalog_for_list_columns(
        self, sql_agent, sample_investigation_memory
    ):
        input_data = SQLAgentInput(
            query="show columns in analytics.fact_sales",
            investigation_memory=sample_investigation_memory,
            database_type="postgresql",
        )

        output = await sql_agent(input_data)

        assert output.success is True
        assert "information_schema.columns" in output.generated_sql.sql
        assert "table_name = 'fact_sales'" in output.generated_sql.sql
        assert output.metadata.llm_calls == 0

    @pytest.mark.asyncio
    async def test_requests_clarification_for_columns_without_table(
        self, sql_agent, sample_investigation_memory
    ):
        memory = InvestigationMemory(
            query="show columns",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="table_sales_001",
                    datapoint_type="Schema",
                    name="Sales",
                    score=0.9,
                    source="hybrid",
                    metadata={"table_name": "public.sales", "key_columns": [{"name": "amount"}]},
                ),
                RetrievedDataPoint(
                    datapoint_id="table_orders_001",
                    datapoint_type="Schema",
                    name="Orders",
                    score=0.85,
                    source="hybrid",
                    metadata={"table_name": "public.orders", "key_columns": [{"name": "order_id"}]},
                ),
            ],
            total_retrieved=2,
            retrieval_mode="hybrid",
            sources_used=["table_sales_001", "table_orders_001"],
        )
        input_data = SQLAgentInput(
            query="show columns",
            investigation_memory=memory,
            database_type="postgresql",
        )

        output = await sql_agent(input_data)

        assert output.needs_clarification is True
        assert "Which table should I list columns for?" in output.generated_sql.clarifying_questions
        assert output.metadata.llm_calls == 0


class TestSelfCorrection:
    """Test SQLAgent self-correction capabilities."""

    @pytest.mark.asyncio
    async def test_self_corrects_missing_table(self, sql_agent, sample_sql_agent_input):
        """Test self-corrects when referencing non-existent table."""
        # First response with wrong table name
        bad_response_json = {
            "sql": "SELECT SUM(amount) FROM wrong_table WHERE date >= '2024-07-01'",
            "explanation": "Query with wrong table",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.95,
            "assumptions": [],
            "clarifying_questions": [],
        }

        bad_llm_response = LLMResponse(
            content=f"```json\n{json.dumps(bad_response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        # Corrected response
        good_response_json = {
            "sql": "SELECT SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-07-01'",
            "explanation": "Corrected query with right table",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.95,
            "assumptions": [],
            "clarifying_questions": [],
        }

        good_llm_response = LLMResponse(
            content=f"```json\n{json.dumps(good_response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        # Mock: first call returns bad SQL, second call returns corrected SQL
        sql_agent.llm.generate.side_effect = [bad_llm_response, good_llm_response]

        output = await sql_agent(sample_sql_agent_input)

        # Should have made correction
        assert output.success is True
        assert len(output.correction_attempts) == 1
        assert output.correction_attempts[0].attempt_number == 1
        assert "wrong_table" in output.correction_attempts[0].original_sql.lower()
        assert "fact_sales" in output.correction_attempts[0].corrected_sql.lower()
        assert output.correction_attempts[0].success is True
        assert output.metadata.llm_calls == 2  # Initial + 1 correction

    @pytest.mark.asyncio
    async def test_self_corrects_syntax_error(self, sql_agent, sample_sql_agent_input):
        """Test self-corrects syntax errors."""
        # Response missing FROM clause
        bad_response_json = {
            "sql": "SELECT SUM(amount) WHERE date >= '2024-07-01'",
            "explanation": "Query missing FROM",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.95,
            "assumptions": [],
            "clarifying_questions": [],
        }

        bad_llm_response = LLMResponse(
            content=f"```json\n{json.dumps(bad_response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        # Corrected response
        good_response_json = {
            "sql": "SELECT SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-07-01'",
            "explanation": "Corrected query with FROM clause",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.95,
            "assumptions": [],
            "clarifying_questions": [],
        }

        good_llm_response = LLMResponse(
            content=f"```json\n{json.dumps(good_response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        sql_agent.llm.generate.side_effect = [bad_llm_response, good_llm_response]

        output = await sql_agent(sample_sql_agent_input)

        assert output.success is True
        assert len(output.correction_attempts) == 1
        assert any(
            issue.issue_type == "syntax" for issue in output.correction_attempts[0].issues_found
        )

    @pytest.mark.asyncio
    async def test_respects_max_correction_attempts(self, sql_agent, sample_sql_agent_input):
        """Test respects maximum correction attempts."""
        # Always return bad SQL
        bad_response_json = {
            "sql": "SELECT SUM(amount) WHERE date >= '2024-07-01'",  # Missing FROM
            "explanation": "Bad query",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.95,
            "assumptions": [],
            "clarifying_questions": [],
        }

        bad_llm_response = LLMResponse(
            content=f"```json\n{json.dumps(bad_response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        sql_agent.llm.generate.return_value = bad_llm_response

        # Set max attempts to 2
        sample_sql_agent_input.max_correction_attempts = 2

        output = await sql_agent(sample_sql_agent_input)

        # Should try: initial + 2 corrections = 3 LLM calls
        assert output.metadata.llm_calls == 3
        assert len(output.correction_attempts) == 2
        assert output.needs_clarification is True  # Has unresolved issues

    @pytest.mark.asyncio
    async def test_correction_parse_failure_returns_clarification_instead_of_error(
        self, sample_sql_agent_input
    ):
        """Malformed correction output should degrade to clarification, not hard-fail."""
        initial_bad_sql = {
            "sql": "SELECT SUM(amount) FROM wrong_table WHERE date >= '2024-07-01'",
            "explanation": "Query with wrong table",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.9,
            "assumptions": [],
            "clarifying_questions": [],
        }
        initial_response = LLMResponse(
            content=f"```json\n{json.dumps(initial_bad_sql)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=400, completion_tokens=120, total_tokens=520),
            finish_reason="stop",
            provider="openai",
        )
        malformed_correction = LLMResponse(
            content='{"explanation":"missing sql in correction"}',
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=300, completion_tokens=40, total_tokens=340),
            finish_reason="stop",
            provider="openai",
        )

        main_provider = Mock()
        main_provider.generate = AsyncMock(
            side_effect=[initial_response, malformed_correction, malformed_correction]
        )
        main_provider.provider = "openai"
        main_provider.model = "gpt-4o"

        mock_settings = Mock()
        mock_settings.llm = Mock(sql_formatter_model=None)
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=False,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
            sql_formatter_fallback_enabled=False,
            sql_force_best_effort_on_clarify=False,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                return_value=main_provider,
            ),
        ):
            agent = SQLAgent()

        output = await agent(sample_sql_agent_input.model_copy(update={"max_correction_attempts": 1}))

        assert output.success is True
        assert output.needs_clarification is True
        assert output.generated_sql.clarifying_questions
        assert len(output.correction_attempts) == 1
        assert "wrong_table" in output.generated_sql.sql.lower()
        assert main_provider.generate.await_count == 3

    @pytest.mark.asyncio
    async def test_correction_two_stage_escalates_after_fast_parse_failure(
        self, sample_sql_agent_input
    ):
        """Correction path should escalate to main model if fast model output is malformed."""
        malformed_fast_response = LLMResponse(
            content='{"explanation":"missing sql"}',
            model="gpt-4o-mini",
            usage=LLMUsage(prompt_tokens=220, completion_tokens=30, total_tokens=250),
            finish_reason="stop",
            provider="openai",
        )
        corrected_response_json = {
            "sql": "SELECT SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-07-01'",
            "explanation": "Corrected query with valid table",
            "used_datapoints": ["table_fact_sales_001"],
            "confidence": 0.93,
            "assumptions": [],
            "clarifying_questions": [],
        }
        corrected_main_response = LLMResponse(
            content=f"```json\n{json.dumps(corrected_response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=260, completion_tokens=90, total_tokens=350),
            finish_reason="stop",
            provider="openai",
        )

        main_provider = Mock()
        main_provider.generate = AsyncMock(return_value=corrected_main_response)
        main_provider.provider = "openai"
        main_provider.model = "gpt-4o"

        fast_provider = Mock()
        fast_provider.generate = AsyncMock(
            side_effect=[malformed_fast_response, malformed_fast_response]
        )
        fast_provider.provider = "openai"
        fast_provider.model = "gpt-4o-mini"

        mock_settings = Mock()
        mock_settings.llm = Mock(sql_formatter_model=None)
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=True,
            sql_two_stage_confidence_threshold=0.7,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
            sql_formatter_fallback_enabled=False,
            sql_force_best_effort_on_clarify=False,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                side_effect=[main_provider, fast_provider],
            ),
        ):
            agent = SQLAgent()

        original_generated_sql = GeneratedSQL(
            sql="SELECT SUM(amount) FROM wrong_table WHERE date >= '2024-07-01'",
            explanation="Initial SQL needs correction",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )
        issues = [
            ValidationIssue(
                issue_type="missing_table",
                message="Table 'WRONG_TABLE' not found in available DataPoints",
                suggested_fix="Use analytics.fact_sales",
            )
        ]

        corrected = await agent._correct_sql(
            generated_sql=original_generated_sql,
            issues=issues,
            input=sample_sql_agent_input,
            metadata=AgentMetadata(agent_name="SQLAgent"),
            runtime_stats={},
        )

        assert "analytics.fact_sales" in corrected.sql.lower()
        assert fast_provider.generate.await_count == 2
        assert main_provider.generate.await_count == 1


class TestValidation:
    """Test SQL validation logic."""

    def test_validates_select_statement(self, sql_agent, sample_sql_agent_input):
        """Test validates SQL starts with SELECT."""
        # Create invalid SQL (no SELECT)
        invalid_sql = GeneratedSQL(
            sql="UPDATE fact_sales SET amount = 100",
            explanation="Update query",
            used_datapoints=[],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(invalid_sql, sample_sql_agent_input)

        assert len(issues) > 0
        assert any(issue.issue_type == "syntax" and "SELECT" in issue.message for issue in issues)

    def test_validates_from_clause(self, sql_agent, sample_sql_agent_input):
        """Test validates SQL has FROM clause."""
        invalid_sql = GeneratedSQL(
            sql="SELECT SUM(amount) WHERE date > '2024-01-01'",
            explanation="Query missing FROM",
            used_datapoints=[],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(invalid_sql, sample_sql_agent_input)

        assert len(issues) > 0
        assert any(issue.issue_type == "syntax" and "FROM" in issue.message for issue in issues)

    def test_validates_table_names(self, sql_agent, sample_sql_agent_input):
        """Test validates table names exist in DataPoints."""
        invalid_sql = GeneratedSQL(
            sql="SELECT SUM(amount) FROM nonexistent_table WHERE date > '2024-01-01'",
            explanation="Query with wrong table",
            used_datapoints=[],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(invalid_sql, sample_sql_agent_input)

        assert len(issues) > 0
        assert any(issue.issue_type == "missing_table" for issue in issues)

    def test_accepts_valid_sql(self, sql_agent, sample_sql_agent_input):
        """Test accepts valid SQL with no issues."""
        valid_sql = GeneratedSQL(
            sql="SELECT SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-07-01'",
            explanation="Valid query",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(valid_sql, sample_sql_agent_input)

        assert len(issues) == 0

    def test_accepts_ctes(self, sql_agent, sample_sql_agent_input):
        """Test accepts CTEs (Common Table Expressions) without flagging as missing tables."""
        # SQL with CTE - the 'sales' CTE should not be flagged as missing_table
        cte_sql = GeneratedSQL(
            sql="""WITH sales AS (
                SELECT amount, date FROM analytics.fact_sales WHERE date >= '2024-07-01'
            )
            SELECT SUM(amount) FROM sales""",
            explanation="Query using CTE",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(cte_sql, sample_sql_agent_input)

        # Should have NO issues - 'sales' is a CTE, not a missing table
        assert len(issues) == 0

    def test_accepts_multiple_ctes(self, sql_agent, sample_sql_agent_input):
        """Test accepts multiple CTEs."""
        multi_cte_sql = GeneratedSQL(
            sql="""WITH
                sales AS (SELECT amount FROM analytics.fact_sales),
                filtered_sales AS (SELECT amount FROM sales WHERE amount > 100)
            SELECT SUM(amount) FROM filtered_sales""",
            explanation="Query with multiple CTEs",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(multi_cte_sql, sample_sql_agent_input)

        # Both 'sales' and 'filtered_sales' are CTEs, should not be flagged
        assert len(issues) == 0

    def test_skips_missing_table_without_schema_datapoints(self, sql_agent):
        """Test missing_table validation is skipped in credentials-only mode."""
        memory = InvestigationMemory(
            query="What is total sales?",
            datapoints=[],
            total_retrieved=0,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query="What is total sales?",
            investigation_memory=memory,
        )
        generated_sql = GeneratedSQL(
            sql="SELECT SUM(amount) FROM sales",
            explanation="Sum sales amount",
            used_datapoints=[],
            confidence=0.7,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(generated_sql, sql_input)

        assert not any(issue.issue_type == "missing_table" for issue in issues)

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT COUNT(*) FROM pg_tables",
            "SELECT COUNT(*) FROM information_schema.tables",
        ],
    )
    def test_accepts_catalog_tables(self, sql_agent, sample_sql_agent_input, sql):
        """Test accepts catalog tables without DataPoints."""
        catalog_sql = GeneratedSQL(
            sql=sql,
            explanation="Catalog query",
            used_datapoints=[],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(catalog_sql, sample_sql_agent_input)

        assert len(issues) == 0

    def test_accepts_clickhouse_system_tables(self, sql_agent, sample_sql_agent_input):
        """Test accepts ClickHouse system tables even when DataPoints exist."""
        sql_input = sample_sql_agent_input.model_copy(update={"database_type": "clickhouse"})
        catalog_sql = GeneratedSQL(
            sql="SELECT name FROM system.tables",
            explanation="ClickHouse catalog query",
            used_datapoints=[],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(catalog_sql, sql_input)

        assert len(issues) == 0

    def test_accepts_mysql_show_tables(self, sql_agent, sample_sql_agent_input):
        """Test accepts MySQL SHOW TABLES in validation."""
        sql_input = sample_sql_agent_input.model_copy(update={"database_type": "mysql"})
        catalog_sql = GeneratedSQL(
            sql="SHOW TABLES",
            explanation="MySQL catalog query",
            used_datapoints=[],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = sql_agent._validate_sql(catalog_sql, sql_input)

        assert len(issues) == 0


class TestPromptBuilding:
    """Test prompt construction logic."""

    @pytest.mark.asyncio
    async def test_builds_generation_prompt_with_schema(self, sql_agent, sample_sql_agent_input):
        """Test generation prompt includes schema context."""
        prompt = await sql_agent._build_generation_prompt(sample_sql_agent_input)

        assert "fact_sales" in prompt
        assert "amount" in prompt
        assert "date" in prompt
        assert sample_sql_agent_input.query in prompt

    @pytest.mark.asyncio
    async def test_builds_generation_prompt_with_business_rules(
        self, sql_agent, sample_sql_agent_input
    ):
        """Test generation prompt includes business rules."""
        prompt = await sql_agent._build_generation_prompt(sample_sql_agent_input)

        assert "Revenue" in prompt
        assert "completed" in prompt or "refund" in prompt.lower()

    @pytest.mark.asyncio
    async def test_build_generation_prompt_includes_operator_guidance(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={"query": "Which 5 SKUs have the highest stockout risk this week?"}
        )

        prompt = await sql_agent._build_generation_prompt(sql_input)

        assert "Analytic operator hints (semantic patterns):" in prompt
        assert "stockout_risk" in prompt

    def test_builds_correction_prompt(self, sql_agent, sample_sql_agent_input):
        """Test correction prompt includes issues and original SQL."""
        generated_sql = GeneratedSQL(
            sql="SELECT amount FROM wrong_table",
            explanation="Wrong query",
            used_datapoints=[],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )

        issues = [
            ValidationIssue(
                issue_type="missing_table",
                message="Table 'wrong_table' not found",
                suggested_fix="Use analytics.fact_sales",
            )
        ]

        prompt = sql_agent._build_correction_prompt(generated_sql, issues, sample_sql_agent_input)

        assert "wrong_table" in prompt
        assert "not found" in prompt.lower()
        assert "analytics.fact_sales" in prompt or "fact_sales" in prompt

    def test_truncate_context_applies_budget(self, sql_agent):
        text = "x" * 100
        truncated = sql_agent._truncate_context(text, 40)
        assert len(truncated) > 40
        assert "Context truncated for latency budget" in truncated

    def test_columns_context_map_applies_focus_and_column_limits(self, sql_agent):
        sql_agent.config.pipeline = Mock(
            sql_prompt_budget_enabled=True,
            sql_prompt_focus_tables=2,
            sql_prompt_max_columns_per_table=1,
        )
        columns_by_table = {
            "public.orders": [("id", "integer"), ("total", "numeric")],
            "public.customers": [("id", "integer"), ("name", "text")],
            "public.items": [("id", "integer"), ("sku", "text")],
        }
        context, focus_tables = sql_agent._build_columns_context_from_map(
            query="show revenue by customer",
            qualified_tables=list(columns_by_table.keys()),
            columns_by_table=columns_by_table,
        )
        assert len(focus_tables) == 2
        table_lines = [line for line in context.splitlines() if line.startswith("- ")]
        assert len(table_lines) == 2
        assert table_lines[0].count("(") == 1
        assert table_lines[1].count("(") == 1


class TestDatabaseContext:
    """Test database context propagation into SQL generation."""

    def test_introspection_query_respects_database_type(self, sql_agent):
        postgres_query = sql_agent._build_introspection_query(
            "what tables are available?",
            database_type="postgresql",
        )
        mysql_query = sql_agent._build_introspection_query(
            "show tables",
            database_type="mysql",
        )
        clickhouse_query = sql_agent._build_introspection_query(
            "what tables are available?",
            database_type="clickhouse",
        )
        bigquery_query = sql_agent._build_introspection_query(
            "list tables",
            database_type="bigquery",
        )
        redshift_query = sql_agent._build_introspection_query(
            "list tables",
            database_type="redshift",
        )
        assert postgres_query is not None
        assert "information_schema.tables" in postgres_query
        assert mysql_query is not None
        assert "information_schema.tables" in mysql_query
        assert clickhouse_query is not None
        assert "system.tables" in clickhouse_query
        assert bigquery_query is not None
        assert "information_schema.tables" in bigquery_query.lower()
        assert redshift_query is not None
        assert "pg_table_def" in redshift_query.lower()

    def test_row_count_fallback_uses_explicit_table(self, sql_agent):
        memory = InvestigationMemory(
            query="How many rows are in pg_tables?",
            datapoints=[],
            total_retrieved=0,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query="How many rows are in pg_tables?",
            investigation_memory=memory,
        )
        sql = sql_agent._build_row_count_fallback(sql_input)
        assert sql == "SELECT COUNT(*) AS row_count FROM pg_tables"

    def test_row_count_fallback_schema_qualified_table(self, sql_agent):
        memory = InvestigationMemory(
            query="How many rows are in information_schema.tables?",
            datapoints=[],
            total_retrieved=0,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query="How many rows are in information_schema.tables?",
            investigation_memory=memory,
        )
        sql = sql_agent._build_row_count_fallback(sql_input)
        assert sql == "SELECT COUNT(*) AS row_count FROM information_schema.tables"

    def test_sample_rows_fallback_uses_explicit_table(self, sql_agent):
        memory = InvestigationMemory(
            query="Show me the first 2 rows from public.orders",
            datapoints=[],
            total_retrieved=0,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query="Show me the first 2 rows from public.orders",
            investigation_memory=memory,
        )
        sql = sql_agent._build_sample_rows_fallback(sql_input)
        assert sql == "SELECT * FROM public.orders LIMIT 2"

    def test_list_columns_fallback_uses_explicit_table(self, sql_agent):
        memory = InvestigationMemory(
            query="show columns in public.orders",
            datapoints=[],
            total_retrieved=0,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query="show columns in public.orders",
            investigation_memory=memory,
            database_type="postgresql",
        )
        sql = sql_agent._build_list_columns_fallback(sql_input)
        assert sql is not None
        assert "information_schema.columns" in sql
        assert "table_name = 'orders'" in sql

    @pytest.mark.asyncio
    async def test_build_prompt_uses_input_database_context(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={
                "database_type": "clickhouse",
                "database_url": "clickhouse://user:pass@click.example.com:8123/analytics",
            }
        )
        with patch.object(
            sql_agent,
            "_get_live_schema_context",
            new=AsyncMock(return_value=None),
        ) as mock_live_context:
            prompt = await sql_agent._build_generation_prompt(sql_input)

        assert "clickhouse" in prompt.lower()
        assert mock_live_context.await_count == 1
        assert mock_live_context.await_args.kwargs["database_type"] == "clickhouse"
        assert mock_live_context.await_args.kwargs["database_url"] == sql_input.database_url
        assert mock_live_context.await_args.kwargs["include_profile"] is False

    @pytest.mark.asyncio
    async def test_prompt_includes_conversation_context(self, sql_agent, sample_sql_agent_input):
        sql_input = sample_sql_agent_input.model_copy(
            update={
                "query": "sales",
                "conversation_history": [
                    {"role": "user", "content": "Show me the first 5 rows"},
                    {"role": "assistant", "content": "Which table should I use?"},
                ],
            }
        )
        prompt = await sql_agent._build_generation_prompt(sql_input)

        assert "conversation" in prompt.lower()
        assert "which table should i use" in prompt.lower()
        assert "show 5 rows from sales" in prompt.lower()

    @pytest.mark.asyncio
    async def test_live_schema_lookup_prefers_input_database_url(self, sql_agent):
        sql_agent.config.database.url = "postgresql://wrong:wrong@wrong-host:5432/wrong_db"

        mock_connector = AsyncMock()
        mock_connector.connect = AsyncMock()
        mock_connector.close = AsyncMock()

        with (
            patch(
                "backend.agents.sql.create_connector", return_value=mock_connector
            ) as connector_factory,
            patch.object(
                sql_agent,
                "_fetch_live_schema_context",
                new=AsyncMock(return_value=("schema-context", ["public.sales"])),
            ),
        ):
            context = await sql_agent._get_live_schema_context(
                query="show tables",
                database_type="postgresql",
                database_url="postgresql://demo:demo@chosen-host:5432/chosen_db",
            )

        assert context == "schema-context"
        connector_factory.assert_called_once_with(
            database_url="postgresql://demo:demo@chosen-host:5432/chosen_db",
            database_type="postgresql",
            pool_size=sql_agent.config.database.pool_size,
            timeout=10,
        )

    def test_build_cached_profile_context_uses_matching_focus_tables(self, sql_agent):
        with patch(
            "backend.agents.sql.load_profile_cache",
            return_value={
                "tables": [
                    {
                        "name": "public.orders",
                        "status": "completed",
                        "row_count": 100,
                        "columns": [
                            {
                                "name": "order_id",
                                "data_type": "integer",
                                "distinct_count": 100,
                                "sample_values": ["1", "2", "3"],
                            },
                            {
                                "name": "status",
                                "data_type": "text",
                                "distinct_count": 3,
                                "sample_values": ["posted", "declined", "reversed"],
                            },
                        ],
                    },
                    {
                        "name": "public.customers",
                        "status": "completed",
                        "row_count": 50,
                        "columns": [{"name": "id", "data_type": "integer"}],
                    },
                ]
            },
        ):
            context = sql_agent._build_cached_profile_context(
                db_type="postgresql",
                db_url="postgresql://demo:demo@localhost:5432/warehouse",
                focus_tables=["public.orders"],
            )

        assert "Auto-profile cache snapshot" in context
        assert "public.orders" in context
        assert "public.customers" not in context
        assert "samples=[posted, declined, reversed]" in context

    def test_select_profile_columns_prioritizes_categorical_tokens(self, sql_agent):
        selected = sql_agent._select_profile_columns(
            query="How many positive feedbacks do we have?",
            columns=[
                ("feedback_id", "integer"),
                ("sentiment", "text"),
                ("created_at", "timestamp"),
                ("score", "numeric"),
            ],
        )

        assert "sentiment" in selected
        assert selected.index("sentiment") < selected.index("score")

    def test_context_accuracy_guard_maps_polarity_literal_to_profile_samples(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={
                "query": "How many positive feedbacks do we have?",
                "database_url": "postgresql://demo:demo@localhost:5432/warehouse",
            }
        )
        generated = GeneratedSQL(
            sql="SELECT COUNT(*) FROM public.ui_feedback WHERE sentiment = 'positive'",
            explanation="Count positive feedback records",
            used_datapoints=[],
            confidence=0.7,
            assumptions=[],
            clarifying_questions=[],
        )

        with patch(
            "backend.agents.sql.load_profile_cache",
            return_value={
                "tables": [
                    {
                        "name": "public.ui_feedback",
                        "columns": [
                            {
                                "name": "sentiment",
                                "sample_values": ["up", "down"],
                            }
                        ],
                    }
                ]
            },
        ):
            rewritten = sql_agent._apply_context_accuracy_guards(generated, sql_input)

        assert "sentiment = 'up'" in rewritten.sql
        assert any("Mapped filter sentiment='positive'" in item for item in rewritten.assumptions)

    def test_context_accuracy_guard_keeps_sql_when_literal_is_observed(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={"database_url": "postgresql://demo:demo@localhost:5432/warehouse"}
        )
        generated = GeneratedSQL(
            sql="SELECT COUNT(*) FROM public.ui_feedback WHERE sentiment = 'up'",
            explanation="Count thumbs-up feedback records",
            used_datapoints=[],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )

        with patch(
            "backend.agents.sql.load_profile_cache",
            return_value={
                "tables": [
                    {
                        "name": "public.ui_feedback",
                        "columns": [
                            {
                                "name": "sentiment",
                                "sample_values": ["up", "down"],
                            }
                        ],
                    }
                ]
            },
        ):
            rewritten = sql_agent._apply_context_accuracy_guards(generated, sql_input)

        assert rewritten.sql == generated.sql
        assert rewritten.assumptions == generated.assumptions

    def test_build_semantic_rows_deterministic_classifies_table(self, sql_agent):
        rows = sql_agent._build_semantic_rows_deterministic(
            columns_by_table={
                "public.orders": [
                    ("order_id", "integer"),
                    ("customer_segment", "text"),
                    ("order_date", "date"),
                    ("total_amount", "numeric"),
                ]
            },
            selected_tables=["public.orders"],
        )

        assert rows
        row = rows[0]
        assert row["table"] == "public.orders"
        assert row["visualization_hint"] == "line"
        assert "order_date" in row["time_columns"]
        assert "total_amount" in row["measures"]


class TestErrorHandling:
    """Test error handling."""

    @pytest.mark.asyncio
    async def test_handles_llm_failure(self, sql_agent, sample_sql_agent_input):
        """Test handles LLM API failures."""
        sql_agent.llm.generate.side_effect = Exception("API Error")

        with pytest.raises(SQLGenerationError) as exc_info:
            await sql_agent(sample_sql_agent_input)

        assert "Failed to generate SQL" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_handles_invalid_json_response(self, sql_agent, sample_sql_agent_input):
        """Test handles invalid JSON in LLM response."""
        invalid_response = LLMResponse(
            content="This is not JSON",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        sql_agent.llm.generate.return_value = invalid_response

        output = await sql_agent(sample_sql_agent_input)
        assert output.needs_clarification is True
        assert output.generated_sql.clarifying_questions

    @pytest.mark.asyncio
    async def test_handles_missing_required_fields(self, sql_agent, sample_sql_agent_input):
        """Test handles JSON missing required fields."""
        # Response missing 'sql' field
        bad_response_json = {"explanation": "Query explanation", "confidence": 0.9}

        bad_response = LLMResponse(
            content=f"```json\n{json.dumps(bad_response_json)}\n```",
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=500, completion_tokens=150, total_tokens=650),
            finish_reason="stop",
            provider="openai",
        )

        sql_agent.llm.generate.return_value = bad_response

        output = await sql_agent(sample_sql_agent_input)
        assert output.needs_clarification is True
        assert output.generated_sql.clarifying_questions

    @pytest.mark.asyncio
    async def test_recovers_missing_sql_with_formatter_fallback(self, sample_sql_agent_input):
        """Formatter fallback should recover malformed SQL JSON output."""
        malformed_response = LLMResponse(
            content='{"explanation":"I can help with SQL","confidence":0.7}',
            model="gemini-2.5-flash",
            usage=LLMUsage(prompt_tokens=350, completion_tokens=40, total_tokens=390),
            finish_reason="stop",
            provider="google",
        )
        formatter_response = LLMResponse(
            content=(
                "```json\n"
                + json.dumps(
                    {
                        "sql": (
                            "SELECT SUM(amount) FROM analytics.fact_sales "
                            "WHERE date >= '2024-07-01' AND date < '2024-10-01'"
                        ),
                        "explanation": "Recovered SQL JSON.",
                        "used_datapoints": ["table_fact_sales_001"],
                        "confidence": 0.86,
                        "assumptions": [],
                        "clarifying_questions": [],
                    }
                )
                + "\n```"
            ),
            model="gemini-2.5-flash-lite",
            usage=LLMUsage(prompt_tokens=180, completion_tokens=60, total_tokens=240),
            finish_reason="stop",
            provider="google",
        )

        main_provider = Mock()
        main_provider.generate = AsyncMock(side_effect=[malformed_response, malformed_response])
        main_provider.provider = "google"
        main_provider.model = "gemini-2.5-flash"

        formatter_provider = Mock()
        formatter_provider.generate = AsyncMock(return_value=formatter_response)
        formatter_provider.provider = "google"
        formatter_provider.model = "gemini-2.5-flash-lite"

        mock_settings = Mock()
        mock_settings.llm = Mock(sql_formatter_model="gemini-2.5-flash-lite")
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=False,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
            sql_formatter_fallback_enabled=True,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                side_effect=[main_provider, formatter_provider],
            ),
        ):
            agent = SQLAgent()

        output = await agent(sample_sql_agent_input)

        assert output.success is True
        assert output.needs_clarification is False
        assert "SELECT SUM(amount)" in output.generated_sql.sql
        assert output.metadata.llm_calls == 3
        assert main_provider.generate.await_count == 2
        assert formatter_provider.generate.await_count == 1

    @pytest.mark.asyncio
    async def test_formatter_fallback_respects_disable_flag(self, sample_sql_agent_input):
        """When disabled, formatter fallback should not run."""
        malformed_response = LLMResponse(
            content='{"explanation":"missing sql"}',
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=120, completion_tokens=20, total_tokens=140),
            finish_reason="stop",
            provider="openai",
        )

        main_provider = Mock()
        main_provider.generate = AsyncMock(side_effect=[malformed_response, malformed_response])
        main_provider.provider = "openai"
        main_provider.model = "gpt-4o"

        formatter_provider = Mock()
        formatter_provider.generate = AsyncMock()
        formatter_provider.provider = "openai"
        formatter_provider.model = "gpt-4o-mini"

        mock_settings = Mock()
        mock_settings.llm = Mock(sql_formatter_model=None)
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=False,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
            sql_formatter_fallback_enabled=False,
            sql_force_best_effort_on_clarify=False,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                side_effect=[main_provider, formatter_provider],
            ),
        ):
            agent = SQLAgent()

        output = await agent(sample_sql_agent_input)

        assert output.needs_clarification is True
        assert main_provider.generate.await_count == 2
        assert formatter_provider.generate.await_count == 0

    @pytest.mark.asyncio
    async def test_parses_sql_from_markdown_block_when_json_missing(
        self, sql_agent, sample_sql_agent_input
    ):
        llm_response = LLMResponse(
            content=(
                "I can run this query:\n\n"
                "```sql\nSELECT SUM(amount) FROM analytics.fact_sales;\n```"
            ),
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=400, completion_tokens=60, total_tokens=460),
            finish_reason="stop",
            provider="openai",
        )
        sql_agent.llm.generate.return_value = llm_response

        output = await sql_agent(sample_sql_agent_input)

        assert output.success is True
        assert output.needs_clarification is False
        assert output.generated_sql.sql == "SELECT SUM(amount) FROM analytics.fact_sales"

    def test_parse_llm_response_recovers_sql_from_partial_json(
        self, sql_agent, sample_sql_agent_input
    ):
        content = (
            '{"sql":"SELECT store_id, SUM(quantity) '
            'FROM public.grocery_sales_transactions GROUP BY store_id", '
            '"explanation":"aggregate'
        )

        generated = sql_agent._parse_llm_response(content, sample_sql_agent_input)

        assert generated.sql.startswith("SELECT store_id")
        assert "grocery_sales_transactions" in generated.sql

    def test_parse_llm_response_handles_dict_payload(self, sql_agent, sample_sql_agent_input):
        generated = sql_agent._parse_llm_response(
            {
                "sql": "SELECT COUNT(*) FROM analytics.fact_sales",
                "confidence": 0.9,
            },
            sample_sql_agent_input,
        )

        assert generated.sql == "SELECT COUNT(*) FROM analytics.fact_sales"

    def test_parse_llm_response_handles_clarification_only_json(
        self, sql_agent, sample_sql_agent_input
    ):
        content = json.dumps(
            {
                "sql": "",
                "explanation": "Need more detail before generating SQL.",
                "confidence": 0.25,
                "clarifying_questions": ["Which table should I use?"],
            }
        )

        generated = sql_agent._parse_llm_response(content, sample_sql_agent_input)

        assert generated.sql == "SELECT 1"
        assert generated.clarifying_questions == ["Which table should I use?"]

    @pytest.mark.asyncio
    async def test_handles_non_string_retry_payload_without_type_error(
        self, sample_sql_agent_input
    ):
        malformed_response = LLMResponse(
            content='{"explanation":"missing sql"}',
            model="gpt-4o",
            usage=LLMUsage(prompt_tokens=20, completion_tokens=10, total_tokens=30),
            finish_reason="stop",
            provider="openai",
        )

        # Simulate unexpected SDK payload type on retry.
        retry_response = Mock()
        retry_response.content = AsyncMock()
        retry_response.usage = Mock(total_tokens=10)
        retry_response.finish_reason = "stop"

        main_provider = Mock()
        main_provider.generate = AsyncMock(side_effect=[malformed_response, retry_response])
        main_provider.provider = "openai"
        main_provider.model = "gpt-4o"

        formatter_provider = Mock()
        formatter_provider.generate = AsyncMock(return_value=malformed_response)
        formatter_provider.provider = "openai"
        formatter_provider.model = "gpt-4o-mini"

        mock_settings = Mock()
        mock_settings.llm = Mock(sql_formatter_model=None)
        mock_settings.database = Mock(url=None, db_type="postgresql", pool_size=5)
        mock_settings.pipeline = Mock(
            sql_two_stage_enabled=False,
            sql_prompt_budget_enabled=False,
            schema_snapshot_cache_enabled=False,
            sql_formatter_fallback_enabled=False,
        )

        with (
            patch("backend.agents.sql.get_settings", return_value=mock_settings),
            patch(
                "backend.agents.sql.LLMProviderFactory.create_agent_provider",
                side_effect=[main_provider, formatter_provider],
            ),
        ):
            agent = SQLAgent()

        output = await agent(sample_sql_agent_input)
        assert output.needs_clarification is True

    def test_does_not_treat_natural_language_show_phrase_as_sql(self, sql_agent):
        text = (
            '{"explanation": "I can show you the first 5 rows if you provide a table.", '
            '"clarifying_questions": ["Which table?"]}'
        )
        extracted = sql_agent._extract_sql_statement(text)
        assert extracted is None

    def test_short_command_like_followup_is_not_treated_as_table_hint(self, sql_agent):
        assert sql_agent._looks_like_followup_hint("show columns") is False

    def test_merge_query_with_table_hint_replaces_old_table_reference(self, sql_agent):
        merged = sql_agent._merge_query_with_table_hint(
            "show 2 rows in public.sales",
            "petra_campuses",
        )
        assert merged == "Show 2 rows from petra_campuses"

    def test_parse_llm_response_caps_implicit_limit_to_default(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(update={"query": "List all grocery stores"})
        content = json.dumps(
            {"sql": "SELECT * FROM public.grocery_stores LIMIT 10000", "confidence": 0.9}
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql == "SELECT * FROM public.grocery_stores LIMIT 100"

    def test_parse_llm_response_respects_explicit_limit_within_hard_cap(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={"query": "Show me the first 50 rows from public.grocery_stores"}
        )
        content = json.dumps(
            {"sql": "SELECT * FROM public.grocery_stores LIMIT 10000", "confidence": 0.9}
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql == "SELECT * FROM public.grocery_stores LIMIT 50"

    def test_parse_llm_response_adds_default_limit_when_missing(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={"query": "Show rows from public.grocery_stores"}
        )
        content = json.dumps({"sql": "SELECT * FROM public.grocery_stores", "confidence": 0.9})

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql == "SELECT * FROM public.grocery_stores LIMIT 100"

    def test_parse_llm_response_adds_outer_limit_when_only_subquery_has_limit(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(update={"query": "Show orders"})
        content = json.dumps(
            {
                "sql": (
                    "SELECT * FROM public.orders o "
                    "WHERE EXISTS ("
                    "SELECT 1 FROM public.order_items oi "
                    "WHERE oi.order_id = o.id LIMIT 1"
                    ")"
                ),
                "confidence": 0.9,
            }
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert "LIMIT 1" in generated.sql
        assert generated.sql.endswith("LIMIT 100")

    def test_parse_llm_response_keeps_top_level_parameterized_limit(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(update={"query": "Show orders"})
        content = json.dumps(
            {
                "sql": (
                    "SELECT * FROM public.orders o "
                    "WHERE EXISTS ("
                    "SELECT 1 FROM public.order_items oi "
                    "WHERE oi.order_id = o.id LIMIT 1"
                    ") LIMIT $1"
                ),
                "confidence": 0.9,
            }
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql.endswith("LIMIT $1")

    def test_parse_llm_response_does_not_force_limit_on_single_aggregate(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(update={"query": "What is total revenue?"})
        content = json.dumps(
            {
                "sql": (
                    "SELECT SUM(total_amount) AS total_revenue "
                    "FROM public.grocery_sales_transactions"
                ),
                "confidence": 0.9,
            }
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql == (
            "SELECT SUM(total_amount) AS total_revenue FROM public.grocery_sales_transactions"
        )

    def test_parse_llm_response_does_not_add_default_limit_to_grouped_aggregate(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={"query": "Show revenue by store"}
        )
        content = json.dumps(
            {
                "sql": (
                    "SELECT store_id, SUM(total_amount) AS revenue "
                    "FROM public.grocery_sales_transactions "
                    "GROUP BY store_id ORDER BY revenue DESC"
                ),
                "confidence": 0.9,
            }
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql == (
            "SELECT store_id, SUM(total_amount) AS revenue FROM public.grocery_sales_transactions "
            "GROUP BY store_id ORDER BY revenue DESC"
        )

    def test_parse_llm_response_keeps_existing_limit_for_aggregate_when_user_did_not_request_limit(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={"query": "Show revenue by store"}
        )
        content = json.dumps(
            {
                "sql": (
                    "SELECT store_id, SUM(total_amount) AS revenue "
                    "FROM public.grocery_sales_transactions "
                    "GROUP BY store_id ORDER BY revenue DESC LIMIT 10000"
                ),
                "confidence": 0.9,
            }
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql.endswith("LIMIT 10000")

    def test_parse_llm_response_applies_requested_limit_for_aggregate_queries(
        self, sql_agent, sample_sql_agent_input
    ):
        sql_input = sample_sql_agent_input.model_copy(
            update={"query": "Show top 5 stores by revenue"}
        )
        content = json.dumps(
            {
                "sql": (
                    "SELECT store_id, SUM(total_amount) AS revenue "
                    "FROM public.grocery_sales_transactions "
                    "GROUP BY store_id ORDER BY revenue DESC LIMIT 10000"
                ),
                "confidence": 0.9,
            }
        )

        generated = sql_agent._parse_llm_response(content, sql_input)

        assert generated.sql.endswith("LIMIT 5")

    @pytest.mark.asyncio
    async def test_uses_finance_net_flow_template_for_segment_variance(self, sql_agent):
        memory = InvestigationMemory(
            query="seed",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="table_bank_transactions_001",
                    datapoint_type="Schema",
                    name="Bank Transactions",
                    score=0.95,
                    source="hybrid",
                    metadata={"table_name": "public.bank_transactions", "key_columns": []},
                ),
                RetrievedDataPoint(
                    datapoint_id="table_bank_accounts_001",
                    datapoint_type="Schema",
                    name="Bank Accounts",
                    score=0.94,
                    source="hybrid",
                    metadata={"table_name": "public.bank_accounts", "key_columns": []},
                ),
                RetrievedDataPoint(
                    datapoint_id="table_bank_customers_001",
                    datapoint_type="Schema",
                    name="Bank Customers",
                    score=0.93,
                    source="hybrid",
                    metadata={"table_name": "public.bank_customers", "key_columns": []},
                ),
            ],
            total_retrieved=3,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query=(
                "Show total deposits, withdrawals, and net flow by segment for the last 8 weeks, "
                "then identify the top 2 segments driving week-over-week net flow decline."
            ),
            investigation_memory=memory,
            database_type="postgresql",
        )

        output = await sql_agent(sql_input)

        assert output.success is True
        assert output.needs_clarification is False
        assert "weekly_segment_flow" in output.generated_sql.sql
        assert "top_decline_driver" in output.generated_sql.sql
        assert output.metadata.llm_calls == 0

    @pytest.mark.asyncio
    async def test_uses_finance_loan_default_rate_template_with_relationship_hints(
        self, sql_agent
    ):
        memory = InvestigationMemory(
            query="seed",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="table_bank_loans_001",
                    datapoint_type="Schema",
                    name="Bank Loans",
                    score=0.95,
                    source="hybrid",
                    metadata={
                        "table_name": "public.bank_loans",
                        "relationships": json.dumps(
                            [{"target_table": "public.bank_customers", "join_column": "customer_id"}]
                        ),
                    },
                ),
            ],
            total_retrieved=1,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query="Show loan default rate by segment.",
            investigation_memory=memory,
            database_type="postgresql",
        )

        output = await sql_agent(sql_input)

        assert output.success is True
        assert output.needs_clarification is False
        assert "default_rate_pct" in output.generated_sql.sql
        assert "JOIN public.bank_customers" in output.generated_sql.sql
        assert output.metadata.llm_calls == 0


class TestInputValidation:
    """Test input validation."""

    @pytest.mark.asyncio
    async def test_validates_input_type(self, sql_agent):
        """Test validates input is SQLAgentInput."""
        from backend.models.agent import AgentInput, ValidationError

        invalid_input = AgentInput(query="test")

        with pytest.raises(ValidationError) as exc_info:
            await sql_agent(invalid_input)

        assert "Expected SQLAgentInput" in str(exc_info.value)


class TestQueryDataPointTemplates:
    """Test QueryDataPoint template matching and execution."""

    def test_try_query_datapoint_template_returns_none_without_datapoints(self, sql_agent):
        """Returns None when no datapoints in memory."""
        memory = InvestigationMemory(
            query="show top customers",
            datapoints=[],
            total_retrieved=0,
            retrieval_mode="hybrid",
            sources_used=[],
        )
        sql_input = SQLAgentInput(
            query="show top customers",
            investigation_memory=memory,
        )

        result = sql_agent._try_query_datapoint_template(sql_input)

        assert result is None

    def test_try_query_datapoint_template_ignores_non_query_datapoints(self, sql_agent):
        """Ignores Schema and Business datapoints."""
        memory = InvestigationMemory(
            query="show top customers",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="table_sales_001",
                    datapoint_type="Schema",
                    name="Sales",
                    score=0.9,
                    source="vector",
                    metadata={"table_name": "sales"},
                ),
            ],
            total_retrieved=1,
            retrieval_mode="hybrid",
            sources_used=["table_sales_001"],
        )
        sql_input = SQLAgentInput(
            query="show top customers",
            investigation_memory=memory,
        )

        result = sql_agent._try_query_datapoint_template(sql_input)

        assert result is None

    def test_try_query_datapoint_template_uses_matching_template(self, sql_agent):
        """Uses QueryDataPoint template when query matches."""
        memory = InvestigationMemory(
            query="show top customers by revenue",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="query_top_customers_001",
                    datapoint_type="Query",
                    name="Top Customers by Revenue",
                    score=0.95,
                    source="vector",
                    metadata={
                        "sql_template": (
                            "SELECT customer_id, SUM(amount) as revenue "
                            "FROM transactions "
                            "GROUP BY customer_id "
                            "ORDER BY revenue DESC LIMIT {limit}"
                        ),
                        "query_description": "Returns top customers by total revenue",
                        "parameters": json.dumps({"limit": {"type": "integer", "default": 10}}),
                    },
                ),
            ],
            total_retrieved=1,
            retrieval_mode="hybrid",
            sources_used=["query_top_customers_001"],
        )
        sql_input = SQLAgentInput(
            query="show top customers by revenue",
            investigation_memory=memory,
        )

        result = sql_agent._try_query_datapoint_template(sql_input)

        assert result is not None
        assert result.confidence == 0.95
        assert "query_top_customers_001" in result.used_datapoints
        assert "LIMIT 10" in result.sql

    def test_try_query_datapoint_template_uses_backend_variant(self, sql_agent):
        """Uses backend-specific SQL variant when available."""
        memory = InvestigationMemory(
            query="show daily sales",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="query_daily_sales_001",
                    datapoint_type="Query",
                    name="Daily Sales Summary",
                    score=0.90,
                    source="vector",
                    metadata={
                        "sql_template": "SELECT date, SUM(amount) FROM sales GROUP BY date",
                        "query_description": "Daily sales aggregation",
                        "backend_variants": json.dumps(
                            {
                                "clickhouse": "SELECT date, SUM(amount) FROM sales GROUP BY date ORDER BY date"
                            }
                        ),
                    },
                ),
            ],
            total_retrieved=1,
            retrieval_mode="hybrid",
            sources_used=["query_daily_sales_001"],
        )
        sql_input = SQLAgentInput(
            query="show daily sales",
            investigation_memory=memory,
            database_type="clickhouse",
        )

        result = sql_agent._try_query_datapoint_template(sql_input)

        assert result is not None
        assert "ORDER BY date" in result.sql

    def test_fill_template_defaults_handles_string_params(self, sql_agent):
        """Fills string parameters with quotes."""
        sql = "SELECT * FROM users WHERE status = {status}"
        params = {"status": {"type": "string", "default": "active"}}

        result = sql_agent._fill_template_defaults(sql, params)

        assert result == "SELECT * FROM users WHERE status = 'active'"

    def test_fill_template_defaults_handles_integer_params(self, sql_agent):
        """Fills integer parameters without quotes."""
        sql = "SELECT * FROM users LIMIT {limit}"
        params = {"limit": {"type": "integer", "default": 10}}

        result = sql_agent._fill_template_defaults(sql, params)

        assert result == "SELECT * FROM users LIMIT 10"

    def test_fill_template_defaults_handles_boolean_params(self, sql_agent):
        """Fills boolean parameters as TRUE/FALSE."""
        sql = "SELECT * FROM users WHERE is_active = {active}"
        params = {"active": {"type": "boolean", "default": True}}

        result = sql_agent._fill_template_defaults(sql, params)

        assert result == "SELECT * FROM users WHERE is_active = TRUE"

    def test_fill_template_defaults_handles_json_string_params(self, sql_agent):
        """Handles parameters as JSON string."""
        sql = "SELECT * FROM users LIMIT {limit}"
        params = '{"limit": {"type": "integer", "default": 5}}'

        result = sql_agent._fill_template_defaults(sql, params)

        assert result == "SELECT * FROM users LIMIT 5"

    def test_fill_template_defaults_extracts_top_n_from_query(self, sql_agent):
        sql = "SELECT customer_id FROM orders ORDER BY revenue DESC LIMIT {top_n}"
        params = {"top_n": {"type": "integer", "default": 10}}

        result = sql_agent._fill_template_defaults(
            sql,
            params,
            query="Show top 3 customers by revenue",
        )

        assert result == "SELECT customer_id FROM orders ORDER BY revenue DESC LIMIT 3"

    def test_fill_template_defaults_extracts_lookback_weeks_from_query(self, sql_agent):
        sql = (
            "SELECT DATE_TRUNC('week', created_at) AS week_start "
            "FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '{lookback_weeks} weeks'"
        )
        params = {"lookback_weeks": {"type": "integer", "default": 8}}

        result = sql_agent._fill_template_defaults(
            sql,
            params,
            query="Show orders for the last 6 weeks",
        )

        assert "INTERVAL '6 weeks'" in result

    def test_fill_template_defaults_extracts_lookback_months_from_query(self, sql_agent):
        sql = (
            "SELECT DATE_TRUNC('month', created_at) AS month_start "
            "FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '{lookback_months} months'"
        )
        params = {"lookback_months": {"type": "integer", "default": 6}}

        result = sql_agent._fill_template_defaults(
            sql,
            params,
            query="Show monthly orders for the last 9 months",
        )

        assert "INTERVAL '9 months'" in result

    def test_query_matches_template_by_name_overlap(self, sql_agent):
        """Matches when query shares words with template name."""
        query = "show top customers by revenue"
        name = "Top Customers Revenue"
        description = "Returns top customers by total revenue"

        assert sql_agent._query_matches_template(query.lower(), name.lower(), description.lower())

    def test_query_matches_template_by_description_keywords(self, sql_agent):
        """Matches when query shares keywords with description."""
        query = "daily sales aggregation report breakdown"
        name = "Sales Query"
        description = "aggregates daily sales totals for reporting breakdown"

        assert sql_agent._query_matches_template(query.lower(), name.lower(), description.lower())

    def test_query_does_not_match_unrelated_template(self, sql_agent):
        """Does not match unrelated queries."""
        query = "show user activity"
        name = "Revenue Report"
        description = "Monthly revenue breakdown by region"

        assert not sql_agent._query_matches_template(
            query.lower(), name.lower(), description.lower()
        )

    def test_try_query_datapoint_template_picks_best_finance_match(self, sql_agent):
        """Prefers the strongest matching finance template instead of a weaker overlap."""
        memory = InvestigationMemory(
            query="Identify the top 2 segments driving week-over-week net flow decline",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="query_weekly_net_flow_001",
                    datapoint_type="Query",
                    name="Weekly Deposits Withdrawals Net Flow by Segment",
                    score=0.91,
                    source="vector",
                    metadata={
                        "sql_template": "SELECT week_start, segment, net_flow FROM weekly_flows",
                        "query_description": (
                            "Returns weekly deposits, withdrawals, and net flow by segment."
                        ),
                        "tags": "fintech,net_flow,segment,weekly",
                    },
                ),
                RetrievedDataPoint(
                    datapoint_id="query_wow_decline_001",
                    datapoint_type="Query",
                    name="Top Segments Driving Week over Week Net Flow Decline",
                    score=0.89,
                    source="vector",
                    metadata={
                        "sql_template": (
                            "SELECT segment, decline_amount "
                            "FROM segment_decline "
                            "ORDER BY decline_amount DESC "
                            "LIMIT {top_n}"
                        ),
                        "query_description": (
                            "Ranks segments by the largest week-over-week net-flow decline."
                        ),
                        "parameters": json.dumps(
                            {"top_n": {"type": "integer", "default": 2}}
                        ),
                        "tags": "fintech,net_flow,week_over_week,decline,segment,drivers",
                    },
                ),
            ],
            total_retrieved=2,
            retrieval_mode="hybrid",
            sources_used=["query_weekly_net_flow_001", "query_wow_decline_001"],
        )
        sql_input = SQLAgentInput(
            query="Identify the top 2 segments driving week-over-week net flow decline",
            investigation_memory=memory,
        )

        result = sql_agent._try_query_datapoint_template(sql_input)

        assert result is not None
        assert result.used_datapoints == ["query_wow_decline_001"]
        assert "LIMIT 2" in result.sql

    def test_try_query_datapoint_template_prefers_deposit_trend_over_generic_balance(
        self, sql_agent
    ):
        memory = InvestigationMemory(
            query="Show weekly deposit trend for the last 6 weeks from the last deposit date",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="query_weekly_balance_001",
                    datapoint_type="Query",
                    name="Weekly Current Balance Trend",
                    score=0.93,
                    source="vector",
                    metadata={
                        "sql_template": (
                            "SELECT DATE_TRUNC('week', opened_at) AS week_start, "
                            "SUM(current_balance) AS total_current_balance "
                            "FROM public.bank_accounts "
                            "WHERE opened_at >= CURRENT_DATE - INTERVAL '{lookback_weeks} weeks' "
                            "GROUP BY 1 ORDER BY 1 DESC"
                        ),
                        "query_description": "Shows weekly current balance totals from account opening dates.",
                        "parameters": json.dumps(
                            {"lookback_weeks": {"type": "integer", "default": 8}}
                        ),
                        "tags": "weekly,trend,balance,accounts",
                    },
                ),
                RetrievedDataPoint(
                    datapoint_id="query_weekly_deposit_001",
                    datapoint_type="Query",
                    name="Weekly Deposit Trend from Latest Deposit Date",
                    score=0.9,
                    source="vector",
                    metadata={
                        "sql_template": (
                            "WITH anchor AS ("
                            "SELECT MAX(business_date) AS anchor_date "
                            "FROM public.bank_transactions WHERE direction = 'credit'"
                            ") "
                            "SELECT DATE_TRUNC('week', business_date) AS week_start, "
                            "SUM(amount) AS total_deposit_amount "
                            "FROM public.bank_transactions "
                            "WHERE direction = 'credit' "
                            "AND business_date >= COALESCE((SELECT anchor_date FROM anchor), CURRENT_DATE) "
                            "- INTERVAL '{lookback_weeks} weeks' "
                            "GROUP BY 1 ORDER BY 1 DESC"
                        ),
                        "query_description": (
                            "Shows weekly deposit totals from the latest deposit date."
                        ),
                        "parameters": json.dumps(
                            {"lookback_weeks": {"type": "integer", "default": 8}}
                        ),
                        "tags": "weekly,trend,deposit,credit,latest_date_anchor",
                    },
                ),
            ],
            total_retrieved=2,
            retrieval_mode="hybrid",
            sources_used=["query_weekly_balance_001", "query_weekly_deposit_001"],
        )
        sql_input = SQLAgentInput(
            query="Show weekly deposit trend for the last 6 weeks from the last deposit date",
            investigation_memory=memory,
        )

        result = sql_agent._try_query_datapoint_template(sql_input)

        assert result is not None
        assert result.used_datapoints == ["query_weekly_deposit_001"]
        assert "direction = 'credit'" in result.sql
        assert "INTERVAL '6 weeks'" in result.sql

    def test_query_template_match_score_penalizes_balance_for_deposit_prompt(self, sql_agent):
        deposit_score = sql_agent._query_template_match_score(
            query_lower="show weekly deposit trend for the last 6 weeks from the last deposit date",
            name_lower="weekly deposit trend from latest deposit date",
            description_lower="shows weekly deposit totals from the latest deposit date",
            tags=["weekly", "trend", "deposit", "credit", "latest_date_anchor"],
        )
        balance_score = sql_agent._query_template_match_score(
            query_lower="show weekly deposit trend for the last 6 weeks from the last deposit date",
            name_lower="weekly current balance trend",
            description_lower="shows weekly current balance totals from account opening dates",
            tags=["weekly", "trend", "balance", "accounts"],
        )

        assert deposit_score > balance_score

    @pytest.mark.asyncio
    async def test_generate_sql_uses_query_datapoint_template(self, sql_agent):
        """_generate_sql uses QueryDataPoint template when matched."""
        memory = InvestigationMemory(
            query="top customers revenue",
            datapoints=[
                RetrievedDataPoint(
                    datapoint_id="query_top_cust_001",
                    datapoint_type="Query",
                    name="Top Customers by Revenue",
                    score=0.95,
                    source="vector",
                    metadata={
                        "sql_template": (
                            "SELECT customer_id, SUM(amount) as revenue "
                            "FROM orders GROUP BY customer_id "
                            "ORDER BY revenue DESC LIMIT {limit}"
                        ),
                        "query_description": "top customers by total revenue",
                        "parameters": '{"limit": {"type": "integer", "default": 5}}',
                    },
                ),
            ],
            total_retrieved=1,
            retrieval_mode="hybrid",
            sources_used=["query_top_cust_001"],
        )
        sql_input = SQLAgentInput(
            query="top customers revenue",
            investigation_memory=memory,
            database_type="postgresql",
        )

        result = await sql_agent._generate_sql(
            sql_input,
            metadata=AgentMetadata(agent_name="SQLAgent"),
            runtime_stats={},
        )

        assert result.confidence >= 0.95
        assert "query_top_cust_001" in result.used_datapoints
        assert "LIMIT 5" in result.sql
        sql_agent.llm.generate.assert_not_called()

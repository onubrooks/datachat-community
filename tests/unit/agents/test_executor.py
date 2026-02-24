"""
Unit tests for ExecutorAgent.

Tests query execution including:
- Query execution with results
- Natural language summarization
- Visualization hints
- Timeout handling
- Result truncation
- Citations
"""

import asyncio
from unittest.mock import AsyncMock

import pytest

from backend.agents.executor import ExecutorAgent
from backend.connectors.base import QueryError
from backend.connectors.base import QueryResult as ConnectorQueryResult
from backend.llm.models import LLMResponse, LLMUsage
from backend.models import ExecutorAgentInput, ValidatedSQL


class TestExecutorAgent:
    """Test suite for ExecutorAgent."""

    @pytest.fixture
    def executor_agent(self, mock_llm_provider, mock_postgres_connector, mock_openai_api_key):
        """Create ExecutorAgent with mocked dependencies."""
        agent = ExecutorAgent()
        agent.llm = mock_llm_provider
        agent._get_connector = AsyncMock(return_value=mock_postgres_connector)
        return agent

    @pytest.fixture
    def sample_validated_sql(self):
        """Sample validated SQL."""
        return ValidatedSQL(
            is_valid=True,
            sql="SELECT customer_id, SUM(amount) as total FROM fact_sales WHERE date >= '2024-01-01' GROUP BY customer_id LIMIT 10",
            errors=[],
            warnings=[],
            suggestions=[],
            is_safe=True,
            performance_score=0.95,
        )

    @pytest.fixture
    def sample_input(self, sample_validated_sql):
        """Sample ExecutorAgentInput."""
        return ExecutorAgentInput(
            query="What were total sales by customer?",
            validated_sql=sample_validated_sql,
            database_type="postgresql",
            max_rows=1000,
            timeout_seconds=30,
            source_datapoints=["table_fact_sales_001"],
        )

    @pytest.fixture
    def mock_postgres_connector(self):
        """Mock PostgreSQL connector."""
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.close = AsyncMock()

        # Default successful query result
        connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"customer_id": 123, "total": 5000.0},
                    {"customer_id": 456, "total": 3200.0},
                ],
                row_count=2,
                columns=["customer_id", "total"],
                execution_time_ms=50.0,
            )
        )

        return connector

    def _llm_response(self, content: str) -> LLMResponse:
        return LLMResponse(
            content=content,
            model="mock-model",
            usage=LLMUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
            finish_reason="stop",
            provider="mock",
            metadata={},
        )

    # ============================================================================
    # Query Execution Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_executes_query_successfully(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test successful query execution."""
        mock_llm_provider.set_response(
            "Answer: Customer 123 had $5,000 in sales and customer 456 had $3,200."
        )

        result = await executor_agent.execute(sample_input)

        assert result.success is True
        assert result.executed_query.query_result.row_count == 2
        assert len(result.executed_query.query_result.rows) == 2
        assert result.executed_query.executed_sql == sample_input.validated_sql.sql
        mock_postgres_connector.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_correct_data_structure(
        self, executor_agent, sample_input, mock_llm_provider
    ):
        """Test that results have correct data structure."""
        mock_llm_provider.set_response("Answer: Two customers found.")

        result = await executor_agent.execute(sample_input)

        query_result = result.executed_query.query_result
        assert isinstance(query_result.rows, list)
        assert isinstance(query_result.columns, list)
        assert isinstance(query_result.row_count, int)
        assert isinstance(query_result.execution_time_ms, float)
        assert query_result.execution_time_ms > 0

    @pytest.mark.asyncio
    async def test_includes_column_names(self, executor_agent, sample_input, mock_llm_provider):
        """Test that column names are included in results."""
        mock_llm_provider.set_response("Answer: Results show customer IDs and totals.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.query_result.columns == ["customer_id", "total"]

    # ============================================================================
    # Natural Language Summary Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_generates_natural_language_answer(
        self, executor_agent, sample_input, mock_llm_provider
    ):
        """Test that NL summary is generated."""
        expected_answer = "Customer 123 had total sales of $5,000 and customer 456 had $3,200."
        mock_llm_provider.set_response(f"Answer: {expected_answer}\nInsights: - Customer 123 leads")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.natural_language_answer == expected_answer
        assert len(result.executed_query.natural_language_answer) > 0

    @pytest.mark.asyncio
    async def test_extracts_key_insights(self, executor_agent, sample_input, mock_llm_provider):
        """Test that key insights are extracted."""
        mock_llm_provider.set_response(
            """Answer: Two customers found.
Insights:
- Customer 123 leads with $5,000
- Customer 456 follows with $3,200"""
        )

        result = await executor_agent.execute(sample_input)

        assert len(result.executed_query.key_insights) == 2
        assert "Customer 123" in result.executed_query.key_insights[0]

    @pytest.mark.asyncio
    async def test_handles_summary_generation_failure(
        self, executor_agent, sample_input, mock_llm_provider
    ):
        """Test graceful handling when LLM summary fails."""
        mock_llm_provider.generate = AsyncMock(side_effect=Exception("LLM error"))

        result = await executor_agent.execute(sample_input)

        # Should fallback to basic summary
        assert result.success is True
        assert len(result.executed_query.natural_language_answer) > 0
        assert "2 results" in result.executed_query.natural_language_answer.lower()

    # ============================================================================
    # Empty Results Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_handles_empty_results(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test handling of queries with no results."""
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[], row_count=0, columns=["customer_id", "total"], execution_time_ms=10.0
            )
        )
        mock_llm_provider.set_response("Answer: No results found.")

        result = await executor_agent.execute(sample_input)

        assert result.success is True
        assert result.executed_query.query_result.row_count == 0
        assert "no results" in result.executed_query.natural_language_answer.lower()

    @pytest.mark.asyncio
    async def test_empty_results_basic_summary(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test basic summary for empty results."""
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[], row_count=0, columns=[], execution_time_ms=10.0
            )
        )
        # Simulate LLM failure to test fallback
        mock_llm_provider.generate = AsyncMock(side_effect=Exception("Failed"))

        result = await executor_agent.execute(sample_input)

        assert "no results" in result.executed_query.natural_language_answer.lower()

    @pytest.mark.asyncio
    async def test_empty_information_schema_columns_is_non_hallucinatory(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        sample_input.validated_sql.sql = (
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = 'sales' AND table_schema = 'public'"
        )
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[],
                row_count=0,
                columns=["table_schema", "table_name", "column_name", "data_type"],
                execution_time_ms=9.0,
            )
        )
        mock_llm_provider.set_response("Answer: hallucinated")

        result = await executor_agent.execute(sample_input)

        assert (
            "No columns were found for table `sales`."
            == result.executed_query.natural_language_answer
        )
        assert result.metadata.llm_calls == 0

    @pytest.mark.asyncio
    async def test_information_schema_tables_summary_is_case_insensitive(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        sample_input.validated_sql.sql = (
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('mysql', 'performance_schema', 'information_schema', 'sys') "
            "ORDER BY table_schema, table_name LIMIT 10"
        )
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"TABLE_SCHEMA": "banking", "TABLE_NAME": "customers"},
                    {"TABLE_SCHEMA": "banking", "TABLE_NAME": "accounts"},
                    {"TABLE_SCHEMA": "banking", "TABLE_NAME": "transactions"},
                ],
                row_count=3,
                columns=["table_schema", "table_name"],
                execution_time_ms=8.0,
            )
        )
        mock_llm_provider.set_response("Answer: hallucinated")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.natural_language_answer.startswith("Found 3 table(s):")
        assert "banking.customers" in result.executed_query.natural_language_answer
        assert result.metadata.llm_calls == 0

    @pytest.mark.asyncio
    async def test_information_schema_columns_summary_is_case_insensitive(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        sample_input.validated_sql.sql = (
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = 'customers'"
        )
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"TABLE_SCHEMA": "banking", "TABLE_NAME": "customers", "COLUMN_NAME": "id"},
                    {"TABLE_SCHEMA": "banking", "TABLE_NAME": "customers", "COLUMN_NAME": "email"},
                ],
                row_count=2,
                columns=["table_schema", "table_name", "column_name"],
                execution_time_ms=8.0,
            )
        )
        mock_llm_provider.set_response("Answer: hallucinated")

        result = await executor_agent.execute(sample_input)

        assert "table has 2 column(s): id, email." in result.executed_query.natural_language_answer
        assert result.metadata.llm_calls == 0

    @pytest.mark.asyncio
    async def test_information_schema_columns_capability_discovery_summary(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        sample_input.query = "What kind of info can I get from available tables?"
        sample_input.validated_sql.sql = (
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY table_schema, table_name, ordinal_position"
        )
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {
                        "table_schema": "public",
                        "table_name": "bank_transactions",
                        "column_name": "posted_at",
                    },
                    {
                        "table_schema": "public",
                        "table_name": "bank_transactions",
                        "column_name": "amount",
                    },
                    {
                        "table_schema": "public",
                        "table_name": "bank_customers",
                        "column_name": "segment",
                    },
                    {
                        "table_schema": "public",
                        "table_name": "bank_customers",
                        "column_name": "kyc_status",
                    },
                ],
                row_count=4,
                columns=["table_schema", "table_name", "column_name"],
                execution_time_ms=8.0,
            )
        )
        mock_llm_provider.set_response("Answer: hallucinated")

        result = await executor_agent.execute(sample_input)

        assert "2 table(s)" in result.executed_query.natural_language_answer
        assert "Likely analysis areas" in result.executed_query.natural_language_answer
        assert "public.bank_transactions" in result.executed_query.natural_language_answer
        assert result.metadata.llm_calls == 0

    # ============================================================================
    # Timeout Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_timeout_prevents_long_queries(
        self, executor_agent, sample_input, mock_postgres_connector
    ):
        """Test that timeout prevents runaway queries."""

        # Simulate slow query
        async def slow_query(*args, **kwargs):
            await asyncio.sleep(2)
            return ConnectorQueryResult(rows=[], row_count=0, columns=[], execution_time_ms=2000.0)

        mock_postgres_connector.execute = slow_query
        sample_input.timeout_seconds = 1  # 1 second timeout

        with pytest.raises(TimeoutError):
            await executor_agent.execute(sample_input)

    # ============================================================================
    # Result Truncation Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_truncates_large_results(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test that large result sets are truncated."""
        # Create 150 rows
        large_result = [{"id": i, "value": i * 100} for i in range(150)]
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=large_result, row_count=150, columns=["id", "value"], execution_time_ms=100.0
            )
        )
        mock_llm_provider.set_response("Answer: Found 100 results (truncated).")

        sample_input.max_rows = 100

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.query_result.row_count == 100
        assert result.executed_query.query_result.was_truncated is True
        assert result.executed_query.query_result.max_rows == 100

    @pytest.mark.asyncio
    async def test_no_truncation_for_small_results(
        self, executor_agent, sample_input, mock_llm_provider
    ):
        """Test that small results are not truncated."""
        mock_llm_provider.set_response("Answer: Two customers found.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.query_result.was_truncated is False
        assert result.executed_query.query_result.max_rows is None

    # ============================================================================
    # Visualization Hint Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_suggests_bar_chart_for_categories(
        self, executor_agent, sample_input, mock_llm_provider
    ):
        """Test bar chart suggestion for categorical data."""
        mock_llm_provider.set_response("Answer: Sales by customer.")

        result = await executor_agent.execute(sample_input)

        # 2 columns, small number of rows -> bar chart
        assert result.executed_query.visualization_hint in ["bar_chart", "table"]

    @pytest.mark.asyncio
    async def test_suggests_table_for_many_rows(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test table suggestion for many rows."""
        # Create 30 rows
        many_rows = [{"id": i, "value": i * 100} for i in range(30)]
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=many_rows, row_count=30, columns=["id", "value"], execution_time_ms=50.0
            )
        )
        mock_llm_provider.set_response("Answer: Found 30 results.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "table"

    @pytest.mark.asyncio
    async def test_suggests_line_chart_for_time_series(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test line chart suggestion for time series data."""
        time_series = [
            {"date": "2024-01-01", "amount": 1000},
            {"date": "2024-01-02", "amount": 1500},
        ]
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=time_series, row_count=2, columns=["date", "amount"], execution_time_ms=20.0
            )
        )
        mock_llm_provider.set_response("Answer: Sales over time.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "line_chart"

    @pytest.mark.asyncio
    async def test_non_temporal_column_name_with_day_substring_does_not_force_line_chart(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Columns like holiday_type should not be inferred as time axes."""
        sample_rows = [
            {"holiday_type": "public", "waste_cost": 120.0},
            {"holiday_type": "seasonal", "waste_cost": 95.0},
        ]
        sample_input.query = "Show waste cost by holiday type"
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=sample_rows,
                row_count=2,
                columns=["holiday_type", "waste_cost"],
                execution_time_ms=20.0,
            )
        )
        mock_llm_provider.set_response("Answer: Waste cost by holiday type.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "bar_chart"

    @pytest.mark.asyncio
    async def test_suggests_none_for_single_value(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test no visualization for single value."""
        single_value = [{"total": 50000.0}]
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=single_value, row_count=1, columns=["total"], execution_time_ms=15.0
            )
        )
        mock_llm_provider.set_response("Answer: Total is $50,000.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "none"

    @pytest.mark.asyncio
    async def test_suggests_bar_for_multi_column_without_scatter_intent(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Multi-column metrics default to bar/table unless scatter intent is explicit."""
        multi_col = [
            {"x": 1, "y": 10, "z": 100},
            {"x": 2, "y": 20, "z": 200},
        ]
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=multi_col, row_count=2, columns=["x", "y", "z"], execution_time_ms=25.0
            )
        )
        mock_llm_provider.set_response("Answer: Multi-dimensional data.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "bar_chart"

    @pytest.mark.asyncio
    async def test_suggests_scatter_when_query_explicitly_requests_it(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Scatter should be selected only for explicit relationship/correlation asks."""
        sample_input.query = "Show correlation between x and y as scatter plot"
        multi_col = [
            {"x": 1, "y": 10, "z": 100},
            {"x": 2, "y": 20, "z": 200},
            {"x": 3, "y": 35, "z": 210},
        ]
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=multi_col,
                row_count=3,
                columns=["x", "y", "z"],
                execution_time_ms=25.0,
            )
        )
        mock_llm_provider.set_response("Answer: Correlation shown.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "scatter"

    @pytest.mark.asyncio
    async def test_query_can_override_default_to_bar_chart(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """When query asks for bar chart, prefer bar chart if shape is valid."""
        time_series = [
            {"date": "2024-01-01", "amount": 1000},
            {"date": "2024-01-02", "amount": 1500},
            {"date": "2024-01-03", "amount": 1300},
        ]
        sample_input.query = "Show trend of sales as a bar chart"
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=time_series,
                row_count=3,
                columns=["date", "amount"],
                execution_time_ms=20.0,
            )
        )
        mock_llm_provider.set_response("Answer: Sales trend by day.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "bar_chart"

    @pytest.mark.asyncio
    async def test_query_can_disable_visualization(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """When query requests no chart, return none hint."""
        sample_input.query = "Show sales by customer with no visualization"
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"customer_id": 123, "total": 5000.0},
                    {"customer_id": 456, "total": 3200.0},
                ],
                row_count=2,
                columns=["customer_id", "total"],
                execution_time_ms=50.0,
            )
        )
        mock_llm_provider.set_response("Answer: Two customers found.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "none"

    @pytest.mark.asyncio
    async def test_line_chart_hint_falls_back_when_no_time_dimension(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Line-chart requests should not force line when result has no time axis."""
        sample_input.query = "Show order line items by customer as a line chart"
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"customer": "A", "total": 100.0},
                    {"customer": "B", "total": 150.0},
                    {"customer": "C", "total": 90.0},
                ],
                row_count=3,
                columns=["customer", "total"],
                execution_time_ms=25.0,
            )
        )
        mock_llm_provider.set_response("Answer: Totals by customer.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "bar_chart"

    # ============================================================================
    @pytest.mark.asyncio
    async def test_query_phrase_line_items_does_not_force_line_chart(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Standalone word line in domain phrasing should not imply line-chart intent."""
        sample_input.query = "Show order line items by customer"
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"customer": "A", "total": 100.0},
                    {"customer": "B", "total": 150.0},
                    {"customer": "C", "total": 90.0},
                ],
                row_count=3,
                columns=["customer", "total"],
                execution_time_ms=25.0,
            )
        )
        mock_llm_provider.set_response("Answer: Totals by customer.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "bar_chart"

    @pytest.mark.asyncio
    async def test_invalid_user_chart_request_is_overridden_with_note(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """If requested chart is invalid for data, override and explain."""
        sample_input.query = "Show revenue by category as a pie chart"
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"category": "A", "revenue": 120.0},
                    {"category": "B", "revenue": -30.0},
                    {"category": "C", "revenue": 75.0},
                ],
                row_count=3,
                columns=["category", "revenue"],
                execution_time_ms=32.0,
            )
        )
        summary_response = LLMResponse(
            content="Answer: Revenue by category.\nInsights: - Category A leads",
            model="mock-model",
            usage=LLMUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
            finish_reason="stop",
            provider="mock",
            metadata={},
        )
        planner_response = LLMResponse(
            content='{"visualization_hint":"bar_chart","confidence":0.92,"reason":"values include negatives"}',
            model="mock-model",
            usage=LLMUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
            finish_reason="stop",
            provider="mock",
            metadata={},
        )
        mock_llm_provider.generate = AsyncMock(side_effect=[summary_response, planner_response])

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "bar_chart"
        assert result.executed_query.visualization_note is not None
        assert "Requested pie chart was overridden" in result.executed_query.visualization_note
        assert result.metadata.llm_calls == 2

    @pytest.mark.asyncio
    async def test_valid_user_chart_request_is_honored_without_override_note(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """If requested chart is valid for data, keep it and skip override note."""
        sample_input.query = "Show revenue breakdown by category as a pie chart"
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=[
                    {"category": "A", "revenue": 120.0},
                    {"category": "B", "revenue": 80.0},
                    {"category": "C", "revenue": 75.0},
                ],
                row_count=3,
                columns=["category", "revenue"],
                execution_time_ms=32.0,
            )
        )
        mock_llm_provider.set_response("Answer: Revenue by category.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "pie_chart"
        assert result.executed_query.visualization_note is None
        assert result.metadata.llm_calls == 1

    # Citations Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_includes_source_citations(self, executor_agent, sample_input, mock_llm_provider):
        """Test that source citations are included."""
        mock_llm_provider.set_response("Answer: Results from fact_sales.")

        result = await executor_agent.execute(sample_input)

        assert len(result.executed_query.source_citations) > 0
        assert "table_fact_sales_001" in result.executed_query.source_citations

    @pytest.mark.asyncio
    async def test_empty_citations_handled(self, executor_agent, sample_input, mock_llm_provider):
        """Test handling of empty citations."""
        sample_input.source_datapoints = []
        mock_llm_provider.set_response("Answer: Results found.")

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.source_citations == []

    # ============================================================================
    # Error Handling Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_handles_query_execution_error(
        self, executor_agent, sample_input, mock_postgres_connector
    ):
        """Test handling of query execution errors."""
        mock_postgres_connector.execute = AsyncMock(side_effect=QueryError("Syntax error in SQL"))

        with pytest.raises(QueryError):
            await executor_agent.execute(sample_input)

    @pytest.mark.asyncio
    async def test_returns_corrected_sql_when_executor_repairs_query(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Final response should expose the repaired SQL that actually executed."""
        corrected_sql = "SELECT customer_id, SUM(amount) AS total FROM fact_sales GROUP BY customer_id"
        mock_postgres_connector.execute = AsyncMock(
            side_effect=[
                QueryError("column missing"),
                ConnectorQueryResult(
                    rows=[{"customer_id": 123, "total": 5000.0}],
                    row_count=1,
                    columns=["customer_id", "total"],
                    execution_time_ms=12.0,
                ),
            ]
        )
        executor_agent._attempt_sql_correction = AsyncMock(return_value=corrected_sql)
        mock_llm_provider.set_response("Answer: Corrected query worked.")

        result = await executor_agent.execute(sample_input)

        assert result.success is True
        assert result.executed_query.executed_sql == corrected_sql
        assert mock_postgres_connector.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_handles_connection_error(self, executor_agent, sample_input):
        """Test handling of database connection errors."""
        executor_agent._get_connector = AsyncMock(side_effect=Exception("Connection failed"))

        with pytest.raises(Exception, match="Connection failed"):
            await executor_agent.execute(sample_input)

    @pytest.mark.asyncio
    async def test_closes_connector_on_error(
        self, executor_agent, sample_input, mock_postgres_connector
    ):
        """Test that connector is closed even on error."""
        mock_postgres_connector.execute = AsyncMock(side_effect=QueryError("Error"))

        try:
            await executor_agent.execute(sample_input)
        except QueryError:
            pass

        mock_postgres_connector.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_closes_connector_on_success(
        self, executor_agent, sample_input, mock_postgres_connector, mock_llm_provider
    ):
        """Test that connector is closed after successful execution."""
        mock_llm_provider.set_response("Answer: Success.")

        await executor_agent.execute(sample_input)

        mock_postgres_connector.close.assert_called_once()

    # ============================================================================
    # Metadata Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_metadata_populated(self, executor_agent, sample_input, mock_llm_provider):
        """Test that metadata is properly populated."""
        mock_llm_provider.set_response("Answer: Results found.")

        result = await executor_agent.execute(sample_input)

        assert result.metadata is not None
        assert result.metadata.agent_name == "ExecutorAgent"
        assert result.metadata.llm_calls >= 1
        assert result.metadata.started_at is not None

    @pytest.mark.asyncio
    async def test_visualization_llm_overrides_invalid_user_request(
        self, executor_agent, sample_input, mock_postgres_connector
    ):
        sample_input.query = "Show sales by customer as a pie chart"
        many_rows = [{"customer": f"C{i}", "total": i * 10.0} for i in range(1, 21)]
        mock_postgres_connector.execute = AsyncMock(
            return_value=ConnectorQueryResult(
                rows=many_rows,
                row_count=20,
                columns=["customer", "total"],
                execution_time_ms=50.0,
            )
        )
        executor_agent.llm.generate = AsyncMock(
            side_effect=[
                self._llm_response("Answer: Sales by customer."),
                self._llm_response(
                    '{"visualization":"bar_chart","reason":"many categories","confidence":0.9}'
                ),
            ]
        )

        result = await executor_agent.execute(sample_input)

        assert result.executed_query.visualization_hint == "bar_chart"
        assert result.executed_query.visualization_metadata is not None
        assert (
            result.executed_query.visualization_metadata.get("resolution_reason")
            == "user_request_incompatible_using_llm_override"
        )

    # ============================================================================
    # Database Type Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_supports_postgresql(self, executor_agent, sample_input, mock_llm_provider):
        """Test PostgreSQL support."""
        sample_input.database_type = "postgresql"
        mock_llm_provider.set_response("Answer: PostgreSQL results.")

        result = await executor_agent.execute(sample_input)

        assert result.success is True

    @pytest.mark.asyncio
    async def test_supports_clickhouse(self, executor_agent, sample_input, mock_llm_provider):
        """Test ClickHouse support."""
        sample_input.database_type = "clickhouse"
        mock_llm_provider.set_response("Answer: ClickHouse results.")

        result = await executor_agent.execute(sample_input)

        assert result.success is True

    @pytest.mark.asyncio
    async def test_rejects_unsupported_database(self, executor_agent, sample_input):
        """Test rejection of unsupported database types."""
        # Reset the mock to allow real _get_connector to run
        executor_agent._get_connector = ExecutorAgent._get_connector.__get__(
            executor_agent, ExecutorAgent
        )

        sample_input.database_type = "mysql"  # Not fully implemented

        # Should raise error for unsupported DB
        # Note: This test depends on actual connector availability
        # In real implementation, mysql might be supported
        with pytest.raises((ValueError, Exception)):
            await executor_agent.execute(sample_input)

    def test_parses_json_correction_from_code_fence(self, executor_agent):
        """Parse SQL from fenced JSON correction payloads."""
        content = '```json\n{ "sql": "SELECT COUNT(*) FROM public.orders", "confidence": 0.8 }\n```'

        parsed = executor_agent._parse_correction_response(content)

        assert parsed == "SELECT COUNT(*) FROM public.orders"

    def test_extract_table_name_from_sql_handles_bigquery_backticks_with_hyphen(
        self, executor_agent
    ):
        sql = "SELECT * FROM `my-project.dataset.table` LIMIT 5"
        extracted = executor_agent._extract_table_name_from_sql(sql)
        assert extracted == "my-project.dataset.table"

    def test_deterministic_summary_uses_full_bigquery_table_name(self, executor_agent):
        query_result = ConnectorQueryResult(
            rows=[{"id": 1, "amount": 100}],
            row_count=1,
            columns=["id", "amount"],
            execution_time_ms=1.0,
        )
        summary = executor_agent._generate_deterministic_summary(
            "show one row",
            "SELECT * FROM `my-project.dataset.table` LIMIT 1",
            query_result,
        )
        assert summary is not None
        answer, _insights = summary
        assert "my-project.dataset.table" in answer

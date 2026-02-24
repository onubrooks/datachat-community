"""
Unit tests for ValidatorAgent.

Tests SQL validation including:
- Syntax validation
- Security checks (SQL injection)
- Schema validation
- Performance warnings
- Strict mode behavior
"""

import pytest

from backend.agents.validator import ValidatorAgent
from backend.models import GeneratedSQL, ValidatorAgentInput


class TestValidatorAgent:
    """Test suite for ValidatorAgent."""

    @pytest.fixture
    def validator_agent(self):
        """Create ValidatorAgent instance."""
        return ValidatorAgent()

    @pytest.fixture
    def sample_generated_sql(self):
        """Sample generated SQL for testing."""
        return GeneratedSQL(
            sql="SELECT customer_id, SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-01-01' GROUP BY customer_id LIMIT 100",
            explanation="Total sales by customer in 2024",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=["Using calendar year 2024"],
            clarifying_questions=[],
        )

    @pytest.fixture
    def sample_input(self, sample_generated_sql):
        """Sample ValidatorAgentInput."""
        return ValidatorAgentInput(
            query="What were total sales by customer in 2024?",
            generated_sql=sample_generated_sql,
            target_database="postgresql",
            strict_mode=False,
        )

    # ============================================================================
    # Valid SQL Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_valid_sql_passes(self, validator_agent, sample_input):
        """Test that valid SQL passes all checks."""
        result = await validator_agent.execute(sample_input)

        assert result.success is True
        assert result.validated_sql.is_valid is True
        assert result.validated_sql.is_safe is True
        assert len(result.validated_sql.errors) == 0
        assert result.validated_sql.performance_score > 0.8

    @pytest.mark.asyncio
    async def test_cte_query_passes(self, validator_agent, sample_input):
        """Test that CTE queries pass validation."""
        cte_sql = GeneratedSQL(
            sql="""WITH sales AS (
                SELECT customer_id, amount, date
                FROM analytics.fact_sales
                WHERE date >= '2024-01-01'
            )
            SELECT customer_id, SUM(amount)
            FROM sales
            GROUP BY customer_id
            LIMIT 100""",
            explanation="Sales with CTE",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = cte_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True
        assert len(result.validated_sql.errors) == 0

    @pytest.mark.asyncio
    async def test_multiple_ctes_pass(self, validator_agent, sample_input):
        """Test that multiple CTEs pass validation."""
        multi_cte_sql = GeneratedSQL(
            sql="""WITH
                sales AS (SELECT customer_id, amount FROM analytics.fact_sales),
                filtered AS (SELECT customer_id, amount FROM sales WHERE amount > 100)
            SELECT customer_id, SUM(amount) FROM filtered GROUP BY customer_id""",
            explanation="Multiple CTEs",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = multi_cte_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True
        assert len(result.validated_sql.errors) == 0

    # ============================================================================
    # Syntax Error Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_syntax_error_detected(self, validator_agent, sample_input):
        """Test that multiple statements are detected as syntax error."""
        # Using multiple statements which is caught by our validation
        invalid_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales; DROP TABLE analytics.fact_sales;",
            explanation="Multiple statements",
            used_datapoints=[],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = invalid_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is False
        assert len(result.validated_sql.errors) > 0
        # Should have both syntax error (multiple statements) and security error (DROP)
        assert any(e.error_type in ["syntax", "security"] for e in result.validated_sql.errors)

    @pytest.mark.asyncio
    async def test_non_select_rejected(self, validator_agent, sample_input):
        """Test that non-SELECT statements are rejected."""
        delete_sql = GeneratedSQL(
            sql="DELETE FROM analytics.fact_sales WHERE date < '2020-01-01'",
            explanation="Delete old data",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = delete_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is False
        assert any(
            e.error_type == "syntax" and "SELECT" in e.message for e in result.validated_sql.errors
        )

    @pytest.mark.asyncio
    async def test_multiple_statements_rejected(self, validator_agent, sample_input):
        """Test that multiple statements are rejected."""
        multi_stmt = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales; SELECT * FROM dim_customer;",
            explanation="Multiple queries",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = multi_stmt

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is False
        assert any(
            e.error_type == "syntax" and "Multiple" in e.message
            for e in result.validated_sql.errors
        )

    # ============================================================================
    # Security Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_sql_injection_drop_detected(self, validator_agent, sample_input):
        """Test that DROP table injection is detected."""
        injection_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales; DROP TABLE analytics.fact_sales;",
            explanation="Malicious query",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = injection_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_safe is False
        assert any(e.error_type == "security" for e in result.validated_sql.errors)

    @pytest.mark.asyncio
    async def test_sql_injection_union_detected(self, validator_agent, sample_input):
        """Test that UNION-based injection is detected."""
        union_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales UNION SELECT * FROM admin_users",
            explanation="Union injection",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = union_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_safe is False
        assert any(
            e.error_type == "security" and "injection" in e.message.lower()
            for e in result.validated_sql.errors
        )

    @pytest.mark.asyncio
    async def test_sql_injection_always_true_detected(self, validator_agent, sample_input):
        """Test that always-true conditions are detected."""
        always_true_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales WHERE customer_id = 123 OR 1=1",
            explanation="Always true injection",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = always_true_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_safe is False
        assert any(e.error_type == "security" for e in result.validated_sql.errors)

    @pytest.mark.asyncio
    async def test_dangerous_functions_detected(self, validator_agent, sample_input):
        """Test that dangerous functions are detected."""
        dangerous_sql = GeneratedSQL(
            sql="SELECT LOAD_FILE('/etc/passwd') FROM analytics.fact_sales",
            explanation="File read attempt",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = dangerous_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_safe is False
        assert any(
            e.error_type == "security" and "LOAD_FILE" in e.message
            for e in result.validated_sql.errors
        )

    @pytest.mark.asyncio
    async def test_cte_with_delete_rejected(self, validator_agent, sample_input):
        """Test that CTE followed by DELETE is rejected (security issue)."""
        cte_delete_sql = GeneratedSQL(
            sql="WITH t AS (SELECT id FROM analytics.fact_sales WHERE amount > 1000) DELETE FROM analytics.fact_sales WHERE id IN (SELECT id FROM t)",
            explanation="CTE with DELETE - should be blocked",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = cte_delete_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is False
        assert result.validated_sql.is_safe is False
        assert any(
            e.error_type == "security" and "DELETE" in e.message
            for e in result.validated_sql.errors
        )

    @pytest.mark.asyncio
    async def test_cte_with_update_rejected(self, validator_agent, sample_input):
        """Test that CTE followed by UPDATE is rejected (security issue)."""
        cte_update_sql = GeneratedSQL(
            sql="WITH t AS (SELECT 1) UPDATE analytics.fact_sales SET amount = 0",
            explanation="CTE with UPDATE - should be blocked",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = cte_update_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is False
        assert result.validated_sql.is_safe is False
        assert any(
            e.error_type == "security" and "UPDATE" in e.message
            for e in result.validated_sql.errors
        )

    @pytest.mark.asyncio
    async def test_cte_with_insert_rejected(self, validator_agent, sample_input):
        """Test that CTE followed by INSERT is rejected (security issue)."""
        cte_insert_sql = GeneratedSQL(
            sql="WITH t AS (SELECT 1) INSERT INTO analytics.fact_sales (amount) VALUES (999)",
            explanation="CTE with INSERT - should be blocked",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.5,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = cte_insert_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is False
        assert result.validated_sql.is_safe is False
        assert any(
            e.error_type == "security" and "INSERT" in e.message
            for e in result.validated_sql.errors
        )

    # ============================================================================
    # Performance Warning Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_select_star_warning(self, validator_agent, sample_input):
        """Test that SELECT * generates performance warning."""
        select_star_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales WHERE date >= '2024-01-01'",
            explanation="All columns",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = select_star_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True  # Warning, not error
        assert any(
            w.warning_type == "performance" and "SELECT *" in w.message
            for w in result.validated_sql.warnings
        )
        assert result.validated_sql.performance_score < 1.0

    @pytest.mark.asyncio
    async def test_missing_where_warning(self, validator_agent, sample_input):
        """Test that missing WHERE clause generates warning."""
        no_where_sql = GeneratedSQL(
            sql="SELECT customer_id, amount FROM analytics.fact_sales",
            explanation="All sales",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = no_where_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True
        assert any(
            w.warning_type == "performance" and "WHERE" in w.message
            for w in result.validated_sql.warnings
        )

    @pytest.mark.asyncio
    async def test_missing_limit_warning(self, validator_agent, sample_input):
        """Test that missing LIMIT generates warning."""
        no_limit_sql = GeneratedSQL(
            sql="SELECT customer_id, amount FROM analytics.fact_sales WHERE date >= '2024-01-01'",
            explanation="Sales in 2024",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = no_limit_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True
        assert any(
            w.warning_type == "performance" and "LIMIT" in w.message
            for w in result.validated_sql.warnings
        )

    @pytest.mark.asyncio
    async def test_many_joins_warning(self, validator_agent, sample_input):
        """Test that many JOINs generate performance warning."""
        many_joins_sql = GeneratedSQL(
            sql="""SELECT s.amount
                FROM analytics.fact_sales s
                JOIN dim_customer c ON s.customer_id = c.id
                JOIN dim_product p ON s.product_id = p.id
                JOIN dim_region r ON s.region_id = r.id
                JOIN dim_date d ON s.date_id = d.id
                JOIN dim_category cat ON p.category_id = cat.id
                WHERE s.date >= '2024-01-01'""",
            explanation="Many joins",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = many_joins_sql

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True
        assert any(
            w.warning_type == "performance" and "JOIN" in w.message
            for w in result.validated_sql.warnings
        )

    # ============================================================================
    # Strict Mode Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_strict_mode_warnings_as_errors(self, validator_agent, sample_input):
        """Test that strict mode treats warnings as errors."""
        select_star_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales WHERE date >= '2024-01-01'",
            explanation="All columns",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.9,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = select_star_sql
        sample_input.strict_mode = True

        result = await validator_agent.execute(sample_input)

        # In strict mode, warnings cause is_valid to be False
        assert result.validated_sql.is_valid is False
        assert len(result.validated_sql.warnings) > 0
        # Warnings are converted to errors in strict mode
        assert any("STRICT MODE" in e.message for e in result.validated_sql.errors)

    # ============================================================================
    # Suggestion Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_suggestions_provided(self, validator_agent, sample_input):
        """Test that actionable suggestions are provided."""
        poor_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales",
            explanation="All sales",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = poor_sql

        result = await validator_agent.execute(sample_input)

        assert len(result.validated_sql.suggestions) > 0
        # Suggestions should be actionable
        for suggestion in result.validated_sql.suggestions:
            assert len(suggestion) > 10  # Non-trivial suggestion
            assert any(
                keyword in suggestion.lower() for keyword in ["add", "replace", "consider", "use"]
            )

    # ============================================================================
    # Database-Specific Tests
    # ============================================================================

    @pytest.mark.asyncio
    async def test_postgresql_specific_validation(self, validator_agent, sample_input):
        """Test PostgreSQL-specific validation."""
        pg_sql = GeneratedSQL(
            sql="SELECT customer_id, SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-01-01' GROUP BY customer_id LIMIT 100 OFFSET 50",
            explanation="PostgreSQL pagination",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = pg_sql
        sample_input.target_database = "postgresql"

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True

    @pytest.mark.asyncio
    async def test_clickhouse_specific_validation(self, validator_agent, sample_input):
        """Test ClickHouse-specific validation."""
        ch_sql = GeneratedSQL(
            sql="SELECT customer_id, SUM(amount) FROM analytics.fact_sales FINAL WHERE date >= '2024-01-01' GROUP BY customer_id",
            explanation="ClickHouse with FINAL",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = ch_sql
        sample_input.target_database = "clickhouse"

        result = await validator_agent.execute(sample_input)

        assert result.validated_sql.is_valid is True

    # ============================================================================
    # Edge Cases
    # ============================================================================

    @pytest.mark.asyncio
    async def test_empty_sql_rejected(self, validator_agent, sample_input):
        """Test that empty SQL is rejected."""
        empty_sql = GeneratedSQL(
            sql="   ",
            explanation="Empty",
            used_datapoints=[],
            confidence=0.0,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = empty_sql

        with pytest.raises(ValueError, match="SQL cannot be empty"):
            await validator_agent.execute(sample_input)

    @pytest.mark.asyncio
    async def test_aggregation_without_where_allowed(self, validator_agent, sample_input):
        """Test that aggregations without WHERE are allowed (no false positives)."""
        count_sql = GeneratedSQL(
            sql="SELECT COUNT(*) FROM analytics.fact_sales",
            explanation="Total count",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = count_sql

        result = await validator_agent.execute(sample_input)

        # COUNT(*) without WHERE is valid - should not have WHERE warning
        assert result.validated_sql.is_valid is True
        where_warnings = [w for w in result.validated_sql.warnings if "WHERE" in w.message]
        assert len(where_warnings) == 0  # COUNT(*) is exempt

    @pytest.mark.asyncio
    async def test_metadata_populated(self, validator_agent, sample_input):
        """Test that metadata is properly populated."""
        result = await validator_agent.execute(sample_input)

        assert result.metadata is not None
        assert result.metadata.agent_name == "ValidatorAgent"
        assert result.metadata.started_at is not None

    @pytest.mark.asyncio
    async def test_performance_score_calculation(self, validator_agent, sample_input):
        """Test that performance score is calculated correctly."""
        # Perfect query
        perfect_sql = GeneratedSQL(
            sql="SELECT customer_id, SUM(amount) FROM analytics.fact_sales WHERE date >= '2024-01-01' GROUP BY customer_id LIMIT 100",
            explanation="Optimized query",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.95,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = perfect_sql

        result = await validator_agent.execute(sample_input)
        perfect_score = result.validated_sql.performance_score

        # Poor query
        poor_sql = GeneratedSQL(
            sql="SELECT * FROM analytics.fact_sales",
            explanation="Unoptimized query",
            used_datapoints=["table_fact_sales_001"],
            confidence=0.8,
            assumptions=[],
            clarifying_questions=[],
        )
        sample_input.generated_sql = poor_sql

        result = await validator_agent.execute(sample_input)
        poor_score = result.validated_sql.performance_score

        # Perfect should have higher score
        assert perfect_score > poor_score
        # Scores should be in [0, 1]
        assert 0 <= perfect_score <= 1
        assert 0 <= poor_score <= 1

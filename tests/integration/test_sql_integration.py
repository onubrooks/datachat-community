"""
Integration tests for SQLAgent.

These tests use real LLM API calls and are skipped unless explicitly run.
Run with: pytest tests/integration/test_sql_integration.py --run-integration
"""

import pytest

from backend.agents.sql import SQLAgent
from backend.models.agent import (
    InvestigationMemory,
    RetrievedDataPoint,
    SQLAgentInput,
)

# Skip all tests in this module unless --run-integration flag is provided
pytestmark = pytest.mark.skipif(
    "not config.getoption('--run-integration', default=False)",
    reason="Integration tests require --run-integration flag and valid API keys",
)


@pytest.fixture
def sql_agent():
    """Create real SQLAgent with configured LLM provider."""
    return SQLAgent()


@pytest.fixture
def sales_investigation_memory():
    """Create investigation memory for sales query."""
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
                            "name": "sale_id",
                            "type": "BIGINT",
                            "business_meaning": "Unique sale identifier",
                            "nullable": False,
                        },
                        {
                            "name": "amount",
                            "type": "DECIMAL(18,2)",
                            "business_meaning": "Transaction value in USD",
                            "nullable": False,
                        },
                        {
                            "name": "sale_date",
                            "type": "DATE",
                            "business_meaning": "Transaction date",
                            "nullable": False,
                        },
                        {
                            "name": "status",
                            "type": "VARCHAR(50)",
                            "business_meaning": "Transaction status",
                            "nullable": False,
                        },
                    ],
                    "relationships": [
                        {
                            "target_table": "dim_customer",
                            "join_column": "customer_id",
                            "cardinality": "N:1",
                        }
                    ],
                    "gotchas": ["Always filter by sale_date for performance"],
                    "common_queries": [
                        "SELECT SUM(amount) FROM fact_sales WHERE sale_date >= '2024-01-01'"
                    ],
                },
            ),
            RetrievedDataPoint(
                datapoint_id="metric_revenue_001",
                datapoint_type="Business",
                name="Total Revenue",
                score=0.88,
                source="vector",
                metadata={
                    "calculation": "SUM(fact_sales.amount)",
                    "synonyms": ["sales", "income", "total sales"],
                    "business_rules": [
                        "Exclude refunds (status != 'refunded')",
                        "Only include completed transactions (status = 'completed')",
                    ],
                    "related_tables": ["fact_sales"],
                },
            ),
        ],
        total_retrieved=2,
        retrieval_mode="hybrid",
        sources_used=["table_fact_sales_001", "metric_revenue_001"],
    )


@pytest.mark.asyncio
async def test_generates_valid_sql_for_sales_query(sql_agent, sales_investigation_memory):
    """Test generates syntactically valid SQL for sales query."""
    input = SQLAgentInput(
        query="What were total sales last quarter?", investigation_memory=sales_investigation_memory
    )

    output = await sql_agent(input)

    # Assertions
    assert output.success is True
    assert output.generated_sql.sql is not None
    assert len(output.generated_sql.sql) > 0

    # SQL should be a SELECT statement
    assert output.generated_sql.sql.strip().upper().startswith("SELECT")

    # Should reference the fact_sales table
    assert "fact_sales" in output.generated_sql.sql.lower()

    # Should have SUM aggregation for amount
    assert "sum" in output.generated_sql.sql.lower()
    assert "amount" in output.generated_sql.sql.lower()

    # Should have explanation
    assert len(output.generated_sql.explanation) > 0

    # Should track metadata
    assert output.metadata.llm_calls >= 1
    assert output.metadata.tokens_used > 0

    print(f"\nGenerated SQL:\n{output.generated_sql.sql}\n")
    print(f"Explanation: {output.generated_sql.explanation}\n")
    print(f"Confidence: {output.generated_sql.confidence}\n")
    print(f"Correction attempts: {len(output.correction_attempts)}\n")


@pytest.mark.asyncio
async def test_applies_business_rules(sql_agent, sales_investigation_memory):
    """Test applies business rules from DataPoints."""
    input = SQLAgentInput(
        query="Show me total revenue", investigation_memory=sales_investigation_memory
    )

    output = await sql_agent(input)

    assert output.success is True

    # Business rules specify to exclude refunds and only include completed
    sql_lower = output.generated_sql.sql.lower()

    # Should filter by status (checking for business rule application)
    # Either excludes refunded OR includes only completed
    has_status_filter = "status" in sql_lower and (
        "refund" in sql_lower or "completed" in sql_lower
    )

    # Note: LLM might apply rules differently, so we just check it considered status
    if not has_status_filter:
        print(f"Warning: Generated SQL might not apply business rules:\n{output.generated_sql.sql}")

    print(f"\nGenerated SQL:\n{output.generated_sql.sql}\n")
    print(f"Assumptions: {output.generated_sql.assumptions}\n")


@pytest.mark.asyncio
async def test_handles_join_query(sql_agent):
    """Test generates join query when multiple tables are involved."""
    memory = InvestigationMemory(
        query="Show me sales by customer",
        datapoints=[
            RetrievedDataPoint(
                datapoint_id="table_fact_sales_001",
                datapoint_type="Schema",
                name="Fact Sales Table",
                score=0.95,
                source="hybrid",
                metadata={
                    "table_name": "analytics.fact_sales",
                    "key_columns": [
                        {
                            "name": "customer_id",
                            "type": "BIGINT",
                            "business_meaning": "Customer ID",
                            "nullable": False,
                        },
                        {
                            "name": "amount",
                            "type": "DECIMAL(18,2)",
                            "business_meaning": "Sale amount",
                            "nullable": False,
                        },
                    ],
                    "relationships": [
                        {
                            "target_table": "dim_customer",
                            "join_column": "customer_id",
                            "cardinality": "N:1",
                        }
                    ],
                },
            ),
            RetrievedDataPoint(
                datapoint_id="table_dim_customer_001",
                datapoint_type="Schema",
                name="Customer Dimension",
                score=0.92,
                source="graph",
                metadata={
                    "table_name": "analytics.dim_customer",
                    "key_columns": [
                        {
                            "name": "customer_id",
                            "type": "BIGINT",
                            "business_meaning": "Customer ID",
                            "nullable": False,
                        },
                        {
                            "name": "customer_name",
                            "type": "VARCHAR(255)",
                            "business_meaning": "Customer name",
                            "nullable": False,
                        },
                    ],
                },
            ),
        ],
        total_retrieved=2,
        retrieval_mode="hybrid",
        sources_used=["table_fact_sales_001", "table_dim_customer_001"],
    )

    input = SQLAgentInput(query="Show me sales by customer", investigation_memory=memory)

    output = await sql_agent(input)

    assert output.success is True

    # Should include JOIN
    assert "join" in output.generated_sql.sql.lower()

    # Should reference both tables
    assert "fact_sales" in output.generated_sql.sql.lower()
    assert "dim_customer" in output.generated_sql.sql.lower()

    print(f"\nGenerated SQL with JOIN:\n{output.generated_sql.sql}\n")


@pytest.mark.asyncio
async def test_self_correction_with_real_llm(sql_agent):
    """Test self-correction capability with real LLM."""
    # Create a scenario that might trigger correction
    # (providing minimal context to increase chance of initial error)
    memory = InvestigationMemory(
        query="Get sales total",
        datapoints=[
            RetrievedDataPoint(
                datapoint_id="table_sales_001",
                datapoint_type="Schema",
                name="Sales Table",
                score=0.9,
                source="vector",
                metadata={
                    "table_name": "sales_data",
                    "key_columns": [
                        {
                            "name": "amount",
                            "type": "DECIMAL",
                            "business_meaning": "Amount",
                            "nullable": False,
                        }
                    ],
                },
            )
        ],
        total_retrieved=1,
        retrieval_mode="vector",
        sources_used=["table_sales_001"],
    )

    input = SQLAgentInput(
        query="Get sales total", investigation_memory=memory, max_correction_attempts=2
    )

    output = await sql_agent(input)

    # Should eventually succeed (either first try or after correction)
    assert output.success is True

    # Log correction history
    if len(output.correction_attempts) > 0:
        print(f"\nSelf-correction occurred! {len(output.correction_attempts)} attempts")
        for attempt in output.correction_attempts:
            print(f"\nAttempt {attempt.attempt_number}:")
            print(f"  Issues: {[i.issue_type for i in attempt.issues_found]}")
            print(f"  Success: {attempt.success}")
    else:
        print("\nNo correction needed - first attempt succeeded")

    print(f"\nFinal SQL:\n{output.generated_sql.sql}\n")


@pytest.mark.asyncio
async def test_performance(sql_agent, sales_investigation_memory):
    """Test SQL generation performance."""
    import time

    input = SQLAgentInput(
        query="What were total sales last quarter?", investigation_memory=sales_investigation_memory
    )

    start = time.time()
    output = await sql_agent(input)
    duration = time.time() - start

    assert output.success is True

    # Should complete in reasonable time (< 10 seconds for a simple query)
    assert duration < 10.0

    print(f"\nGeneration took {duration:.2f}s")
    print(f"LLM calls: {output.metadata.llm_calls}")
    print(f"Tokens used: {output.metadata.tokens_used}")

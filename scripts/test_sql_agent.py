#!/usr/bin/env python3
"""
Manual test script for SQLAgent.

This script allows you to test the SQLAgent with sample data to see
SQL generation and self-correction in action.

Requirements:
- Valid OpenAI API key in environment or .env file
- Run from project root: python scripts/test_sql_agent.py
"""

import asyncio
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.agents.sql import SQLAgent
from backend.models.agent import (
    InvestigationMemory,
    RetrievedDataPoint,
    SQLAgentInput,
)

# Sample DataPoints that mimic what ContextAgent would provide
SAMPLE_DATAPOINTS = [
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
                    "business_meaning": "Transaction status (completed, pending, refunded)",
                    "nullable": False,
                },
                {
                    "name": "customer_id",
                    "type": "BIGINT",
                    "business_meaning": "Customer identifier",
                    "nullable": False,
                },
            ],
            "relationships": [
                {"target_table": "dim_customer", "join_column": "customer_id", "cardinality": "N:1"}
            ],
            "gotchas": ["Always filter by sale_date for performance"],
            "common_queries": [
                "SELECT SUM(amount) FROM fact_sales WHERE sale_date >= '2024-01-01'"
            ],
        },
    ),
    RetrievedDataPoint(
        datapoint_id="table_dim_customer_001",
        datapoint_type="Schema",
        name="Customer Dimension",
        score=0.90,
        source="graph",
        metadata={
            "table_name": "analytics.dim_customer",
            "schema": "analytics",
            "business_purpose": "Customer master data",
            "key_columns": [
                {
                    "name": "customer_id",
                    "type": "BIGINT",
                    "business_meaning": "Unique customer identifier",
                    "nullable": False,
                },
                {
                    "name": "customer_name",
                    "type": "VARCHAR(255)",
                    "business_meaning": "Customer full name",
                    "nullable": False,
                },
                {
                    "name": "region",
                    "type": "VARCHAR(100)",
                    "business_meaning": "Customer region",
                    "nullable": True,
                },
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
            "synonyms": ["sales", "income", "total sales", "earnings"],
            "business_rules": [
                "Exclude refunds (status != 'refunded')",
                "Only include completed transactions (status = 'completed')",
            ],
            "related_tables": ["fact_sales"],
        },
    ),
]


async def test_query(query: str):
    """Test a single query."""
    print(f"\n{'=' * 80}")
    print(f"QUERY: {query}")
    print(f"{'=' * 80}\n")

    # Create investigation memory (what ContextAgent would provide)
    memory = InvestigationMemory(
        query=query,
        datapoints=SAMPLE_DATAPOINTS,
        total_retrieved=len(SAMPLE_DATAPOINTS),
        retrieval_mode="hybrid",
        sources_used=[dp.datapoint_id for dp in SAMPLE_DATAPOINTS],
    )

    # Create input for SQLAgent
    input_data = SQLAgentInput(query=query, investigation_memory=memory, max_correction_attempts=3)

    # Initialize SQLAgent
    print("🤖 Initializing SQLAgent...")
    agent = SQLAgent()

    # Generate SQL
    print("🔄 Generating SQL...\n")
    output = await agent(input_data)

    # Display results
    print("✅ SUCCESS!\n")
    print("📊 GENERATED SQL:")
    print(f"{'-' * 80}")
    print(output.generated_sql.sql)
    print(f"{'-' * 80}\n")

    print("📝 EXPLANATION:")
    print(f"   {output.generated_sql.explanation}\n")

    print(f"🎯 CONFIDENCE: {output.generated_sql.confidence:.2%}\n")

    if output.generated_sql.used_datapoints:
        print("📚 USED DATAPOINTS:")
        for dp_id in output.generated_sql.used_datapoints:
            print(f"   - {dp_id}")
        print()

    if output.generated_sql.assumptions:
        print("💭 ASSUMPTIONS:")
        for assumption in output.generated_sql.assumptions:
            print(f"   - {assumption}")
        print()

    if output.generated_sql.clarifying_questions:
        print("❓ CLARIFYING QUESTIONS:")
        for question in output.generated_sql.clarifying_questions:
            print(f"   - {question}")
        print()

    if output.correction_attempts:
        print(f"🔧 SELF-CORRECTION ATTEMPTS: {len(output.correction_attempts)}")
        for attempt in output.correction_attempts:
            print(f"\n   Attempt #{attempt.attempt_number}:")
            print(f"   Original SQL: {attempt.original_sql[:100]}...")
            print(f"   Issues Found: {len(attempt.issues_found)}")
            for issue in attempt.issues_found:
                print(f"      - {issue.issue_type}: {issue.message}")
            print(f"   Corrected: {'✅ Yes' if attempt.success else '❌ No'}")
        print()

    print("⏱️  METADATA:")
    print(f"   - LLM Calls: {output.metadata.llm_calls}")
    print(f"   - Tokens Used: {output.metadata.tokens_used}")
    print(f"   - Duration: {output.metadata.duration_ms:.2f}ms")
    print()


async def main():
    """Run manual tests."""
    print("\n" + "=" * 80)
    print("SQLAgent Manual Testing")
    print("=" * 80)

    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("\n❌ ERROR: OPENAI_API_KEY not found in environment!")
        print("   Please set it in your .env file or environment variables.")
        print("   Example: export OPENAI_API_KEY='sk-...'")
        sys.exit(1)

    # Test queries
    queries = [
        # Simple aggregation
        "What were total sales last quarter?",
        # Query with join
        "Show me sales by customer name",
        # Query that should apply business rules
        "What is our total revenue?",
        # More complex query
        "Which customers had the highest sales in 2024?",
        # Query that might use CTE
        "Calculate average sales per customer",
    ]

    for query in queries:
        try:
            await test_query(query)
        except Exception as e:
            print(f"\n❌ ERROR: {e}\n")
            import traceback

            traceback.print_exc()

        # Pause between queries
        print("\nPress Enter to continue to next query (or Ctrl+C to exit)...")
        input()

    print("\n" + "=" * 80)
    print("✅ All tests complete!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())

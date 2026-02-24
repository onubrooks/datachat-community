#!/usr/bin/env python3
"""
Interactive SQLAgent Demo

A simple interactive demo where you can type queries and see the generated SQL.
Press Ctrl+C to exit.
"""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.agents.sql import SQLAgent
from backend.models.agent import InvestigationMemory, RetrievedDataPoint, SQLAgentInput

# Simple sample data
SAMPLE_CONTEXT = InvestigationMemory(
    query="",
    datapoints=[
        RetrievedDataPoint(
            datapoint_id="table_sales_001",
            datapoint_type="Schema",
            name="Sales Table",
            score=0.95,
            source="hybrid",
            metadata={
                "table_name": "analytics.fact_sales",
                "key_columns": [
                    {
                        "name": "amount",
                        "type": "DECIMAL",
                        "business_meaning": "Sale amount",
                        "nullable": False,
                    },
                    {
                        "name": "sale_date",
                        "type": "DATE",
                        "business_meaning": "Date of sale",
                        "nullable": False,
                    },
                    {
                        "name": "customer_id",
                        "type": "BIGINT",
                        "business_meaning": "Customer ID",
                        "nullable": False,
                    },
                ],
            },
        ),
        RetrievedDataPoint(
            datapoint_id="metric_revenue_001",
            datapoint_type="Business",
            name="Revenue",
            score=0.90,
            source="vector",
            metadata={
                "calculation": "SUM(amount)",
                "business_rules": ["Exclude refunded transactions"],
            },
        ),
    ],
    total_retrieved=2,
    retrieval_mode="hybrid",
    sources_used=["table_sales_001", "metric_revenue_001"],
)


async def main():
    print("\n" + "=" * 80)
    print("SQLAgent Interactive Demo")
    print("=" * 80)
    print("\nAvailable sample data:")
    print("  - Table: analytics.fact_sales (amount, sale_date, customer_id)")
    print("  - Metric: Revenue (SUM of amounts, excludes refunds)")
    print("\nType your queries and see the generated SQL!")
    print("Press Ctrl+C to exit.\n")
    print("=" * 80 + "\n")

    # Check API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ ERROR: OPENAI_API_KEY not found!")
        print("Set it: export OPENAI_API_KEY='sk-...'")
        sys.exit(1)

    # Initialize agent
    agent = SQLAgent()
    print("✅ SQLAgent initialized\n")

    while True:
        try:
            # Get query from user
            query = input("💬 Your query: ").strip()
            if not query:
                continue

            print("\n🔄 Generating SQL...\n")

            # Create input
            input_data = SQLAgentInput(query=query, investigation_memory=SAMPLE_CONTEXT)

            # Generate
            output = await agent(input_data)

            # Display
            print("📊 Generated SQL:")
            print("-" * 80)
            print(output.generated_sql.sql)
            print("-" * 80)
            print(f"\n📝 {output.generated_sql.explanation}")
            print(f"🎯 Confidence: {output.generated_sql.confidence:.0%}")

            if output.correction_attempts:
                print(f"🔧 Corrections: {len(output.correction_attempts)}")

            print(
                f"⏱️  Time: {output.metadata.duration_ms:.0f}ms | Tokens: {output.metadata.tokens_used}\n"
            )

        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!\n")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}\n")


if __name__ == "__main__":
    asyncio.run(main())

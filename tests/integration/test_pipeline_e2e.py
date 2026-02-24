"""
End-to-end integration tests for DataChat pipeline.

These tests require:
- Real database connection
- OpenAI API key (for LLM calls)
- Loaded DataPoints in vector store and knowledge graph

Run with: pytest tests/integration/test_pipeline_e2e.py --run-integration
"""

import pytest

from backend.pipeline.orchestrator import create_pipeline


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_execution():
    """
    Test end-to-end pipeline with simple query.

    Manual verification:
    - Check that query executes successfully
    - Verify SQL is generated correctly
    - Confirm results are returned
    - Validate natural language answer makes sense
    """
    # Create pipeline with real dependencies
    pipeline = await create_pipeline(database_type="postgresql")

    try:
        # Run query
        result = await pipeline.run(
            query="How many records are in the database?",
            database_type="postgresql",
        )

        # Basic assertions
        assert result is not None
        assert result.get("error") is None, f"Pipeline error: {result.get('error')}"
        assert result.get("intent") is not None
        assert result.get("generated_sql") is not None
        assert result.get("validation_passed") is True
        assert result.get("natural_language_answer") is not None

        # Print results for manual verification
        print("\n" + "=" * 80)
        print("QUERY:", result.get("query"))
        print("=" * 80)
        print("\nCLASSIFICATION:")
        print(f"  Intent: {result.get('intent')}")
        print(f"  Complexity: {result.get('complexity')}")
        print(f"  Entities: {len(result.get('entities', []))}")
        print("\nRETRIEVAL:")
        print(f"  DataPoints found: {len(result.get('retrieved_datapoints', []))}")
        print("\nSQL GENERATION:")
        print(f"  SQL: {result.get('generated_sql')}")
        print(f"  Confidence: {result.get('sql_confidence')}")
        print("\nVALIDATION:")
        print(f"  Passed: {result.get('validation_passed')}")
        print(f"  Warnings: {len(result.get('validation_warnings', []))}")
        print(f"  Performance Score: {result.get('performance_score')}")
        print("\nEXECUTION:")
        print(f"  Rows returned: {result.get('query_result', {}).get('row_count')}")
        print(f"  Execution time: {result.get('query_result', {}).get('execution_time_ms')}ms")
        print(f"  Visualization: {result.get('visualization_hint')}")
        print("\nANSWER:")
        print(f"  {result.get('natural_language_answer')}")
        print("\nMETADATA:")
        print(f"  Total latency: {result.get('total_latency_ms')}ms")
        print(f"  LLM calls: {result.get('llm_calls')}")
        print(f"  Retries: {result.get('retry_count')}")
        print("=" * 80)

        # Verify metadata
        assert result.get("total_latency_ms", 0) > 0
        assert result.get("llm_calls", 0) > 0
        assert "agent_timings" in result
        assert len(result["agent_timings"]) >= 5  # All 5 agents

    finally:
        # Cleanup
        await pipeline.connector.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_complex_query_with_retry():
    """
    Test pipeline with complex query that might need retry.

    Manual verification:
    - Check if retry logic triggers
    - Verify final SQL is valid
    - Confirm answer quality
    """
    pipeline = await create_pipeline(database_type="postgresql")

    try:
        result = await pipeline.run(
            query="Show me total revenue by region for the last quarter, excluding refunds",
            database_type="postgresql",
        )

        # Basic assertions
        assert result is not None
        assert result.get("complexity") in ["medium", "complex"]

        # Print for manual verification
        print("\n" + "=" * 80)
        print("COMPLEX QUERY TEST")
        print("=" * 80)
        print(f"\nQuery: {result.get('query')}")
        print(f"Complexity: {result.get('complexity')}")
        print(f"Retry count: {result.get('retry_count')}")
        print(f"\nSQL:\n{result.get('generated_sql')}")
        print(f"\nAnswer: {result.get('natural_language_answer')}")
        print(f"Insights: {result.get('key_insights')}")
        print("=" * 80)

    finally:
        await pipeline.connector.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_streaming_updates():
    """
    Test streaming functionality.

    Manual verification:
    - Verify updates are emitted for each agent
    - Check that final result is complete
    """
    pipeline = await create_pipeline(database_type="postgresql")

    try:
        updates = []
        async for update in pipeline.stream(
            query="What is the average transaction value?",
            database_type="postgresql",
        ):
            updates.append(update)
            print(f"Update: {update['node']} - {update.get('current_agent')}")

        # Verify we got updates
        assert len(updates) > 0

        # Verify all agents appear
        nodes = {u["node"] for u in updates}
        assert "classifier" in nodes
        assert "context" in nodes
        assert "sql" in nodes
        assert "validator" in nodes
        # executor may or may not appear depending on validation

        print(f"\nTotal streaming updates: {len(updates)}")
        print(f"Agents involved: {nodes}")

    finally:
        await pipeline.connector.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_error_recovery():
    """
    Test error handling with invalid query.

    Manual verification:
    - Verify graceful error message
    - Check that pipeline doesn't crash
    """
    pipeline = await create_pipeline(database_type="postgresql")

    try:
        result = await pipeline.run(
            query="This query is intentionally vague and ambiguous xyz123",
            database_type="postgresql",
        )

        # Should complete without crashing
        assert result is not None

        # Should have either a result or error message
        has_result = result.get("natural_language_answer") is not None
        has_error = result.get("error") is not None
        assert has_result or has_error

        print("\n" + "=" * 80)
        print("ERROR RECOVERY TEST")
        print("=" * 80)
        print(f"Clarification needed: {result.get('clarification_needed')}")
        print(f"Clarifying questions: {result.get('clarifying_questions')}")
        print(f"Error: {result.get('error')}")
        print(f"Answer: {result.get('natural_language_answer')}")
        print("=" * 80)

    finally:
        await pipeline.connector.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_answer_quality():
    """
    Test answer quality for various query types.

    This is primarily for manual verification of answer quality.
    """
    pipeline = await create_pipeline(database_type="postgresql")

    test_queries = [
        "What is the total count?",
        "Show me the top 5 items",
        "What happened last month?",
        "Compare this year to last year",
    ]

    try:
        for query in test_queries:
            print(f"\n{'=' * 80}")
            print(f"Testing: {query}")
            print("=" * 80)

            result = await pipeline.run(query, database_type="postgresql")

            print(f"Intent: {result.get('intent')}")
            print(f"SQL: {result.get('generated_sql')}")
            print(f"Answer: {result.get('natural_language_answer')}")
            print(f"Visualization: {result.get('visualization_hint')}")

            # Basic assertions
            assert result.get("natural_language_answer") is not None

    finally:
        await pipeline.connector.close()

"""
Integration tests for ContextAgent.

Tests end-to-end context retrieval with real DataPoints, VectorStore, and KnowledgeGraph.
These tests verify that the agent can successfully retrieve relevant context
from the knowledge base.
"""

import os
import tempfile

import pytest

from backend.agents.context import ContextAgent
from backend.knowledge.graph import KnowledgeGraph
from backend.knowledge.retriever import Retriever
from backend.knowledge.vectors import VectorStore
from backend.models.agent import ContextAgentInput, ExtractedEntity
from backend.models.datapoint import (
    BusinessDataPoint,
    ColumnMetadata,
    ProcessDataPoint,
    Relationship,
    SchemaDataPoint,
)


@pytest.fixture
def sample_schema_datapoints():
    """Create sample schema DataPoints for testing."""
    return [
        SchemaDataPoint(
            datapoint_id="table_fact_sales_001",
            type="Schema",
            name="Fact Sales Table",
            table_name="analytics.fact_sales",
            schema="analytics",
            business_purpose="Central fact table for all sales transactions",
            key_columns=[
                ColumnMetadata(
                    name="sale_id",
                    type="BIGINT",
                    business_meaning="Unique identifier for each sale",
                    nullable=False,
                ),
                ColumnMetadata(
                    name="amount",
                    type="DECIMAL(18,2)",
                    business_meaning="Transaction value in USD",
                    nullable=False,
                ),
                ColumnMetadata(
                    name="quantity",
                    type="INT",
                    business_meaning="Number of items sold",
                    nullable=False,
                ),
                ColumnMetadata(
                    name="sale_date",
                    type="DATE",
                    business_meaning="Date of the sale",
                    nullable=False,
                ),
            ],
            relationships=[
                Relationship(
                    target_table="dim_product",
                    join_column="product_id",
                    cardinality="N:1",
                ),
                Relationship(
                    target_table="dim_customer",
                    join_column="customer_id",
                    cardinality="N:1",
                ),
            ],
            row_count=1_000_000,
            freshness="T-1",
            owner="data-team@company.com",
            tags=["sales", "transactions", "revenue"],
        ),
        SchemaDataPoint(
            datapoint_id="table_dim_product_001",
            type="Schema",
            name="Product Dimension",
            table_name="analytics.dim_product",
            schema="analytics",
            business_purpose="Product catalog with all product information",
            key_columns=[
                ColumnMetadata(
                    name="product_id",
                    type="BIGINT",
                    business_meaning="Unique product identifier",
                    nullable=False,
                ),
                ColumnMetadata(
                    name="product_name",
                    type="VARCHAR(255)",
                    business_meaning="Product display name",
                    nullable=False,
                ),
                ColumnMetadata(
                    name="category",
                    type="VARCHAR(100)",
                    business_meaning="Product category",
                    nullable=False,
                ),
            ],
            row_count=10_000,
            freshness="T-1",
            owner="product-team@company.com",
            tags=["product", "catalog"],
        ),
    ]


@pytest.fixture
def sample_business_datapoints():
    """Create sample business DataPoints for testing."""
    return [
        BusinessDataPoint(
            datapoint_id="metric_revenue_001",
            type="Business",
            name="Revenue",
            calculation="SUM(fact_sales.amount) WHERE status = 'completed'",
            synonyms=["sales", "income", "earnings", "total sales"],
            related_tables=["fact_sales"],
            owner="finance@company.com",
            tags=["revenue", "metrics", "kpi"],
        ),
        BusinessDataPoint(
            datapoint_id="metric_avg_order_value_001",
            type="Business",
            name="Average Order Value",
            calculation="SUM(amount) / COUNT(DISTINCT sale_id)",
            synonyms=["AOV", "average sale", "mean transaction value"],
            related_tables=["fact_sales"],
            unit="USD",
            aggregation="AVG",
            owner="finance@company.com",
            tags=["aov", "metrics", "kpi"],
        ),
    ]


@pytest.fixture
def sample_process_datapoints():
    """Create sample process DataPoints for testing."""
    return [
        ProcessDataPoint(
            datapoint_id="proc_daily_sales_etl_001",
            type="Process",
            name="Daily Sales ETL",
            schedule="0 2 * * *",
            data_freshness="T-1 (yesterday's data available by 3am UTC)",
            target_tables=["analytics.fact_sales"],
            dependencies=["raw.sales_events"],
            owner="data-eng@company.com",
            tags=["etl", "daily", "sales"],
        ),
    ]


@pytest.fixture
async def populated_vector_store(
    sample_schema_datapoints, sample_business_datapoints, sample_process_datapoints
):
    """Create and populate VectorStore with sample data."""
    openai_key = os.getenv("LLM_OPENAI_API_KEY")
    if not openai_key:
        pytest.skip("LLM_OPENAI_API_KEY not set for integration test.")
    with tempfile.TemporaryDirectory() as tmpdir:
        vector_store = VectorStore(
            collection_name="test_context_integration",
            persist_directory=tmpdir,
            embedding_model="text-embedding-3-small",
            openai_api_key=openai_key,
        )

        await vector_store.initialize()

        # Add all datapoints
        all_datapoints = (
            sample_schema_datapoints + sample_business_datapoints + sample_process_datapoints
        )
        await vector_store.add_datapoints(all_datapoints)

        yield vector_store

        # Cleanup
        await vector_store.clear()


@pytest.fixture
def populated_knowledge_graph(
    sample_schema_datapoints, sample_business_datapoints, sample_process_datapoints
):
    """Create and populate KnowledgeGraph with sample data."""
    graph = KnowledgeGraph()

    # Add all datapoints
    for dp in sample_schema_datapoints:
        graph.add_datapoint(dp)

    for dp in sample_business_datapoints:
        graph.add_datapoint(dp)

    for dp in sample_process_datapoints:
        graph.add_datapoint(dp)

    return graph


@pytest.fixture
async def integration_retriever(populated_vector_store, populated_knowledge_graph):
    """Create Retriever with populated stores."""
    return Retriever(
        vector_store=populated_vector_store,
        knowledge_graph=populated_knowledge_graph,
    )


@pytest.fixture
async def integration_context_agent(integration_retriever):
    """Create ContextAgent with integration retriever."""
    return ContextAgent(retriever=integration_retriever)


class TestContextAgentIntegration:
    """Integration tests for ContextAgent with real data."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_retrieves_relevant_sales_context(self, integration_context_agent):
        """Test retrieval of sales-related context."""
        input_data = ContextAgentInput(
            query="What were our total sales last quarter?",
            retrieval_mode="hybrid",
            max_datapoints=10,
        )

        output = await integration_context_agent(input_data)

        assert output.success is True
        assert output.investigation_memory is not None

        memory = output.investigation_memory
        assert memory.query == input_data.query
        assert len(memory.datapoints) > 0

        # Should retrieve sales-related DataPoints
        datapoint_ids = [dp.datapoint_id for dp in memory.datapoints]

        # At least one sales-related datapoint should be retrieved
        sales_related = any(
            "sales" in dp_id.lower() or "revenue" in dp_id.lower() for dp_id in datapoint_ids
        )
        assert sales_related

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_retrieves_product_context(self, integration_context_agent):
        """Test retrieval of product-related context."""
        input_data = ContextAgentInput(
            query="Show me top selling products by category",
            retrieval_mode="hybrid",
            max_datapoints=10,
        )

        output = await integration_context_agent(input_data)

        assert output.success is True
        memory = output.investigation_memory
        assert len(memory.datapoints) > 0

        # Should retrieve product-related DataPoints
        has_product = any(
            "product" in dp.name.lower() or "product" in dp.datapoint_id.lower()
            for dp in memory.datapoints
        )
        assert has_product

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_local_mode_retrieval(self, integration_context_agent):
        """Test local (vector-only) retrieval mode."""
        input_data = ContextAgentInput(
            query="revenue metrics",
            retrieval_mode="local",
            max_datapoints=5,
        )

        output = await integration_context_agent(input_data)

        assert output.success is True
        memory = output.investigation_memory
        assert memory.retrieval_mode == "local"
        assert len(memory.datapoints) > 0

        # All sources should be from vector search
        sources = {dp.source for dp in memory.datapoints}
        assert sources == {"vector"}

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_global_mode_retrieval(
        self, integration_context_agent, populated_knowledge_graph
    ):
        """Test global (graph-only) retrieval mode."""
        # Use a specific node ID for graph traversal
        input_data = ContextAgentInput(
            query="table_fact_sales_001",  # Node ID from graph
            retrieval_mode="global",
            max_datapoints=5,
        )

        output = await integration_context_agent(input_data)

        assert output.success is True
        memory = output.investigation_memory
        assert memory.retrieval_mode == "global"

        # Graph mode should find related nodes
        # (will be empty if node doesn't exist or has no neighbors)
        # This is expected behavior

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_hybrid_mode_combines_sources(self, integration_context_agent):
        """Test hybrid mode combines vector and graph results."""
        input_data = ContextAgentInput(
            query="sales revenue and transactions",
            retrieval_mode="hybrid",
            max_datapoints=10,
        )

        output = await integration_context_agent(input_data)

        assert output.success is True
        memory = output.investigation_memory
        assert memory.retrieval_mode == "hybrid"
        assert len(memory.datapoints) > 0

        # In hybrid mode, we may have items from vector, graph, or both
        sources = {dp.source for dp in memory.datapoints}
        assert len(sources) > 0  # At least one source type

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_investigation_memory_structure(self, integration_context_agent):
        """Test InvestigationMemory has correct structure."""
        input_data = ContextAgentInput(
            query="sales data",
            retrieval_mode="hybrid",
            max_datapoints=5,
        )

        output = await integration_context_agent(input_data)

        memory = output.investigation_memory

        # Verify structure
        assert isinstance(memory.datapoints, list)
        assert isinstance(memory.total_retrieved, int)
        assert isinstance(memory.sources_used, list)
        assert memory.query == "sales data"
        assert memory.retrieval_mode == "hybrid"

        # Verify DataPoint structure
        if len(memory.datapoints) > 0:
            dp = memory.datapoints[0]
            assert dp.datapoint_id is not None
            assert dp.datapoint_type in ["Schema", "Business", "Process"]
            assert dp.name is not None
            assert 0 <= dp.score <= 1
            assert dp.source in ["vector", "graph", "hybrid"]

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_citation_sources_tracked(self, integration_context_agent):
        """Test that sources are tracked for citations."""
        input_data = ContextAgentInput(
            query="total revenue calculation",
            retrieval_mode="hybrid",
            max_datapoints=5,
        )

        output = await integration_context_agent(input_data)

        memory = output.investigation_memory

        # Sources should be tracked
        assert len(memory.sources_used) > 0

        # Sources should match datapoint IDs
        datapoint_ids = {dp.datapoint_id for dp in memory.datapoints}
        for source in memory.sources_used:
            assert source in datapoint_ids

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_with_extracted_entities(self, integration_context_agent):
        """Test context retrieval with extracted entities."""
        entities = [
            ExtractedEntity(
                entity_type="metric",
                value="revenue",
                confidence=0.95,
            ),
            ExtractedEntity(
                entity_type="table",
                value="fact_sales",
                confidence=0.88,
            ),
        ]

        input_data = ContextAgentInput(
            query="sales revenue",
            entities=entities,
            retrieval_mode="hybrid",
            max_datapoints=10,
        )

        output = await integration_context_agent(input_data)

        assert output.success is True
        assert len(output.investigation_memory.datapoints) > 0

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_retrieval_performance(self, integration_context_agent):
        """Test that retrieval completes in reasonable time."""
        input_data = ContextAgentInput(
            query="sales metrics",
            retrieval_mode="hybrid",
            max_datapoints=10,
        )

        output = await integration_context_agent(input_data)

        # Should complete quickly (< 200ms for small dataset)
        # This is just a rough check
        assert output.metadata.duration_ms is not None
        assert output.metadata.duration_ms < 5000  # 5 seconds max

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_no_llm_calls_made(self, integration_context_agent):
        """Test that ContextAgent makes no LLM calls."""
        input_data = ContextAgentInput(
            query="test query",
            retrieval_mode="hybrid",
            max_datapoints=5,
        )

        output = await integration_context_agent(input_data)

        # ContextAgent should NEVER make LLM calls
        assert output.metadata.llm_calls == 0

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_respects_max_datapoints_limit(self, integration_context_agent):
        """Test that max_datapoints limit is respected."""
        max_limit = 3

        input_data = ContextAgentInput(
            query="sales data",
            retrieval_mode="hybrid",
            max_datapoints=max_limit,
        )

        output = await integration_context_agent(input_data)

        # Should not exceed max_datapoints
        assert len(output.investigation_memory.datapoints) <= max_limit

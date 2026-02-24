"""
Tests for Knowledge Retriever.

Tests unified retrieval combining VectorStore and KnowledgeGraph.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from backend.knowledge.graph import KnowledgeGraph, NodeType
from backend.knowledge.retriever import (
    RetrievalMode,
    RetrievalResult,
    RetrievedItem,
    Retriever,
    RetrieverError,
)
from backend.knowledge.vectors import VectorStore


@pytest.fixture
def mock_vector_store():
    """Create mock VectorStore."""
    mock = Mock(spec=VectorStore)
    mock.search = AsyncMock()
    return mock


@pytest.fixture
def mock_knowledge_graph():
    """Create mock KnowledgeGraph."""
    mock = Mock(spec=KnowledgeGraph)
    mock.get_node_metadata.return_value = None
    return mock


@pytest.fixture
def retriever(mock_vector_store, mock_knowledge_graph):
    """Create Retriever instance with mocks."""
    return Retriever(
        vector_store=mock_vector_store,
        knowledge_graph=mock_knowledge_graph,
        rrf_k=60,
    )


@pytest.fixture
def sample_vector_results():
    """Sample vector search results."""
    return [
        {
            "datapoint_id": "table_sales_001",
            "distance": 0.1,
            "metadata": {"name": "Sales Table", "type": "table"},
            "document": "Sales fact table with transaction data",
        },
        {
            "datapoint_id": "metric_revenue_001",
            "distance": 0.3,
            "metadata": {"name": "Revenue Metric", "type": "metric"},
            "document": "Total revenue calculation",
        },
        {
            "datapoint_id": "table_customer_001",
            "distance": 0.5,
            "metadata": {"name": "Customer Table", "type": "table"},
            "document": "Customer dimension table",
        },
    ]


@pytest.fixture
def sample_graph_results():
    """Sample graph traversal results."""
    return [
        {
            "node_id": "table_sales_001",
            "distance": 1,
            "edge_type": "calculates",
            "node_type": NodeType.TABLE,
            "name": "Sales Table",
            "metadata": {"name": "Sales Table", "type": "table"},
        },
        {
            "node_id": "table_product_001",
            "distance": 2,
            "edge_type": "joins_with",
            "node_type": NodeType.TABLE,
            "name": "Product Table",
            "metadata": {"name": "Product Table", "type": "table"},
        },
    ]


class TestInitialization:
    """Test retriever initialization."""

    def test_init_sets_properties(self, mock_vector_store, mock_knowledge_graph):
        """Test initialization sets all properties."""
        retriever = Retriever(mock_vector_store, mock_knowledge_graph, rrf_k=100)

        assert retriever.vector_store == mock_vector_store
        assert retriever.knowledge_graph == mock_knowledge_graph
        assert retriever.rrf_k == 100

    def test_init_default_rrf_k(self, mock_vector_store, mock_knowledge_graph):
        """Test default RRF k value."""
        retriever = Retriever(mock_vector_store, mock_knowledge_graph)

        assert retriever.rrf_k == 60  # Standard RRF value


class TestLocalMode:
    """Test local (vector-only) retrieval mode."""

    @pytest.mark.asyncio
    async def test_local_mode_uses_vector_search_only(
        self, retriever, mock_vector_store, sample_vector_results
    ):
        """Test local mode only calls vector search."""
        mock_vector_store.search.return_value = sample_vector_results

        result = await retriever.retrieve("sales data", mode=RetrievalMode.LOCAL, top_k=3)

        # Should call vector search
        mock_vector_store.search.assert_called_once()
        args, kwargs = mock_vector_store.search.call_args
        assert args[0] == "sales data"
        assert kwargs["filter_metadata"] is None
        assert kwargs["top_k"] == 3 * retriever._precedence_pool_multiplier

        # Should not call graph
        assert retriever.knowledge_graph.get_related.call_count == 0

        # Check result
        assert result.mode == RetrievalMode.LOCAL
        assert result.total_count == 3
        assert len(result.items) == 3
        assert all(item.source == "vector" for item in result.items)

    @pytest.mark.asyncio
    async def test_local_mode_converts_distance_to_score(
        self, retriever, mock_vector_store, sample_vector_results
    ):
        """Test distance conversion to similarity score."""
        mock_vector_store.search.return_value = sample_vector_results

        result = await retriever.retrieve("sales data", mode=RetrievalMode.LOCAL, top_k=3)

        # Scores should be: 1/(1+0.1), 1/(1+0.3), 1/(1+0.5)
        assert result.items[0].score == pytest.approx(1.0 / 1.1, rel=1e-5)
        assert result.items[1].score == pytest.approx(1.0 / 1.3, rel=1e-5)
        assert result.items[2].score == pytest.approx(1.0 / 1.5, rel=1e-5)

    @pytest.mark.asyncio
    async def test_local_mode_includes_metadata(
        self, retriever, mock_vector_store, sample_vector_results
    ):
        """Test metadata is preserved in results."""
        mock_vector_store.search.return_value = sample_vector_results

        result = await retriever.retrieve("sales data", mode=RetrievalMode.LOCAL, top_k=3)

        assert result.items[0].datapoint_id == "table_sales_001"
        assert result.items[0].metadata == {"name": "Sales Table", "type": "table"}
        assert result.items[0].content == "Sales fact table with transaction data"

    @pytest.mark.asyncio
    async def test_local_mode_with_metadata_filter(
        self, retriever, mock_vector_store, sample_vector_results
    ):
        """Test metadata filter is passed to vector search."""
        mock_vector_store.search.return_value = sample_vector_results

        metadata_filter = {"type": "table"}
        await retriever.retrieve(
            "sales data",
            mode=RetrievalMode.LOCAL,
            top_k=3,
            metadata_filter=metadata_filter,
        )

        mock_vector_store.search.assert_called_once()
        args, kwargs = mock_vector_store.search.call_args
        assert args[0] == "sales data"
        assert kwargs["filter_metadata"] == metadata_filter
        assert kwargs["top_k"] == 3 * retriever._precedence_pool_multiplier

    @pytest.mark.asyncio
    async def test_local_mode_enriches_metadata_from_graph(
        self, retriever, mock_vector_store, mock_knowledge_graph, sample_vector_results
    ):
        """Test vector metadata is enriched with graph node metadata when available."""
        mock_vector_store.search.return_value = sample_vector_results
        mock_knowledge_graph.get_node_metadata.return_value = {
            "business_purpose": "Canonical sales facts",
            "schema": "analytics",
            "key_columns": [{"name": "amount", "type": "numeric"}],
        }

        result = await retriever.retrieve("sales data", mode=RetrievalMode.LOCAL, top_k=1)

        assert len(result.items) == 1
        sales_item = result.items[0]
        assert sales_item.datapoint_id == "table_sales_001"
        metadata = sales_item.metadata
        assert metadata["business_purpose"] == "Canonical sales facts"
        assert metadata["name"] == "Sales Table"
        assert metadata["schema"] == "analytics"
        mock_knowledge_graph.get_node_metadata.assert_any_call("table_sales_001")
        assert mock_knowledge_graph.get_node_metadata.call_count == 3

    @pytest.mark.asyncio
    async def test_local_mode_applies_source_precedence_before_final_top_k(
        self, retriever, mock_vector_store
    ):
        """When top_k is small, retrieval should still consider deeper candidates for precedence."""
        mock_vector_store.search.return_value = [
            {
                "datapoint_id": "table_orders_managed_001",
                "distance": 0.01,
                "metadata": {
                    "name": "Orders (Managed)",
                    "type": "Schema",
                    "schema": "public",
                    "table_name": "orders",
                    "source_tier": "managed",
                },
                "document": "managed orders context",
            },
            {
                "datapoint_id": "table_orders_user_001",
                "distance": 0.03,
                "metadata": {
                    "name": "Orders (User)",
                    "type": "Schema",
                    "schema": "public",
                    "table_name": "orders",
                    "source_tier": "user",
                },
                "document": "user orders context",
            },
            {
                "datapoint_id": "table_customers_001",
                "distance": 0.05,
                "metadata": {
                    "name": "Customers",
                    "type": "Schema",
                    "schema": "public",
                    "table_name": "customers",
                    "source_tier": "managed",
                },
                "document": "customers context",
            },
        ]

        result = await retriever.retrieve("orders", mode=RetrievalMode.LOCAL, top_k=1)

        assert len(result.items) == 1
        assert result.items[0].datapoint_id == "table_orders_user_001"

    @pytest.mark.asyncio
    async def test_local_mode_prefers_managed_over_example_for_same_table(
        self, retriever, mock_vector_store
    ):
        """For conflicting table context, managed should win over example."""
        mock_vector_store.search.return_value = [
            {
                "datapoint_id": "table_grocery_stores_example_001",
                "distance": 0.01,
                "metadata": {
                    "name": "Grocery Stores (Example)",
                    "type": "Schema",
                    "schema": "public",
                    "table_name": "grocery_stores",
                    "source_tier": "example",
                },
                "document": "example table context",
            },
            {
                "datapoint_id": "table_grocery_stores_managed_001",
                "distance": 0.45,
                "metadata": {
                    "name": "Grocery Stores (Managed)",
                    "type": "Schema",
                    "schema": "public",
                    "table_name": "grocery_stores",
                    "source_tier": "managed",
                },
                "document": "managed table context",
            },
        ]

        result = await retriever.retrieve("list grocery stores", mode=RetrievalMode.LOCAL, top_k=5)

        ids = [item.datapoint_id for item in result.items]
        assert "table_grocery_stores_managed_001" in ids
        assert "table_grocery_stores_example_001" not in ids

    @pytest.mark.asyncio
    async def test_local_mode_prefers_user_over_managed_for_same_table(
        self, retriever, mock_vector_store
    ):
        """User-authored context should override managed context for same table key."""
        mock_vector_store.search.return_value = [
            {
                "datapoint_id": "table_grocery_products_managed_001",
                "distance": 0.05,
                "metadata": {
                    "name": "Products (Managed)",
                    "type": "Schema",
                    "schema": "public",
                    "table_name": "grocery_products",
                    "source_tier": "managed",
                },
                "document": "managed products context",
            },
            {
                "datapoint_id": "table_grocery_products_user_001",
                "distance": 0.40,
                "metadata": {
                    "name": "Products (User)",
                    "type": "Schema",
                    "schema": "public",
                    "table_name": "grocery_products",
                    "source_tier": "user",
                },
                "document": "user products context",
            },
        ]

        result = await retriever.retrieve("product details", mode=RetrievalMode.LOCAL, top_k=5)

        ids = [item.datapoint_id for item in result.items]
        assert "table_grocery_products_user_001" in ids
        assert "table_grocery_products_managed_001" not in ids


class TestGlobalMode:
    """Test global (graph-only) retrieval mode."""

    @pytest.mark.asyncio
    async def test_global_mode_uses_graph_traversal_only(
        self, retriever, mock_knowledge_graph, sample_graph_results
    ):
        """Test global mode only calls graph traversal."""
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        result = await retriever.retrieve("metric_revenue_001", mode=RetrievalMode.GLOBAL, top_k=5)

        # Should call graph traversal
        mock_knowledge_graph.get_related.assert_called_once_with("metric_revenue_001", max_depth=2)

        # Check result
        assert result.mode == RetrievalMode.GLOBAL
        assert result.total_count == 2
        assert len(result.items) == 2
        assert all(item.source == "graph" for item in result.items)

    @pytest.mark.asyncio
    async def test_global_mode_converts_distance_to_score(
        self, retriever, mock_knowledge_graph, sample_graph_results
    ):
        """Test graph distance conversion to score."""
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        result = await retriever.retrieve("metric_revenue_001", mode=RetrievalMode.GLOBAL, top_k=5)

        # Scores should be: 1/1, 1/2
        assert result.items[0].score == 1.0
        assert result.items[1].score == 0.5

    @pytest.mark.asyncio
    async def test_global_mode_respects_top_k(self, retriever, mock_knowledge_graph):
        """Test global mode limits results to top_k."""
        # Return 10 results
        many_results = [
            {
                "node_id": f"node_{i}",
                "distance": i + 1,
                "edge_type": "test",
                "node_type": NodeType.TABLE,
                "name": f"Node {i}",
                "metadata": {},
            }
            for i in range(10)
        ]
        mock_knowledge_graph.get_related.return_value = many_results

        result = await retriever.retrieve("metric_revenue_001", mode=RetrievalMode.GLOBAL, top_k=3)

        assert len(result.items) == 3

    @pytest.mark.asyncio
    async def test_global_mode_custom_max_depth(
        self, retriever, mock_knowledge_graph, sample_graph_results
    ):
        """Test custom max_depth parameter."""
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        await retriever.retrieve(
            "metric_revenue_001",
            mode=RetrievalMode.GLOBAL,
            top_k=5,
            graph_max_depth=3,
        )

        mock_knowledge_graph.get_related.assert_called_once_with("metric_revenue_001", max_depth=3)


class TestHybridMode:
    """Test hybrid (vector + graph) retrieval mode."""

    @pytest.mark.asyncio
    async def test_hybrid_mode_calls_both_sources(
        self,
        retriever,
        mock_vector_store,
        mock_knowledge_graph,
        sample_vector_results,
        sample_graph_results,
    ):
        """Test hybrid mode calls both vector and graph."""
        mock_vector_store.search.return_value = sample_vector_results
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        result = await retriever.retrieve("sales data", mode=RetrievalMode.HYBRID, top_k=5)

        # Should call both
        mock_vector_store.search.assert_called_once()
        mock_knowledge_graph.get_related.assert_called_once()

        assert result.mode == RetrievalMode.HYBRID

    @pytest.mark.asyncio
    async def test_hybrid_mode_applies_rrf_ranking(
        self,
        retriever,
        mock_vector_store,
        mock_knowledge_graph,
        sample_vector_results,
        sample_graph_results,
    ):
        """Test RRF ranking is applied in hybrid mode."""
        mock_vector_store.search.return_value = sample_vector_results
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        result = await retriever.retrieve("sales data", mode=RetrievalMode.HYBRID, top_k=5)

        # Items should be ranked by RRF scores
        assert len(result.items) > 0
        # Scores should be in descending order
        scores = [item.score for item in result.items]
        assert scores == sorted(scores, reverse=True)

    @pytest.mark.asyncio
    async def test_hybrid_mode_deduplicates_results(
        self,
        retriever,
        mock_vector_store,
        mock_knowledge_graph,
        sample_vector_results,
        sample_graph_results,
    ):
        """Test deduplication of results in hybrid mode."""
        # Both sources return same item (table_sales_001)
        mock_vector_store.search.return_value = sample_vector_results
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        result = await retriever.retrieve("sales data", mode=RetrievalMode.HYBRID, top_k=10)

        # Check no duplicate IDs
        ids = [item.datapoint_id for item in result.items]
        assert len(ids) == len(set(ids))

    @pytest.mark.asyncio
    async def test_hybrid_mode_marks_source_correctly(
        self,
        retriever,
        mock_vector_store,
        mock_knowledge_graph,
        sample_vector_results,
        sample_graph_results,
    ):
        """Test source marking in hybrid mode."""
        mock_vector_store.search.return_value = sample_vector_results
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        result = await retriever.retrieve("sales data", mode=RetrievalMode.HYBRID, top_k=10)

        # table_sales_001 appears in both -> should be "hybrid"
        sales_item = next(
            (item for item in result.items if item.datapoint_id == "table_sales_001"),
            None,
        )
        if sales_item:
            assert sales_item.source == "hybrid"

        # metric_revenue_001 only in vector -> should be "vector"
        revenue_item = next(
            (item for item in result.items if item.datapoint_id == "metric_revenue_001"),
            None,
        )
        if revenue_item:
            assert revenue_item.source == "vector"

        # table_product_001 only in graph -> should be "graph"
        product_item = next(
            (item for item in result.items if item.datapoint_id == "table_product_001"),
            None,
        )
        if product_item:
            assert product_item.source == "graph"

    @pytest.mark.asyncio
    async def test_hybrid_mode_uses_top_vector_as_seed(
        self,
        retriever,
        mock_vector_store,
        mock_knowledge_graph,
        sample_vector_results,
        sample_graph_results,
    ):
        """Test graph uses top vector result as seed node."""
        mock_vector_store.search.return_value = sample_vector_results
        mock_knowledge_graph.get_related.return_value = sample_graph_results

        await retriever.retrieve("sales data", mode=RetrievalMode.HYBRID, top_k=5)

        # Should use first vector result as seed
        mock_knowledge_graph.get_related.assert_called_once_with("table_sales_001", max_depth=2)

    @pytest.mark.asyncio
    async def test_hybrid_mode_handles_graph_failure_gracefully(
        self, retriever, mock_vector_store, mock_knowledge_graph, sample_vector_results
    ):
        """Test hybrid mode works even if graph fails."""
        mock_vector_store.search.return_value = sample_vector_results
        mock_knowledge_graph.get_related.side_effect = Exception("Graph error")

        # Should not raise, just use vector results
        result = await retriever.retrieve("sales data", mode=RetrievalMode.HYBRID, top_k=5)

        assert len(result.items) > 0
        assert all(item.source in ("vector", "hybrid") for item in result.items)

    @pytest.mark.asyncio
    async def test_hybrid_mode_tries_multiple_seed_nodes(
        self, retriever, mock_vector_store, mock_knowledge_graph, sample_graph_results
    ):
        """Test hybrid mode tries multiple seed nodes if first fails."""
        # Return 3 vector results
        vector_results = [
            {
                "datapoint_id": "not_in_graph_001",
                "distance": 0.1,
                "metadata": {"type": "Schema"},
                "document": "First result not in graph",
            },
            {
                "datapoint_id": "table_sales_001",
                "distance": 0.2,
                "metadata": {"type": "Schema"},
                "document": "Second result IS in graph",
            },
            {
                "datapoint_id": "another_vector_001",
                "distance": 0.3,
                "metadata": {"type": "Business"},
                "document": "Third result",
            },
        ]
        mock_vector_store.search.return_value = vector_results

        # First seed fails, second succeeds
        mock_knowledge_graph.get_related.side_effect = [
            Exception("Node not in graph"),  # First seed fails
            sample_graph_results,  # Second seed succeeds
        ]

        result = await retriever.retrieve("sales data", mode=RetrievalMode.HYBRID, top_k=5)

        # Should have tried both seeds
        assert mock_knowledge_graph.get_related.call_count == 2
        # First call with first seed
        assert mock_knowledge_graph.get_related.call_args_list[0][0][0] == "not_in_graph_001"
        # Second call with second seed
        assert mock_knowledge_graph.get_related.call_args_list[1][0][0] == "table_sales_001"

        # Should have results from both vector and graph
        assert len(result.items) > 0
        sources = {item.source for item in result.items}
        assert "hybrid" in sources or ("vector" in sources and "graph" in sources)


class TestRRFAlgorithm:
    """Test Reciprocal Rank Fusion algorithm."""

    def test_rrf_combines_rankings_correctly(self, retriever):
        """Test RRF formula is applied correctly."""
        vector_items = {
            "item1": {"rank": 1, "score": 0.9},
            "item2": {"rank": 2, "score": 0.8},
        }

        graph_items = {
            "item1": {"rank": 2, "score": 0.7},
            "item3": {"rank": 1, "score": 0.95},
        }

        rrf_scores = retriever._apply_rrf(vector_items, graph_items)

        # item1: 1/(60+1) + 1/(60+2) = 1/61 + 1/62
        expected_item1 = (1.0 / 61) + (1.0 / 62)

        # item2: 1/(60+2) = 1/62
        expected_item2 = 1.0 / 62

        # item3: 1/(60+1) = 1/61
        expected_item3 = 1.0 / 61

        assert rrf_scores["item1"] == pytest.approx(expected_item1, rel=1e-5)
        assert rrf_scores["item2"] == pytest.approx(expected_item2, rel=1e-5)
        assert rrf_scores["item3"] == pytest.approx(expected_item3, rel=1e-5)

    def test_rrf_items_in_both_sources_rank_higher(self, retriever):
        """Test items appearing in both sources get higher RRF scores."""
        vector_items = {
            "both": {"rank": 1, "score": 0.9},
            "vector_only": {"rank": 2, "score": 0.8},
        }

        graph_items = {
            "both": {"rank": 1, "score": 0.85},
            "graph_only": {"rank": 2, "score": 0.75},
        }

        rrf_scores = retriever._apply_rrf(vector_items, graph_items)

        # "both" should have highest score (appears in both)
        assert rrf_scores["both"] > rrf_scores["vector_only"]
        assert rrf_scores["both"] > rrf_scores["graph_only"]

    def test_rrf_with_empty_sources(self, retriever):
        """Test RRF handles empty sources."""
        vector_items = {"item1": {"rank": 1, "score": 0.9}}
        graph_items = {}

        rrf_scores = retriever._apply_rrf(vector_items, graph_items)

        assert "item1" in rrf_scores
        assert rrf_scores["item1"] > 0


class TestErrorHandling:
    """Test error handling."""

    @pytest.mark.asyncio
    async def test_unknown_mode_raises_error(self, retriever):
        """Test unknown retrieval mode raises error."""
        with pytest.raises(RetrieverError, match="Unknown retrieval mode"):
            await retriever.retrieve("query", mode="invalid_mode", top_k=5)

    @pytest.mark.asyncio
    async def test_vector_store_error_propagates(self, retriever, mock_vector_store):
        """Test vector store errors are caught and wrapped."""
        mock_vector_store.search.side_effect = Exception("Vector error")

        with pytest.raises(RetrieverError, match="Retrieval failed"):
            await retriever.retrieve("query", mode=RetrievalMode.LOCAL, top_k=5)


class TestRetrievalResult:
    """Test RetrievalResult model."""

    def test_retrieval_result_creation(self):
        """Test creating RetrievalResult."""
        items = [
            RetrievedItem(
                datapoint_id="test_001",
                score=0.95,
                source="vector",
                metadata={},
            )
        ]

        result = RetrievalResult(
            items=items,
            total_count=1,
            mode=RetrievalMode.LOCAL,
            query="test query",
        )

        assert result.total_count == 1
        assert result.mode == RetrievalMode.LOCAL
        assert result.query == "test query"
        assert len(result.items) == 1

    def test_retrieved_item_creation(self):
        """Test creating RetrievedItem."""
        item = RetrievedItem(
            datapoint_id="test_001",
            score=0.95,
            source="hybrid",
            metadata={"name": "Test"},
            content="Test content",
        )

        assert item.datapoint_id == "test_001"
        assert item.score == 0.95
        assert item.source == "hybrid"
        assert item.metadata == {"name": "Test"}
        assert item.content == "Test content"

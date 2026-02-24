"""
Tests for Vector Store.

Tests Chroma-based vector store operations with DataPoints.
"""

from unittest.mock import AsyncMock, patch

import pytest

from backend.knowledge.vectors import VectorStore, VectorStoreError
from backend.models.datapoint import (
    BusinessDataPoint,
    ProcessDataPoint,
    SchemaDataPoint,
)


class MockEmbeddingFunction:
    """Mock embedding function for testing."""

    def __call__(self, input: list[str]) -> list[list[float]]:
        """Generate simple mock embeddings."""
        # Return simple deterministic embeddings based on input length
        return [[float(i) for i in range(384)] for _ in input]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple documents (for adding to collection)."""
        return self(texts)

    def embed_query(self, input: str | list[str]) -> list[float] | list[list[float]]:
        """Embed a single query (for search)."""
        # Chroma might pass either a string or a list
        if isinstance(input, list):
            return [[float(i) for i in range(384)] for _ in input]
        return [float(i) for i in range(384)]

    @staticmethod
    def name() -> str:
        """Return the name of the embedding function."""
        return "mock-embedding-function"

    def is_legacy(self) -> bool:
        """Indicate this embedding function uses the new interface."""
        return False

    def supported_spaces(self) -> list[str]:
        """Return supported distance metrics."""
        return ["cosine", "l2", "ip"]

    def get_config(self) -> dict:
        """Return embedding function configuration."""
        return {}

    @classmethod
    def build_from_config(cls, config: dict):
        """Reconstruct embedding function from config."""
        return cls()


@pytest.fixture
def mock_openai_embeddings():
    """Mock OpenAI embedding function for all tests."""
    with patch(
        "backend.knowledge.vectors.OpenAIEmbeddingFunction",
        return_value=MockEmbeddingFunction(),
    ) as mock:
        yield mock


@pytest.fixture
async def test_vector_store(tmp_path, mock_openai_embeddings):
    """Create a test vector store instance."""
    store = VectorStore(
        collection_name="test_collection",
        persist_directory=tmp_path / "chroma_test",
        embedding_model="text-embedding-3-small",
        openai_api_key="sk-test-key-for-unit-tests-only-not-real",
    )
    await store.initialize()
    yield store
    # Cleanup
    await store.clear()


@pytest.fixture
def sample_schema_datapoint():
    """Create a sample Schema DataPoint."""
    return SchemaDataPoint(
        datapoint_id="table_test_sales_001",
        type="Schema",
        name="Test Sales Table",
        table_name="test.sales",
        schema="test",
        business_purpose="Test sales data for unit tests",
        key_columns=[
            {
                "name": "sale_id",
                "type": "INTEGER",
                "business_meaning": "Sale identifier",
                "nullable": False,
            },
            {
                "name": "amount",
                "type": "DECIMAL",
                "business_meaning": "Sale amount",
                "nullable": False,
            },
        ],
        owner="test@example.com",
        tags=["test", "sales"],
    )


@pytest.fixture
def sample_business_datapoint():
    """Create a sample Business DataPoint."""
    return BusinessDataPoint(
        datapoint_id="metric_test_revenue_001",
        type="Business",
        name="Test Revenue",
        calculation="SUM(sales.amount)",
        synonyms=["total sales", "income"],
        business_rules=["Exclude refunds"],
        related_tables=["test.sales"],
        owner="test@example.com",
        tags=["test", "metric"],
    )


@pytest.fixture
def sample_process_datapoint():
    """Create a sample Process DataPoint."""
    return ProcessDataPoint(
        datapoint_id="proc_test_etl_001",
        type="Process",
        name="Test ETL",
        schedule="0 1 * * *",
        data_freshness="T-1",
        target_tables=["test.sales"],
        dependencies=["raw.sales"],
        owner="test@example.com",
        tags=["test", "etl"],
    )


class TestInitialization:
    """Test vector store initialization."""

    @pytest.mark.asyncio
    async def test_initialize_creates_directory(self, tmp_path, mock_openai_embeddings):
        """Test that initialization creates persist directory."""
        persist_dir = tmp_path / "test_chroma"
        assert not persist_dir.exists()

        store = VectorStore(
            collection_name="test",
            persist_directory=persist_dir,
            embedding_model="text-embedding-3-small",
            openai_api_key="sk-test-key-for-unit-tests-only-not-real",
        )
        await store.initialize()

        assert persist_dir.exists()
        assert persist_dir.is_dir()

    @pytest.mark.asyncio
    async def test_initialize_creates_collection(self, test_vector_store):
        """Test that initialization creates Chroma collection."""
        assert test_vector_store.collection is not None
        assert test_vector_store.client is not None

    @pytest.mark.asyncio
    async def test_initialize_idempotent(self, test_vector_store):
        """Test that re-initialization works."""
        # Initialize again
        await test_vector_store.initialize()

        # Should still work
        count = await test_vector_store.get_count()
        assert count == 0


class TestAddDataPoints:
    """Test adding datapoints to vector store."""

    @pytest.mark.asyncio
    async def test_add_single_datapoint(self, test_vector_store, sample_schema_datapoint):
        """Test adding a single datapoint."""
        added = await test_vector_store.add_datapoints([sample_schema_datapoint])

        assert added == 1

        count = await test_vector_store.get_count()
        assert count == 1

    @pytest.mark.asyncio
    async def test_add_multiple_datapoints(
        self,
        test_vector_store,
        sample_schema_datapoint,
        sample_business_datapoint,
        sample_process_datapoint,
    ):
        """Test adding multiple datapoints."""
        datapoints = [
            sample_schema_datapoint,
            sample_business_datapoint,
            sample_process_datapoint,
        ]

        added = await test_vector_store.add_datapoints(datapoints)

        assert added == 3

        count = await test_vector_store.get_count()
        assert count == 3

    @pytest.mark.asyncio
    async def test_add_empty_list(self, test_vector_store):
        """Test adding empty list returns 0."""
        added = await test_vector_store.add_datapoints([])

        assert added == 0

    @pytest.mark.asyncio
    async def test_add_without_initialization(self, tmp_path, mock_openai_embeddings):
        """Test that adding without initialization raises error."""
        store = VectorStore(
            collection_name="test",
            persist_directory=tmp_path / "test",
            embedding_model="text-embedding-3-small",
            openai_api_key="sk-test-key-for-unit-tests-only-not-real",
        )
        # Don't initialize

        with pytest.raises(VectorStoreError, match="not initialized"):
            await store.add_datapoints([])

    @pytest.mark.asyncio
    async def test_add_datapoint_with_metadata(self, test_vector_store, sample_schema_datapoint):
        """Test that metadata is stored correctly."""
        await test_vector_store.add_datapoints([sample_schema_datapoint])

        # Search to verify metadata
        results = await test_vector_store.search("sales", top_k=1)

        assert len(results) == 1
        assert results[0]["metadata"]["datapoint_id"] == "table_test_sales_001"
        assert results[0]["metadata"]["type"] == "Schema"
        assert results[0]["metadata"]["name"] == "Test Sales Table"
        assert results[0]["metadata"]["table_name"] == "test.sales"
        assert "key_columns" in results[0]["metadata"]
        assert "sale_id" in results[0]["metadata"]["key_columns"]

    @pytest.mark.asyncio
    async def test_business_metadata_includes_metric_fields(
        self, test_vector_store, sample_business_datapoint
    ):
        """Business datapoint metadata should persist calculation and semantic hints."""
        await test_vector_store.add_datapoints([sample_business_datapoint])

        results = await test_vector_store.search("revenue", top_k=1)

        assert len(results) == 1
        metadata = results[0]["metadata"]
        assert metadata["calculation"] == "SUM(sales.amount)"
        assert "total sales" in metadata["synonyms"]
        assert "Exclude refunds" in metadata["business_rules"]


class TestSearch:
    """Test semantic search functionality."""

    @pytest.mark.asyncio
    async def test_search_returns_similar_datapoints(
        self,
        test_vector_store,
        sample_schema_datapoint,
        sample_business_datapoint,
    ):
        """Test that search returns semantically similar datapoints."""
        await test_vector_store.add_datapoints([sample_schema_datapoint, sample_business_datapoint])

        # Search for sales-related content
        results = await test_vector_store.search("sales data", top_k=2)

        assert len(results) == 2
        # Should return both since they're both about sales
        ids = {r["datapoint_id"] for r in results}
        assert "table_test_sales_001" in ids
        assert "metric_test_revenue_001" in ids

    @pytest.mark.asyncio
    async def test_search_respects_top_k(
        self,
        test_vector_store,
        sample_schema_datapoint,
        sample_business_datapoint,
        sample_process_datapoint,
    ):
        """Test that search respects top_k parameter."""
        await test_vector_store.add_datapoints(
            [
                sample_schema_datapoint,
                sample_business_datapoint,
                sample_process_datapoint,
            ]
        )

        results = await test_vector_store.search("sales", top_k=2)

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_search_with_metadata_filter(
        self,
        test_vector_store,
        sample_schema_datapoint,
        sample_business_datapoint,
    ):
        """Test search with metadata filtering."""
        await test_vector_store.add_datapoints([sample_schema_datapoint, sample_business_datapoint])

        # Search only for Schema type
        results = await test_vector_store.search(
            "sales", top_k=10, filter_metadata={"type": "Schema"}
        )

        assert len(results) == 1
        assert results[0]["metadata"]["type"] == "Schema"

    @pytest.mark.asyncio
    async def test_search_returns_distance(self, test_vector_store, sample_schema_datapoint):
        """Test that search results include distance scores."""
        await test_vector_store.add_datapoints([sample_schema_datapoint])

        results = await test_vector_store.search("sales table", top_k=1)

        assert len(results) == 1
        assert "distance" in results[0]
        assert results[0]["distance"] is not None
        # Cosine distance can be negative (range: -1 to 1)
        assert isinstance(results[0]["distance"], (int, float))

    @pytest.mark.asyncio
    async def test_search_returns_document(self, test_vector_store, sample_schema_datapoint):
        """Test that search results include original document."""
        await test_vector_store.add_datapoints([sample_schema_datapoint])

        results = await test_vector_store.search("sales", top_k=1)

        assert len(results) == 1
        assert "document" in results[0]
        # Document should contain searchable text
        assert "Test Sales Table" in results[0]["document"]
        assert "sales data" in results[0]["document"]

    @pytest.mark.asyncio
    async def test_search_empty_store(self, test_vector_store):
        """Test search on empty store returns no results."""
        results = await test_vector_store.search("anything", top_k=10)

        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_search_without_initialization(self, tmp_path, mock_openai_embeddings):
        """Test that search without initialization raises error."""
        store = VectorStore(
            collection_name="test",
            persist_directory=tmp_path / "test",
            embedding_model="text-embedding-3-small",
            openai_api_key="sk-test-key-for-unit-tests-only-not-real",
        )

        with pytest.raises(VectorStoreError, match="not initialized"):
            await store.search("test")

    @pytest.mark.asyncio
    async def test_search_returns_empty_on_recoverable_storage_error(self, test_vector_store):
        """Recoverable index errors should not fail the request path."""
        with (
            patch.object(
                test_vector_store.collection,
                "query",
                side_effect=Exception("Error creating hnsw segment reader: Nothing found on disk"),
            ),
            patch.object(
                test_vector_store,
                "_recover_from_storage_error",
                new=AsyncMock(return_value=False),
            ) as recover_mock,
        ):
            results = await test_vector_store.search("sales", top_k=3)

        assert results == []
        recover_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_search_retries_after_recovery(self, test_vector_store):
        """If recovery succeeds, search should retry and return formatted results."""
        successful_payload = {
            "ids": [["table_test_sales_001"]],
            "distances": [[0.1]],
            "metadatas": [[{"type": "Schema"}]],
            "documents": [["sample"]],
        }

        with (
            patch.object(
                test_vector_store.collection,
                "query",
                side_effect=[
                    Exception("Error creating hnsw segment reader: Nothing found on disk"),
                    successful_payload,
                ],
            ),
            patch.object(
                test_vector_store,
                "_recover_from_storage_error",
                new=AsyncMock(return_value=True),
            ) as recover_mock,
        ):
            results = await test_vector_store.search("sales", top_k=1)

        assert len(results) == 1
        assert results[0]["datapoint_id"] == "table_test_sales_001"
        recover_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_search_raises_on_non_recoverable_error(self, test_vector_store):
        """Non-recoverable search failures should still bubble as VectorStoreError."""
        with patch.object(
            test_vector_store.collection,
            "query",
            side_effect=Exception("embedding provider unavailable"),
        ):
            with pytest.raises(VectorStoreError, match="Search failed"):
                await test_vector_store.search("sales", top_k=3)


class TestDelete:
    """Test deleting datapoints from vector store."""

    @pytest.mark.asyncio
    async def test_delete_single_datapoint(self, test_vector_store, sample_schema_datapoint):
        """Test deleting a single datapoint."""
        await test_vector_store.add_datapoints([sample_schema_datapoint])

        assert await test_vector_store.get_count() == 1

        deleted = await test_vector_store.delete(["table_test_sales_001"])

        assert deleted == 1
        assert await test_vector_store.get_count() == 0

    @pytest.mark.asyncio
    async def test_delete_multiple_datapoints(
        self,
        test_vector_store,
        sample_schema_datapoint,
        sample_business_datapoint,
    ):
        """Test deleting multiple datapoints."""
        await test_vector_store.add_datapoints([sample_schema_datapoint, sample_business_datapoint])

        assert await test_vector_store.get_count() == 2

        deleted = await test_vector_store.delete(
            ["table_test_sales_001", "metric_test_revenue_001"]
        )

        assert deleted == 2
        assert await test_vector_store.get_count() == 0

    @pytest.mark.asyncio
    async def test_delete_nonexistent_datapoint(self, test_vector_store):
        """Test deleting non-existent datapoint doesn't raise error."""
        # Chroma doesn't raise error for non-existent IDs
        deleted = await test_vector_store.delete(["nonexistent_001"])

        assert deleted == 1  # Returns count of IDs provided

    @pytest.mark.asyncio
    async def test_delete_empty_list(self, test_vector_store):
        """Test deleting empty list returns 0."""
        deleted = await test_vector_store.delete([])

        assert deleted == 0

    @pytest.mark.asyncio
    async def test_delete_without_initialization(self, tmp_path, mock_openai_embeddings):
        """Test that delete without initialization raises error."""
        store = VectorStore(
            collection_name="test",
            persist_directory=tmp_path / "test",
            embedding_model="text-embedding-3-small",
            openai_api_key="sk-test-key-for-unit-tests-only-not-real",
        )

        with pytest.raises(VectorStoreError, match="not initialized"):
            await store.delete(["test_001"])


class TestPersistence:
    """Test vector store persistence across restarts."""

    @pytest.mark.asyncio
    async def test_persistence_across_restarts(
        self, tmp_path, sample_schema_datapoint, mock_openai_embeddings
    ):
        """Test that data persists across store restarts."""
        persist_dir = tmp_path / "chroma_persist"

        # Create store and add data
        store1 = VectorStore(
            collection_name="persist_test",
            persist_directory=persist_dir,
            embedding_model="text-embedding-3-small",
            openai_api_key="sk-test-key-for-unit-tests-only-not-real",
        )
        await store1.initialize()
        await store1.add_datapoints([sample_schema_datapoint])

        count1 = await store1.get_count()
        assert count1 == 1

        # Create new store instance with same directory
        store2 = VectorStore(
            collection_name="persist_test",
            persist_directory=persist_dir,
            embedding_model="text-embedding-3-small",
            openai_api_key="sk-test-key-for-unit-tests-only-not-real",
        )
        await store2.initialize()

        # Data should still be there
        count2 = await store2.get_count()
        assert count2 == 1

        # Should be able to search
        results = await store2.search("sales", top_k=1)
        assert len(results) == 1
        assert results[0]["datapoint_id"] == "table_test_sales_001"


class TestClear:
    """Test clearing vector store."""

    @pytest.mark.asyncio
    async def test_clear_removes_all_datapoints(
        self,
        test_vector_store,
        sample_schema_datapoint,
        sample_business_datapoint,
    ):
        """Test that clear removes all datapoints."""
        await test_vector_store.add_datapoints([sample_schema_datapoint, sample_business_datapoint])

        assert await test_vector_store.get_count() == 2

        await test_vector_store.clear()

        assert await test_vector_store.get_count() == 0

    @pytest.mark.asyncio
    async def test_clear_empty_store(self, test_vector_store):
        """Test clearing empty store doesn't raise error."""
        await test_vector_store.clear()

        assert await test_vector_store.get_count() == 0


class TestGetCount:
    """Test getting datapoint count."""

    @pytest.mark.asyncio
    async def test_get_count_empty(self, test_vector_store):
        """Test count on empty store."""
        count = await test_vector_store.get_count()
        assert count == 0

    @pytest.mark.asyncio
    async def test_get_count_after_add(self, test_vector_store, sample_schema_datapoint):
        """Test count after adding datapoints."""
        await test_vector_store.add_datapoints([sample_schema_datapoint])

        count = await test_vector_store.get_count()
        assert count == 1

    @pytest.mark.asyncio
    async def test_get_count_after_delete(self, test_vector_store, sample_schema_datapoint):
        """Test count after deleting datapoints."""
        await test_vector_store.add_datapoints([sample_schema_datapoint])
        await test_vector_store.delete(["table_test_sales_001"])

        count = await test_vector_store.get_count()
        assert count == 0


class TestDocumentCreation:
    """Test document text creation from DataPoints."""

    @pytest.mark.asyncio
    async def test_schema_datapoint_document(self, test_vector_store, sample_schema_datapoint):
        """Test document creation for Schema DataPoint."""
        doc = test_vector_store._create_document(sample_schema_datapoint)

        assert "Name: Test Sales Table" in doc
        assert "Type: Schema" in doc
        assert "Purpose: Test sales data" in doc
        assert "Table: test.sales" in doc
        assert "Tags: test, sales" in doc

    @pytest.mark.asyncio
    async def test_business_datapoint_document(self, test_vector_store, sample_business_datapoint):
        """Test document creation for Business DataPoint."""
        doc = test_vector_store._create_document(sample_business_datapoint)

        assert "Name: Test Revenue" in doc
        assert "Type: Business" in doc
        assert "Calculation: SUM(sales.amount)" in doc
        assert "Synonyms: total sales, income" in doc
        assert "Rules: Exclude refunds" in doc

    @pytest.mark.asyncio
    async def test_process_datapoint_document(self, test_vector_store, sample_process_datapoint):
        """Test document creation for Process DataPoint."""
        doc = test_vector_store._create_document(sample_process_datapoint)

        assert "Name: Test ETL" in doc
        assert "Type: Process" in doc
        assert "Schedule: 0 1 * * *" in doc


class TestMetadataCreation:
    """Test metadata creation from DataPoints."""

    def test_schema_datapoint_metadata(self, test_vector_store, sample_schema_datapoint):
        """Test metadata creation for Schema DataPoint."""
        metadata = test_vector_store._create_metadata(sample_schema_datapoint)

        assert metadata["datapoint_id"] == "table_test_sales_001"
        assert metadata["type"] == "Schema"
        assert metadata["name"] == "Test Sales Table"
        assert metadata["table_name"] == "test.sales"
        assert metadata["schema"] == "test"
        assert metadata["tags"] == "test,sales"

    def test_business_datapoint_metadata(self, test_vector_store, sample_business_datapoint):
        """Test metadata creation for Business DataPoint."""
        sample_business_datapoint.metadata = {"connection_id": "conn-fintech"}
        metadata = test_vector_store._create_metadata(sample_business_datapoint)

        assert metadata["datapoint_id"] == "metric_test_revenue_001"
        assert metadata["type"] == "Business"
        assert metadata["related_tables"] == "test.sales"
        assert metadata["connection_id"] == "conn-fintech"
        assert metadata["tags"] == "test,metric"

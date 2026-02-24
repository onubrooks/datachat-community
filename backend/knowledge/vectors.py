"""
Vector Store

Chroma-based vector store for DataPoint embeddings with async interface.
Supports semantic search, persistence, and metadata storage.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import chromadb
from chromadb.config import Settings
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

from backend.config import get_settings
from backend.models.datapoint import DataPoint

logger = logging.getLogger(__name__)


class VectorStoreError(Exception):
    """Raised when vector store operations fail."""

    pass


class VectorStore:
    """
    Vector store for DataPoint embeddings using Chroma.

    Provides async interface for adding, searching, and deleting DataPoints
    with semantic search capabilities. Uses OpenAI embeddings by default.

    Usage:
        store = VectorStore()
        await store.initialize()

        # Add datapoints
        await store.add_datapoints([datapoint1, datapoint2])

        # Search
        results = await store.search("sales data", top_k=5)

        # Delete
        await store.delete(["datapoint_id_1", "datapoint_id_2"])
    """

    def __init__(
        self,
        collection_name: str | None = None,
        persist_directory: str | Path | None = None,
        embedding_model: str | None = None,
        openai_api_key: str | None = None,
    ):
        """
        Initialize the vector store.

        Args:
            collection_name: Name of the Chroma collection (default from config)
            persist_directory: Directory for persistence (default from config)
            embedding_model: OpenAI embedding model (default from config)
            openai_api_key: OpenAI API key (default from config)
        """
        # Only load config if needed (allows tests to avoid config validation)
        if (
            collection_name is None
            or persist_directory is None
            or embedding_model is None
            or openai_api_key is None
        ):
            config = get_settings()
            self.collection_name = collection_name or config.chroma.collection_name
            self.persist_directory = Path(persist_directory or config.chroma.persist_dir)
            self.embedding_model = embedding_model or config.chroma.embedding_model
            self.openai_api_key = openai_api_key or config.llm.openai_api_key
        else:
            self.collection_name = collection_name
            self.persist_directory = Path(persist_directory)
            self.embedding_model = embedding_model
            self.openai_api_key = openai_api_key

        self.client: chromadb.ClientAPI | None = None
        self.collection: chromadb.Collection | None = None
        self.embedding_function: OpenAIEmbeddingFunction | None = None

        logger.info(
            f"VectorStore initialized: collection={self.collection_name}, "
            f"persist_dir={self.persist_directory}, embedding_model={self.embedding_model}"
        )

    async def initialize(self):
        """
        Initialize the Chroma client and collection.

        Must be called before using the vector store.
        Creates the persist directory if it doesn't exist.

        Raises:
            VectorStoreError: If initialization fails
        """
        try:
            # Ensure persist directory exists
            self.persist_directory.mkdir(parents=True, exist_ok=True)

            # Initialize Chroma client (sync, will wrap in to_thread)
            await asyncio.to_thread(self._init_client)

            logger.info("VectorStore initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize VectorStore: {e}")
            raise VectorStoreError(f"Initialization failed: {e}") from e

    def _init_client(self):
        """Initialize Chroma client (sync, called via to_thread)."""
        self.client = chromadb.PersistentClient(
            path=str(self.persist_directory),
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True,
            ),
        )

        # Create OpenAI embedding function
        self.embedding_function = OpenAIEmbeddingFunction(
            api_key=self.openai_api_key,
            model_name=self.embedding_model,
        )

        # Get or create collection with OpenAI embeddings
        self.collection = self.client.get_or_create_collection(
            name=self.collection_name,
            metadata={"hnsw:space": "cosine"},
            embedding_function=self.embedding_function,
        )

        logger.debug(
            f"Chroma collection '{self.collection_name}' ready "
            f"with {self.collection.count()} documents"
        )

    async def add_datapoints(
        self,
        datapoints: list[DataPoint],
        batch_size: int = 100,
    ) -> int:
        """
        Add DataPoints to the vector store.

        Generates embeddings for each DataPoint's searchable content and
        stores them with metadata.

        Args:
            datapoints: List of DataPoint objects to add
            batch_size: Number of datapoints to add per batch

        Returns:
            Number of datapoints successfully added

        Raises:
            VectorStoreError: If adding fails
        """
        if not self.collection:
            raise VectorStoreError("VectorStore not initialized. Call initialize() first.")

        if not datapoints:
            logger.warning("No datapoints to add")
            return 0

        try:
            total_added = 0
            unique_datapoints = list({dp.datapoint_id: dp for dp in datapoints}.values())

            # Process in batches
            for i in range(0, len(unique_datapoints), batch_size):
                batch = unique_datapoints[i : i + batch_size]

                # Prepare batch data
                ids = [dp.datapoint_id for dp in batch]
                documents = [self._create_document(dp) for dp in batch]
                metadatas = [self._create_metadata(dp) for dp in batch]

                # Add to Chroma (async wrapper)
                await asyncio.to_thread(
                    self.collection.upsert,
                    ids=ids,
                    documents=documents,
                    metadatas=metadatas,
                )

                total_added += len(batch)
                logger.debug(f"Upserted batch of {len(batch)} datapoints ({total_added} total)")

            logger.info(f"Successfully upserted {total_added} datapoints to vector store")
            return total_added

        except Exception as e:
            logger.error(f"Failed to add datapoints: {e}")
            raise VectorStoreError(f"Failed to add datapoints: {e}") from e

    async def search(
        self,
        query: str,
        top_k: int = 10,
        filter_metadata: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search for similar DataPoints using semantic search.

        Args:
            query: Search query text
            top_k: Number of results to return
            filter_metadata: Optional metadata filters (e.g., {"type": "Schema"})

        Returns:
            List of search results with datapoint_id, distance, metadata, and document

        Raises:
            VectorStoreError: If search fails
        """
        if not self.collection:
            raise VectorStoreError("VectorStore not initialized. Call initialize() first.")

        try:
            # Query Chroma (async wrapper)
            results = await asyncio.to_thread(
                self.collection.query,
                query_texts=[query],
                n_results=top_k,
                where=filter_metadata,
            )

            # Format results
            formatted_results = []
            if results["ids"] and results["ids"][0]:
                for i in range(len(results["ids"][0])):
                    formatted_results.append(
                        {
                            "datapoint_id": results["ids"][0][i],
                            "distance": results["distances"][0][i]
                            if results["distances"]
                            else None,
                            "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                            "document": results["documents"][0][i] if results["documents"] else "",
                        }
                    )

            logger.debug(f"Search for '{query}' returned {len(formatted_results)} results")
            return formatted_results

        except Exception as e:
            if self._is_recoverable_index_error(e):
                logger.warning(
                    "Vector index appears unavailable on disk; attempting automatic recovery."
                )
                recovered = await self._recover_from_storage_error()
                if recovered:
                    try:
                        results = await asyncio.to_thread(
                            self.collection.query,
                            query_texts=[query],
                            n_results=top_k,
                            where=filter_metadata,
                        )
                        formatted_results = []
                        if results["ids"] and results["ids"][0]:
                            for i in range(len(results["ids"][0])):
                                formatted_results.append(
                                    {
                                        "datapoint_id": results["ids"][0][i],
                                        "distance": results["distances"][0][i]
                                        if results["distances"]
                                        else None,
                                        "metadata": results["metadatas"][0][i]
                                        if results["metadatas"]
                                        else {},
                                        "document": results["documents"][0][i]
                                        if results["documents"]
                                        else "",
                                    }
                                )
                        logger.info(
                            "Vector search recovered after index repair (results=%s).",
                            len(formatted_results),
                        )
                        return formatted_results
                    except Exception as retry_error:
                        logger.warning("Vector search retry failed after recovery: %s", retry_error)
                logger.warning(
                    "Falling back to empty vector results due to recoverable index error: %s", e
                )
                return []
            logger.error(f"Search failed: {e}")
            raise VectorStoreError(f"Search failed: {e}") from e

    async def delete(self, datapoint_ids: list[str]) -> int:
        """
        Delete DataPoints from the vector store.

        Args:
            datapoint_ids: List of datapoint IDs to delete

        Returns:
            Number of datapoints deleted

        Raises:
            VectorStoreError: If deletion fails
        """
        if not self.collection:
            raise VectorStoreError("VectorStore not initialized. Call initialize() first.")

        if not datapoint_ids:
            logger.warning("No datapoint IDs to delete")
            return 0

        try:
            # Delete from Chroma (async wrapper)
            await asyncio.to_thread(
                self.collection.delete,
                ids=datapoint_ids,
            )

            logger.info(f"Deleted {len(datapoint_ids)} datapoints from vector store")
            return len(datapoint_ids)

        except Exception as e:
            logger.error(f"Failed to delete datapoints: {e}")
            raise VectorStoreError(f"Failed to delete datapoints: {e}") from e

    async def get_count(self) -> int:
        """
        Get the total number of DataPoints in the store.

        Returns:
            Number of datapoints

        Raises:
            VectorStoreError: If operation fails
        """
        if not self.collection:
            raise VectorStoreError("VectorStore not initialized. Call initialize() first.")

        try:
            count = await asyncio.to_thread(self.collection.count)
            return count

        except Exception as e:
            logger.error(f"Failed to get count: {e}")
            raise VectorStoreError(f"Failed to get count: {e}") from e

    async def list_datapoints(self, limit: int = 1000, offset: int = 0) -> list[dict[str, Any]]:
        """
        List DataPoints without embedding calls.

        Args:
            limit: Maximum number of datapoints to return
            offset: Offset for pagination

        Returns:
            List of datapoints with metadata
        """
        if not self.collection:
            raise VectorStoreError("VectorStore not initialized. Call initialize() first.")

        try:
            results = await asyncio.to_thread(
                self.collection.get,
                limit=limit,
                offset=offset,
                include=["metadatas"],
            )
            items = []
            ids = results.get("ids") or []
            metadatas = results.get("metadatas") or []
            for idx, datapoint_id in enumerate(ids):
                metadata = metadatas[idx] if idx < len(metadatas) else {}
                items.append(
                    {
                        "datapoint_id": datapoint_id,
                        "metadata": metadata,
                    }
                )
            return items
        except Exception as e:
            logger.error(f"Failed to list datapoints: {e}")
            raise VectorStoreError(f"Failed to list datapoints: {e}") from e

    async def clear(self):
        """
        Clear all DataPoints from the vector store.

        Warning: This removes all data from the collection.

        Raises:
            VectorStoreError: If operation fails
        """
        if not self.collection:
            raise VectorStoreError("VectorStore not initialized. Call initialize() first.")

        try:
            # Get all IDs and delete them
            results = await asyncio.to_thread(
                self.collection.get,
                limit=1000000,  # Large limit to get all
            )

            if results["ids"]:
                await asyncio.to_thread(
                    self.collection.delete,
                    ids=results["ids"],
                )
                logger.info(f"Cleared {len(results['ids'])} datapoints from vector store")
            else:
                logger.info("Vector store already empty")

        except Exception as e:
            logger.error(f"Failed to clear vector store: {e}")
            raise VectorStoreError(f"Failed to clear: {e}") from e

    def _is_recoverable_index_error(self, error: Exception) -> bool:
        """Return True for known on-disk index corruption/missing-segment errors."""
        message = str(error).lower()
        recoverable_markers = (
            "nothing found on disk",
            "hnsw segment reader",
            "no such file or directory",
            "segment metadata",
            "segment index",
            "segment not found",
        )
        return any(marker in message for marker in recoverable_markers)

    async def _recover_from_storage_error(self) -> bool:
        """
        Try to recover from broken/externally-reset vector persistence.

        Strategy:
        1) Re-open client/collection.
        2) If still broken, recreate collection and optionally repopulate from local datapoint files.
        """
        try:
            await asyncio.to_thread(self._init_client)
            await asyncio.to_thread(self.collection.count)
            return True
        except Exception as reconnect_error:
            logger.warning("Vector reconnect failed, trying full rebuild: %s", reconnect_error)

        try:
            if self.client is not None:
                try:
                    await asyncio.to_thread(
                        self.client.delete_collection,
                        name=self.collection_name,
                    )
                except Exception:
                    # Collection may already be missing/corrupted; continue rebuild path.
                    pass

            await asyncio.to_thread(self._init_client)

            datapoints_root = Path("datapoints")
            if not datapoints_root.exists():
                return True

            from backend.knowledge.datapoints import DataPointLoader

            loader = DataPointLoader()
            datapoints = loader.load_directory(datapoints_root, recursive=True, skip_errors=True)
            if datapoints:
                await self.add_datapoints(datapoints)
                logger.info(
                    "Rebuilt vector index from local datapoint files (%s).", len(datapoints)
                )
            return True
        except Exception as rebuild_error:
            logger.warning("Vector rebuild failed: %s", rebuild_error)
            return False

    def _create_document(self, datapoint: DataPoint) -> str:
        """
        Create searchable document text from a DataPoint.

        Combines name, description/purpose, and type-specific fields
        into a single searchable text.

        Args:
            datapoint: DataPoint object

        Returns:
            Document text for embedding
        """
        parts = [
            f"Name: {datapoint.name}",
            f"Type: {datapoint.type}",
        ]

        # Add type-specific fields
        if hasattr(datapoint, "business_purpose"):
            parts.append(f"Purpose: {datapoint.business_purpose}")
        if hasattr(datapoint, "key_columns") and datapoint.key_columns:
            column_summaries = []
            for col in datapoint.key_columns[:12]:
                meaning = col.business_meaning if col.business_meaning else ""
                segment = f"{col.name} ({col.type})"
                if meaning:
                    segment = f"{segment}: {meaning}"
                column_summaries.append(segment)
            if column_summaries:
                parts.append(f"Key Columns: {'; '.join(column_summaries)}")
        if hasattr(datapoint, "relationships") and datapoint.relationships:
            rels = [
                f"{rel.join_column}->{rel.target_table} ({rel.cardinality})"
                for rel in datapoint.relationships[:8]
            ]
            parts.append(f"Relationships: {'; '.join(rels)}")
        if hasattr(datapoint, "common_queries") and datapoint.common_queries:
            parts.append(f"Common Queries: {'; '.join(datapoint.common_queries[:5])}")
        if hasattr(datapoint, "gotchas") and datapoint.gotchas:
            parts.append(f"Gotchas: {'; '.join(datapoint.gotchas[:5])}")

        if hasattr(datapoint, "calculation"):
            parts.append(f"Calculation: {datapoint.calculation}")

        if hasattr(datapoint, "synonyms") and datapoint.synonyms:
            parts.append(f"Synonyms: {', '.join(datapoint.synonyms)}")

        if hasattr(datapoint, "business_rules") and datapoint.business_rules:
            parts.append(f"Rules: {'; '.join(datapoint.business_rules)}")

        if hasattr(datapoint, "table_name"):
            parts.append(f"Table: {datapoint.table_name}")

        if hasattr(datapoint, "schedule"):
            parts.append(f"Schedule: {datapoint.schedule}")
        if hasattr(datapoint, "data_freshness"):
            parts.append(f"Data Freshness: {datapoint.data_freshness}")
        if hasattr(datapoint, "target_tables") and datapoint.target_tables:
            parts.append(f"Target Tables: {', '.join(datapoint.target_tables[:10])}")
        if hasattr(datapoint, "dependencies") and datapoint.dependencies:
            parts.append(f"Dependencies: {', '.join(datapoint.dependencies[:10])}")

        if hasattr(datapoint, "sql_template"):
            parts.append(f"SQL Template: {datapoint.sql_template}")
        if hasattr(datapoint, "description") and datapoint.type == "Query":
            parts.append(f"Query Description: {datapoint.description}")
        if hasattr(datapoint, "parameters") and datapoint.parameters:
            param_list = [f"{name}: {p.type}" for name, p in datapoint.parameters.items()]
            parts.append(f"Parameters: {', '.join(param_list[:10])}")

        # Add tags
        if datapoint.tags:
            parts.append(f"Tags: {', '.join(datapoint.tags)}")

        return "\n".join(parts)

    def _create_metadata(self, datapoint: DataPoint) -> dict[str, Any]:
        """
        Create metadata dictionary for a DataPoint.

        Args:
            datapoint: DataPoint object

        Returns:
            Metadata dictionary
        """
        metadata = {
            "datapoint_id": datapoint.datapoint_id,
            "type": datapoint.type,
            "name": datapoint.name,
            "owner": datapoint.owner,
        }

        if datapoint.metadata:
            source_tier = datapoint.metadata.get("source_tier")
            source_path = datapoint.metadata.get("source_path")
            connection_id = datapoint.metadata.get("connection_id")
            lifecycle = datapoint.metadata.get("lifecycle")
            if source_tier:
                metadata["source_tier"] = str(source_tier)
            if source_path:
                metadata["source_path"] = str(source_path)
            if connection_id:
                metadata["connection_id"] = str(connection_id)
            if isinstance(lifecycle, dict):
                if lifecycle.get("version"):
                    metadata["lifecycle_version"] = str(lifecycle["version"])
                if lifecycle.get("reviewer"):
                    metadata["lifecycle_reviewer"] = str(lifecycle["reviewer"])
                if lifecycle.get("changed_by"):
                    metadata["lifecycle_changed_by"] = str(lifecycle["changed_by"])
                if lifecycle.get("changed_reason"):
                    metadata["lifecycle_changed_reason"] = str(lifecycle["changed_reason"])
                if lifecycle.get("changed_at"):
                    metadata["lifecycle_changed_at"] = str(lifecycle["changed_at"])

        # Add type-specific metadata
        if hasattr(datapoint, "table_name"):
            metadata["table_name"] = datapoint.table_name
            metadata["schema"] = datapoint.schema_name
        if hasattr(datapoint, "business_purpose") and datapoint.business_purpose:
            metadata["business_purpose"] = datapoint.business_purpose
        if hasattr(datapoint, "key_columns") and datapoint.key_columns:
            metadata["key_columns"] = json.dumps(
                [
                    {
                        "name": col.name,
                        "type": col.type,
                        "business_meaning": col.business_meaning,
                    }
                    for col in datapoint.key_columns[:20]
                ]
            )
        if hasattr(datapoint, "relationships") and datapoint.relationships:
            metadata["relationships"] = json.dumps(
                [
                    {
                        "target_table": rel.target_table,
                        "join_column": rel.join_column,
                        "cardinality": rel.cardinality,
                    }
                    for rel in datapoint.relationships[:20]
                ]
            )
        if hasattr(datapoint, "common_queries") and datapoint.common_queries:
            metadata["common_queries"] = json.dumps(datapoint.common_queries[:20])
        if hasattr(datapoint, "gotchas") and datapoint.gotchas:
            metadata["gotchas"] = json.dumps(datapoint.gotchas[:20])
        if hasattr(datapoint, "freshness") and datapoint.freshness:
            metadata["freshness"] = datapoint.freshness

        if hasattr(datapoint, "related_tables") and datapoint.related_tables:
            metadata["related_tables"] = ",".join(datapoint.related_tables)
        if hasattr(datapoint, "calculation") and datapoint.calculation:
            metadata["calculation"] = datapoint.calculation
        if hasattr(datapoint, "synonyms") and datapoint.synonyms:
            metadata["synonyms"] = json.dumps(datapoint.synonyms[:20])
        if hasattr(datapoint, "business_rules") and datapoint.business_rules:
            metadata["business_rules"] = json.dumps(datapoint.business_rules[:20])
        if hasattr(datapoint, "unit") and datapoint.unit:
            metadata["unit"] = datapoint.unit
        if hasattr(datapoint, "aggregation") and datapoint.aggregation:
            metadata["aggregation"] = datapoint.aggregation

        if hasattr(datapoint, "schedule") and datapoint.schedule:
            metadata["schedule"] = datapoint.schedule
        if hasattr(datapoint, "data_freshness") and datapoint.data_freshness:
            metadata["data_freshness"] = datapoint.data_freshness
        if hasattr(datapoint, "target_tables") and datapoint.target_tables:
            metadata["target_tables"] = ",".join(datapoint.target_tables)
        if hasattr(datapoint, "dependencies") and datapoint.dependencies:
            metadata["dependencies"] = ",".join(datapoint.dependencies)

        if hasattr(datapoint, "sql_template") and datapoint.sql_template:
            metadata["sql_template"] = datapoint.sql_template
        if hasattr(datapoint, "parameters") and datapoint.parameters:
            metadata["parameters"] = json.dumps(
                {
                    name: {
                        "type": p.type,
                        "required": p.required,
                        "default": p.default,
                        "description": p.description,
                    }
                    for name, p in datapoint.parameters.items()
                }
            )
        if (
            hasattr(datapoint, "description")
            and datapoint.type == "Query"
            and datapoint.description
        ):
            metadata["query_description"] = datapoint.description
        if hasattr(datapoint, "backend_variants") and datapoint.backend_variants:
            metadata["backend_variants"] = json.dumps(datapoint.backend_variants)
        if hasattr(datapoint, "validation") and datapoint.validation:
            metadata["validation"] = json.dumps(datapoint.validation)
        if hasattr(datapoint, "related_tables") and datapoint.related_tables:
            metadata["related_tables"] = ",".join(datapoint.related_tables)

        if datapoint.tags:
            metadata["tags"] = ",".join(datapoint.tags)

        return metadata

"""
Knowledge Retriever

Combines VectorStore and KnowledgeGraph for unified contextual retrieval.
Supports local (vector), global (graph), and hybrid retrieval modes.
"""

import logging
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from backend.knowledge.graph import KnowledgeGraph
from backend.knowledge.vectors import VectorStore

logger = logging.getLogger(__name__)


class RetrievalMode(StrEnum):
    """Retrieval modes for context gathering."""

    LOCAL = "local"  # Vector search only (semantic similarity)
    GLOBAL = "global"  # Graph traversal only (structural relationships)
    HYBRID = "hybrid"  # Both vector and graph (combined with RRF)


class RetrievedItem(BaseModel):
    """A single retrieved item with metadata."""

    datapoint_id: str = Field(..., description="DataPoint identifier")
    score: float = Field(..., description="Relevance score (0-1, higher is better)")
    source: str = Field(..., description="Retrieval source (vector/graph/hybrid)")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    content: str | None = Field(None, description="Retrieved content/document")


class RetrievalResult(BaseModel):
    """Result from retrieval operation."""

    items: list[RetrievedItem] = Field(
        default_factory=list, description="Retrieved items ranked by relevance"
    )
    total_count: int = Field(..., description="Total number of items retrieved")
    mode: RetrievalMode = Field(..., description="Retrieval mode used")
    query: str = Field(..., description="Original query")
    trace: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured retrieval diagnostics and candidate traces.",
    )


class RetrieverError(Exception):
    """Raised when retrieval operations fail."""

    pass


class Retriever:
    """
    Unified retriever combining vector search and graph traversal.

    Supports three retrieval modes:
    - Local: Semantic search via vector embeddings
    - Global: Structural search via knowledge graph
    - Hybrid: Combined search with RRF ranking

    Usage:
        retriever = Retriever(vector_store, knowledge_graph)

        # Local mode (vector search)
        result = await retriever.retrieve("revenue metrics", mode="local", top_k=5)

        # Global mode (graph traversal)
        result = await retriever.retrieve(
            "metric_revenue_001",
            mode="global",
            top_k=10
        )

        # Hybrid mode (combined)
        result = await retriever.retrieve(
            "sales data analysis",
            mode="hybrid",
            top_k=10
        )
    """

    def __init__(
        self,
        vector_store: VectorStore,
        knowledge_graph: KnowledgeGraph,
        rrf_k: int = 60,
    ):
        """
        Initialize the retriever.

        Args:
            vector_store: VectorStore instance for semantic search
            knowledge_graph: KnowledgeGraph instance for structural search
            rrf_k: RRF constant (default: 60, standard value from literature)
        """
        self.vector_store = vector_store
        self.knowledge_graph = knowledge_graph
        self.rrf_k = rrf_k
        self._source_tier_priority = {
            "user": 4,
            "managed": 3,
            "custom": 2,
            "unknown": 2,
            "example": 1,
        }
        self._precedence_pool_multiplier = 3

        logger.info(f"Retriever initialized with RRF k={rrf_k}")

    async def retrieve(
        self,
        query: str,
        mode: RetrievalMode = RetrievalMode.HYBRID,
        top_k: int = 10,
        vector_top_k: int | None = None,
        graph_top_k: int | None = None,
        graph_max_depth: int = 2,
        metadata_filter: dict[str, Any] | None = None,
    ) -> RetrievalResult:
        """
        Retrieve relevant DataPoints based on query.

        Args:
            query: Search query (text for local/hybrid, node ID for global)
            mode: Retrieval mode (local/global/hybrid)
            top_k: Number of results to return
            vector_top_k: Top-k for vector search (default: top_k * 2 for hybrid)
            graph_top_k: Top-k for graph search (default: top_k * 2 for hybrid)
            graph_max_depth: Max graph traversal depth (default: 2)
            metadata_filter: Optional metadata filter for vector search

        Returns:
            RetrievalResult with ranked items

        Raises:
            RetrieverError: If retrieval fails
        """
        try:
            retrieval_top_k = max(top_k, top_k * self._precedence_pool_multiplier)
            if mode == RetrievalMode.LOCAL:
                items, mode_trace = await self._retrieve_local(query, retrieval_top_k, metadata_filter)
            elif mode == RetrievalMode.GLOBAL:
                items, mode_trace = await self._retrieve_global(query, retrieval_top_k, graph_max_depth)
            elif mode == RetrievalMode.HYBRID:
                items, mode_trace = await self._retrieve_hybrid(
                    query,
                    retrieval_top_k,
                    vector_top_k or retrieval_top_k * 2,
                    graph_top_k or retrieval_top_k * 2,
                    graph_max_depth,
                    metadata_filter,
                )
            else:
                raise RetrieverError(f"Unknown retrieval mode: {mode}")

            prioritized_items, precedence_trace = self._apply_source_precedence(items)
            final_items = prioritized_items[:top_k]

            logger.info(
                "Retrieved %s items using %s mode for query: '%s...'",
                len(final_items),
                mode,
                query[:50],
            )

            return RetrievalResult(
                items=final_items,
                total_count=len(final_items),
                mode=mode,
                query=query,
                trace={
                    "mode": str(mode),
                    "requested_top_k": top_k,
                    "retrieval_pool_top_k": retrieval_top_k,
                    "metadata_filter": metadata_filter or {},
                    "candidate_counts": {
                        "initial": len(items),
                        "after_precedence": len(prioritized_items),
                        "final": len(final_items),
                    },
                    **mode_trace,
                    "precedence": precedence_trace,
                    "final_selected": [self._item_to_trace_payload(item) for item in final_items],
                },
            )

        except Exception as e:
            logger.error(f"Retrieval failed: {e}")
            raise RetrieverError(f"Retrieval failed: {e}") from e

    async def _retrieve_local(
        self, query: str, top_k: int, metadata_filter: dict[str, Any] | None = None
    ) -> tuple[list[RetrievedItem], dict[str, Any]]:
        """Retrieve using vector search only."""
        vector_results = await self.vector_store.search(
            query, top_k=top_k, filter_metadata=metadata_filter
        )

        items = []
        trace_candidates: list[dict[str, Any]] = []
        for result in vector_results:
            # Convert distance to similarity score (cosine distance: lower is better)
            # Score = 1 / (1 + distance), normalized to 0-1 range
            distance = result["distance"]
            score = 1.0 / (1.0 + abs(distance))
            metadata = self._enrich_with_graph_metadata(
                result["datapoint_id"],
                result["metadata"],
            )

            items.append(
                RetrievedItem(
                    datapoint_id=result["datapoint_id"],
                    score=score,
                    source="vector",
                    metadata=metadata,
                    content=result.get("document"),
                )
            )
            trace_candidates.append(
                {
                    "datapoint_id": result["datapoint_id"],
                    "distance": distance,
                    "score": score,
                    "source": "vector",
                    "name": metadata.get("name", result["datapoint_id"]),
                    "source_tier": metadata.get("source_tier"),
                    "rank": len(trace_candidates) + 1,
                }
            )

        logger.debug(f"Vector search returned {len(items)} items")
        return items, {"vector_candidates": trace_candidates}

    async def _retrieve_global(
        self, node_id: str, top_k: int, max_depth: int
    ) -> tuple[list[RetrievedItem], dict[str, Any]]:
        """Retrieve using graph traversal only."""
        # Get related nodes from graph
        related_nodes = self.knowledge_graph.get_related(node_id, max_depth=max_depth)

        items = []
        trace_candidates: list[dict[str, Any]] = []
        for node in related_nodes[:top_k]:
            # Convert distance to score (graph distance: 1 is closest)
            # Score = 1 / distance, normalized
            distance = node["distance"]
            score = 1.0 / distance if distance > 0 else 1.0

            items.append(
                RetrievedItem(
                    datapoint_id=node["node_id"],
                    score=score,
                    source="graph",
                    metadata=node["metadata"],
                    content=None,  # Graph doesn't have document content
                )
            )
            trace_candidates.append(
                {
                    "datapoint_id": node["node_id"],
                    "distance": distance,
                    "score": score,
                    "source": "graph",
                    "name": node.get("metadata", {}).get("name", node["node_id"]),
                    "source_tier": node.get("metadata", {}).get("source_tier"),
                    "rank": len(trace_candidates) + 1,
                }
            )

        logger.debug(f"Graph traversal returned {len(items)} items")
        return items, {"graph_candidates": trace_candidates}

    async def _retrieve_hybrid(
        self,
        query: str,
        top_k: int,
        vector_top_k: int,
        graph_top_k: int,
        graph_max_depth: int,
        metadata_filter: dict[str, Any] | None = None,
    ) -> tuple[list[RetrievedItem], dict[str, Any]]:
        """Retrieve using both vector and graph, combined with RRF."""
        # Get vector results
        vector_results = await self.vector_store.search(
            query, top_k=vector_top_k, filter_metadata=metadata_filter
        )

        vector_items = {}
        vector_trace_candidates: list[dict[str, Any]] = []
        for rank, result in enumerate(vector_results, start=1):
            datapoint_id = result["datapoint_id"]
            distance = result["distance"]
            score = 1.0 / (1.0 + abs(distance))
            metadata = self._enrich_with_graph_metadata(
                datapoint_id,
                result["metadata"],
            )

            vector_items[datapoint_id] = {
                "rank": rank,
                "score": score,
                "metadata": metadata,
                "content": result.get("document"),
            }
            vector_trace_candidates.append(
                {
                    "datapoint_id": datapoint_id,
                    "rank": rank,
                    "distance": distance,
                    "score": score,
                    "name": metadata.get("name", datapoint_id),
                    "source_tier": metadata.get("source_tier"),
                }
            )

        # Get graph results by trying vector results as seed nodes
        graph_items = {}
        graph_trace_candidates: list[dict[str, Any]] = []
        seed_node_used: str | None = None
        graph_fallback_reason: str | None = None
        if vector_results:
            # Try each vector result as a seed until we get graph results
            # This handles cases where top results aren't in the graph
            for vector_result in vector_results:
                seed_node = vector_result["datapoint_id"]

                try:
                    related_nodes = self.knowledge_graph.get_related(
                        seed_node, max_depth=graph_max_depth
                    )

                    # Successfully got results, build graph items
                    seed_node_used = seed_node
                    for rank, node in enumerate(related_nodes[:graph_top_k], start=1):
                        datapoint_id = node["node_id"]
                        distance = node["distance"]
                        score = 1.0 / distance if distance > 0 else 1.0

                        graph_items[datapoint_id] = {
                            "rank": rank,
                            "score": score,
                            "metadata": node["metadata"],
                            "content": None,
                        }
                        graph_trace_candidates.append(
                            {
                                "datapoint_id": datapoint_id,
                                "rank": rank,
                                "distance": distance,
                                "score": score,
                                "name": node["metadata"].get("name", datapoint_id),
                                "source_tier": node["metadata"].get("source_tier"),
                            }
                        )

                    # Successfully retrieved graph results, stop trying
                    logger.debug(f"Graph seeded from node: {seed_node}")
                    break

                except Exception as e:
                    # This seed node failed (not in graph or error), try next
                    logger.debug(f"Seed node {seed_node} failed: {e}")
                    continue

            # If we exhausted all seeds without success, log warning
            if not graph_items:
                try:
                    graph_stats_raw = self.knowledge_graph.get_stats()
                    graph_stats = graph_stats_raw if isinstance(graph_stats_raw, dict) else {}
                except Exception:  # noqa: BLE001
                    graph_stats = {}
                total_nodes = int(graph_stats.get("total_nodes", 0) or 0)
                if total_nodes == 0:
                    graph_fallback_reason = "empty_graph"
                    logger.info(
                        "Knowledge graph is empty; using vector-only retrieval for query '%s...'",
                        query[:60],
                    )
                else:
                    graph_fallback_reason = "seed_traversal_failed"
                    logger.warning(
                        f"All {len(vector_results)} seed candidates failed graph traversal, "
                        "using vector-only results"
                    )

        # Apply RRF (Reciprocal Rank Fusion)
        rrf_scores = self._apply_rrf(vector_items, graph_items)

        # Deduplicate and create final items
        items = []
        seen_ids: set[str] = set()

        for datapoint_id, rrf_score in sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)[
            :top_k
        ]:
            if datapoint_id in seen_ids:
                continue
            seen_ids.add(datapoint_id)

            # Get metadata from either source
            metadata = self._merge_metadata(
                graph_items.get(datapoint_id, {}).get("metadata", {}),
                vector_items.get(datapoint_id, {}).get("metadata", {}),
            )
            content = vector_items.get(datapoint_id, {}).get("content")

            # Determine source
            in_vector = datapoint_id in vector_items
            in_graph = datapoint_id in graph_items
            source = "hybrid" if (in_vector and in_graph) else ("vector" if in_vector else "graph")

            items.append(
                RetrievedItem(
                    datapoint_id=datapoint_id,
                    score=rrf_score,
                    source=source,
                    metadata=metadata,
                    content=content,
                )
            )

        logger.debug(
            f"Hybrid retrieval: {len(vector_items)} vector + {len(graph_items)} graph "
            f"→ {len(items)} final items"
        )

        rrf_trace_candidates: list[dict[str, Any]] = []
        for datapoint_id, rrf_score in sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True):
            rrf_trace_candidates.append(
                {
                    "datapoint_id": datapoint_id,
                    "rrf_score": rrf_score,
                    "vector_rank": vector_items.get(datapoint_id, {}).get("rank"),
                    "graph_rank": graph_items.get(datapoint_id, {}).get("rank"),
                    "source": (
                        "hybrid"
                        if datapoint_id in vector_items and datapoint_id in graph_items
                        else ("vector" if datapoint_id in vector_items else "graph")
                    ),
                    "name": (
                        graph_items.get(datapoint_id, {}).get("metadata", {}).get("name")
                        or vector_items.get(datapoint_id, {}).get("metadata", {}).get("name")
                        or datapoint_id
                    ),
                }
            )

        return items, {
            "vector_candidates": vector_trace_candidates,
            "graph_candidates": graph_trace_candidates,
            "rrf_candidates": rrf_trace_candidates,
            "graph_seed_node": seed_node_used,
            "graph_fallback_reason": graph_fallback_reason,
        }

    def _enrich_with_graph_metadata(
        self,
        datapoint_id: str,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Merge vector metadata with graph node metadata when available."""
        base = metadata if isinstance(metadata, dict) else {}
        graph_metadata = self.knowledge_graph.get_node_metadata(datapoint_id) or {}
        return self._merge_metadata(graph_metadata, base)

    @staticmethod
    def _merge_metadata(
        primary: dict[str, Any] | None,
        secondary: dict[str, Any] | None,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        if isinstance(primary, dict):
            merged.update(primary)
        if isinstance(secondary, dict):
            merged.update(secondary)
        return merged

    def _apply_rrf(
        self,
        vector_items: dict[str, dict[str, Any]],
        graph_items: dict[str, dict[str, Any]],
    ) -> dict[str, float]:
        """
        Apply Reciprocal Rank Fusion (RRF) to combine rankings.

        RRF formula: score(d) = Σ 1 / (k + rank(d))
        where k is a constant (typically 60) and rank(d) is the rank of document d.

        Args:
            vector_items: Dict of {datapoint_id: {rank, score, ...}}
            graph_items: Dict of {datapoint_id: {rank, score, ...}}

        Returns:
            Dict of {datapoint_id: rrf_score}
        """
        rrf_scores = {}

        # Get all unique datapoint IDs
        all_ids = set(vector_items.keys()) | set(graph_items.keys())

        for datapoint_id in all_ids:
            rrf_score = 0.0

            # Add vector contribution
            if datapoint_id in vector_items:
                vector_rank = vector_items[datapoint_id]["rank"]
                rrf_score += 1.0 / (self.rrf_k + vector_rank)

            # Add graph contribution
            if datapoint_id in graph_items:
                graph_rank = graph_items[datapoint_id]["rank"]
                rrf_score += 1.0 / (self.rrf_k + graph_rank)

            rrf_scores[datapoint_id] = rrf_score

        return rrf_scores

    def _apply_source_precedence(
        self, items: list[RetrievedItem]
    ) -> tuple[list[RetrievedItem], dict[str, Any]]:
        """
        Resolve conflicting DataPoints by source tier, then keep rank-based ordering.

        Conflict resolution:
        - If multiple items map to the same table/metric conflict key, keep the item from the
          highest-precedence source tier (`user` > `managed` > `custom`/`unknown` > `example`).
        - If source tiers are equal, keep the higher score item.
        - Items without a conflict key are kept as-is.
        """
        if not items:
            return [], {"kept": [], "filtered_out": []}

        resolved_by_key: dict[str, RetrievedItem] = {}
        passthrough: list[RetrievedItem] = []
        filtered_out: list[dict[str, Any]] = []

        for item in items:
            conflict_key = self._build_conflict_key(item)
            if not conflict_key:
                passthrough.append(item)
                continue

            existing = resolved_by_key.get(conflict_key)
            if existing is None:
                resolved_by_key[conflict_key] = item
                continue

            existing_priority = self._source_priority(existing.metadata)
            candidate_priority = self._source_priority(item.metadata)
            if candidate_priority > existing_priority:
                filtered_out.append(
                    {
                        "datapoint_id": existing.datapoint_id,
                        "reason": "source_precedence_conflict",
                        "conflict_key": conflict_key,
                        "kept_datapoint_id": item.datapoint_id,
                    }
                )
                resolved_by_key[conflict_key] = item
                continue
            if candidate_priority == existing_priority and item.score > existing.score:
                filtered_out.append(
                    {
                        "datapoint_id": existing.datapoint_id,
                        "reason": "lower_score_same_precedence",
                        "conflict_key": conflict_key,
                        "kept_datapoint_id": item.datapoint_id,
                    }
                )
                resolved_by_key[conflict_key] = item
                continue
            filtered_out.append(
                {
                    "datapoint_id": item.datapoint_id,
                    "reason": (
                        "lower_source_precedence"
                        if candidate_priority < existing_priority
                        else "lower_score_same_precedence"
                    ),
                    "conflict_key": conflict_key,
                    "kept_datapoint_id": existing.datapoint_id,
                }
            )

        combined = passthrough + list(resolved_by_key.values())
        combined.sort(
            key=lambda entry: (
                entry.score,
                self._source_priority(entry.metadata),
            ),
            reverse=True,
        )
        return combined, {
            "kept": [self._item_to_trace_payload(item) for item in combined],
            "filtered_out": filtered_out,
        }

    @staticmethod
    def _item_to_trace_payload(item: RetrievedItem) -> dict[str, Any]:
        metadata = item.metadata if isinstance(item.metadata, dict) else {}
        return {
            "datapoint_id": item.datapoint_id,
            "name": metadata.get("name", item.datapoint_id),
            "score": item.score,
            "source": item.source,
            "source_tier": metadata.get("source_tier"),
            "conflict_key": None,
        }

    def _build_conflict_key(self, item: RetrievedItem) -> str | None:
        """Build a coarse conflict key for precedence handling."""
        metadata = item.metadata if isinstance(item.metadata, dict) else {}

        table_name = (
            metadata.get("table_name") or metadata.get("table") or metadata.get("table_key")
        )
        schema_name = metadata.get("schema") or metadata.get("schema_name")
        if table_name:
            table_key = str(table_name).strip().lower()
            if "." not in table_key and schema_name:
                table_key = f"{str(schema_name).strip().lower()}.{table_key}"
            return f"table::{table_key}"

        related_tables = self._coerce_string_list(metadata.get("related_tables"))
        metric_name = str(metadata.get("name", "")).strip().lower()
        dp_type = str(metadata.get("type", "")).strip().lower()
        if metric_name and related_tables:
            return f"metric::{metric_name}::{'|'.join(sorted(related_tables))}"
        if dp_type == "query" and metric_name:
            return (
                f"query::{metric_name}::{'|'.join(sorted(related_tables))}"
                if related_tables
                else None
            )
        return None

    def _source_priority(self, metadata: dict[str, Any] | None) -> int:
        tier = self._source_tier(metadata)
        return self._source_tier_priority.get(tier, self._source_tier_priority["unknown"])

    def _source_tier(self, metadata: dict[str, Any] | None) -> str:
        if not isinstance(metadata, dict):
            return "unknown"
        raw = metadata.get("source_tier")
        if not isinstance(raw, str):
            return "unknown"
        normalized = raw.strip().lower()
        if not normalized:
            return "unknown"
        return normalized

    @staticmethod
    def _coerce_string_list(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip().lower() for item in value if str(item).strip()]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if "," in stripped:
                return [part.strip().lower() for part in stripped.split(",") if part.strip()]
            return [stripped.lower()]
        return []

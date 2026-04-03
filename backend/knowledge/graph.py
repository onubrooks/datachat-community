"""
Knowledge Graph

NetworkX-based knowledge graph for DataPoint relationships.
Supports tables, columns, metrics, queries, and process dependencies.
"""

import json
import logging
import re
from enum import StrEnum
from pathlib import Path
from typing import Any

import networkx as nx
from networkx.readwrite import json_graph

from backend.models.datapoint import (
    BusinessDataPoint,
    DataPoint,
    ProcessDataPoint,
    QueryDataPoint,
    SchemaDataPoint,
)

logger = logging.getLogger(__name__)


class NodeType(StrEnum):
    """Types of nodes in the knowledge graph."""

    TABLE = "table"
    COLUMN = "column"
    METRIC = "metric"
    QUERY = "query"
    PROCESS = "process"
    GLOSSARY = "glossary"


class EdgeType(StrEnum):
    """Types of edges in the knowledge graph."""

    BELONGS_TO = "belongs_to"  # Column -> Table
    JOINS_WITH = "joins_with"  # Table -> Table
    CALCULATES = "calculates"  # Metric -> Table
    QUERIES = "queries"  # Query -> Table
    USES = "uses"  # Process -> Table
    SYNONYMOUS = "synonymous"  # Metric -> Metric (same concept)
    DEPENDS_ON = "depends_on"  # Process -> Process


class KnowledgeGraphError(Exception):
    """Raised when knowledge graph operations fail."""

    pass


class KnowledgeGraph:
    """
    Knowledge graph for DataPoint relationships using NetworkX.

    Builds a directed graph of tables, columns, metrics, queries, and processes
    with their relationships for contextual retrieval.

    Usage:
        graph = KnowledgeGraph()
        graph.add_datapoint(schema_datapoint)
        graph.add_datapoint(business_datapoint)

        # Get related nodes
        related = graph.get_related("table_fact_sales_001", max_depth=2)

        # Find join path
        path = graph.find_path("table_sales_001", "table_customers_001")

        # Save/load
        graph.save_to_file("knowledge_graph.json")
        graph.load_from_file("knowledge_graph.json")
    """

    def __init__(self):
        """Initialize an empty knowledge graph."""
        self.graph = nx.DiGraph()
        self._datapoint_count = 0
        self._node_count = 0
        self._edge_count = 0

        logger.info("KnowledgeGraph initialized")

    def add_datapoint(self, datapoint: DataPoint) -> int:
        """
        Add a DataPoint to the knowledge graph.

        Creates appropriate nodes and edges based on DataPoint type.

        Args:
            datapoint: DataPoint to add (Schema, Business, Process, or Query)

        Returns:
            Number of nodes added

        Raises:
            KnowledgeGraphError: If adding fails
        """
        try:
            nodes_added = 0

            if isinstance(datapoint, SchemaDataPoint):
                nodes_added = self._add_schema_datapoint(datapoint)
            elif isinstance(datapoint, BusinessDataPoint):
                nodes_added = self._add_business_datapoint(datapoint)
            elif isinstance(datapoint, ProcessDataPoint):
                nodes_added = self._add_process_datapoint(datapoint)
            elif isinstance(datapoint, QueryDataPoint):
                nodes_added = self._add_query_datapoint(datapoint)
            else:
                raise KnowledgeGraphError(f"Unknown DataPoint type: {type(datapoint)}")

            self._datapoint_count += 1
            logger.debug(
                f"Added {datapoint.type} datapoint '{datapoint.datapoint_id}' ({nodes_added} nodes)"
            )

            return nodes_added

        except Exception as e:
            logger.error(f"Failed to add datapoint '{datapoint.datapoint_id}': {e}")
            raise KnowledgeGraphError(
                f"Failed to add datapoint '{datapoint.datapoint_id}': {e}"
            ) from e

    def _add_schema_datapoint(self, datapoint: SchemaDataPoint) -> int:
        """Add Schema DataPoint (table and columns)."""
        nodes_added = 0

        # Add table node
        table_id = datapoint.datapoint_id
        self.graph.add_node(
            table_id,
            node_type=NodeType.TABLE,
            name=datapoint.name,
            table_name=datapoint.table_name,
            schema=datapoint.schema_name,
            business_purpose=datapoint.business_purpose,
            owner=datapoint.owner,
            tags=datapoint.tags or [],
            source_tier=(datapoint.metadata or {}).get("source_tier"),
            source_path=(datapoint.metadata or {}).get("source_path"),
            connection_id=(datapoint.metadata or {}).get("connection_id"),
            freshness=getattr(datapoint, "freshness", None),
            row_count=getattr(datapoint, "row_count", None),
        )
        nodes_added += 1

        # Add column nodes
        if datapoint.key_columns:
            for col in datapoint.key_columns:
                col_id = f"{table_id}__col__{col.name}"
                self.graph.add_node(
                    col_id,
                    node_type=NodeType.COLUMN,
                    name=col.name,
                    column_type=col.type,
                    business_meaning=col.business_meaning,
                    nullable=col.nullable,
                    parent_table=table_id,
                )
                # Edge: Column -> Table
                self.graph.add_edge(col_id, table_id, edge_type=EdgeType.BELONGS_TO, weight=1.0)
                nodes_added += 1

        # Add relationship edges (foreign keys)
        if datapoint.relationships:
            for rel in datapoint.relationships:
                target_table_id = self._find_table_by_name(rel.target_table)
                if target_table_id:
                    # Edge: Table -> Table (join)
                    self.graph.add_edge(
                        table_id,
                        target_table_id,
                        edge_type=EdgeType.JOINS_WITH,
                        join_column=rel.join_column,
                        cardinality=rel.cardinality,
                        weight=0.8,
                    )

        return nodes_added

    def _add_business_datapoint(self, datapoint: BusinessDataPoint) -> int:
        """Add Business DataPoint (metric)."""
        nodes_added = 0

        # Add metric node
        metric_id = datapoint.datapoint_id
        self.graph.add_node(
            metric_id,
            node_type=NodeType.METRIC,
            name=datapoint.name,
            calculation=datapoint.calculation,
            synonyms=datapoint.synonyms or [],
            business_rules=datapoint.business_rules or [],
            owner=datapoint.owner,
            tags=datapoint.tags or [],
            source_tier=(datapoint.metadata or {}).get("source_tier"),
            source_path=(datapoint.metadata or {}).get("source_path"),
            connection_id=(datapoint.metadata or {}).get("connection_id"),
        )
        nodes_added += 1

        # Add edges to related tables
        if datapoint.related_tables:
            for table_name in datapoint.related_tables:
                table_id = self._find_table_by_name(table_name)
                if table_id:
                    # Edge: Metric -> Table (calculates from)
                    self.graph.add_edge(
                        metric_id,
                        table_id,
                        edge_type=EdgeType.CALCULATES,
                        weight=0.9,
                    )

        # Add synonym edges (connect metrics with similar names)
        if datapoint.synonyms:
            for existing_metric in self._find_metrics_by_synonyms(datapoint.synonyms):
                if existing_metric != metric_id:
                    # Bidirectional synonym edge
                    self.graph.add_edge(
                        metric_id,
                        existing_metric,
                        edge_type=EdgeType.SYNONYMOUS,
                        weight=0.7,
                    )
                    self.graph.add_edge(
                        existing_metric,
                        metric_id,
                        edge_type=EdgeType.SYNONYMOUS,
                        weight=0.7,
                    )

        return nodes_added

    def _add_process_datapoint(self, datapoint: ProcessDataPoint) -> int:
        """Add Process DataPoint (ETL process)."""
        nodes_added = 0

        # Add process node
        process_id = datapoint.datapoint_id
        self.graph.add_node(
            process_id,
            node_type=NodeType.PROCESS,
            name=datapoint.name,
            schedule=datapoint.schedule,
            data_freshness=datapoint.data_freshness,
            owner=datapoint.owner,
            tags=datapoint.tags or [],
            source_tier=(datapoint.metadata or {}).get("source_tier"),
            source_path=(datapoint.metadata or {}).get("source_path"),
            connection_id=(datapoint.metadata or {}).get("connection_id"),
        )
        nodes_added += 1

        # Add edges to target tables
        if datapoint.target_tables:
            for table_name in datapoint.target_tables:
                table_id = self._find_table_by_name(table_name)
                if table_id:
                    # Edge: Process -> Table (populates)
                    self.graph.add_edge(process_id, table_id, edge_type=EdgeType.USES, weight=0.85)

        # Add edges to dependencies
        if datapoint.dependencies:
            for dep_name in datapoint.dependencies:
                # Dependencies can be tables or other processes
                # First try to find as a table by name
                dep_id = self._find_table_by_name(dep_name)

                # If not a table, try to find as a process by ID
                if not dep_id:
                    dep_id = self._find_process_by_id(dep_name)

                if dep_id:
                    self.graph.add_edge(
                        process_id, dep_id, edge_type=EdgeType.DEPENDS_ON, weight=0.9
                    )

        return nodes_added

    def _add_query_datapoint(self, datapoint: QueryDataPoint) -> int:
        """Add Query DataPoint (reusable SQL template)."""
        nodes_added = 0

        query_id = datapoint.datapoint_id
        self.graph.add_node(
            query_id,
            node_type=NodeType.QUERY,
            name=datapoint.name,
            description=datapoint.description,
            sql_template=datapoint.sql_template,
            parameter_names=sorted(datapoint.parameters.keys()),
            related_tables=datapoint.related_tables or [],
            owner=datapoint.owner,
            tags=datapoint.tags or [],
            source_tier=(datapoint.metadata or {}).get("source_tier"),
            source_path=(datapoint.metadata or {}).get("source_path"),
            connection_id=(datapoint.metadata or {}).get("connection_id"),
        )
        nodes_added += 1

        # Add edges to related tables.
        if datapoint.related_tables:
            for table_name in datapoint.related_tables:
                table_id = self._find_table_by_name(table_name)
                if table_id:
                    self.graph.add_edge(query_id, table_id, edge_type=EdgeType.USES, weight=0.88)

        return nodes_added

    def get_related(
        self,
        node_id: str,
        max_depth: int = 2,
        edge_types: list[EdgeType] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get related nodes within max_depth hops.

        Args:
            node_id: Starting node ID
            max_depth: Maximum traversal depth (default: 2)
            edge_types: Optional filter for edge types

        Returns:
            List of related nodes with metadata and distance

        Raises:
            KnowledgeGraphError: If node doesn't exist
        """
        if node_id not in self.graph:
            raise KnowledgeGraphError(f"Node '{node_id}' not found in graph")

        related_nodes = []
        visited = {node_id}

        # BFS traversal
        queue = [(node_id, 0)]  # (node, depth)

        while queue:
            current_node, depth = queue.pop(0)

            if depth >= max_depth:
                continue

            # Get neighbors (both incoming and outgoing edges)
            successors = list(self.graph.successors(current_node))
            predecessors = list(self.graph.predecessors(current_node))

            for neighbor in successors + predecessors:
                if neighbor in visited:
                    continue

                # Get edge data
                edge_data = None
                if self.graph.has_edge(current_node, neighbor):
                    edge_data = self.graph[current_node][neighbor]
                else:
                    edge_data = self.graph[neighbor][current_node]

                # Filter by edge type if specified
                if edge_types and edge_data.get("edge_type") not in edge_types:
                    continue

                visited.add(neighbor)
                queue.append((neighbor, depth + 1))

                # Add to results
                node_data = self.graph.nodes[neighbor].copy()
                related_nodes.append(
                    {
                        "node_id": neighbor,
                        "distance": depth + 1,
                        "edge_type": edge_data.get("edge_type"),
                        "node_type": node_data.get("node_type"),
                        "name": node_data.get("name"),
                        "metadata": node_data,
                    }
                )

        logger.debug(
            f"Found {len(related_nodes)} related nodes for '{node_id}' (max_depth={max_depth})"
        )

        return related_nodes

    def search_nodes(self, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        """
        Lightweight keyword search across graph nodes.

        Used as a fallback when vector retrieval misses or is too weak to seed graph
        traversal reliably. Scores nodes by token overlap across common descriptive
        fields such as name, table_name, synonyms, and business metadata.
        """
        query_tokens = self._tokenize_text(query)
        if not query_tokens:
            return []

        candidates: list[dict[str, Any]] = []
        for node_id, node_data in self.graph.nodes(data=True):
            node_tokens = self._node_search_tokens(node_data)
            if not node_tokens:
                continue
            overlap = query_tokens & node_tokens
            if not overlap:
                continue

            score = len(overlap) / len(query_tokens)
            candidates.append(
                {
                    "node_id": node_id,
                    "score": score,
                    "node_type": node_data.get("node_type"),
                    "name": node_data.get("name"),
                    "metadata": node_data.copy(),
                    "matched_tokens": sorted(overlap),
                }
            )

        candidates.sort(
            key=lambda item: (-float(item["score"]), str(item.get("name") or item["node_id"]))
        )
        return candidates[:top_k]

    def find_path(
        self, source_id: str, target_id: str, cutoff: int | None = 5
    ) -> list[dict[str, Any]] | None:
        """
        Find shortest path between two nodes.

        Useful for finding join paths between tables.

        Args:
            source_id: Starting node ID
            target_id: Target node ID
            cutoff: Maximum path length (default: 5)

        Returns:
            List of nodes in path with edge info, or None if no path exists

        Raises:
            KnowledgeGraphError: If nodes don't exist
        """
        if source_id not in self.graph:
            raise KnowledgeGraphError(f"Source node '{source_id}' not found")
        if target_id not in self.graph:
            raise KnowledgeGraphError(f"Target node '{target_id}' not found")

        try:
            # Find shortest path (ignoring edge direction)
            # Use has_path with cutoff first
            undirected = self.graph.to_undirected()
            if not nx.has_path(undirected, source_id, target_id):
                return None

            path = nx.shortest_path(undirected, source_id, target_id)

            # Apply cutoff manually
            if cutoff and len(path) > cutoff:
                return None

            # Build path with edge information
            path_with_edges = []
            for i, node_id in enumerate(path):
                node_data = self.graph.nodes[node_id].copy()
                edge_info = None

                # Get edge to next node
                if i < len(path) - 1:
                    next_node = path[i + 1]
                    if self.graph.has_edge(node_id, next_node):
                        edge_info = self.graph[node_id][next_node].copy()
                    elif self.graph.has_edge(next_node, node_id):
                        edge_info = self.graph[next_node][node_id].copy()
                        edge_info["reversed"] = True

                path_with_edges.append(
                    {
                        "node_id": node_id,
                        "node_type": node_data.get("node_type"),
                        "name": node_data.get("name"),
                        "metadata": node_data,
                        "edge_to_next": edge_info,
                    }
                )

            logger.debug(f"Found path from '{source_id}' to '{target_id}': {len(path)} nodes")

            return path_with_edges

        except nx.NetworkXNoPath:
            logger.debug(f"No path found from '{source_id}' to '{target_id}'")
            return None
        except nx.NodeNotFound:
            # This shouldn't happen since we check above, but handle it
            logger.debug("Node not found when finding path")
            return None

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        """
        Get node data by ID.

        Args:
            node_id: Node identifier

        Returns:
            Node data dict, or None if not found
        """
        if node_id not in self.graph:
            return None

        return dict(self.graph.nodes[node_id])

    def get_node_metadata(self, node_id: str) -> dict[str, Any] | None:
        """
        Return a copy of node metadata for a DataPoint/node ID.

        Args:
            node_id: Node identifier

        Returns:
            Node metadata dictionary or None if node does not exist.
        """
        node = self.get_node(node_id)
        if node is None:
            return None
        return dict(node)

    def get_stats(self) -> dict[str, Any]:
        """
        Get graph statistics.

        Returns:
            Dict with node counts, edge counts, etc.
        """
        node_type_counts = {}
        for _node, data in self.graph.nodes(data=True):
            node_type = data.get("node_type", "unknown")
            node_type_counts[node_type] = node_type_counts.get(node_type, 0) + 1

        edge_type_counts = {}
        for _source, _target, data in self.graph.edges(data=True):
            edge_type = data.get("edge_type", "unknown")
            edge_type_counts[edge_type] = edge_type_counts.get(edge_type, 0) + 1

        return {
            "datapoints_added": self._datapoint_count,
            "total_nodes": self.graph.number_of_nodes(),
            "total_edges": self.graph.number_of_edges(),
            "node_types": node_type_counts,
            "edge_types": edge_type_counts,
            "is_connected": nx.is_weakly_connected(self.graph)
            if self.graph.number_of_nodes() > 0
            else False,
        }

    def save_to_file(self, file_path: str | Path) -> None:
        """
        Save graph to JSON file.

        Args:
            file_path: Path to save file

        Raises:
            KnowledgeGraphError: If save fails
        """
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert graph to JSON-serializable format
            graph_data = json_graph.node_link_data(self.graph, edges="links")

            # Add metadata
            data = {
                "datapoint_count": self._datapoint_count,
                "graph": graph_data,
            }

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved knowledge graph to {file_path}")

        except Exception as e:
            logger.error(f"Failed to save graph to {file_path}: {e}")
            raise KnowledgeGraphError(f"Failed to save graph: {e}") from e

    def load_from_file(self, file_path: str | Path) -> None:
        """
        Load graph from JSON file.

        Args:
            file_path: Path to load from

        Raises:
            KnowledgeGraphError: If load fails
        """
        try:
            file_path = Path(file_path)

            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)

            # Reconstruct graph
            self.graph = json_graph.node_link_graph(data["graph"], directed=True, edges="links")
            self._datapoint_count = data.get("datapoint_count", 0)

            logger.info(
                f"Loaded knowledge graph from {file_path}: "
                f"{self.graph.number_of_nodes()} nodes, "
                f"{self.graph.number_of_edges()} edges"
            )

        except Exception as e:
            logger.error(f"Failed to load graph from {file_path}: {e}")
            raise KnowledgeGraphError(f"Failed to load graph: {e}") from e

    def clear(self) -> None:
        """Clear all nodes and edges from the graph."""
        self.graph.clear()
        self._datapoint_count = 0
        logger.info("Knowledge graph cleared")

    def remove_datapoint(self, datapoint_id: str) -> None:
        """Remove a DataPoint and its nodes from the graph."""
        nodes_to_remove = []
        for node, data in self.graph.nodes(data=True):
            if node == datapoint_id:
                nodes_to_remove.append(node)
            elif data.get("parent_table") == datapoint_id:
                nodes_to_remove.append(node)
            elif isinstance(node, str) and node.startswith(f"{datapoint_id}__col__"):
                nodes_to_remove.append(node)

        if nodes_to_remove:
            self.graph.remove_nodes_from(nodes_to_remove)
            self._datapoint_count = max(self._datapoint_count - 1, 0)
            logger.info(f"Removed datapoint '{datapoint_id}' from knowledge graph")

    # Helper methods

    def _find_table_by_name(self, table_name: str) -> str | None:
        """Find table node ID by table name."""
        for node, data in self.graph.nodes(data=True):
            if data.get("node_type") == NodeType.TABLE and data.get("table_name") == table_name:
                return node
        return None

    def _find_process_by_id(self, process_id: str) -> str | None:
        """Find process node by datapoint ID."""
        # Direct lookup if the ID is already in the graph
        if process_id in self.graph:
            node_data = self.graph.nodes[process_id]
            if node_data.get("node_type") == NodeType.PROCESS:
                return process_id
        return None

    @staticmethod
    def _tokenize_text(value: Any) -> set[str]:
        if value is None:
            return set()
        text = str(value).strip().lower()
        if not text:
            return set()
        return {
            token
            for token in re.split(r"[^a-z0-9]+", text)
            if token and len(token) > 1
        }

    def _node_search_tokens(self, node_data: dict[str, Any]) -> set[str]:
        tokens: set[str] = set()

        for field in (
            "name",
            "table_name",
            "schema_name",
            "business_purpose",
            "description",
            "calculation",
        ):
            tokens.update(self._tokenize_text(node_data.get(field)))

        for list_field in ("synonyms", "related_tables", "tags", "dependencies"):
            value = node_data.get(list_field)
            if isinstance(value, list):
                for item in value:
                    tokens.update(self._tokenize_text(item))

        return tokens

    def _find_metrics_by_synonyms(self, synonyms: list[str]) -> list[str]:
        """Find metric node IDs that share synonyms."""
        matching_metrics = []
        synonym_set = {s.lower() for s in synonyms}

        for node, data in self.graph.nodes(data=True):
            if data.get("node_type") == NodeType.METRIC:
                node_synonyms = {s.lower() for s in data.get("synonyms", [])}
                if synonym_set & node_synonyms:  # Intersection
                    matching_metrics.append(node)

        return matching_metrics

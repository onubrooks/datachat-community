"""
Tests for Knowledge Graph.

Tests NetworkX-based knowledge graph operations with DataPoints.
"""

import json

import pytest

from backend.knowledge.graph import (
    EdgeType,
    KnowledgeGraph,
    KnowledgeGraphError,
    NodeType,
)
from backend.models.datapoint import (
    BusinessDataPoint,
    ProcessDataPoint,
    QueryDataPoint,
    SchemaDataPoint,
)


@pytest.fixture
def knowledge_graph():
    """Create empty knowledge graph instance."""
    return KnowledgeGraph()


@pytest.fixture
def sample_schema_datapoint():
    """Create sample Schema DataPoint."""
    return SchemaDataPoint(
        datapoint_id="table_fact_sales_001",
        type="Schema",
        name="Fact Sales Table",
        table_name="analytics.fact_sales",
        schema="analytics",
        business_purpose="Central fact table for all sales transactions",
        key_columns=[
            {
                "name": "sale_id",
                "type": "INTEGER",
                "business_meaning": "Unique sale identifier",
                "nullable": False,
            },
            {
                "name": "customer_id",
                "type": "INTEGER",
                "business_meaning": "Customer identifier",
                "nullable": False,
            },
            {
                "name": "amount",
                "type": "DECIMAL(18,2)",
                "business_meaning": "Sale amount in USD",
                "nullable": False,
            },
        ],
        relationships=[
            {
                "target_table": "analytics.dim_customer",
                "join_column": "customer_id",
                "cardinality": "N:1",
            }
        ],
        owner="data@example.com",
        tags=["sales", "fact"],
    )


@pytest.fixture
def sample_dimension_datapoint():
    """Create sample dimension table DataPoint."""
    return SchemaDataPoint(
        datapoint_id="table_dim_customer_001",
        type="Schema",
        name="Customer Dimension",
        table_name="analytics.dim_customer",
        schema="analytics",
        business_purpose="Customer master data",
        key_columns=[
            {
                "name": "customer_id",
                "type": "INTEGER",
                "business_meaning": "Unique customer identifier",
                "nullable": False,
            },
            {
                "name": "customer_name",
                "type": "VARCHAR(255)",
                "business_meaning": "Customer full name",
                "nullable": False,
            },
        ],
        owner="data@example.com",
        tags=["customer", "dimension"],
    )


@pytest.fixture
def sample_business_datapoint():
    """Create sample Business DataPoint."""
    return BusinessDataPoint(
        datapoint_id="metric_revenue_001",
        type="Business",
        name="Total Revenue",
        calculation="SUM(fact_sales.amount) WHERE status = 'completed'",
        synonyms=["sales", "income", "earnings"],
        business_rules=["Exclude refunds", "Include tax"],
        related_tables=["analytics.fact_sales"],
        owner="finance@example.com",
        tags=["metric", "revenue"],
    )


@pytest.fixture
def sample_process_datapoint():
    """Create sample Process DataPoint."""
    return ProcessDataPoint(
        datapoint_id="proc_daily_sales_etl_001",
        type="Process",
        name="Daily Sales ETL",
        schedule="0 2 * * *",
        data_freshness="T-1 (yesterday's data by 3am)",
        target_tables=["analytics.fact_sales"],
        dependencies=["raw.sales_events"],
        owner="data-eng@example.com",
        tags=["etl", "daily"],
    )


@pytest.fixture
def sample_query_datapoint():
    """Create sample Query DataPoint."""
    return QueryDataPoint(
        datapoint_id="query_top_customers_001",
        type="Query",
        name="Top Customers by Revenue",
        sql_template=(
            "SELECT customer_id, SUM(amount) AS revenue "
            "FROM analytics.fact_sales "
            "GROUP BY customer_id "
            "ORDER BY revenue DESC "
            "LIMIT {limit}"
        ),
        parameters={
            "limit": {
                "type": "integer",
                "required": False,
                "default": 10,
                "description": "Number of customers to return",
            }
        },
        description="Returns top customers by revenue",
        related_tables=["analytics.fact_sales"],
        owner="analytics@example.com",
        tags=["query", "revenue"],
    )


class TestInitialization:
    """Test knowledge graph initialization."""

    def test_initialize_creates_empty_graph(self, knowledge_graph):
        """Test that initialization creates empty graph."""
        assert knowledge_graph.graph.number_of_nodes() == 0
        assert knowledge_graph.graph.number_of_edges() == 0

    def test_get_stats_empty_graph(self, knowledge_graph):
        """Test stats for empty graph."""
        stats = knowledge_graph.get_stats()

        assert stats["total_nodes"] == 0
        assert stats["total_edges"] == 0
        assert stats["datapoints_added"] == 0
        assert stats["is_connected"] is False

    def test_get_node_metadata_returns_none_for_missing_node(self, knowledge_graph):
        """Missing nodes should return None metadata."""
        assert knowledge_graph.get_node_metadata("missing_node") is None


class TestAddSchemaDataPoint:
    """Test adding Schema DataPoints to graph."""

    def test_add_table_creates_table_node(self, knowledge_graph, sample_schema_datapoint):
        """Test adding table creates table node."""
        nodes_added = knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Should add 1 table + 3 columns = 4 nodes
        assert nodes_added == 4
        assert knowledge_graph.graph.number_of_nodes() == 4

        # Check table node
        table_node = knowledge_graph.get_node("table_fact_sales_001")
        assert table_node is not None
        assert table_node["node_type"] == NodeType.TABLE
        assert table_node["name"] == "Fact Sales Table"
        assert table_node["table_name"] == "analytics.fact_sales"
        assert table_node["schema"] == "analytics"

    def test_add_table_creates_column_nodes(self, knowledge_graph, sample_schema_datapoint):
        """Test adding table creates column nodes."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Check column nodes
        col_node = knowledge_graph.get_node("table_fact_sales_001__col__sale_id")
        assert col_node is not None
        assert col_node["node_type"] == NodeType.COLUMN
        assert col_node["name"] == "sale_id"
        assert col_node["column_type"] == "INTEGER"
        assert col_node["parent_table"] == "table_fact_sales_001"

    def test_add_table_creates_belongs_to_edges(self, knowledge_graph, sample_schema_datapoint):
        """Test column->table edges are created."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Check edge from column to table
        assert knowledge_graph.graph.has_edge(
            "table_fact_sales_001__col__sale_id", "table_fact_sales_001"
        )

        edge_data = knowledge_graph.graph["table_fact_sales_001__col__sale_id"][
            "table_fact_sales_001"
        ]
        assert edge_data["edge_type"] == EdgeType.BELONGS_TO

    def test_add_related_tables_creates_join_edges(
        self, knowledge_graph, sample_schema_datapoint, sample_dimension_datapoint
    ):
        """Test relationship edges are created between tables."""
        # Add dimension table first (so relationship can be created)
        knowledge_graph.add_datapoint(sample_dimension_datapoint)
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Check join edge
        assert knowledge_graph.graph.has_edge("table_fact_sales_001", "table_dim_customer_001")

        edge_data = knowledge_graph.graph["table_fact_sales_001"]["table_dim_customer_001"]
        assert edge_data["edge_type"] == EdgeType.JOINS_WITH
        assert edge_data["join_column"] == "customer_id"
        assert edge_data["cardinality"] == "N:1"


class TestAddBusinessDataPoint:
    """Test adding Business DataPoints to graph."""

    def test_add_metric_creates_metric_node(self, knowledge_graph, sample_business_datapoint):
        """Test adding metric creates metric node."""
        nodes_added = knowledge_graph.add_datapoint(sample_business_datapoint)

        assert nodes_added == 1
        assert knowledge_graph.graph.number_of_nodes() == 1

        # Check metric node
        metric_node = knowledge_graph.get_node("metric_revenue_001")
        assert metric_node is not None
        assert metric_node["node_type"] == NodeType.METRIC
        assert metric_node["name"] == "Total Revenue"
        assert metric_node["calculation"] == "SUM(fact_sales.amount) WHERE status = 'completed'"
        assert metric_node["synonyms"] == ["sales", "income", "earnings"]

    def test_add_metric_creates_calculates_edge(
        self, knowledge_graph, sample_schema_datapoint, sample_business_datapoint
    ):
        """Test metric->table edges are created."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_business_datapoint)

        # Check edge from metric to table
        assert knowledge_graph.graph.has_edge("metric_revenue_001", "table_fact_sales_001")

        edge_data = knowledge_graph.graph["metric_revenue_001"]["table_fact_sales_001"]
        assert edge_data["edge_type"] == EdgeType.CALCULATES

    def test_add_metrics_with_synonyms_creates_synonym_edges(self, knowledge_graph):
        """Test synonym edges between metrics."""
        metric1 = BusinessDataPoint(
            datapoint_id="metric_revenue_001",
            type="Business",
            name="Revenue",
            calculation="SUM(sales.amount)",
            synonyms=["sales", "income"],
            related_tables=["sales"],
            owner="test@example.com",
        )

        metric2 = BusinessDataPoint(
            datapoint_id="metric_sales_002",
            type="Business",
            name="Sales",
            calculation="SUM(sales.total)",
            synonyms=["revenue", "income"],
            related_tables=["sales"],
            owner="test@example.com",
        )

        knowledge_graph.add_datapoint(metric1)
        knowledge_graph.add_datapoint(metric2)

        # Check bidirectional synonym edges
        assert knowledge_graph.graph.has_edge("metric_revenue_001", "metric_sales_002")
        assert knowledge_graph.graph.has_edge("metric_sales_002", "metric_revenue_001")

        edge_data = knowledge_graph.graph["metric_revenue_001"]["metric_sales_002"]
        assert edge_data["edge_type"] == EdgeType.SYNONYMOUS


class TestAddProcessDataPoint:
    """Test adding Process DataPoints to graph."""

    def test_add_process_creates_process_node(self, knowledge_graph, sample_process_datapoint):
        """Test adding process creates process node."""
        nodes_added = knowledge_graph.add_datapoint(sample_process_datapoint)

        assert nodes_added == 1
        assert knowledge_graph.graph.number_of_nodes() == 1

        # Check process node
        process_node = knowledge_graph.get_node("proc_daily_sales_etl_001")
        assert process_node is not None
        assert process_node["node_type"] == NodeType.PROCESS
        assert process_node["name"] == "Daily Sales ETL"
        assert process_node["schedule"] == "0 2 * * *"

    def test_add_process_creates_uses_edge(
        self, knowledge_graph, sample_schema_datapoint, sample_process_datapoint
    ):
        """Test process->table edges are created."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_process_datapoint)

        # Check edge from process to table
        assert knowledge_graph.graph.has_edge("proc_daily_sales_etl_001", "table_fact_sales_001")

        edge_data = knowledge_graph.graph["proc_daily_sales_etl_001"]["table_fact_sales_001"]
        assert edge_data["edge_type"] == EdgeType.USES

    def test_add_process_with_process_dependency(self, knowledge_graph):
        """Test process->process dependency edges are created."""
        # Create upstream process
        upstream_process = ProcessDataPoint(
            datapoint_id="proc_extract_raw_001",
            type="Process",
            name="Extract Raw Data",
            schedule="0 1 * * *",
            data_freshness="T-1",
            target_tables=["raw.sales_events"],
            owner="data-eng@example.com",
        )

        # Create downstream process that depends on upstream
        downstream_process = ProcessDataPoint(
            datapoint_id="proc_transform_sales_002",
            type="Process",
            name="Transform Sales",
            schedule="0 2 * * *",
            data_freshness="T-1",
            target_tables=["staging.sales"],
            dependencies=["proc_extract_raw_001"],  # Process dependency
            owner="data-eng@example.com",
        )

        knowledge_graph.add_datapoint(upstream_process)
        knowledge_graph.add_datapoint(downstream_process)

        # Check edge from downstream to upstream process
        assert knowledge_graph.graph.has_edge("proc_transform_sales_002", "proc_extract_raw_001")

        edge_data = knowledge_graph.graph["proc_transform_sales_002"]["proc_extract_raw_001"]
        assert edge_data["edge_type"] == EdgeType.DEPENDS_ON

    def test_add_process_with_mixed_dependencies(self, knowledge_graph, sample_schema_datapoint):
        """Test process with both table and process dependencies."""
        # Add a table
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Create upstream process
        upstream_process = ProcessDataPoint(
            datapoint_id="proc_upstream_001",
            type="Process",
            name="Upstream Process",
            schedule="0 1 * * *",
            data_freshness="T-1",
            target_tables=["raw.data"],
            owner="data-eng@example.com",
        )

        # Create process with mixed dependencies
        mixed_process = ProcessDataPoint(
            datapoint_id="proc_mixed_002",
            type="Process",
            name="Mixed Dependencies Process",
            schedule="0 3 * * *",
            data_freshness="T-1",
            target_tables=["staging.combined"],
            dependencies=[
                "analytics.fact_sales",  # Table dependency
                "proc_upstream_001",  # Process dependency
            ],
            owner="data-eng@example.com",
        )

        knowledge_graph.add_datapoint(upstream_process)
        knowledge_graph.add_datapoint(mixed_process)

        # Check both edges exist
        assert knowledge_graph.graph.has_edge("proc_mixed_002", "table_fact_sales_001")  # Table dep
        assert knowledge_graph.graph.has_edge("proc_mixed_002", "proc_upstream_001")  # Process dep

        # Both should be DEPENDS_ON edges
        table_edge = knowledge_graph.graph["proc_mixed_002"]["table_fact_sales_001"]
        process_edge = knowledge_graph.graph["proc_mixed_002"]["proc_upstream_001"]

        assert table_edge["edge_type"] == EdgeType.DEPENDS_ON
        assert process_edge["edge_type"] == EdgeType.DEPENDS_ON


class TestAddQueryDataPoint:
    """Test adding Query DataPoints to graph."""

    def test_add_query_creates_query_node(self, knowledge_graph, sample_query_datapoint):
        """Test adding query creates query node."""
        nodes_added = knowledge_graph.add_datapoint(sample_query_datapoint)

        assert nodes_added == 1
        assert knowledge_graph.graph.number_of_nodes() == 1

        query_node = knowledge_graph.get_node("query_top_customers_001")
        assert query_node is not None
        assert query_node["node_type"] == NodeType.QUERY
        assert query_node["name"] == "Top Customers by Revenue"
        assert query_node["parameter_names"] == ["limit"]

    def test_add_query_creates_uses_edge_to_related_table(
        self, knowledge_graph, sample_schema_datapoint, sample_query_datapoint
    ):
        """Test query->table edges are created for related tables."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_query_datapoint)

        assert knowledge_graph.graph.has_edge("query_top_customers_001", "table_fact_sales_001")

        edge_data = knowledge_graph.graph["query_top_customers_001"]["table_fact_sales_001"]
        assert edge_data["edge_type"] == EdgeType.USES


class TestGetRelated:
    """Test finding related nodes."""

    def test_get_related_returns_empty_for_isolated_node(
        self, knowledge_graph, sample_business_datapoint
    ):
        """Test get_related returns empty for isolated node."""
        knowledge_graph.add_datapoint(sample_business_datapoint)

        related = knowledge_graph.get_related("metric_revenue_001", max_depth=2)
        assert len(related) == 0

    def test_get_related_finds_connected_nodes(
        self, knowledge_graph, sample_schema_datapoint, sample_business_datapoint
    ):
        """Test get_related finds connected nodes."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_business_datapoint)

        # Get related from metric
        related = knowledge_graph.get_related("metric_revenue_001", max_depth=2)

        # Should find: 1 table + 3 columns = 4 nodes
        assert len(related) >= 1  # At least the table

        # Check table is in results
        table_result = next((r for r in related if r["node_id"] == "table_fact_sales_001"), None)
        assert table_result is not None
        assert table_result["distance"] == 1
        assert table_result["edge_type"] == EdgeType.CALCULATES

    def test_get_related_respects_max_depth(self, knowledge_graph, sample_schema_datapoint):
        """Test get_related respects max_depth parameter."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Depth 1: should only get columns
        related_1 = knowledge_graph.get_related("table_fact_sales_001", max_depth=1)
        assert len(related_1) == 3  # 3 columns

        # Depth 2: should get columns + possibly more
        related_2 = knowledge_graph.get_related("table_fact_sales_001", max_depth=2)
        assert len(related_2) >= len(related_1)

    def test_get_related_filters_by_edge_type(self, knowledge_graph, sample_schema_datapoint):
        """Test get_related filters by edge type."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Filter for BELONGS_TO edges only
        related = knowledge_graph.get_related(
            "table_fact_sales_001",
            max_depth=1,
            edge_types=[EdgeType.BELONGS_TO],
        )

        # Should find 3 columns
        assert len(related) == 3
        for r in related:
            assert r["node_type"] == NodeType.COLUMN

    def test_get_related_raises_for_nonexistent_node(self, knowledge_graph):
        """Test get_related raises for non-existent node."""
        with pytest.raises(KnowledgeGraphError, match="not found"):
            knowledge_graph.get_related("nonexistent_node_123")


class TestFindPath:
    """Test finding paths between nodes."""

    def test_find_path_between_connected_tables(
        self, knowledge_graph, sample_schema_datapoint, sample_dimension_datapoint
    ):
        """Test find_path finds join path between tables."""
        knowledge_graph.add_datapoint(sample_dimension_datapoint)
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        path = knowledge_graph.find_path("table_fact_sales_001", "table_dim_customer_001")

        assert path is not None
        assert len(path) == 2
        assert path[0]["node_id"] == "table_fact_sales_001"
        assert path[1]["node_id"] == "table_dim_customer_001"
        assert path[0]["edge_to_next"]["edge_type"] == EdgeType.JOINS_WITH

    def test_find_path_returns_none_for_disconnected_nodes(self, knowledge_graph):
        """Test find_path returns None for disconnected nodes."""
        metric1 = BusinessDataPoint(
            datapoint_id="metric_first_metric_001",
            type="Business",
            name="Metric 1",
            calculation="SUM(a)",
            related_tables=["table_a"],
            owner="test@example.com",
        )

        metric2 = BusinessDataPoint(
            datapoint_id="metric_second_metric_002",
            type="Business",
            name="Metric 2",
            calculation="SUM(b)",
            related_tables=["table_b"],
            owner="test@example.com",
        )

        knowledge_graph.add_datapoint(metric1)
        knowledge_graph.add_datapoint(metric2)

        path = knowledge_graph.find_path("metric_first_metric_001", "metric_second_metric_002")
        assert path is None

    def test_find_path_respects_cutoff(
        self, knowledge_graph, sample_schema_datapoint, sample_dimension_datapoint
    ):
        """Test find_path respects cutoff parameter."""
        knowledge_graph.add_datapoint(sample_dimension_datapoint)
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        # Path exists within cutoff
        path = knowledge_graph.find_path("table_fact_sales_001", "table_dim_customer_001", cutoff=5)
        assert path is not None

        # Path doesn't exist with cutoff=1 (path length is 2)
        path = knowledge_graph.find_path("table_fact_sales_001", "table_dim_customer_001", cutoff=1)
        assert path is None

    def test_find_path_raises_for_nonexistent_nodes(self, knowledge_graph):
        """Test find_path raises for non-existent nodes."""
        with pytest.raises(KnowledgeGraphError, match="Source node.*not found"):
            knowledge_graph.find_path("nonexistent_1", "nonexistent_2")


class TestSerialization:
    """Test JSON serialization and deserialization."""

    def test_save_to_file_creates_json(self, knowledge_graph, sample_schema_datapoint, tmp_path):
        """Test save_to_file creates JSON file."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        file_path = tmp_path / "test_graph.json"
        knowledge_graph.save_to_file(file_path)

        assert file_path.exists()

        # Check JSON is valid
        with open(file_path) as f:
            data = json.load(f)

        assert "graph" in data
        assert "datapoint_count" in data
        assert data["datapoint_count"] == 1

    def test_load_from_file_reconstructs_graph(
        self, knowledge_graph, sample_schema_datapoint, tmp_path
    ):
        """Test load_from_file reconstructs graph correctly."""
        # Save graph
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        file_path = tmp_path / "test_graph.json"
        knowledge_graph.save_to_file(file_path)

        original_stats = knowledge_graph.get_stats()

        # Load into new graph
        new_graph = KnowledgeGraph()
        new_graph.load_from_file(file_path)

        # Check reconstruction
        new_stats = new_graph.get_stats()
        assert new_stats["total_nodes"] == original_stats["total_nodes"]
        assert new_stats["total_edges"] == original_stats["total_edges"]
        assert new_stats["datapoints_added"] == original_stats["datapoints_added"]

        # Check node data preserved
        table_node = new_graph.get_node("table_fact_sales_001")
        assert table_node is not None
        assert table_node["node_type"] == NodeType.TABLE
        assert table_node["name"] == "Fact Sales Table"

    def test_save_load_roundtrip_preserves_data(
        self,
        knowledge_graph,
        sample_schema_datapoint,
        sample_business_datapoint,
        tmp_path,
    ):
        """Test save/load roundtrip preserves all data."""
        # Build complex graph
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_business_datapoint)

        # Save
        file_path = tmp_path / "roundtrip.json"
        knowledge_graph.save_to_file(file_path)

        # Load
        loaded_graph = KnowledgeGraph()
        loaded_graph.load_from_file(file_path)

        # Verify relationships preserved
        related = loaded_graph.get_related("metric_revenue_001", max_depth=2)
        assert len(related) >= 1

        # Verify edges preserved
        assert loaded_graph.graph.has_edge("metric_revenue_001", "table_fact_sales_001")


class TestGetStats:
    """Test graph statistics."""

    def test_get_stats_counts_node_types(
        self,
        knowledge_graph,
        sample_schema_datapoint,
        sample_business_datapoint,
        sample_process_datapoint,
    ):
        """Test get_stats counts node types correctly."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_business_datapoint)
        knowledge_graph.add_datapoint(sample_process_datapoint)

        stats = knowledge_graph.get_stats()

        assert stats["datapoints_added"] == 3
        assert stats["node_types"][NodeType.TABLE] == 1
        assert stats["node_types"][NodeType.COLUMN] == 3
        assert stats["node_types"][NodeType.METRIC] == 1
        assert stats["node_types"][NodeType.PROCESS] == 1

    def test_get_stats_counts_edge_types(
        self, knowledge_graph, sample_schema_datapoint, sample_business_datapoint
    ):
        """Test get_stats counts edge types correctly."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_business_datapoint)

        stats = knowledge_graph.get_stats()

        assert EdgeType.BELONGS_TO in stats["edge_types"]
        assert stats["edge_types"][EdgeType.BELONGS_TO] == 3  # 3 columns
        assert stats["edge_types"][EdgeType.CALCULATES] == 1  # metric->table

    def test_get_stats_includes_query_node_type(
        self, knowledge_graph, sample_schema_datapoint, sample_query_datapoint
    ):
        """Test get_stats includes Query node types after adding query datapoints."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)
        knowledge_graph.add_datapoint(sample_query_datapoint)

        stats = knowledge_graph.get_stats()

        assert stats["node_types"][NodeType.QUERY] == 1
        assert stats["edge_types"][EdgeType.USES] >= 1


class TestClear:
    """Test clearing graph."""

    def test_clear_removes_all_nodes_and_edges(self, knowledge_graph, sample_schema_datapoint):
        """Test clear removes all data."""
        knowledge_graph.add_datapoint(sample_schema_datapoint)

        assert knowledge_graph.graph.number_of_nodes() > 0
        assert knowledge_graph.graph.number_of_edges() > 0

        knowledge_graph.clear()

        assert knowledge_graph.graph.number_of_nodes() == 0
        assert knowledge_graph.graph.number_of_edges() == 0
        assert knowledge_graph._datapoint_count == 0

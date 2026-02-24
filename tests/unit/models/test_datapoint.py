"""
Unit tests for DataPoint models.

Tests Pydantic validation, discriminated unions, and custom validators.
"""

import pytest
from pydantic import ValidationError

from backend.models.datapoint import (
    BusinessDataPoint,
    ColumnMetadata,
    ProcessDataPoint,
    QueryDataPoint,
    QueryParameter,
    Relationship,
    SchemaDataPoint,
)


class TestColumnMetadata:
    """Test ColumnMetadata model."""

    def test_valid_column_metadata(self):
        """Valid column metadata is accepted."""
        column = ColumnMetadata(
            name="customer_id",
            type="BIGINT",
            business_meaning="Unique identifier for customer",
            nullable=False,
        )

        assert column.name == "customer_id"
        assert column.type == "BIGINT"
        assert column.business_meaning == "Unique identifier for customer"
        assert column.nullable is False

    def test_column_with_default_value(self):
        """Column with default value."""
        column = ColumnMetadata(
            name="created_at",
            type="TIMESTAMP",
            business_meaning="Record creation timestamp",
            nullable=False,
            default_value="CURRENT_TIMESTAMP",
        )

        assert column.default_value == "CURRENT_TIMESTAMP"

    def test_empty_column_name_rejected(self):
        """Empty column name is rejected."""
        with pytest.raises(ValidationError, match="at least 1 character"):
            ColumnMetadata(name="", type="INT", business_meaning="Test", nullable=True)


class TestRelationship:
    """Test Relationship model."""

    def test_valid_relationship(self):
        """Valid relationship is accepted."""
        rel = Relationship(
            target_table="dim_customer", join_column="customer_id", cardinality="N:1"
        )

        assert rel.target_table == "dim_customer"
        assert rel.join_column == "customer_id"
        assert rel.cardinality == "N:1"
        assert rel.relationship_type == "foreign_key"  # Default

    def test_relationship_with_schema(self):
        """Relationship can reference table with schema."""
        rel = Relationship(
            target_table="analytics.dim_customer", join_column="customer_id", cardinality="N:1"
        )

        assert "analytics" in rel.target_table

    def test_invalid_cardinality_rejected(self):
        """Invalid cardinality is rejected."""
        with pytest.raises(ValidationError):
            Relationship(
                target_table="dim_customer",
                join_column="customer_id",
                cardinality="1:many",  # Invalid
            )

    def test_logical_relationship(self):
        """Logical relationship type."""
        rel = Relationship(
            target_table="dim_product",
            join_column="product_id",
            cardinality="1:N",
            relationship_type="logical",
        )

        assert rel.relationship_type == "logical"


class TestSchemaDataPoint:
    """Test SchemaDataPoint model."""

    def test_valid_schema_datapoint(self):
        """Valid schema datapoint is accepted."""
        datapoint = SchemaDataPoint(
            datapoint_id="table_fact_sales_001",
            name="Fact Sales Table",
            table_name="analytics.fact_sales",
            schema="analytics",
            business_purpose="Central fact table for all sales transactions",
            key_columns=[
                ColumnMetadata(
                    name="amount",
                    type="DECIMAL(18,2)",
                    business_meaning="Transaction value in USD",
                    nullable=False,
                )
            ],
            owner="data-team@company.com",
        )

        assert datapoint.type == "Schema"
        assert datapoint.datapoint_id == "table_fact_sales_001"
        assert datapoint.table_name == "analytics.fact_sales"
        assert len(datapoint.key_columns) == 1

    def test_schema_datapoint_with_relationships(self):
        """Schema datapoint with relationships."""
        datapoint = SchemaDataPoint(
            datapoint_id="table_fact_orders_001",
            name="Fact Orders",
            table_name="fact_orders",
            schema="public",
            business_purpose="Order transactions",
            key_columns=[
                ColumnMetadata(
                    name="order_id", type="BIGINT", business_meaning="Order ID", nullable=False
                )
            ],
            relationships=[
                Relationship(
                    target_table="dim_customer", join_column="customer_id", cardinality="N:1"
                ),
                Relationship(
                    target_table="dim_product", join_column="product_id", cardinality="N:1"
                ),
            ],
            owner="data@example.com",
        )

        assert len(datapoint.relationships) == 2
        assert datapoint.relationships[0].target_table == "dim_customer"

    def test_schema_datapoint_with_common_queries(self):
        """Schema datapoint with common queries and gotchas."""
        datapoint = SchemaDataPoint(
            datapoint_id="table_fact_sales_002",
            name="Sales Fact",
            table_name="fact_sales",
            schema="analytics",
            business_purpose="Sales data",
            key_columns=[
                ColumnMetadata(name="id", type="INT", business_meaning="ID", nullable=False)
            ],
            common_queries=["SUM(amount)", "GROUP BY date"],
            gotchas=["Always filter by date", "Use index on customer_id"],
            freshness="T-1",
            owner="team@example.com",
        )

        assert len(datapoint.common_queries) == 2
        assert len(datapoint.gotchas) == 2
        assert datapoint.freshness == "T-1"

    def test_schema_datapoint_requires_key_columns(self):
        """Schema datapoint requires at least one key column."""
        with pytest.raises(ValidationError, match="at least 1 item"):
            SchemaDataPoint(
                datapoint_id="table_test_001",
                name="Test Table",
                table_name="test",
                schema="public",
                business_purpose="Test table purpose description",
                key_columns=[],  # Empty list not allowed
                owner="test@example.com",
            )

    def test_schema_datapoint_row_count(self):
        """Schema datapoint can include row count."""
        datapoint = SchemaDataPoint(
            datapoint_id="table_large_001",
            name="Large Table",
            table_name="large_table",
            schema="public",
            business_purpose="Large table",
            key_columns=[
                ColumnMetadata(name="id", type="INT", business_meaning="ID", nullable=False)
            ],
            row_count=1000000,
            owner="data@example.com",
        )

        assert datapoint.row_count == 1000000

    def test_negative_row_count_rejected(self):
        """Negative row count is rejected."""
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            SchemaDataPoint(
                datapoint_id="table_test_001",
                name="Test",
                table_name="test",
                schema="public",
                business_purpose="Test table",
                key_columns=[
                    ColumnMetadata(name="id", type="INT", business_meaning="ID", nullable=False)
                ],
                row_count=-100,  # Invalid
                owner="test@example.com",
            )


class TestBusinessDataPoint:
    """Test BusinessDataPoint model."""

    def test_valid_business_datapoint(self):
        """Valid business datapoint is accepted."""
        datapoint = BusinessDataPoint(
            datapoint_id="metric_revenue_001",
            name="Revenue",
            calculation="SUM(fact_sales.amount) WHERE status = 'completed'",
            owner="finance@company.com",
        )

        assert datapoint.type == "Business"
        assert datapoint.datapoint_id == "metric_revenue_001"
        assert "SUM" in datapoint.calculation

    def test_business_datapoint_with_synonyms(self):
        """Business datapoint with synonyms."""
        datapoint = BusinessDataPoint(
            datapoint_id="metric_revenue_002",
            name="Total Revenue",
            calculation="SUM(amount)",
            synonyms=["sales", "income", "earnings", "total sales"],
            owner="finance@example.com",
        )

        assert len(datapoint.synonyms) == 4
        assert "sales" in datapoint.synonyms

    def test_business_datapoint_with_business_rules(self):
        """Business datapoint with business rules."""
        datapoint = BusinessDataPoint(
            datapoint_id="metric_revenue_003",
            name="Net Revenue",
            calculation="SUM(amount) - SUM(refunds)",
            business_rules=[
                "Exclude refunds (status != 'refunded')",
                "Convert to USD using daily rate",
                "Include only completed transactions",
            ],
            related_tables=["fact_sales", "dim_currency", "fact_refunds"],
            owner="finance@example.com",
        )

        assert len(datapoint.business_rules) == 3
        assert len(datapoint.related_tables) == 3

    def test_business_datapoint_with_unit_and_aggregation(self):
        """Business datapoint with unit and aggregation type."""
        datapoint = BusinessDataPoint(
            datapoint_id="metric_avg_order_001",
            name="Average Order Value",
            calculation="AVG(amount)",
            unit="USD",
            aggregation="AVG",
            owner="analytics@example.com",
        )

        assert datapoint.unit == "USD"
        assert datapoint.aggregation == "AVG"

    def test_invalid_aggregation_rejected(self):
        """Invalid aggregation type is rejected."""
        with pytest.raises(ValidationError):
            BusinessDataPoint(
                datapoint_id="metric_test_001",
                name="Test Metric",
                calculation="MEDIAN(amount)",
                aggregation="MEDIAN",  # Not in allowed list
                owner="test@example.com",
            )


class TestProcessDataPoint:
    """Test ProcessDataPoint model."""

    def test_valid_process_datapoint(self):
        """Valid process datapoint is accepted."""
        datapoint = ProcessDataPoint(
            datapoint_id="proc_daily_etl_001",
            name="Daily Sales ETL",
            schedule="0 2 * * *",
            data_freshness="T-1 (yesterday's data available by 3am UTC)",
            target_tables=["analytics.fact_sales"],
            owner="data-eng@company.com",
        )

        assert datapoint.type == "Process"
        assert datapoint.schedule == "0 2 * * *"
        assert len(datapoint.target_tables) == 1

    def test_process_datapoint_with_dependencies(self):
        """Process datapoint with dependencies."""
        datapoint = ProcessDataPoint(
            datapoint_id="proc_transform_001",
            name="Transform Process",
            schedule="0 3 * * *",
            data_freshness="T-1",
            target_tables=["analytics.fact_transformed"],
            dependencies=["raw.sales_events", "raw.customer_events"],
            owner="etl@example.com",
        )

        assert len(datapoint.dependencies) == 2
        assert "raw.sales_events" in datapoint.dependencies

    def test_process_datapoint_with_sla(self):
        """Process datapoint with SLA."""
        datapoint = ProcessDataPoint(
            datapoint_id="proc_critical_001",
            name="Critical ETL",
            schedule="*/15 * * * *",
            data_freshness="Real-time (15min delay)",
            target_tables=["analytics.real_time_metrics"],
            sla="Complete within 10 minutes",
            monitoring_url="https://monitor.example.com/etl/critical",
            owner="sre@example.com",
        )

        assert datapoint.sla == "Complete within 10 minutes"
        assert datapoint.monitoring_url is not None

    def test_process_datapoint_requires_target_tables(self):
        """Process datapoint requires at least one target table."""
        with pytest.raises(ValidationError, match="at least 1 item"):
            ProcessDataPoint(
                datapoint_id="proc_test_001",
                name="Test Process",
                schedule="0 0 * * *",
                data_freshness="Daily",
                target_tables=[],  # Empty not allowed
                owner="test@example.com",
            )


class TestDataPointIDValidation:
    """Test datapoint_id validation."""

    def test_valid_datapoint_id_formats(self):
        """Valid datapoint_id formats are accepted."""
        valid_ids = [
            "table_sales",
            "table_sales_1",
            "table_fact_sales_001",
            "metric_revenue_001",
            "proc_daily_etl_001",
            "table_dim_customer_123",
            "metric_avg_order_value_999",
            "table_sales_001_extra",
        ]

        for datapoint_id in valid_ids:
            datapoint = SchemaDataPoint(
                datapoint_id=datapoint_id,
                name="Test",
                table_name="test",
                schema="public",
                business_purpose="Test table purpose",
                key_columns=[
                    ColumnMetadata(name="id", type="INT", business_meaning="ID", nullable=False)
                ],
                owner="test@example.com",
            )
            assert datapoint.datapoint_id == datapoint_id

    def test_invalid_datapoint_id_formats(self):
        """Invalid datapoint_id formats are rejected."""
        invalid_ids = [
            "invalid",  # No underscores or number
            "TABLE_SALES_001",  # Uppercase
            "table-sales-001",  # Hyphens instead of underscores
            "table__sales_001",  # Double underscore
            "table__",  # Missing name
            "table",  # Missing separator and name
        ]

        for invalid_id in invalid_ids:
            with pytest.raises(ValidationError, match="Invalid datapoint_id format"):
                SchemaDataPoint(
                    datapoint_id=invalid_id,
                    name="Test",
                    table_name="test",
                    schema="public",
                    business_purpose="Test table",
                    key_columns=[
                        ColumnMetadata(name="id", type="INT", business_meaning="ID", nullable=False)
                    ],
                    owner="test@example.com",
                )


class TestOwnerValidation:
    """Test owner email validation."""

    def test_valid_email_addresses(self):
        """Valid email addresses are accepted."""
        valid_emails = [
            "user@example.com",
            "data-team@company.com",
            "john.doe@subdomain.example.org",
            "analytics+reports@company.io",
        ]

        for email in valid_emails:
            datapoint = BusinessDataPoint(
                datapoint_id="metric_test_001", name="Test", calculation="SUM(x)", owner=email
            )
            assert datapoint.owner == email.lower()  # Lowercased

    def test_invalid_email_addresses(self):
        """Invalid email addresses are rejected."""
        invalid_emails = [
            "not-an-email",
            "missing-at-sign.com",
            "@no-local-part.com",
            "no-domain@",
            "spaces in@email.com",
        ]

        for email in invalid_emails:
            with pytest.raises(ValidationError, match="valid email address"):
                BusinessDataPoint(
                    datapoint_id="metric_test_001", name="Test", calculation="SUM(x)", owner=email
                )


class TestDiscriminatedUnion:
    """Test discriminated union deserialization."""

    def test_deserialize_schema_datapoint(self):
        """JSON with type='Schema' deserializes to SchemaDataPoint."""
        data = {
            "datapoint_id": "table_test_001",
            "type": "Schema",
            "name": "Test Table",
            "table_name": "test",
            "schema": "public",
            "business_purpose": "Test table for unit tests",
            "key_columns": [
                {"name": "id", "type": "INT", "business_meaning": "Primary key", "nullable": False}
            ],
            "owner": "test@example.com",
        }

        from pydantic import TypeAdapter

        from backend.models.datapoint import DataPoint

        adapter = TypeAdapter(DataPoint)
        datapoint = adapter.validate_python(data)

        assert isinstance(datapoint, SchemaDataPoint)
        assert datapoint.table_name == "test"

    def test_deserialize_business_datapoint(self):
        """JSON with type='Business' deserializes to BusinessDataPoint."""
        data = {
            "datapoint_id": "metric_test_001",
            "type": "Business",
            "name": "Test Metric",
            "calculation": "COUNT(*)",
            "owner": "test@example.com",
        }

        from pydantic import TypeAdapter

        from backend.models.datapoint import DataPoint

        adapter = TypeAdapter(DataPoint)
        datapoint = adapter.validate_python(data)

        assert isinstance(datapoint, BusinessDataPoint)
        assert datapoint.calculation == "COUNT(*)"

    def test_deserialize_process_datapoint(self):
        """JSON with type='Process' deserializes to ProcessDataPoint."""
        data = {
            "datapoint_id": "proc_test_001",
            "type": "Process",
            "name": "Test Process",
            "schedule": "0 0 * * *",
            "data_freshness": "Daily",
            "target_tables": ["test_table"],
            "owner": "test@example.com",
        }

        from pydantic import TypeAdapter

        from backend.models.datapoint import DataPoint

        adapter = TypeAdapter(DataPoint)
        datapoint = adapter.validate_python(data)

        assert isinstance(datapoint, ProcessDataPoint)
        assert datapoint.schedule == "0 0 * * *"

    def test_invalid_type_rejected(self):
        """Invalid type value is rejected."""
        data = {
            "datapoint_id": "test_invalid_001",
            "type": "Invalid",  # Not a valid type
            "name": "Test",
            "owner": "test@example.com",
        }

        from pydantic import TypeAdapter

        from backend.models.datapoint import DataPoint

        adapter = TypeAdapter(DataPoint)

        with pytest.raises(ValidationError):
            adapter.validate_python(data)


class TestJSONSerialization:
    """Test JSON serialization and deserialization."""

    def test_schema_datapoint_round_trip(self):
        """SchemaDataPoint serializes and deserializes correctly."""
        original = SchemaDataPoint(
            datapoint_id="table_test_001",
            name="Test Table",
            table_name="test_table",
            schema="public",
            business_purpose="Test table for serialization",
            key_columns=[
                ColumnMetadata(
                    name="id", type="BIGINT", business_meaning="Primary key", nullable=False
                )
            ],
            owner="test@example.com",
            tags=["test", "example"],
        )

        # Serialize to JSON
        json_str = original.model_dump_json()

        # Deserialize back
        deserialized = SchemaDataPoint.model_validate_json(json_str)

        assert deserialized.datapoint_id == original.datapoint_id
        assert deserialized.name == original.name
        assert len(deserialized.key_columns) == len(original.key_columns)
        assert deserialized.tags == original.tags


class TestQueryParameter:
    """Test QueryParameter model."""

    def test_valid_query_parameter(self):
        """Valid query parameter is accepted."""
        param = QueryParameter(
            type="integer",
            required=False,
            default=10,
            description="Number of results to return",
        )

        assert param.type == "integer"
        assert param.required is False
        assert param.default == 10
        assert param.description == "Number of results to return"

    def test_required_parameter(self):
        """Required parameter without default."""
        param = QueryParameter(type="timestamp", required=True)

        assert param.required is True
        assert param.default is None

    def test_enum_parameter(self):
        """Enum parameter with allowed values."""
        param = QueryParameter(
            type="enum",
            required=True,
            values=["asc", "desc"],
            description="Sort order",
        )

        assert param.type == "enum"
        assert param.values == ["asc", "desc"]

    def test_invalid_type_rejected(self):
        """Invalid parameter type is rejected."""
        with pytest.raises(ValidationError):
            QueryParameter(type="array")  # Not in allowed types


class TestQueryDataPoint:
    """Test QueryDataPoint model."""

    def test_valid_query_datapoint(self):
        """Valid query datapoint is accepted."""
        datapoint = QueryDataPoint(
            datapoint_id="query_top_customers_001",
            name="Top Customers by Revenue",
            sql_template=(
                "SELECT customer_id, SUM(amount) as revenue "
                "FROM transactions WHERE status = 'completed' "
                "GROUP BY customer_id ORDER BY revenue DESC LIMIT {limit}"
            ),
            parameters={
                "limit": QueryParameter(
                    type="integer", default=10, description="Number of customers"
                )
            },
            description="Returns top customers by total revenue",
            owner="sales-team@company.com",
        )

        assert datapoint.type == "Query"
        assert datapoint.datapoint_id == "query_top_customers_001"
        assert "SELECT" in datapoint.sql_template
        assert "limit" in datapoint.parameters

    def test_query_datapoint_with_backend_variants(self):
        """Query datapoint with backend-specific SQL."""
        datapoint = QueryDataPoint(
            datapoint_id="query_daily_sales_001",
            name="Daily Sales Summary",
            sql_template="SELECT date, SUM(amount) FROM sales GROUP BY date",
            description="Daily sales aggregation",
            backend_variants={
                "clickhouse": "SELECT date, SUM(amount) FROM sales GROUP BY date ORDER BY date",
            },
            related_tables=["sales"],
            owner="analytics@company.com",
        )

        assert datapoint.backend_variants is not None
        assert "clickhouse" in datapoint.backend_variants
        assert len(datapoint.related_tables) == 1

    def test_query_datapoint_with_validation_rules(self):
        """Query datapoint with result validation."""
        datapoint = QueryDataPoint(
            datapoint_id="query_metrics_001",
            name="Key Metrics",
            sql_template="SELECT metric, value FROM metrics",
            description="Fetch key business metrics",
            validation={
                "expected_columns": ["metric", "value"],
                "max_rows": 100,
            },
            owner="data@company.com",
        )

        assert datapoint.validation is not None
        assert datapoint.validation["max_rows"] == 100

    def test_short_sql_template_rejected(self):
        """SQL template must be at least 10 characters."""
        with pytest.raises(ValidationError, match="at least 10 characters"):
            QueryDataPoint(
                datapoint_id="query_test_001",
                name="Test Query",
                sql_template="SELECT 1",  # Too short
                description="Test query",
                owner="test@example.com",
            )

    def test_short_description_rejected(self):
        """Description must be at least 10 characters."""
        with pytest.raises(ValidationError, match="at least 10 characters"):
            QueryDataPoint(
                datapoint_id="query_test_001",
                name="Test Query",
                sql_template="SELECT * FROM users LIMIT {limit}",
                description="Short",  # Too short
                owner="test@example.com",
            )

    def test_query_datapoint_with_multiple_parameters(self):
        """Query datapoint with multiple parameters."""
        datapoint = QueryDataPoint(
            datapoint_id="query_range_001",
            name="Query by Date Range",
            sql_template="SELECT * FROM events WHERE date >= {start} AND date < {end} LIMIT {limit}",
            parameters={
                "start": QueryParameter(type="timestamp", required=True),
                "end": QueryParameter(type="timestamp", required=True),
                "limit": QueryParameter(type="integer", default=100),
            },
            description="Query events within a date range",
            owner="team@company.com",
        )

        assert len(datapoint.parameters) == 3
        assert datapoint.parameters["start"].required is True
        assert datapoint.parameters["limit"].default == 100


class TestQueryDataPointDeserialization:
    """Test QueryDataPoint deserialization via discriminated union."""

    def test_deserialize_query_datapoint(self):
        """JSON with type='Query' deserializes to QueryDataPoint."""
        data = {
            "datapoint_id": "query_test_001",
            "type": "Query",
            "name": "Test Query",
            "sql_template": "SELECT * FROM users WHERE id = {user_id}",
            "parameters": {
                "user_id": {"type": "integer", "required": True, "description": "User ID"}
            },
            "description": "Fetch user by ID",
            "owner": "test@example.com",
        }

        from pydantic import TypeAdapter

        from backend.models.datapoint import DataPoint

        adapter = TypeAdapter(DataPoint)
        datapoint = adapter.validate_python(data)

        assert isinstance(datapoint, QueryDataPoint)
        assert datapoint.sql_template == "SELECT * FROM users WHERE id = {user_id}"
        assert "user_id" in datapoint.parameters

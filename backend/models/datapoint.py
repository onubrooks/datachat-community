"""
DataPoint Models

Pydantic models for DataPoints - the knowledge units that power context retrieval.
DataPoints represent tables, metrics, and processes in the data warehouse.

DataPoint types:
    - Schema: Table and column metadata
    - Business: Metrics, calculations, and business glossary
    - Process: ETL processes, data freshness, and dependencies
    - Query: Reusable SQL templates with parameters (Level 2.5)

Usage:
    from backend.models.datapoint import DataPoint, SchemaDataPoint

    # Load from JSON
    datapoint = DataPoint.model_validate_json(json_string)

    # Type-safe access
    if isinstance(datapoint, SchemaDataPoint):
        print(datapoint.table_name)
        print(datapoint.key_columns)
"""

import re
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ============================================================================
# Column and Relationship Models
# ============================================================================


class ColumnMetadata(BaseModel):
    """Metadata for a database column."""

    name: str = Field(..., description="Column name", min_length=1)
    type: str = Field(..., description="SQL data type (e.g., VARCHAR(255), INT)")
    business_meaning: str = Field(
        ..., description="Plain-English explanation of what this column represents", min_length=1
    )
    nullable: bool = Field(..., description="Whether column can contain NULL values")
    default_value: str | None = Field(None, description="Default value if any")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "customer_id",
                "type": "BIGINT",
                "business_meaning": "Unique identifier for customer",
                "nullable": False,
            }
        }
    )


class Relationship(BaseModel):
    """Foreign key relationship to another table."""

    target_table: str = Field(
        ..., description="Name of the target table (can include schema)", min_length=1
    )
    join_column: str = Field(..., description="Column used for the join", min_length=1)
    cardinality: Literal["1:1", "1:N", "N:1", "N:N"] = Field(
        ..., description="Relationship cardinality"
    )
    relationship_type: Literal["foreign_key", "logical"] | None = Field(
        default="foreign_key", description="Type of relationship"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "target_table": "dim_customer",
                "join_column": "customer_id",
                "cardinality": "N:1",
            }
        }
    )


# ============================================================================
# Base DataPoint
# ============================================================================


class BaseDataPoint(BaseModel):
    """
    Base class for all DataPoints.

    Common fields shared across all DataPoint types.
    """

    datapoint_id: str = Field(
        ...,
        description="Unique identifier with format: {type_prefix}_{name}_{number}",
        min_length=1,
        max_length=100,
    )
    type: Literal["Schema", "Business", "Process", "Query"] = Field(
        ..., description="DataPoint type for discriminated union"
    )
    name: str = Field(..., description="Human-readable name", min_length=1, max_length=200)
    owner: str = Field(..., description="Email of the team/person responsible", min_length=1)
    tags: list[str] = Field(default_factory=list, description="Optional tags for categorization")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata key-value pairs"
    )

    model_config = ConfigDict(
        extra="forbid",  # Don't allow extra fields
        str_strip_whitespace=True,
    )

    @field_validator("datapoint_id")
    @classmethod
    def validate_datapoint_id(cls, v: str) -> str:
        """
        Validate datapoint_id format.

        Format: {type_prefix}_{descriptive_name}_{number}
        Examples: table_fact_sales_001, metric_revenue_001, proc_daily_etl_001
        """
        # Pattern: prefix_name_###
        # prefix: lowercase letters
        # name: lowercase letters/numbers/underscores (but not starting/ending with _)
        # number: exactly 3 digits
        pattern = r"^[a-z]+_[a-z0-9]+(?:_[a-z0-9]+)*(?:_\d{3})?$"
        if not re.match(pattern, v):
            raise ValueError(
                f"Invalid datapoint_id format: '{v}'. "
                "Expected format: {prefix}_{name} or {prefix}_{name}_{number} "
                "(e.g., 'table_fact_sales' or 'table_fact_sales_001')"
            )
        return v

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        """Validate owner is an email address."""
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(email_pattern, v):
            raise ValueError(f"Owner must be a valid email address, got: '{v}'")
        return v.lower()


# ============================================================================
# Schema DataPoint
# ============================================================================


class SchemaDataPoint(BaseDataPoint):
    """
    DataPoint representing table and column metadata.

    Captures schema information, relationships, common queries, and gotchas
    for database tables.
    """

    type: Literal["Schema"] = Field(
        default="Schema", description="Type discriminator for Schema DataPoints"
    )
    table_name: str = Field(
        ...,
        description="Full table name (can include schema, e.g., 'analytics.fact_sales')",
        min_length=1,
    )
    schema_name: str = Field(
        ...,
        alias="schema",
        description="Database schema name",
        min_length=1,
    )
    business_purpose: str = Field(
        ..., description="Plain-English explanation of table's purpose", min_length=10
    )
    key_columns: list[ColumnMetadata] = Field(
        ..., description="Important columns in this table", min_length=1
    )
    relationships: list[Relationship] = Field(
        default_factory=list, description="Foreign key relationships to other tables"
    )
    common_queries: list[str] = Field(
        default_factory=list, description="Common SQL patterns for this table"
    )
    gotchas: list[str] = Field(
        default_factory=list, description="Important notes, caveats, or performance tips"
    )
    freshness: str | None = Field(
        None, description="Data freshness (e.g., 'T-1', 'Real-time', 'Monthly')"
    )
    row_count: int | None = Field(None, ge=0, description="Approximate number of rows")

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "datapoint_id": "table_fact_sales_001",
                "type": "Schema",
                "name": "Fact Sales Table",
                "table_name": "analytics.fact_sales",
                "schema": "analytics",
                "business_purpose": "Central fact table for all sales transactions",
                "key_columns": [
                    {
                        "name": "amount",
                        "type": "DECIMAL(18,2)",
                        "business_meaning": "Transaction value in USD",
                        "nullable": False,
                    }
                ],
                "relationships": [
                    {"target_table": "dim_region", "join_column": "region_id", "cardinality": "N:1"}
                ],
                "common_queries": ["SUM(amount)", "GROUP BY region"],
                "gotchas": ["Always filter by date for performance"],
                "freshness": "T-1",
                "owner": "data-team@company.com",
            }
        },
    )


# ============================================================================
# Business DataPoint
# ============================================================================


class BusinessDataPoint(BaseDataPoint):
    """
    DataPoint representing business metrics, KPIs, and glossary terms.

    Captures metric definitions, calculations, synonyms, and business rules.
    """

    type: Literal["Business"] = Field(
        default="Business", description="Type discriminator for Business DataPoints"
    )
    calculation: str = Field(..., description="SQL calculation or metric definition", min_length=1)
    synonyms: list[str] = Field(
        default_factory=list, description="Alternative names users might use for this metric"
    )
    business_rules: list[str] = Field(
        default_factory=list, description="Business logic and rules for calculating this metric"
    )
    related_tables: list[str] = Field(
        default_factory=list, description="Tables involved in calculating this metric"
    )
    unit: str | None = Field(
        None, description="Unit of measurement (e.g., 'USD', 'count', 'percentage')"
    )
    aggregation: Literal["SUM", "AVG", "COUNT", "MIN", "MAX", "CUSTOM"] | None = Field(
        None, description="Type of aggregation used"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "datapoint_id": "metric_revenue_001",
                "type": "Business",
                "name": "Revenue",
                "calculation": "SUM(fact_sales.amount) WHERE status = 'completed'",
                "synonyms": ["sales", "income", "earnings", "total sales"],
                "business_rules": [
                    "Exclude refunds (status != 'refunded')",
                    "Convert to USD using daily rate",
                ],
                "related_tables": ["fact_sales", "dim_currency"],
                "unit": "USD",
                "aggregation": "SUM",
                "owner": "finance@company.com",
            }
        }
    )


# ============================================================================
# Process DataPoint
# ============================================================================


class ProcessDataPoint(BaseDataPoint):
    """
    DataPoint representing ETL processes and data pipelines.

    Captures process schedules, data freshness, dependencies, and SLAs.
    """

    type: Literal["Process"] = Field(
        default="Process", description="Type discriminator for Process DataPoints"
    )
    schedule: str = Field(..., description="Cron schedule or frequency description", min_length=1)
    data_freshness: str = Field(
        ..., description="When data becomes available (e.g., 'T-1 by 3am UTC')", min_length=1
    )
    target_tables: list[str] = Field(
        ..., description="Tables updated by this process", min_length=1
    )
    dependencies: list[str] = Field(
        default_factory=list, description="Upstream tables or processes this depends on"
    )
    sla: str | None = Field(
        None, description="Service level agreement (e.g., '99.9% uptime', '< 1 hour')"
    )
    monitoring_url: str | None = Field(None, description="URL to monitoring dashboard or logs")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "datapoint_id": "proc_daily_etl_001",
                "type": "Process",
                "name": "Daily Sales ETL",
                "schedule": "0 2 * * *",
                "data_freshness": "T-1 (yesterday's data available by 3am UTC)",
                "target_tables": ["analytics.fact_sales"],
                "dependencies": ["raw.sales_events"],
                "sla": "Complete within 1 hour",
                "owner": "data-eng@company.com",
            }
        }
    )


# ============================================================================
# Query DataPoint (Level 2.5)
# ============================================================================


class QueryParameter(BaseModel):
    """Parameter definition for a QueryDataPoint SQL template."""

    type: Literal["string", "integer", "float", "timestamp", "enum", "boolean"] = Field(
        ..., description="Parameter data type"
    )
    required: bool = Field(default=False, description="Whether parameter is required")
    default: str | int | float | bool | None = Field(
        None, description="Default value if not provided"
    )
    description: str | None = Field(None, description="Parameter description")
    values: list[str] | None = Field(None, description="Allowed values for enum type")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "integer",
                "required": False,
                "default": 10,
                "description": "Number of results to return",
            }
        }
    )


class QueryDataPoint(BaseDataPoint):
    """
    DataPoint representing a reusable SQL template.

    QueryDataPoints enable:
    - Pre-validated SQL templates for common queries
    - Parameterized execution with validation
    - Backend-specific SQL variants
    - Skip SQL generation for known patterns (faster, more consistent)

    This is a Level 2.5 feature.
    """

    type: Literal["Query"] = Field(
        default="Query", description="Type discriminator for Query DataPoints"
    )
    sql_template: str = Field(
        ..., description="SQL template with {parameter} placeholders", min_length=10
    )
    parameters: dict[str, QueryParameter] = Field(
        default_factory=dict, description="Parameter definitions for the template"
    )
    description: str = Field(
        ..., description="What this query does and when to use it", min_length=10
    )
    backend_variants: dict[str, str] | None = Field(
        None, description="Backend-specific SQL variants (e.g., {'clickhouse': '...'})"
    )
    validation: dict[str, Any] | None = Field(
        None, description="Result validation rules (expected_columns, max_rows, etc.)"
    )
    related_tables: list[str] = Field(default_factory=list, description="Tables used in this query")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "datapoint_id": "query_top_customers_001",
                "type": "Query",
                "name": "Top Customers by Revenue",
                "sql_template": "SELECT customer_id, SUM(amount) as revenue FROM transactions WHERE status = 'completed' AND transaction_time >= {start_time} GROUP BY customer_id ORDER BY revenue DESC LIMIT {limit}",
                "parameters": {
                    "limit": {
                        "type": "integer",
                        "default": 10,
                        "description": "Number of customers",
                    },
                    "start_time": {"type": "timestamp", "required": True},
                },
                "description": "Returns top customers by total revenue for a given time period",
                "backend_variants": {
                    "clickhouse": "SELECT customer_id, SUM(amount) as revenue FROM transactions WHERE status = 'completed' AND transaction_time >= {start_time} GROUP BY customer_id ORDER BY revenue DESC LIMIT {limit}"
                },
                "related_tables": ["transactions"],
                "owner": "sales-team@company.com",
            }
        }
    )


# ============================================================================
# Discriminated Union
# ============================================================================


DataPoint = Annotated[
    SchemaDataPoint | BusinessDataPoint | ProcessDataPoint | QueryDataPoint,
    Field(discriminator="type"),
]
"""
Discriminated union of all DataPoint types.

Pydantic automatically deserializes to the correct type based on the 'type' field.

Usage:
    datapoint = DataPoint.model_validate(data)
    if isinstance(datapoint, SchemaDataPoint):
        # Handle schema datapoint
        pass
"""

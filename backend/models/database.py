"""
Database connection models.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, SecretStr


class DatabaseConnection(BaseModel):
    """Stored database connection details."""

    connection_id: UUID = Field(default_factory=uuid4, description="Connection identifier")
    name: str = Field(..., min_length=1, description="User-friendly name")
    database_url: SecretStr = Field(..., description="Encrypted database URL")
    database_type: Literal["postgresql", "clickhouse", "mysql"] = Field(
        ..., description="Database engine type"
    )
    is_active: bool = Field(default=True, description="Whether the connection is active")
    is_default: bool = Field(default=False, description="Whether this is the default connection")
    tags: list[str] = Field(default_factory=list, description="Tags for grouping")
    description: str | None = Field(None, description="Optional description")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Creation timestamp"
    )
    last_profiled: datetime | None = Field(None, description="Last profiling timestamp")
    datapoint_count: int = Field(default=0, description="Linked DataPoint count")


class DatabaseConnectionCreate(BaseModel):
    """Payload for creating a database connection."""

    name: str = Field(..., min_length=1, description="User-friendly name")
    database_url: SecretStr = Field(..., description="Database URL")
    database_type: Literal["postgresql", "clickhouse", "mysql"] = Field(
        ..., description="Database engine type"
    )
    tags: list[str] = Field(default_factory=list, description="Tags for grouping")
    description: str | None = Field(None, description="Optional description")
    is_default: bool = Field(default=False, description="Set as default connection")


class DatabaseConnectionUpdate(BaseModel):
    """Payload for updating an existing connection."""

    name: str | None = Field(default=None, min_length=1, description="User-friendly name")
    database_url: SecretStr | None = Field(default=None, description="Database URL")
    database_type: Literal["postgresql", "clickhouse", "mysql"] | None = Field(
        default=None, description="Database engine type"
    )
    description: str | None = Field(default=None, description="Optional description")


class DatabaseConnectionUpdateDefault(BaseModel):
    """Payload for setting the default connection."""

    is_default: bool = Field(default=True, description="Set connection as default")


class DatabaseSchemaColumn(BaseModel):
    """Schema metadata for a single column."""

    name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="Column SQL type")
    is_nullable: bool = Field(..., description="Whether the column allows NULL")
    is_primary_key: bool = Field(default=False, description="Whether this column is a PK")
    is_foreign_key: bool = Field(default=False, description="Whether this column is an FK")
    foreign_table: str | None = Field(None, description="Referenced table for FK")
    foreign_column: str | None = Field(None, description="Referenced column for FK")


class DatabaseSchemaTable(BaseModel):
    """Schema metadata for a single table."""

    schema_name: str = Field(..., description="Database schema name")
    table_name: str = Field(..., description="Table name")
    row_count: int | None = Field(None, description="Approximate row count")
    table_type: str = Field(..., description="Table type (TABLE, VIEW, etc.)")
    columns: list[DatabaseSchemaColumn] = Field(default_factory=list)


class DatabaseSchemaResponse(BaseModel):
    """Schema payload returned to frontend schema explorer."""

    connection_id: UUID = Field(..., description="Connection identifier")
    database_type: Literal["postgresql", "clickhouse", "mysql"] = Field(
        ..., description="Database engine type"
    )
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Schema fetch timestamp"
    )
    tables: list[DatabaseSchemaTable] = Field(default_factory=list)

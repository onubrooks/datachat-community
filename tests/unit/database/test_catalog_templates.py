"""Unit tests for catalog query templates used in credentials-only mode."""

from backend.database.catalog_templates import (
    get_catalog_aliases,
    get_catalog_schemas,
    get_list_columns_query,
    get_list_tables_query,
    normalize_database_type,
)


def test_normalize_database_type_maps_postgres_aliases():
    assert normalize_database_type("postgres") == "postgresql"
    assert normalize_database_type("postgresql") == "postgresql"


def test_list_tables_query_exists_for_supported_databases():
    for db_type in ("postgresql", "mysql", "clickhouse", "bigquery", "redshift"):
        query = get_list_tables_query(db_type)
        assert query is not None
        assert "table" in query.lower()


def test_list_tables_query_excludes_internal_service_tables_for_postgres_and_mysql():
    postgres_query = get_list_tables_query("postgresql")
    mysql_query = get_list_tables_query("mysql")

    assert postgres_query is not None
    assert mysql_query is not None

    for table_name in (
        "database_connections",
        "profiling_jobs",
        "profiling_profiles",
        "pending_datapoints",
        "datapoint_generation_jobs",
    ):
        assert table_name in postgres_query
        assert table_name in mysql_query


def test_catalog_schemas_include_common_system_namespaces():
    assert "information_schema" in get_catalog_schemas("postgresql")
    assert "system" in get_catalog_schemas("clickhouse")
    assert "pg_catalog" in get_catalog_schemas("redshift")


def test_catalog_aliases_cover_redshift_and_clickhouse_system_tables():
    redshift_aliases = get_catalog_aliases("redshift")
    clickhouse_aliases = get_catalog_aliases("clickhouse")
    assert "svv_tables" in redshift_aliases
    assert "system.tables" in clickhouse_aliases


def test_list_columns_query_exists_for_supported_databases():
    for db_type in ("postgresql", "mysql", "clickhouse", "bigquery", "redshift"):
        query = get_list_columns_query(db_type, table_name="sales")
        assert query is not None
        assert "column" in query.lower()


def test_list_columns_query_accepts_schema_override():
    query = get_list_columns_query(
        "postgresql",
        table_name="sales",
        schema_name="analytics",
    )
    assert "table_schema = 'analytics'" in query

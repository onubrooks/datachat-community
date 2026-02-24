"""Database catalog query templates for credentials-only discovery."""

from __future__ import annotations

from typing import NamedTuple


class CatalogTemplates(NamedTuple):
    """System-catalog templates for metadata discovery."""

    list_tables: str
    list_columns: str


_CATALOG_TEMPLATES: dict[str, CatalogTemplates] = {
    "postgresql": CatalogTemplates(
        list_tables=(
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
            "AND table_name NOT IN ("
            "'database_connections', "
            "'profiling_jobs', "
            "'profiling_profiles', "
            "'pending_datapoints', "
            "'datapoint_generation_jobs'"
            ") "
            "ORDER BY table_schema, table_name"
        ),
        list_columns=(
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = '{table}' "
            "{schema_predicate}"
            "ORDER BY table_schema, table_name, ordinal_position"
        ),
    ),
    "mysql": CatalogTemplates(
        list_tables=(
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('mysql', 'performance_schema', 'information_schema', 'sys') "
            "AND table_name NOT IN ("
            "'database_connections', "
            "'profiling_jobs', "
            "'profiling_profiles', "
            "'pending_datapoints', "
            "'datapoint_generation_jobs'"
            ") "
            "ORDER BY table_schema, table_name"
        ),
        list_columns=(
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = '{table}' "
            "{schema_predicate}"
            "ORDER BY table_schema, table_name, ordinal_position"
        ),
    ),
    "clickhouse": CatalogTemplates(
        list_tables=(
            "SELECT database, name "
            "FROM system.tables "
            "WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') "
            "ORDER BY database, name"
        ),
        list_columns=(
            "SELECT database AS table_schema, table AS table_name, name AS column_name, type AS data_type "
            "FROM system.columns "
            "WHERE table = '{table}' "
            "{schema_predicate}"
            "ORDER BY table_schema, table_name, position"
        ),
    ),
    "bigquery": CatalogTemplates(
        list_tables=(
            "SELECT table_schema, table_name "
            "FROM INFORMATION_SCHEMA.TABLES "
            "ORDER BY table_schema, table_name"
        ),
        list_columns=(
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_name = '{table}' "
            "{schema_predicate}"
            "ORDER BY table_schema, table_name, ordinal_position"
        ),
    ),
    "redshift": CatalogTemplates(
        list_tables=(
            "SELECT schemaname AS table_schema, tablename AS table_name "
            "FROM pg_table_def "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "GROUP BY schemaname, tablename "
            "ORDER BY schemaname, tablename"
        ),
        list_columns=(
            "SELECT schemaname AS table_schema, tablename AS table_name, "
            "\"column\" AS column_name, type AS data_type "
            "FROM pg_table_def "
            "WHERE tablename = '{table}' "
            "{schema_predicate}"
            "ORDER BY table_schema, table_name, column_name"
        ),
    ),
}


_CATALOG_SCHEMAS: dict[str, set[str]] = {
    "postgresql": {"information_schema", "pg_catalog"},
    "mysql": {"information_schema", "mysql", "performance_schema", "sys"},
    "clickhouse": {"information_schema", "system"},
    "bigquery": {"information_schema"},
    "redshift": {"information_schema", "pg_catalog", "pg_internal"},
}


_CATALOG_ALIASES: dict[str, set[str]] = {
    "postgresql": {
        "pg_tables",
        "pg_class",
        "pg_namespace",
        "pg_attribute",
        "pg_stat_all_tables",
        "pg_stat_user_tables",
        "pg_indexes",
        "pg_constraint",
        "pg_description",
        "pg_type",
        "pg_roles",
        "pg_stat_activity",
        "pg_stat_database",
        "pg_locks",
        "pg_settings",
    },
    "mysql": {
        "information_schema.tables",
        "information_schema.columns",
        "information_schema.statistics",
        "information_schema.key_column_usage",
        "mysql.user",
        "mysql.db",
        "performance_schema.threads",
        "performance_schema.events_statements_summary_by_digest",
    },
    "clickhouse": {
        "system.tables",
        "system.columns",
        "system.databases",
        "system.parts",
        "system.settings",
    },
    "bigquery": {
        "information_schema.tables",
        "information_schema.columns",
        "information_schema.schemata",
        "information_schema.table_options",
    },
    "redshift": {
        "svv_tables",
        "svv_columns",
        "svv_table_info",
        "svv_views",
        "svv_schema",
        "stl_query",
        "stl_scan",
        "stl_wlm_query",
        "svl_qlog",
        "svl_query_report",
        "pg_table_def",
    },
}


def normalize_database_type(database_type: str | None) -> str:
    """Map aliases to canonical database type keys."""
    normalized = (database_type or "").strip().lower()
    if normalized in {"postgres", "postgresql"}:
        return "postgresql"
    return normalized


def get_list_tables_query(database_type: str | None) -> str | None:
    """Return catalog query used for table discovery."""
    db_type = normalize_database_type(database_type)
    templates = _CATALOG_TEMPLATES.get(db_type)
    return templates.list_tables if templates else None


def get_list_columns_query(
    database_type: str | None,
    *,
    table_name: str,
    schema_name: str | None = None,
) -> str | None:
    """Return catalog query used for column discovery."""
    db_type = normalize_database_type(database_type)
    templates = _CATALOG_TEMPLATES.get(db_type)
    if not templates:
        return None

    if schema_name:
        schema_predicate = f"AND table_schema = '{schema_name}' "  # nosec B608
        if db_type == "clickhouse":
            schema_predicate = f"AND database = '{schema_name}' "  # nosec B608
        if db_type == "redshift":
            schema_predicate = f"AND schemaname = '{schema_name}' "  # nosec B608
    else:
        schema_predicate = ""
        if db_type in {"postgresql", "redshift"}:
            schema_predicate = (
                "AND table_schema NOT IN ('pg_catalog', 'information_schema') "
            )
        elif db_type == "mysql":
            schema_predicate = (
                "AND table_schema NOT IN ('mysql', 'performance_schema', 'information_schema', 'sys') "
            )
        elif db_type == "clickhouse":
            schema_predicate = (
                "AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') "
            )

    return templates.list_columns.format(
        table=table_name,
        schema_predicate=schema_predicate,
    )


def get_catalog_schemas(database_type: str | None) -> set[str]:
    """Return schema names treated as catalog/system schemas."""
    db_type = normalize_database_type(database_type)
    return _CATALOG_SCHEMAS.get(db_type, {"information_schema"})


def get_catalog_aliases(database_type: str | None) -> set[str]:
    """Return table aliases treated as catalog/system objects."""
    db_type = normalize_database_type(database_type)
    return _CATALOG_ALIASES.get(db_type, set())

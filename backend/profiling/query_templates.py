"""Query templates for schema profiling across warehouse dialects."""

from __future__ import annotations

from typing import NamedTuple


class ProfilingQueryTemplates(NamedTuple):
    """Dialect-specific system queries used during profiling."""

    list_tables: str
    list_columns: str
    row_estimate: str | None = None
    relationships: str | None = None
    column_stats: str | None = None


_PROFILING_TEMPLATES: dict[str, ProfilingQueryTemplates] = {
    "postgresql": ProfilingQueryTemplates(
        list_tables=(
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE' "
            "AND table_schema NOT IN ('pg_catalog', 'information_schema') "
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
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2 "
            "ORDER BY ordinal_position"
        ),
        row_estimate=(
            "SELECT reltuples::BIGINT AS estimate "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = $2"
        ),
        relationships=(
            "SELECT "
            "kcu.table_name AS source_table, "
            "kcu.column_name AS source_column, "
            "ccu.table_name AS target_table, "
            "ccu.column_name AS target_column "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            " AND tc.table_schema = kcu.table_schema "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON ccu.constraint_name = tc.constraint_name "
            " AND ccu.table_schema = tc.table_schema "
            "WHERE tc.constraint_type = 'FOREIGN KEY' "
            "AND kcu.table_schema = $1 "
            "AND kcu.table_name = $2"
        ),
        column_stats=(
            "SELECT "
            "COUNT(*) FILTER (WHERE {column} IS NULL) AS null_count, "
            "COUNT(DISTINCT {column}) AS distinct_count, "
            "MIN({column})::text AS min_value, "
            "MAX({column})::text AS max_value "
            "FROM {table}"
        ),
    ),
    "mysql": ProfilingQueryTemplates(
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
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position"
        ),
    ),
    "bigquery": ProfilingQueryTemplates(
        list_tables=(
            "SELECT table_schema, table_name "
            "FROM INFORMATION_SCHEMA.TABLES "
            "ORDER BY table_schema, table_name"
        ),
        list_columns=(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_schema = @schema AND table_name = @table "
            "ORDER BY ordinal_position"
        ),
    ),
    "clickhouse": ProfilingQueryTemplates(
        list_tables=(
            "SELECT database AS table_schema, name AS table_name "
            "FROM system.tables "
            "WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') "
            "ORDER BY database, name"
        ),
        list_columns=(
            "SELECT name AS column_name, type AS data_type, "
            "if(default_kind = '', 'YES', 'NO') AS is_nullable, default_expression AS column_default "
            "FROM system.columns "
            "WHERE database = {schema:String} AND table = {table:String} "
            "ORDER BY position"
        ),
    ),
    "redshift": ProfilingQueryTemplates(
        list_tables=(
            "SELECT schemaname AS table_schema, tablename AS table_name "
            "FROM pg_table_def "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "GROUP BY schemaname, tablename "
            "ORDER BY schemaname, tablename"
        ),
        list_columns=(
            "SELECT \"column\" AS column_name, type AS data_type, "
            "CASE WHEN \"notnull\" THEN 'NO' ELSE 'YES' END AS is_nullable, NULL AS column_default "
            "FROM pg_table_def "
            "WHERE schemaname = :schema AND tablename = :table "
            "ORDER BY column_name"
        ),
    ),
}


def normalize_database_type(database_type: str | None) -> str:
    """Map aliases to canonical profiling template keys."""
    value = (database_type or "").strip().lower()
    if value in {"postgres", "postgresql"}:
        return "postgresql"
    return value


def get_profiling_templates(database_type: str | None) -> ProfilingQueryTemplates | None:
    """Return profiling query templates for a database type."""
    return _PROFILING_TEMPLATES.get(normalize_database_type(database_type))


def supported_profile_template_databases() -> list[str]:
    """List database types with profiling query templates."""
    return sorted(_PROFILING_TEMPLATES.keys())

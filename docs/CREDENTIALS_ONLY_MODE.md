# Credentials-Only Mode

Credentials-only mode lets DataChat answer questions with only database credentials
and live metadata, without loading DataPoints.

## How It Works

1. Live schema snapshot from system catalogs (tables and columns).
2. Deterministic SQL fallbacks for common requests:
   - list tables
   - sample rows
   - row counts
3. Lightweight profile context (Postgres):
   - row estimates from `pg_class`
   - column stats from `pg_stats`
4. Join inference heuristics from `*_id` column patterns.
5. LLM SQL generation as fallback when deterministic paths are not enough.

## Support Matrix

| Database | Catalog query templates | Live connector execution | Notes |
| --- | --- | --- | --- |
| PostgreSQL | Yes | Yes | Full credentials-only path. |
| ClickHouse | Yes | Yes | Catalog templates + connector supported. |
| MySQL | Yes | Not yet | Templates/validation ready; connector pending. |
| BigQuery | Yes | Not yet | Templates ready for connector onboarding. |
| Redshift | Yes | Not yet | Templates ready; execution will work after connector integration. |

Important:

- "Catalog query templates" means DataChat can generate correct system-catalog SQL.
- "Live connector execution" means DataChat can connect and execute those queries today.

## Capabilities vs. Limits

| Capability | Status | Notes |
| --- | --- | --- |
| Basic table discovery | Supported | Uses catalog tables directly. |
| Basic column discovery | Supported | Included in live schema context. |
| Row count questions | Supported | Deterministic fallback when table is explicit. |
| Sample rows | Supported | Deterministic fallback with explicit table hints. |
| Join suggestions | Partial | Heuristic only, may miss complex relationships. |
| Business metrics (strict KPI definitions) | Limited | Requires DataPoints/docs for precision. |
| Cross-domain semantic interpretation | Limited | Improves with DataPoints and domain docs. |

## Best Practices

- Ask with explicit table names (`first 5 rows from public.orders`).
- For ambiguous metrics, specify both table and column.
- Move critical metrics to DataPoints once definitions stabilize.

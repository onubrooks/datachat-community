# Multi-Database Guide

This document covers currently implemented multi-database behavior.

## What Is Supported Now

- Store multiple DB connections in the system database.
- Mark one default connection.
- Route chat requests to a specific connection with `target_database`.
- Route tool execution to a specific connection with `target_database`.
- Encrypted URL storage using `DATABASE_CREDENTIALS_KEY`.

## Prerequisites

Set both:

```env
SYSTEM_DATABASE_URL=postgresql://user:password@host:5432/datachat
DATABASE_CREDENTIALS_KEY=<fernet-key>
```

Generate key:

```bash
python - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
```

## Database Types (Registry Validation)

Accepted today:

- `postgresql`
- `clickhouse`
- `mysql`

Notes:

- Credentials-only catalog templates also exist for `bigquery` and `redshift`.
- Runtime connectors are currently implemented for `postgresql`, `clickhouse`, and `mysql`.
- Profiling execution still runs on PostgreSQL only.

## Add / List / Set Default / Delete

Add:

```bash
curl -X POST http://localhost:8000/api/v1/databases \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Analytics Warehouse",
    "database_url": "postgresql://user:pass@host:5432/analytics",
    "database_type": "postgresql",
    "tags": ["prod"],
    "is_default": true
  }'
```

List:

```bash
curl http://localhost:8000/api/v1/databases
```

Set default:

```bash
curl -X PUT http://localhost:8000/api/v1/databases/<id>/default \
  -H "Content-Type: application/json" \
  -d '{"is_default": true}'
```

Delete:

```bash
curl -X DELETE http://localhost:8000/api/v1/databases/<id>
```

## Per-Request Routing

### Chat endpoint

```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Top 10 customers by revenue",
    "target_database": "<connection-id>"
  }'
```

### Tools endpoint

```bash
curl -X POST http://localhost:8000/api/v1/tools/execute \
  -H "Content-Type: application/json" \
  -d '{
    "name": "list_tables",
    "arguments": {"schema": "public"},
    "target_database": "<connection-id>"
  }'
```

## Behavior Guarantees

- If `target_database` is present and valid, that connection is used.
- If `target_database` is omitted, the default connection is used when available.
- If `target_database` is provided but registry is unavailable, request fails (no silent fallback).
- If `target_database` is invalid/unknown, request fails with `400/404`.

## DataPoint Scoping (Important)

DataChat keeps one shared vector index, but retrieval is scoped at runtime:

- Keep DataPoints that match `metadata.connection_id == target_database`
- Keep explicitly shared/global DataPoints (`metadata.scope=global` or `metadata.shared=true`)
- If no scoped/global DataPoints are available for a query, legacy unscoped DataPoints can still be used as fallback

To avoid cross-database context bleed, scope DataPoints during sync:

```bash
# Scope DataPoints to one database connection
datachat dp sync \
  --datapoints-dir datapoints/examples/fintech_bank \
  --connection-id <connection-id>
```

For intentionally shared reference DataPoints:

```bash
datachat dp sync \
  --datapoints-dir datapoints/shared \
  --global-scope
```

UI equivalent:

- Open `Database Management` -> `Sync Status`
- Choose `Scope: selected database` or `Scope: global/shared`
- Click `Sync Now`

## Operational Recommendation

Use registry mode for team environments and any setup where accidental default-db execution is risky.

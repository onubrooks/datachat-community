# DataChat API

Base URL: `http://localhost:8000/api/v1`

## System Initialization

- `GET /system/status` - Returns initialization status and setup steps.
- `POST /system/initialize` - Initialize with a database URL and optional auto-profiling.
- `POST /system/entry-event` - Record lightweight setup/quickstart telemetry events.

Request body:

```json
{
  "database_url": "postgresql://user:pass@host:5432/db",
  "system_database_url": "postgresql://user:pass@host:5432/datachat",
  "auto_profile": true
}
```

Notes:

- When provided, `database_url` and `system_database_url` are persisted to `~/.datachat/config.json`.
- `is_initialized=true` means a target database is connected and chat can run.
- DataPoints are optional enrichment; when absent, chat runs in live schema mode.

Entry telemetry payload:

```json
{
  "flow": "phase1_4_quickstart_ui",
  "step": "profile_database",
  "status": "started",
  "source": "ui",
  "metadata": {
    "connection_id": "..."
  }
}
```

## Chat

- `POST /chat` - Submit a query.
- `WS /ws/chat` - WebSocket streaming for agent updates and answer chunks.

Request body:

```json
{
  "message": "What was revenue last quarter?",
  "conversation_id": "conv_123",
  "target_database": "optional-connection-id",
  "session_summary": "Intent summary: last_goal=What was revenue last quarter?",
  "session_state": {
    "last_goal": "What was revenue last quarter?"
  },
  "synthesize_simple_sql": true,
  "workflow_mode": "auto"
}
```

Notes:

- If `target_database` is provided, SQL generation and execution both use that
  connection's database type and URL.
- If `target_database` is omitted, the default registry connection is used when set.
- `synthesize_simple_sql` is optional. When `false`, simple SQL answers skip
  response synthesis for lower latency.
- `workflow_mode` is optional:
  - `auto` (default): infer workflow packaging from query/source signals.
  - `finance_variance_v1`: force finance-brief packaging when possible.
- `session_summary` and `session_state` are optional turn-to-turn memory fields.
  Clients should echo these from one response into the next request to improve
  follow-up continuity with bounded token usage.
- Responses include `decision_trace` (stage/decision/reason tuples) for routing
  observability and regression evaluation.
- When no DataPoints are loaded, responses include a live schema mode notice.
- Single prompts with multiple questions may be decomposed (up to 3 sub-questions).
  In that case, response includes `sub_answers` and an aggregated `answer`.

Response fields (selected):

- `answer`: final aggregated answer text.
- `clarifying_questions`: follow-up questions, tagged per sub-question when decomposed.
- `sub_answers`: per-subquestion entries with:
  - `index`
  - `query`
  - `answer`
  - `answer_source`
  - `answer_confidence`
  - `sql`
  - `clarifying_questions`
  - `error`
- `session_summary`: compact summary to pass into the next turn.
- `session_state`: structured memory object to pass into the next turn.
- `decision_trace`: deterministic routing trace entries used by eval/ops gates.
- `workflow_artifacts` (optional): decision-ready finance brief package with:
  - `summary`
  - `metrics` (label/value pairs)
  - `drivers` (top contributors)
  - `caveats`
  - `sources`
  - `follow_ups`

## Database Connections

- `POST /databases` - Create a connection.
- `GET /databases` - List connections.
- `GET /databases/{id}` - Fetch a single connection.
- `GET /databases/{id}/schema` - Introspect tables/columns for schema explorer.
- `PUT /databases/{id}/default` - Set default connection.
- `DELETE /databases/{id}` - Remove a connection.

Notes:

- If `DATABASE_URL` is configured, `GET /databases` also returns a virtual
  `Environment Database` entry (read-only in registry endpoints).
- `POST /databases` validates that `database_type` matches the URL scheme
  (`postgresql://`, `mysql://`, `clickhouse://`) and returns `400` on mismatch.
- `GET /databases/{id}/schema` opens a read-only connector, introspects live
  schema metadata, and returns table + column details (including PK/FK flags).

## Conversation History

- `GET /conversations?limit=20` - List saved UI conversation snapshots ordered by `updated_at`.
- `PUT /conversations/{frontend_session_id}` - Upsert a conversation snapshot.
- `DELETE /conversations/{frontend_session_id}` - Delete a saved snapshot.

Notes:

- Backed by `ui_conversations` in the system database when `SYSTEM_DATABASE_URL` is configured.
- If system DB persistence is unavailable, upserts fall back to logs-only behavior and clients can still use local cache.

## Profiling and DataPoint Generation

- `POST /databases/{id}/profile` - Start profiling a database.
- `GET /profiling/jobs/{id}` - Check profiling job status.
- `POST /datapoints/generate` - Generate DataPoints from a profile.
- `GET /datapoints/pending` - List pending DataPoints.
- `POST /datapoints/pending/{id}/approve` - Approve a DataPoint.
- `POST /datapoints/pending/{id}/reject` - Reject a DataPoint.
- `POST /datapoints/pending/bulk-approve` - Approve all pending DataPoints.

Profiling request payload (bounded/safe by default):

```json
{
  "sample_size": 100,
  "max_tables": 50,
  "max_columns_per_table": 100,
  "query_timeout_seconds": 5,
  "per_table_timeout_seconds": 20,
  "total_timeout_seconds": 180,
  "fail_fast": false,
  "tables": ["orders", "customers"]
}
```

Profiling progress now includes partial coverage metadata:

- `total_tables`
- `tables_completed`
- `tables_failed`
- `tables_skipped`

Notes:

- Profiling is resilient to per-table failures and timeouts; a job can complete with partial coverage.
- Lightweight profiling snapshots are cached locally and used to enrich credentials-only SQL prompts.
- Query templates are available for `postgresql`, `mysql`, `bigquery`, `clickhouse`, and `redshift`.
- Runtime query connectors are available for `postgresql`, `clickhouse`, and `mysql`.
- Profiling execution remains PostgreSQL-only.

Approve payload supports optional edits:

```json
{
  "review_note": "optional",
  "datapoint": { "datapoint_id": "table_users_001", "...": "..." }
}
```

## DataPoint Sync

- `POST /sync` - Trigger a full sync.
- `GET /sync/status` - Get sync job status.
- `GET /datapoints` - List locally available DataPoints.
- `POST /datapoints` - Create a DataPoint.
- `PUT /datapoints/{id}` - Update a DataPoint.
- `DELETE /datapoints/{id}` - Delete a DataPoint.

`POST /sync` supports conflict handling controls:

```json
{
  "scope": "auto",
  "connection_id": null,
  "conflict_mode": "error"
}
```

`conflict_mode` values:

- `error` (default): fail sync on conflicting semantic definitions.
- `prefer_user`: keep user-tier definitions when conflicts exist.
- `prefer_managed`: keep managed-tier definitions when conflicts exist.
- `prefer_latest`: keep the highest lifecycle version / most recent lifecycle timestamp.

`GET /datapoints` returns DataPoints currently loaded in the vector store
(the same effective set used during retrieval/chat), deduplicated by
`datapoint_id` with priority:

- `user` > `managed` > `custom`/`unknown` > `example`

List item shape includes:

- `datapoint_id`
- `type`
- `name`
- `source_tier` (for example `managed`, `example`, `custom`)
- `source_path` (source file path when available)
- `lifecycle_version`
- `lifecycle_reviewer`
- `lifecycle_changed_by`
- `lifecycle_changed_reason`
- `lifecycle_changed_at`

`POST /datapoints` and `PUT /datapoints/{id}` auto-populate `metadata.lifecycle`
for authoring auditability:

- `owner`, `reviewer`, `version`, `changed_by`, `changed_reason`, `changed_at`

## Tools

- `GET /tools` - List available tools and typed parameter schemas.
- `POST /tools/execute` - Execute a tool call.

Tool execute request:

```json
{
  "name": "get_table_sample",
  "arguments": {
    "table": "orders",
    "schema": "public",
    "limit": 5
  },
  "target_database": "optional-connection-id",
  "approved": false,
  "user_id": "optional-user-id",
  "correlation_id": "optional-correlation-id"
}
```

Notes:

- `target_database` is optional; when provided, tool execution uses that connection's database type/URL context.
- If `target_database` is omitted, the default connection context is used when available.
- `/tools/execute` injects runtime metadata for built-ins (`retriever`, `database_type`, `database_url`, registry/connector handles) so `context_answer`, `run_sql`, `list_tables`, `list_columns`, and `get_table_sample` work consistently from API calls.
- Tool parameter schemas are typed from Python annotations (for example `integer`, `boolean`, `number`, `array`, `object`) instead of all-string placeholders.

## Health

- `GET /health` - Health check.
- `GET /ready` - Readiness check.

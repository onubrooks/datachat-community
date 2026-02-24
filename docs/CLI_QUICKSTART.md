# CLI Quickstart

Use the CLI to initialize DataChat, profile a database, and ask questions from the terminal.

## Prerequisites

- Python 3.10+
- Backend running (`uvicorn backend.api.main:app --reload --port 8000`)
- CLI installed in editable mode from the repo root:

```bash
pip install -e .
datachat --help
datachat cheat-sheet
```

## 1) Run The Onboarding Wizard (Recommended)

For first-time evaluation, use one command:

```bash
datachat onboarding wizard \
  --database-url postgresql://postgres:@localhost:5432/postgres \
  --system-db postgresql://postgres:@localhost:5432/postgres
```

What this does automatically:

1. Registers/sets the target connection as default.
2. Profiles schema + sampled column structure.
3. Generates managed metadata (schema summaries + business metrics + semantic hints).
4. Indexes generated managed DataPoints in vector retrieval.
5. Writes a run summary report under `reports/onboarding_wizard_<connection_id>.md`.

## 2) Ask Questions Immediately

```bash
datachat ask "What tables are available in the database?"
```

Control clarification prompts:

```bash
datachat ask --max-clarifications 3 "Show me the first 5 rows"
```

Quick tutorial (interactive):

```bash
datachat chat
```

## 3) Advanced/Manual Onboarding (Optional)

Use this path only when you need manual control over each step:

```bash
datachat setup
datachat profile start --connection-id <connection_id>
datachat dp generate --profile-id <profile_id> --depth metrics_full
datachat dp pending approve-all --latest
```

Example flow:

1. Ask: `Show me the first 5 rows`
2. When prompted, answer with a table: `sales`
3. Follow up with a column or limit if asked (for example: `amount`, `first 2 rows`)

Multi-question prompt (single round trip):

```bash
datachat ask "List active accounts and what is total deposits?"
```

Expected:

- One response with a combined answer.
- If decomposition applies, output includes numbered sub-answers internally and
  clarifications can be tagged by sub-question (`[Q1]`, `[Q2]`).

For long outputs, use a pager to keep the answer at the top and scroll as needed:

```bash
datachat ask --pager "Describe the public.events table"
```

Start an interactive session:

```bash
datachat chat --pager --max-clarifications 3
```

Use built-in query templates:

```bash
datachat ask --list-templates
datachat ask --template list-tables
datachat ask --template sample-rows --table public.orders
```

Run direct SQL mode (read-only):

```bash
datachat ask --execution-mode direct_sql "SELECT * FROM public.orders LIMIT 10"
```

In interactive chat, switch modes at runtime:

```text
/mode sql
SELECT * FROM public.orders LIMIT 10
/mode nl
```

Target a specific registry connection for one request/session:

```bash
datachat ask --target-database <connection_uuid> "Show total revenue this month"
datachat chat --target-database <connection_uuid>
```

Control table output pagination in terminal:

```bash
datachat ask --page 1 --page-size 10 "Show first 100 rows from public.orders"
```

Use schema explorer commands from CLI:

```bash
datachat schema tables --search orders
datachat schema columns public.orders
datachat schema sample public.orders --rows 25 --offset 0
```

Persist and resume CLI sessions:

```bash
datachat chat --session-id sales-debug
datachat session list
datachat session resume sales-debug
datachat session clear sales-debug
```

## 4) Troubleshooting

- **Auto-profiling unavailable**
  - Ensure `SYSTEM_DATABASE_URL` and `DATABASE_CREDENTIALS_KEY` are set.
- **No DataPoints loaded**
  - Run `datachat dp sync --datapoints-dir datapoints/managed` after adding files.

## 5) Manual Validation Checklist

Use this checklist after backend changes to intent routing or credentials-only mode.

1. Intent gate (exit + non-data intent):

```bash
datachat ask "let's talk later"
datachat ask "tell me a joke"
```

Expected:

- No SQL is generated.
- Response source is system/intent, not clarification/sql.

2. Clarification flow in `ask`:

```bash
datachat ask --max-clarifications 3 "Show me the first 5 rows"
```

When prompted:

- Reply with a table name (for example `public.orders`).
- Confirm the next answer returns SQL and rows.

3. Credentials-only deterministic fallbacks:

```bash
datachat ask "list tables"
datachat ask "How many rows are in information_schema.tables?"
datachat ask "Show me the first 2 rows from information_schema.tables"
```

Expected:

- Deterministic SQL should be produced without clarification loops.
- Results should execute successfully on a connected Postgres target.

4. Chat-mode guardrails:

```bash
datachat chat --max-clarifications 3
```

Inside chat:

- `ok` should ask for clarification (not run full SQL pipeline).
- `exit`, `no further questions`, or `talk later` should end the session cleanly.

# DataChat

DataChat solves the trust and speed gap between raw enterprise data and business decisions by turning fragmented data plus domain knowledge into governed, actionable, context-aware answers and workflows.

DataChat is a decision workflow system for finance teams today, and an AI platform for business decision makers over time.

DataChat lets you ask questions in plain English and get SQL, results, and clarifications. It supports a credentials-only path (no DataPoints required) and a richer path with DataPoints for business context.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Next.js](https://img.shields.io/badge/next.js-15-black.svg)](https://nextjs.org/)

---

## Product Direction (Finance-First)

- Current wedge: decision workflow system for finance teams.
- Build model: finance workflow needs pull platform work (metadata ops, governance, traceability, and policy-safe agent execution).
- Expansion rule: broaden beyond finance only after finance workflow KPIs in `docs/PRD.md` stay green.

---

## Current Status (Implemented)

- Natural-language query flow (`chat`, `ask`, `/api/v1/chat`) with SQL generation, validation, and execution.
- Credentials-only mode: works without DataPoints using live schema + deterministic catalog queries.
- Clarification flow with bounded retries (`--max-clarifications`, default `3` in CLI).
- Multi-database registry with per-request routing via `target_database`.
- Tools API (`/api/v1/tools`, `/api/v1/tools/execute`) with typed parameter schemas and policy checks.
- Auto-profiling pipeline that generates pending DataPoints for review/approval.
- Live schema mode notice when DataPoints are absent.

Runtime connector support today:
- PostgreSQL
- ClickHouse

Catalog/profiling SQL templates exist for additional engines (MySQL, BigQuery, Redshift), but live execution depends on connector implementation.

## Planned / Not Yet Implemented

- Workspace/folder ingestion and codebase indexing as a first-class product feature.
- Runtime connectors for MySQL, BigQuery, and Redshift.
- Levels 3-5 capabilities (executable metric templates, optimization automation, anomaly/root-cause automation).

---

## Quick Start

### Using Docker Compose

```bash
# 1. Clone
git clone https://github.com/onubrooks/datachat.git
cd datachat

# 2. Configure env
cp .env.example .env
# Add at least: LLM API key + DATABASE_URL

# 3. Start
docker-compose up
```

If `datachat --version` returns "command not found":

```bash
pip install -e .
```

Open:
- Frontend: <http://localhost:3000>
- Backend docs: <http://localhost:8000/docs>

### Manual Backend Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
cp .env.example .env
uvicorn backend.api.main:app --reload --port 8000
```

### Fast Validation

```bash
datachat status
datachat ask "list tables"
```

---

## Credentials-Only First

You can start with only:

```env
DATABASE_URL=postgresql://user:pass@host:5432/db
LLM_OPENAI_API_KEY=...
```

DataPoints are optional but recommended for business metric precision and richer evidence.

See:
- [`docs/CREDENTIALS_ONLY_MODE.md`](docs/CREDENTIALS_ONLY_MODE.md)
- [`GETTING_STARTED.md`](GETTING_STARTED.md)

---

## Multi-Database

For stored connections + `target_database` routing, set:

```env
SYSTEM_DATABASE_URL=postgresql://.../datachat
DATABASE_CREDENTIALS_KEY=... # Fernet key
```

Then use `/api/v1/databases` to manage connections and pass `target_database` in `/api/v1/chat` or `/api/v1/tools/execute`.

See [`docs/MULTI_DATABASE.md`](docs/MULTI_DATABASE.md).

---

## CLI

```bash
# Interactive mode
datachat chat

# Single question
datachat ask "show first 5 rows from public.users"

# Built-in templates
datachat ask --list-templates
datachat ask --template sample-rows --table public.users

# Direct SQL mode (read-only)
datachat ask --execution-mode direct_sql "SELECT * FROM public.users LIMIT 10"

# Schema explorer commands
datachat schema tables --search users
datachat schema columns public.users
datachat schema sample public.users --rows 10

# Session management
datachat chat --session-id onboarding
datachat session list
datachat session resume onboarding

# Setup helper
datachat setup

# System status
datachat status

# DataPoints lifecycle
datachat dp sync --datapoints-dir ./datapoints
datachat dp pending list
datachat dp pending approve <pending_id>
```

---

## API

Core endpoints:
- `POST /api/v1/chat`
- `GET /api/v1/system/status`
- `POST /api/v1/system/initialize`
- `GET/POST/PUT/DELETE /api/v1/databases*`
- `GET /api/v1/tools`
- `POST /api/v1/tools/execute`

See [`docs/API.md`](docs/API.md).

---

## Documentation

- [`GETTING_STARTED.md`](GETTING_STARTED.md)
- [`docs/API.md`](docs/API.md)
- [`docs/CREDENTIALS_ONLY_MODE.md`](docs/CREDENTIALS_ONLY_MODE.md)
- [`docs/MULTI_DATABASE.md`](docs/MULTI_DATABASE.md)
- [`docs/LEVELS.md`](docs/LEVELS.md)
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/PRD.md`](docs/PRD.md)

---

## Development

```bash
# Lint
ruff check .

# Tests
pytest -q
```

---

## License

Apache License 2.0. See [`LICENSE`](LICENSE).

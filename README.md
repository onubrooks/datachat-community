# DataChat

DataChat helps teams move from raw database access to decision-ready answers by combining natural-language querying, SQL visibility, and evidence-backed outputs.

`datachat-community` is the open-source community edition focused on fast onboarding, practical workflows, and public iteration.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Next.js](https://img.shields.io/badge/next.js-15-black.svg)](https://nextjs.org/)

---

## Community Direction

- Get new users to first useful answer in 5 minutes or less.
- Keep workflows practical across both UI and CLI.
- Improve answer trust with SQL transparency and evidence.
- Improve DataPoint and retrieval quality in public, iterative releases.

---

## Current Status (Implemented)

- Natural-language query flow (`chat`, `ask`, `/api/v1/chat`) with SQL generation, validation, and execution.
- Credentials-only mode (no DataPoints required).
- Multi-database registry with per-request routing via `target_database`.
- Tool endpoints (`/api/v1/tools`, `/api/v1/tools/execute`) with typed parameters and policy checks.
- Auto-profiling and managed pending DataPoint generation/review.
- Result surfaces across UI/CLI/API: answer, SQL, table, visualization, evidence, timing.

Current database connector support:

- PostgreSQL
- MySQL
- ClickHouse

## Near-Term Community Enhancements

- Retrieval explainability views (why a DataPoint was selected).
- Better onboarding metadata generation quality.
- Retrieval evaluation loop for context-only inspection and feedback.
- Stronger managed DataPoint editing and approval workflows.

---

## 5-Minute Quickstart

```bash
git clone https://github.com/onubrooks/datachat-community.git
cd datachat-community
cp .env.example .env
```

Then choose one database setup path:

- Path A (quickest): set `DATABASE_URL` directly in `.env`.
- Path B (wizard): keep registry settings in `.env` and add database via UI/CLI onboarding wizard.

Also set one LLM key in `.env` (OpenAI, Google, or Anthropic provider key).

Start:

```bash
docker-compose up
```

Validate:

```bash
datachat ask "list tables"
```

Or open the UI at <http://localhost:3000> and ask in natural language.

---

## Quick Start (Detailed)

### Using Docker Compose

```bash
# 1. Clone
git clone https://github.com/onubrooks/datachat-community.git
cd datachat-community

# 2. Configure env
cp .env.example .env
# Set one LLM key, then either:
# - set DATABASE_URL directly, or
# - use onboarding wizard to add a connection

# 3. Start
docker-compose up
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

## Database Setup Paths

### Path A: Credentials-only

Set in `.env`:

```env
DATABASE_URL=postgresql://user:pass@host:5432/db
LLM_OPENAI_API_KEY=...
```

Use this when you want immediate querying with minimal setup.

### Path B: Wizard-based connection setup

Set registry env in `.env`:

```env
SYSTEM_DATABASE_URL=postgresql://.../datachat
DATABASE_CREDENTIALS_KEY=... # Fernet key
```

Then add the target connection using either:

- UI onboarding wizard (Databases page), or
- CLI wizard:

```bash
datachat onboarding wizard
```

---

## Multi-Database

When registry mode is enabled, use `/api/v1/databases` to manage saved connections and pass `target_database` in `/api/v1/chat` or `/api/v1/tools/execute`.

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

# Schema explorer
datachat schema tables --search users
datachat schema columns public.users
datachat schema sample public.users --rows 10

# System setup + status
datachat setup
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
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/CAPABILITY_CONTRACT.md`](docs/CAPABILITY_CONTRACT.md)
- [`docs/COMMUNITY_V0_1_SCOPE_LOCK.md`](docs/COMMUNITY_V0_1_SCOPE_LOCK.md)
- [`docs/LAUNCH_CHECKLIST.md`](docs/LAUNCH_CHECKLIST.md)
- [`CONTRIBUTING.md`](CONTRIBUTING.md)

---

## Development

```bash
# Lint
ruff check .

# Tests
pytest -q
```

---

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

Use label `needs-private-cherry-pick` for PRs that should be imported into the private `datachat` repo.

---

## License

Apache License 2.0. See [`LICENSE`](LICENSE).

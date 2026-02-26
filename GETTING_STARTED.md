# Getting Started with DataChat

This guide reflects current, implemented behavior in `datachat-community`.

Supported database engines today:

- PostgreSQL
- MySQL
- ClickHouse

## 0. 5-Minute Path (Recommended)

```bash
git clone https://github.com/onubrooks/datachat-community.git
cd datachat-community
cp .env.example .env
```

Choose one database setup path:

- Path A: set `DATABASE_URL` directly in `.env`.
- Path B: use onboarding wizard (UI or CLI) to add a connection.

Set one LLM provider key in `.env`, or add it later in **Settings**.
Then run:

```bash
docker-compose up
```

In another terminal:

```bash
datachat ask "list tables"
```

Alternative setup path:
- keep provider keys in `.env`, then run `datachat onboarding wizard` to guide DB setup
- or use the UI onboarding flow in `/databases`

If this succeeds, continue with the full guide below.

---

## 1. Choose Your Mode

### Mode A: Credentials-only (fastest)

Use this when you want immediate querying with only DB credentials.

Required before asking questions:

- target database URL
- one LLM key (for example `LLM_OPENAI_API_KEY`)

You can provide both via `.env` or in the **Settings** page.

### Mode B: Registry + profiling (recommended for teams)

Use this when you need:

- multiple saved database connections
- `target_database` routing
- profiling + managed pending DataPoint review/approval

Additional required env:

- `SYSTEM_DATABASE_URL`
- `DATABASE_CREDENTIALS_KEY` (Fernet key)

---

## 2. Install

```bash
git clone https://github.com/onubrooks/datachat-community.git
cd datachat-community
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

Optional frontend:

```bash
cd frontend
npm install
cd ..
```

---

## 3. Configure Environment

```bash
cp .env.example .env
```

Set one LLM key (or add it in Settings after startup).

For Mode A, set `DATABASE_URL` directly:

```env
DATABASE_URL=postgresql://user:password@host:5432/your_database
LLM_OPENAI_API_KEY=sk-...
```

Settings-first alternative:
- start backend/frontend
- open `/settings`
- set provider/key and target database URL
- save runtime settings, then ask your first question

For Mode B, set registry env and add DB through wizard:

```env
SYSTEM_DATABASE_URL=postgresql://user:password@host:5432/datachat
DATABASE_CREDENTIALS_KEY=your_fernet_key
LLM_OPENAI_API_KEY=sk-...
```

Generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 4. Start Services

Backend:

```bash
uvicorn backend.api.main:app --reload --port 8000
```

Optional frontend:

```bash
cd frontend
npm run dev
```

Or run both:

```bash
datachat dev
```

---

## 5. Add Database (Wizard Path)

If you did not set `DATABASE_URL`, add a database via wizard:

- UI wizard: Databases page -> Quickstart/Onboarding
- CLI wizard:

```bash
datachat onboarding wizard
```

Then validate:

```bash
datachat status
datachat ask "list tables"
```

---

## 6. Optional Setup Helpers

```bash
datachat setup
```

Quickstart wrapper:

```bash
datachat quickstart --database-url postgresql://user:pass@host:5432/db
```

With demo load + first question:

```bash
datachat quickstart \
  --database-url postgresql://user:pass@host:5432/db \
  --dataset grocery \
  --question "list all grocery stores"
```

---

## 7. Optional DataPoints (Quality Boost)

Load existing DataPoints:

```bash
datachat dp sync --datapoints-dir ./datapoints
```

Review generated pending DataPoints:

```bash
datachat dp pending list
datachat dp pending approve <pending_id>
```

---

## 8. Multi-Database Routing (Optional)

When registry mode is enabled, add connections and route per request:

- API chat: `target_database` in `POST /api/v1/chat`
- tools: `target_database` in `POST /api/v1/tools/execute`

If `target_database` is sent while registry is unavailable, the request fails.

See [`docs/MULTI_DATABASE.md`](docs/MULTI_DATABASE.md).

---

## 9. What Works Without DataPoints

Supported now:

- table discovery
- column discovery
- row counts
- sample rows
- SQL generation/execution from live schema context

More limited without DataPoints:

- strict KPI semantics (for example company-specific "revenue")
- domain-specific business logic and definitions

See [`docs/CREDENTIALS_ONLY_MODE.md`](docs/CREDENTIALS_ONLY_MODE.md).

---

## 10. Known Product Boundaries

Not fully implemented yet:

- retrieval explainability UI/API (planned)
- richer onboarding metadata quality signals (planned)
- retrieval evaluation loop for context-only inspection (planned)

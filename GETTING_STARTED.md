# Getting Started with DataChat

This guide reflects current, implemented behavior.

## 1. Decide Your Mode

### Mode A: Credentials-only (fastest)

Use this when you want immediate querying with only DB credentials.

Required:
- `DATABASE_URL`
- one LLM key (for example `LLM_OPENAI_API_KEY`)

DataPoints are optional in this mode.

### Mode B: Registry + profiling (recommended for teams)

Use this when you need:
- multiple saved database connections
- `target_database` routing
- profiling + pending DataPoint review/approval

Additional required env:
- `SYSTEM_DATABASE_URL`
- `DATABASE_CREDENTIALS_KEY` (Fernet key)

---

## 2. Install

```bash
git clone https://github.com/onubrooks/datachat.git
cd datachat
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

Minimum credentials-only example:

```env
DATABASE_URL=postgresql://user:password@host:5432/your_database
LLM_OPENAI_API_KEY=sk-...
```

Optional registry/profiling add-ons:

```env
SYSTEM_DATABASE_URL=postgresql://user:password@host:5432/datachat
DATABASE_CREDENTIALS_KEY=your_fernet_key
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

Or run both (if frontend deps are installed):

```bash
datachat dev
```

---

## 5. Verify Setup

```bash
datachat status
datachat ask "list tables"
```

Expected:
- database connectivity passes
- query returns SQL/result or a targeted clarification

---

## 6. Optional Setup Wizard

```bash
datachat setup
```

What it does today:
- validates/saves database URLs
- initializes runtime components
- can trigger auto-profiling when registry prerequisites are present

## 6.1 Quickstart Wrapper (Phase 1.4)

Use this to run connect + setup in one command:

```bash
datachat quickstart --database-url postgresql://user:pass@host:5432/db
```

With optional demo load and first question:

```bash
datachat quickstart \
  --database-url postgresql://user:pass@host:5432/db \
  --dataset grocery \
  --question "list all grocery stores"
```

---

## 7. Optional DataPoints (quality boost)

DataPoints improve semantic accuracy for business questions.

Load existing DataPoints:

```bash
datachat dp sync --datapoints-dir ./datapoints
```

Review generated pending DataPoints (from profiling):

```bash
datachat dp pending list
datachat dp pending approve <pending_id>
```

Thin training helper over existing flows:

```bash
# Wrapper over dp sync
datachat train --mode sync --datapoints-dir ./datapoints

# Wrapper over profile start (+ optional generation)
datachat train --mode profile --profile-connection-id <connection_id> --generate-after-profile
```

---

## 8. Multi-Database Routing (Optional)

When registry is enabled, add connections and route per request:
- API chat: `target_database` in `POST /api/v1/chat`
- tools: `target_database` in `POST /api/v1/tools/execute`

If `target_database` is sent while registry is unavailable, the request fails (no silent fallback).

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
- workspace/folder indexing as a production feature
- runtime connectors for BigQuery/Redshift (templates exist, connectors pending)
- automated Level 3-5 intelligence features

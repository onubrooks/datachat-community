# DataChat Smoke Test (10-15 minutes)

Quick checklist for validating a working local setup before demos or deeper testing.

---

## Prerequisites

- `.env` configured with `OPENAI_API_KEY`, `DATABASE_URL`, and `SYSTEM_DATABASE_URL`.
- Generate a credentials key if needed:

  ```bash
  python -c "import secrets; print(secrets.token_hex(32))"
  ```

  Set `DATABASE_CREDENTIALS_KEY` in `.env`.
- CLI installed:

  ```bash
  pip install -e .
  ```

- Backend running on `http://localhost:8000`:

  ```bash
  uvicorn backend.api.main:app --reload --port 8000
  ```

- Optional frontend running on `http://localhost:3000`:

  ```bash
  cd frontend
  npm install
  npm run dev
  ```

- Or run both servers together (requires frontend deps installed):

  ```bash
  datachat dev
  ```

- Setup saves database URLs to `~/.datachat/config.json`.
- DataPoint generation is async with UI progress updates (WebSocket).
- Database Manager includes a Reset System button for clearing local state.
- Reset everything (optional):

  ```bash
  datachat reset
  ```

---

## 1) Backend Health

```bash
curl http://localhost:8000/api/v1/health
```

Expected: `status: healthy`.

---

## 2) System Status

```bash
curl http://localhost:8000/api/v1/system/status
```

Expected: setup steps show what is missing (if any).

---

## 3) Load Demo Data (Fast Path)

```bash
datachat demo --persona base --reset
```

Expected: demo tables + demo DataPoints loaded.

---

## 4) Simple Chat Query

```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What tables are available in the database?",
    "conversation_id": "smoke_conv_1"
  }'
```

Expected: response includes `answer`, `sql`, `metrics`, and empty `validation_errors`.

---

## 4b) Analyst Demo (Current Capability)

```bash
datachat demo --persona analyst --reset
```

Ask:

- "What was total revenue last 30 days?"
- "Top 5 users by orders"
- "How many active users are there?"

Expected: answers return with SQL + results; validation errors are empty or minimal.

---

## 5) CLI Sanity

```bash
datachat --version
datachat status
datachat setup --target-db postgresql://postgres:@localhost:5432/postgres \
  --system-db postgresql://datachat:datachat_password@localhost:5432/datachat \
  --auto-profile --max-tables 10 --non-interactive
datachat ask "How many users are in the database?"
datachat dp pending list
```

Expected: status shows healthy components; `ask` returns SQL + results.

---

## 6) Frontend (Optional)

- Open `http://localhost:3000`
- Ask: "Show me top 5 users by orders"
- Confirm: answer, SQL, and table render; agent status updates in real-time.

---

## 7) Multi-DB Registry (Optional)

Requires `DATABASE_CREDENTIALS_KEY` in `.env`.

```bash
curl -X POST http://localhost:8000/api/v1/databases \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Local Postgres",
    "database_url": "postgresql://datachat:datachat_password@localhost:5432/datachat",
    "database_type": "postgresql",
    "tags": ["local"],
    "is_default": true
  }'
```

Expected: connection created and listed via `GET /api/v1/databases`.

---

## Pass Criteria

- Health + system status work.
- Demo data loads without errors.
- Chat returns valid JSON with SQL and metrics.
- CLI `ask` works.
- Frontend chat works (if running).

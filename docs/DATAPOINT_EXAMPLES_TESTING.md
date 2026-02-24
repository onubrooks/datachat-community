# DataPoint-Driven Testing: Example Datasets

This guide validates DataPoint-driven quality (not credentials-only fallback only)
using the packaged example datasets.

## 1) Prepare a test database

Create a fresh PostgreSQL database and seed one of the sample domains.

Grocery:

```bash
createdb datachat_grocery
psql "postgresql://postgres:@localhost:5432/datachat_grocery" -f scripts/grocery_seed.sql
```

Fintech:

```bash
createdb datachat_fintech
psql "postgresql://postgres:@localhost:5432/datachat_fintech" -f scripts/fintech_seed.sql
```

Shortcut (uses `DATABASE_URL` target + loads matching example DataPoints):

```bash
datachat demo --dataset grocery --reset
datachat demo --dataset fintech --reset
```

Set target DB in `.env` (or via CLI):

```env
DATABASE_URL=postgresql://postgres:@localhost:5432/datachat_grocery
# or
DATABASE_URL=postgresql://postgres:@localhost:5432/datachat_fintech
```

Important:

- `DATABASE_URL` is the query target used by `datachat ask/chat`.
- `SYSTEM_DATABASE_URL` is only for registry/profiling metadata and does not change query execution target.

Quick verification:

```bash
datachat status
```

Expected:

- `Connection` shows the intended target DB.

## 2) Start backend (and optional frontend)

```bash
uvicorn backend.api.main:app --reload --port 8000
```

Optional UI:

```bash
cd frontend
npm run dev
```

## 3) Exercise DataPoint add flow (single file)

Grocery:

```bash
datachat dp add schema datapoints/examples/grocery_store/table_grocery_stores_001.json
```

Fintech:

```bash
datachat dp add schema datapoints/examples/fintech_bank/table_bank_accounts_001.json
```

Then verify:

```bash
datachat dp list
```

Expected:

- one new Schema DataPoint appears
- no validation errors

## 4) Exercise DataPoint sync flow (bulk)

Grocery:

```bash
datachat dp sync --datapoints-dir datapoints/examples/grocery_store
```

Fintech:

```bash
datachat dp sync --datapoints-dir datapoints/examples/fintech_bank
```

Then verify:

```bash
datachat dp list
```

Expected:

- all domain DataPoints load (schema + business + process)
- vector store count increases
- no failed files

## 5) CLI quality smoke checks

Grocery:

```bash
datachat ask "List all grocery stores"
datachat ask "What is total grocery revenue?"
datachat ask "Show gross margin by category"
datachat ask "Daily waste cost trend"
```

Fintech:

```bash
datachat ask "List active bank accounts"
datachat ask "What is total deposits?"
datachat ask "Show failed transaction rate by day"
datachat ask "What is loan default rate?"
```

Expected:

- SQL references the selected domain tables
- metric prompts use metric-related tables/columns
- answer source is mostly `sql` or grounded `context` with evidence

## 6) Run retrieval eval (grocery baseline)

```bash
python scripts/eval_runner.py \
  --mode retrieval \
  --dataset eval/grocery/retrieval.json \
  --min-hit-rate 0.60 \
  --min-recall 0.50 \
  --min-mrr 0.40
```

Expected:

- exit code `0`
- summary prints Hit rate, Recall@K, MRR, Coverage

## 7) Run end-to-end QA eval (grocery baseline)

```bash
python scripts/eval_runner.py \
  --mode qa \
  --dataset eval/grocery/qa.json \
  --min-sql-match-rate 0.60 \
  --min-answer-type-rate 0.60
```

Expected:

- exit code `0`
- summary prints SQL and answer-type match rates

## 8) UI manual checks

In chat UI, run domain-specific checks.

Grocery sample:

1. `What is total grocery revenue this week?`
2. `How do we compute gross margin?`
3. `Show stockout rate by store`
4. `What waste reasons are most common?`

Fintech sample:

1. `What is total deposits across active accounts?`
2. `How is net interest income calculated?`
3. `Show failed transaction rate trend`
4. `Which loans are 90+ days past due?`

Validate:

- Responses use domain-specific vocabulary
- SQL references domain tables
- Follow-up clarifications preserve intent
- No hallucinated table names
- If both example and auto-profiled DataPoints exist for the same table, managed/user DataPoints should win over examples.

Also validate DataPoint visibility in `Manage DataPoints`:

1. Open `Manage DataPoints`.
2. Confirm approved DataPoints include entries loaded from `datapoints/examples/...`.
3. Confirm the list is populated even if you have not generated pending DataPoints from profiling yet.

Verify vector-store resilience (UI path):

1. Keep backend running.
2. In another terminal, run:

  ```bash
  datachat reset --yes
  datachat dp sync --datapoints-dir datapoints/examples/grocery_store
  # or
  datachat dp sync --datapoints-dir datapoints/examples/fintech_bank
  ```

3. Return to UI and ask a domain question.

Expected:

- No `hnsw`/`Nothing found on disk` retrieval error appears in the answer.
- Request still resolves (SQL answer or safe clarification).

## 9) Reset between runs (optional)

```bash
datachat reset --yes --keep-config --keep-vectors
```

Then rerun `dp sync` if needed.

Useful reset options:

- Keep user datapoints: `--keep-user-datapoints`
- Keep example/demo datapoints: `--keep-example-datapoints`
- Fully clear example/demo datapoints too: `--clear-example-datapoints`

## 10) MySQL credentials-only manual checks

Set target DB:

```env
DATABASE_URL=mysql://root:password@localhost:3306/datachat_demo
```

Then run:

```bash
datachat ask "list all available tables"
datachat ask "show columns in customers"
datachat ask "show me 3 rows from customers"
datachat ask "how many rows are in customers"
```

Expected:

- queries execute without DataPoints
- `answer_source` is `sql` for deterministic metadata intents

## 11) Global/shared DataPoints manual checks

Global reference DataPoints are stored here:

- `datapoints/examples/global_reference`

Load them as shared/global:

```bash
datachat dp sync --datapoints-dir datapoints/examples/global_reference --global-scope
```

Or in UI `Database Management`:

1. In `Sync Status`, set `Scope: global/shared`.
2. Click `Sync Now`.

Then test from **both** grocery and fintech selected databases:

```bash
datachat ask "How is gross margin calculated?"
datachat ask "What is loan default rate?"
datachat ask "Define failed transaction rate."
```

Expected:

- `answer_source` should be `context` (definition-driven) unless SQL is explicitly required.
- The same definition intent should work across different selected databases.
- Global definitions should not force table-specific SQL when no tables are requested.
- generated SQL uses MySQL-compatible catalog queries (`information_schema.*`)

Optional automated check:

```bash
python scripts/eval_runner.py \
  --mode catalog \
  --dataset eval/catalog/mysql_credentials.json \
  --min-sql-match-rate 0.70 \
  --min-source-match-rate 0.80 \
  --min-clarification-match-rate 0.80
```

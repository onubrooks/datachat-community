# DataChat RAG Evaluation Plan

Lightweight evaluation plan for retrieval quality and end-to-end answer fidelity.

---

## Goals

- Validate that the right DataPoints are retrieved for typical queries.
- Track regressions when prompts, embeddings, or retrieval logic change.
- Provide a small, repeatable harness for demos and CI gates.

---

## Datasets

### 1) Retrieval Evaluation Set

Small, human-labeled dataset of (query → expected DataPoint IDs).

Example schema:

```json
{
  "query": "What was total revenue last month?",
  "expected_datapoint_ids": ["table_orders_001", "metric_revenue_001"],
  "notes": "Revenue derived from completed orders"
}
```

### 2) End-to-End QA Set

Queries with expected SQL patterns + answer types (table, single value, time series).

Example schema:

```json
{
  "query": "Top 5 users by orders",
  "expected_sql_contains": ["COUNT", "orders", "GROUP BY", "LIMIT 5"],
  "expected_answer_type": "table"
}
```

---

## Metrics

### Retrieval

- **Recall@K**: fraction of expected DataPoints retrieved in top K.
- **MRR**: position of first relevant DataPoint.
- **Coverage**: % of queries with at least 1 expected DataPoint retrieved.

### End-to-End

- **SQL match rate**: heuristic match against expected SQL patterns.
- **Answer type accuracy**: table vs single value vs time series.
- **Validation error rate**: % of runs with critical validation errors.

---

## Evaluation Workflow

1. Load demo data + DataPoints.
2. Run retrieval-only evaluation (ContextAgent).
3. Run full pipeline evaluation (Classifier → Context → SQL → Validator → Executor).
4. Record metrics and compare against baseline.

## Minimal Runner

Run the starter eval runner against the local API:

```bash
# Retrieval checks (uses sources from /chat)
python scripts/eval_runner.py --mode retrieval --dataset eval/retrieval.json

# End-to-end checks
python scripts/eval_runner.py --mode qa --dataset eval/qa.json

# Intent + credentials-only checks
python scripts/eval_runner.py --mode intent --dataset eval/intent_credentials.json

# Deterministic catalog checks (credentials-only metadata intents)
python scripts/eval_runner.py --mode catalog --dataset eval/catalog/mysql_credentials.json

# Deterministic routing checks (intent gate + path selection)
python scripts/eval_runner.py --mode route --dataset eval/routes_credentials.json

# Query-compiler quality checks (table/path decisions via decision_trace)
python scripts/eval_runner.py --mode compiler --dataset eval/compiler/grocery_query_compiler.json
```

Notes:

- Retrieval mode infers hits from `sources` returned by `/api/v1/chat`.
- Answer type checks support both columnar API payloads and row-list payloads.
- Intent mode tracks source accuracy, clarification behavior, SQL pattern checks,
  and latency/LLM-call averages for credentials-only flows.
- Catalog mode focuses on deterministic metadata intents
  (`list tables`, `show columns`, `sample rows`, `row count`) and validates SQL/source/clarification behavior.
- Route mode validates orchestration path choices via response `decision_trace`.
- Compiler mode validates query-compiler selections (`selected_tables`) and plan path
  (`deterministic` or `llm_refined`) via response `decision_trace`.

Optional thresholds (non-zero exit on failure):

```bash
python scripts/eval_runner.py --mode retrieval --dataset eval/retrieval.json \
  --min-hit-rate 0.6 --min-recall 0.5 --min-mrr 0.4

python scripts/eval_runner.py --mode qa --dataset eval/qa.json \
  --min-sql-match-rate 0.6 --min-answer-type-rate 0.6

python scripts/eval_runner.py --mode catalog --dataset eval/catalog/mysql_credentials.json \
  --min-sql-match-rate 0.7 --min-source-match-rate 0.8 --min-clarification-match-rate 0.8

python scripts/eval_runner.py --mode route --dataset eval/routes_credentials.json \
  --min-route-match-rate 0.8 --min-source-match-rate 0.8

python scripts/eval_runner.py --mode compiler --dataset eval/compiler/grocery_query_compiler.json \
  --min-compiler-table-match-rate 0.8 --min-compiler-path-match-rate 0.8 --min-source-match-rate 0.5
```

---

## Suggested CLI Hooks (Future)

```text
datachat eval retrieval --dataset ./eval/retrieval.json
datachat eval endtoend --dataset ./eval/qa.json
datachat eval compare --baseline ./eval/v1.json --candidate ./eval/v2.json
```

---

## Demo-Ready Subset

Keep a tiny set (5-10 queries) for quick regression checks before demos.

---

## Next Steps

- Create `eval/` datasets and add a minimal runner.
- Wire into CI (fail on regression beyond threshold).

## Starter Datasets

- `eval/retrieval.json` - expected DataPoint IDs per query.
- `eval/qa.json` - SQL pattern + answer type checks.
- `eval/grocery/retrieval.json` - grocery DataPoint retrieval checks.
- `eval/grocery/qa.json` - grocery end-to-end SQL/answer checks.
- `eval/catalog/mysql_credentials.json` - deterministic catalog intent checks for MySQL credentials-only mode.
- `eval/routes_credentials.json` - deterministic routing-path checks using `decision_trace`.
- `eval/compiler/grocery_query_compiler.json` - query-compiler table/path decision checks.

# Routing Policy and Decision Trace

DataChat orchestration uses unified routing via QueryAnalyzerAgent with deterministic pattern matching plus LLM decisions.

## Architecture

The pipeline uses a unified routing system with `QueryAnalyzerAgent` as the single entry point:

```txt
Query → QueryAnalyzerAgent → Route Dispatcher
                                    ↓
              ┌─────────────────────┼─────────────────────┐
              ↓                     ↓                     ↓
           SQL Route          Context Route           End Route
              ↓                     ↓                     ↓
        Context → SQL        ContextAnswer          System Response
              ↓                     ↓
         Execute SQL          Answer from DataPoints
```

## Policy Knobs

All knobs are configured with `PIPELINE_` env vars:

- `PIPELINE_INTENT_LLM_CONFIDENCE_THRESHOLD` (default: `0.45`)
  - If the query analyzer returns `data_query` below this confidence on ambiguous input, route to clarification.
- `PIPELINE_CONTEXT_ANSWER_CONFIDENCE_THRESHOLD` (default: `0.7`)
  - Minimum confidence to answer from DataPoints directly; otherwise route to SQL.
- `PIPELINE_SEMANTIC_SQL_CLARIFICATION_CONFIDENCE_THRESHOLD` (default: `0.55`)
  - Minimum SQL generation confidence for semantic queries before forcing clarification.
- `PIPELINE_AMBIGUOUS_QUERY_MAX_TOKENS` (default: `3`)
  - Max token length treated as ambiguous if no data keywords are detected.

## Deterministic Patterns

`QueryPatternMatcher` handles these without LLM calls:

- **Table listing**: `list tables`, `show tables`, `what tables exist`
- **Column listing**: `show columns in X`, `describe table X`
- **Row count**: `how many rows in X`, `row count of X`
- **Sample rows**: `show first N rows from X`, `preview X`
- **Exit**: `I'm done`, `talk later`, `never mind`
- **Out of scope**: `tell me a joke`, non-data queries

## Decision Trace

`/api/v1/chat` and `WS /ws/chat` responses include `decision_trace`:

- `stage` (for example `query_analyzer`, `context_vs_sql`)
- `decision` (for example `sql`, `context`, `clarification`, `end`)
- `reason` (deterministic reason label)
- `details` (optional structured values)

When SQL generation runs, an additional `query_compiler` stage is emitted with:

- compiler path (`deterministic` or `llm_refined`)
- selected candidate tables
- operator hints and confidence

This enables:

- route-level regression checks in eval runner
- reproducible debugging when routing choices change

## Route Eval

Run deterministic route checks:

```bash
python scripts/eval_runner.py \
  --mode route \
  --dataset eval/routes_credentials.json \
  --min-route-match-rate 0.8 \
  --min-source-match-rate 0.8

python scripts/eval_runner.py \
  --mode compiler \
  --dataset eval/compiler/grocery_query_compiler.json \
  --min-compiler-table-match-rate 0.8 \
  --min-compiler-path-match-rate 0.8 \
  --min-source-match-rate 0.5
```

## Manual Testing

1. Start backend and open UI chat.
2. Ask `list tables`.
3. Confirm response is `answer_source=sql`.
4. Ask `let's talk later`.
5. Confirm response is `answer_source=system`.
6. Ask `ok`.
7. Confirm response is clarification-oriented (`answer_source=clarification`).
8. Use API dev tools or network tab to inspect `decision_trace` in responses.

## LLM Call Budget

| Query Type | LLM Calls |
|------------|-----------|
| Deterministic (list tables, row count) | 0 |
| Context (definition, exploration) | 1 |
| Simple SQL | 1-2 |
| Complex SQL | 2-3 |

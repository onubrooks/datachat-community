# Latency Tuning Guide

This page explains the pipeline latency optimizations, how to run benchmarks, and how to roll changes out safely.

## Quick Summary

- `schema_snapshot_cache` showed the clearest isolated latency win in the latest benchmark.
- `sql_two_stage` did not improve isolated latency in the latest benchmark and is disabled by default.
- Other optimizations remain available behind `PIPELINE_*` flags and should be enabled only after benchmarking on your own workload.

## Optimizations (1 to 6) in Simple Terms

1. SQL two-stage generation (`PIPELINE_SQL_TWO_STAGE_ENABLED`)

- First try SQL generation with a smaller/faster model.
- If confidence is low, automatically retry with the main model.
- Goal: reduce average SQL generation latency and cost.

2. Prompt budget (`PIPELINE_SQL_PROMPT_BUDGET_ENABLED`)

- Shrink schema/context text sent to the SQL model.
- Goal: faster prompt processing.
- Tradeoff: model may miss details if context is too aggressively trimmed.

3. Simple SQL synthesis toggle (`PIPELINE_SYNTHESIZE_SIMPLE_SQL_ANSWERS`)

- Skip extra response-polish LLM step for straightforward SQL answers.
- Goal: faster final response for simple queries.
- Tradeoff: wording may be less polished.

4. Classifier deep gate (`PIPELINE_CLASSIFIER_DEEP_LOW_CONFIDENCE_THRESHOLD`, `PIPELINE_CLASSIFIER_DEEP_MIN_QUERY_LENGTH`)

- Run expensive deep classification only for low-confidence or longer/ambiguous inputs.
- Goal: reduce unnecessary classifier latency.

5. Selective tool planner (`PIPELINE_SELECTIVE_TOOL_PLANNER_ENABLED`)

- Skip tool planning for standard data questions; run it for likely tool/action intents.
- Goal: avoid extra planner overhead on normal SQL questions.

6. Schema snapshot cache (`PIPELINE_SCHEMA_SNAPSHOT_CACHE_ENABLED`)

- Reuse recent schema snapshots instead of rebuilding on every query.
- Goal: reduce context-building latency on repeated queries against same DB.

## Environment Variables

All vars use the `PIPELINE_` prefix and can be added to `.env`.

```env
PIPELINE_SQL_TWO_STAGE_ENABLED=false
PIPELINE_SQL_TWO_STAGE_CONFIDENCE_THRESHOLD=0.78
PIPELINE_SQL_FORMATTER_FALLBACK_ENABLED=true
PIPELINE_SQL_PROMPT_BUDGET_ENABLED=false
PIPELINE_SQL_PROMPT_MAX_TABLES=80
PIPELINE_SQL_PROMPT_FOCUS_TABLES=8
PIPELINE_SQL_PROMPT_MAX_COLUMNS_PER_TABLE=18
PIPELINE_SQL_PROMPT_MAX_CONTEXT_CHARS=12000
PIPELINE_SYNTHESIZE_SIMPLE_SQL_ANSWERS=true
PIPELINE_CLASSIFIER_DEEP_LOW_CONFIDENCE_THRESHOLD=0.6
PIPELINE_CLASSIFIER_DEEP_MIN_QUERY_LENGTH=1
PIPELINE_SELECTIVE_TOOL_PLANNER_ENABLED=false
PIPELINE_SCHEMA_SNAPSHOT_CACHE_ENABLED=true
PIPELINE_SCHEMA_SNAPSHOT_CACHE_TTL_SECONDS=21600
# Optional model override for formatter fallback (uses mini model when unset)
LLM_SQL_FORMATTER_MODEL=
```

After changing any value, restart backend/CLI process.

`PIPELINE_SCHEMA_SNAPSHOT_CACHE_TTL_SECONDS` defaults to `21600` (6 hours).

## Run the Progressive Benchmark

Default query set:

```bash
python scripts/benchmark_latency_progressive.py --iterations 2
```

Custom query file (one query per line):

```bash
python scripts/benchmark_latency_progressive.py \
  --iterations 2 \
  --mode isolated \
  --queries-file eval/latency_queries.txt
```

Modes:

- `--mode cumulative`: stage2 includes stage1, stage3 includes stage1+2, etc.
- `--mode isolated`: each stage is run independently on top of baseline only.

Output files are written to:

- `reports/latency_progressive_<timestamp>.json`
- `reports/latency_progressive_<timestamp>.md`

## Latest Measured Results (Business Query Suite, Isolated Mode)

Source: `reports/latency_progressive_20260212T130443Z.md`

- Baseline mean latency: `9533ms`
- Stage 1 (SQL two-stage): `25391ms` (slower)
- Stage 2 (prompt budget): `20952ms` (slower)
- Stage 3 (simple synthesis off): `10101ms` (slightly slower mean, better p95; but lower success in this run)
- Stage 4 (classifier deep gate): `18333ms` (slower)
- Stage 5 (selective tool planner): `20157ms` (slower)
- Stage 6 (schema snapshot cache): `6329ms` (faster mean and p95)

Important:

- These isolated results were run with the active LLM/provider stack and query set at benchmark time.
- Intermittent LLM parse/correction failures can affect both latency and success metrics.
- Run with your real queries and multiple iterations before enabling additional flags by default.

## Recommended Rollout

1. Keep defaults (`sql_two_stage=false`, `schema_snapshot_cache=true`).
2. Benchmark on your own query mix.
3. Enable one extra optimization at a time.
4. Re-run benchmark and compare:

- mean and p95 latency
- success rate
- clarification rate
- SQL answer rate

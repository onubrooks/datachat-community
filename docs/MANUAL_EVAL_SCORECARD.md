# Manual Eval Scorecard (UI + CLI)

This scorecard lets you compare DataChat behavior in two modes:

- `without_datapoints`
- `with_datapoints`

Use it with:

- `/Users/onuh/Documents/Work/Open Source/datachat/docs/DOMAIN_QUESTION_BANK.md`
- `/Users/onuh/Documents/Work/Open Source/datachat/scripts/manual_eval_runner.py`

## Rubric (per question)

Each field is scored `0`, `1`, or `2`.

| Metric | 0 | 1 | 2 |
|---|---|---|---|
| SQL correctness | Wrong SQL / no SQL when expected | Partially correct | Correct |
| Answer usefulness | Not actionable | Partially useful | Decision-ready |
| Clarification overhead | Too many unnecessary clarifications | One needed clarification | No clarification needed |
| Source quality | Missing or weak sources | Partial grounding | Clear relevant grounding |
| Latency | Noticeably slow/problematic | Acceptable | Fast |

Total per question: `/10`

## Suggested execution flow

1. Seed test data:
   - `psql \"postgresql://postgres:@localhost:5432/datachat_grocery\" -f scripts/grocery_seed.sql`
   - `psql \"postgresql://postgres:@localhost:5432/datachat_fintech\" -f scripts/fintech_seed.sql`
2. Run score pass without DataPoints.
3. Run score pass with DataPoints.
4. Compare summary metrics:
   - average score
   - average latency
   - clarification count
   - answer-source distribution

## Automated prompt runner (interactive scoring)

### Grocery without DataPoints

```bash
python scripts/manual_eval_runner.py \
  --domain grocery \
  --mode-label without_dp_grocery \
  --target-database <grocery_connection_id>
```

### Grocery with DataPoints

```bash
python scripts/manual_eval_runner.py \
  --domain grocery \
  --mode-label with_dp_grocery \
  --target-database <grocery_connection_id>
```

### Fintech without DataPoints

```bash
python scripts/manual_eval_runner.py \
  --domain fintech \
  --mode-label without_dp_fintech \
  --target-database <fintech_connection_id>
```

### Fintech with DataPoints

```bash
python scripts/manual_eval_runner.py \
  --domain fintech \
  --mode-label with_dp_fintech \
  --target-database <fintech_connection_id>
```

### Collect responses only (no scoring prompts)

```bash
python scripts/manual_eval_runner.py --domain all --no-score-prompt
```

## Output artifacts

The script writes both:

- `reports/manual_eval/manual_eval_<run_id>.json`
- `reports/manual_eval/manual_eval_<run_id>.csv`

Use these to compare `without_dp` vs `with_dp` runs side-by-side in spreadsheets or notebooks.

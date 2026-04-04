# Finance Workflow v1 Manual Test

Use this runbook to validate the finance wedge workflow with existing demo data.

If you want a faster end-user flow, start with `docs/finance/FINANCE_END_USER_QUICKSTART.md` first.

## Goal

Confirm DataChat can produce decision-grade finance answers (summary + drivers + caveats + sources) with reproducible prompts.

## Prerequisites

- backend API running
- frontend running (optional for UI pass)
- target database reachable
- optional system DB configured for registry/profiling features

Seeded fintech coverage reference:

- `bank_transactions`: `2025-09-01` through `2026-04-30`
- `bank_fx_rates`: `2025-09-01` through `2026-04-30`
- `bank_loan_payments`: `2025-01-15` through `2026-04-15`

Use this when validating weekly and monthly prompts in February, March, and April 2026.

## Track A: Fast Local Validation (global scope)

1. Seed fintech demo data:

```bash
datachat quickstart \
  --database-url <TARGET_DATABASE_URL> \
  --dataset fintech \
  --demo-reset \
  --non-interactive
```

2. Sync fintech DataPoints globally:

```bash
datachat dp sync \
  --datapoints-dir datapoints/examples/fintech_bank \
  --global-scope
```

3. Run prompts in CLI:

```bash
datachat ask "Show total deposits and net flow trend by segment for the last 8 weeks."
datachat ask "Which segments contributed most to week-over-week decline in net flow?"
datachat ask "Show failed transaction rate by day for the last 30 days and top driving transaction types."
datachat ask "Which loans are at highest risk based on days past due and recent payment status?"
```

## Track B: Scoped Validation (recommended)

Use when validating multi-database routing behavior.

1. Seed fintech and grocery datasets:

```bash
datachat demo --dataset fintech --reset --no-workspace
datachat demo --dataset grocery --reset --no-workspace
```

2. Sync DataPoints scoped to each connection:

```bash
datachat dp sync --datapoints-dir datapoints/examples/fintech_bank --connection-id <FINTECH_CONNECTION_ID>
datachat dp sync --datapoints-dir datapoints/examples/grocery_store --connection-id <GROCERY_CONNECTION_ID>
```

3. UI test:

- select fintech connection and run finance prompts from Track A
- verify no grocery entities appear in sources
- switch to grocery connection and ask grocery-only prompt:
  - `Which 5 SKUs have the highest stockout risk this week based on on-hand, reserved, and reorder level?`

## Track C: Definition and Authority Validation (FND-007, planned)

Use this track when canonical finance-global metric datapoints are enabled.

1. Run definition prompts from `docs/finance/FINANCE_PROMPT_PACK_V1.md` (`D01`-`D05`).
2. For each prompt, verify:
   - explicit formula is shown;
   - interpretation is business-usable (not only textbook text);
   - caveats/assumptions are explicit;
   - authority citation is visible.
3. Record results in `reports/finance_workflow_scorecard.csv` using existing columns.

## Pass/Fail Checklist

Mark pass only if all are true:

- answer includes direct summary (not only SQL)
- key driver breakdown is present
- caveats/assumptions are explicit and method-specific (not only generic warnings)
- source evidence is visible (tables/datapoints)
- follow-up question can continue the same investigation context
- multi-database run respects selected connection scope
- (Track C) definition answers include formula + authority citation + caveats

## KPI Capture Sheet (manual)

For each prompt, capture:

- time-to-answer (start to final response)
- clarification count
- has_source_attribution (`yes/no`)
- reviewer confidence (`high/medium/low`)
- rework needed (`yes/no`)

Use template:

```bash
cp docs/templates/finance_workflow_scorecard.csv reports/finance_workflow_scorecard.csv
```

Required scorecard columns for gate script:

- `prompt_id`
- `has_source_attribution` (`yes/no`)
- `source_count` (integer)
- `clarification_count` (number)
- `driver_quality_pass` (`yes/no`)
- `consistency_applicable` (`yes/no`)
- `consistency_pass` (`yes/no`)
- `reproducibility_pass` (`yes/no`)

## Quality Bar (release gate for workflow-mode contract)

Pass this gate before enabling workflow-mode request contract broadly:

- source coverage >= 95% (`has_source_attribution=yes` and >= 2 sources for each passed prompt)
- average clarifications <= 0.5 per prompt
- driver quality pass rate >= 80% (top drivers are directional and materially explain variance)
- consistency pass rate >= 95% for prompts that include deposits/withdrawals/net flow arithmetic
- reproducibility pass rate >= 90% across two reruns on unchanged data

Suggested minimum sample:

- 10 fintech prompts
- 5 cross-check prompts with scoped routing

Run gate:

```bash
python scripts/finance_workflow_gate.py \
  --scorecard reports/finance_workflow_scorecard.csv \
  --report-json reports/finance_workflow_gate.json
```

Exit code `0` means gate pass; non-zero means at least one threshold failed.

## Suggested Prompt Set (Finance Wedge)

Use `docs/finance/FINANCE_PROMPT_PACK_V1.md` for the full scripted 20-prompt pack with:

- prompt IDs (`P01`-`P20`)
- mapped primary datapoints
- expected signal checks
- clarification recovery replies

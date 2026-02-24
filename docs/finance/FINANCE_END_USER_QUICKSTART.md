# Finance End-User Quickstart (Fintech Demo)

Use this guide to get value quickly as an end user: load realistic fintech data, import finance datapoints, run high-value prompts, and verify output quality.

## 1) Load the Fintech Demo Data

### Option A: if your fintech DB already exists

```bash
psql "postgresql://postgres:@localhost:5432/datachat_fintech" -f scripts/fintech_seed.sql
```

### Option B: create DB first

```bash
createdb datachat_fintech
psql "postgresql://postgres:@localhost:5432/datachat_fintech" -f scripts/fintech_seed.sql
```

## 2) Verify Data Volume (Meaningful Demo Scale)

Run:

```bash
psql "postgresql://postgres:@localhost:5432/datachat_fintech" -f scripts/fintech_demo_queries.sql
```

Look at the first query block (`row_count` by table). Key tables should now have large enough volume for realistic demos, especially:

- `bank_transactions`
- `bank_accounts`
- `bank_customers`
- `bank_loan_payments`

## 3) Add the Finance Datapoints (User Action)

This imports schema/business/process plus the new finance query datapoints.

```bash
datachat dp sync --datapoints-dir datapoints/examples/fintech_bank --global-scope
```

If you use connection-scoped sync:

```bash
datachat dp sync --datapoints-dir datapoints/examples/fintech_bank --connection-id <FINTECH_CONNECTION_ID>
```

## 4) Start UI and Choose Finance Workflow Mode

1. Open the chat UI.
2. Select your fintech connection in **Target database**.
3. Set **Workflow mode** to **Finance Brief v1**.
4. Ask prompts from the list below.

## 5) Prompt Pack to Run

Use these prompts directly:

1. `Show total deposits, withdrawals, and net flow by segment for the last 8 weeks.`
2. `Identify the top 2 segments driving week-over-week net flow decline.`
3. `Show failed transaction rate by day for the last 30 days.`
4. `Which transaction types are driving failed transactions in the last 30 days?`
5. `Which loans are highest risk based on days past due and payment behavior?`
6. `Show loan default rate by segment.`
7. `Show fee income mix by transaction type for the last 30 days.`
8. `Show top customers by deposit concentration risk.`

For the full scripted 20-prompt demo sequence, use:

- `docs/finance/FINANCE_PROMPT_PACK_V1.md`

## 6) What Good Output Looks Like

For each prompt, verify:

- direct answer (not only SQL)
- clear drivers (top contributors)
- caveats / assumptions included
- sources shown
- consistent numbers (especially deposits/withdrawals/net flow)

## 7) How to Add or Edit a Query Datapoint Yourself

1. Copy one of the query datapoint JSON files:
   - `datapoints/examples/fintech_bank/query_bank_weekly_net_flow_by_segment_001.json`
2. Rename `datapoint_id` and `name`.
3. Edit `sql_template`, `description`, and `parameters`.
4. Keep metadata fields (`grain`, `exclusions`, `confidence_notes`) populated.
5. Re-sync:

```bash
datachat dp sync --datapoints-dir datapoints/examples/fintech_bank --global-scope
```

## 8) Run Quality Gate (Optional but Recommended)

1. Copy scorecard template:

```bash
cp docs/templates/finance_workflow_scorecard.csv reports/finance_workflow_scorecard.csv
```

2. Fill one row per tested prompt.
3. Run gate:

```bash
uv run python scripts/finance_workflow_gate.py \
  --scorecard reports/finance_workflow_scorecard.csv \
  --report-json reports/finance_workflow_gate.json
```

Pass means your finance workflow is meeting agreed quality thresholds.

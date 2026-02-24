# Finance Prompt Pack v1 (End-User Demo Script)

Use this pack to run a consistent finance demo from start to finish.

Prerequisites:

1. Seed fintech data (`scripts/fintech_seed.sql`).
2. Sync fintech datapoints (`datapoints/examples/fintech_bank`).
3. In UI, set:
   - **Target database** = fintech connection
   - **Workflow mode** = `Finance Brief v1`

---

## How to Run

1. Run prompts in order from `P01` to `P20`.
2. For each prompt, record:
   - did answer complete without clarification loop?
   - were sources shown?
   - were drivers + caveats present?
3. Fill `reports/finance_workflow_scorecard.csv` and run:

```bash
uv run python scripts/finance_workflow_gate.py \
  --scorecard reports/finance_workflow_scorecard.csv \
  --report-json reports/finance_workflow_gate.json
```

---

## Prompt List (20)

| ID | Prompt | Primary DataPoint(s) | Expected Signal |
|---|---|---|---|
| P01 | Show total deposits, withdrawals, and net flow by segment for the last 8 weeks. | `query_bank_weekly_net_flow_by_segment_001` | Weekly rows by segment with all 3 metrics. |
| P02 | Identify the top 2 segments driving week-over-week net flow decline. | `query_bank_top_segments_wow_net_flow_decline_001` | Segment-ranked decline output with previous/current week values. |
| P03 | Show failed transaction rate by day for the last 30 days. | `query_bank_daily_failed_transaction_rate_001` | Daily trend with total, failed, failed_rate_pct. |
| P04 | Which transaction types are driving failed transactions in the last 30 days? | `query_bank_top_failure_transaction_types_001` | Ranked transaction types by failed count/rate. |
| P05 | Which loans are highest risk based on days past due and payment behavior? | `query_bank_high_risk_loans_001` | Loan-level ranking with risk_score and delinquency context. |
| P06 | Show loan default rate by segment. | `query_bank_loan_default_rate_by_segment_001` | Segment-level total/default/default_rate output. |
| P07 | Show fee income mix by transaction type for the last 30 days. | `query_bank_fee_income_mix_001` | Fee mix with percent share by txn type. |
| P08 | Show top customers by deposit concentration risk. | `query_bank_deposit_concentration_top_customers_001` | Ranked top balances + share of total. |
| P09 | Show concentration risk: top 10 customers by total balance and percent of total deposits. | `query_bank_deposit_concentration_top_customers_001` | Same as P08 but with explicit top-10 request. |
| P10 | What is card block rate by segment and kyc_status? | `table_bank_cards_001`, `table_bank_customers_001` | Grouped block-rate view by segment + KYC status. |
| P11 | Compare posted vs reversed vs declined transaction volumes and value by day. | `table_bank_transactions_001` | Daily status breakdown, counts and amounts. |
| P12 | What is average account balance and active account count by country and segment? | `table_bank_accounts_001`, `table_bank_customers_001`, `metric_total_deposits_bank_001` | Country-segment aggregation with avg balance + active count. |
| P13 | Which customers have the highest fee burden (fees as percent of debit volume)? | `table_bank_transactions_001`, `table_bank_accounts_001`, `table_bank_customers_001` | Customer-level fee ratio ranking. |
| P14 | What share of fee income comes from card purchases versus transfers versus cash withdrawals? | `query_bank_fee_income_mix_001` | Fee contribution by these key types; shares should be interpretable. |
| P15 | Which transaction types are most associated with declines for restricted or pending-review users? | `table_bank_transactions_001`, `table_bank_accounts_001`, `table_bank_customers_001` | Decline affinity by txn_type under KYC constraints. |
| P16 | Show repayment performance cohorts by disbursement quarter (posted, partial, missed). | `table_bank_loans_001`, `table_bank_loan_payments_001` | Quarter cohorts with payment-status mix. |
| P17 | What is net interest income trend by month based on loan payments and interest credits? | `metric_net_interest_income_bank_001`, `table_bank_loan_payments_001`, `table_bank_transactions_001` | Monthly trend with explainable components. |
| P18 | If declined transfers were recovered at 30%, how much incremental transaction value would that add monthly? | `query_bank_top_failure_transaction_types_001`, `table_bank_transactions_001` | Scenario estimate with assumptions called out. |
| P19 | Which countries have the highest decline rate after adjusting for transaction volume? | `table_bank_transactions_001`, `table_bank_accounts_001`, `table_bank_customers_001` | Volume-adjusted country ranking (avoid tiny-volume bias). |
| P20 | Summarize top 3 liquidity risk signals from the last 30 days with caveats. | `query_bank_weekly_net_flow_by_segment_001`, `query_bank_deposit_concentration_top_customers_001`, `proc_daily_transaction_rollup_bank_001` | Prioritized risk summary with explicit caveats and sources. |

---

## Definition and Authority Track (FND-007, Planned Extension)

Use this optional track once canonical finance-global metrics are enabled.

| ID | Prompt | Expected Signal |
|---|---|---|
| D01 | Define Net Interest Margin and show the exact formula used. | Formula + interpretation + caveat + authority citation |
| D02 | Define Loan Default Rate and explain when this metric can be misleading. | Formula + explicit caveats + source authority |
| D03 | Compare ROA vs ROE and when each should be preferred in banking analysis. | Distinct definitions with usage guidance and citations |
| D04 | Explain Tier 1 Capital Ratio and list key assumptions before comparing banks. | Regulatory definition with assumption checklist + source |
| D05 | Define Liquidity Coverage Ratio and show related metrics I should check with it. | Canonical definition + linked related metrics + provenance |

Scoring notes for `reports/finance_workflow_scorecard.csv`:

- `has_source_attribution=yes` only when authority citation is explicit.
- `driver_quality_pass=yes` only when caveats/assumptions are materially useful and method-specific.
- `reproducibility_pass=yes` only when rerun preserves formula and interpretation intent.

---

## Clarification Recovery Replies (Copy/Paste)

If a prompt asks for more detail, reply with one of these:

- `Use public.bank_transactions for transaction-level calculations.`
- `Use amount as the transaction value and status='posted' unless stated otherwise.`
- `Use public.bank_loans and public.bank_loan_payments for loan risk and repayment analysis.`
- `Use bank_accounts joined to bank_customers for segment and country breakdowns.`

---

## Demo Flow (Recommended)

1. Start with `P01` and `P02` (executive liquidity story).
2. Run `P03` + `P04` (operations risk story).
3. Run `P05` + `P06` + `P16` (credit risk story).
4. Run `P07` + `P08` + `P17` (profitability + concentration story).
5. Use `P20` to close with a leadership-ready summary.

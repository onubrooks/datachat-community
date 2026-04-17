# Fintech Bank DataPoints

This folder contains a realistic sample DataPoint bundle for banking/fintech analytics.

Seeded date coverage for the matching demo dataset:

- `bank_transactions`: `2025-09-01` through `2026-04-30`
- `bank_fx_rates`: `2025-09-01` through `2026-04-30`
- `bank_loan_payments`: `2025-01-15` through `2026-04-15`

That coverage is intentional so prompts like weekly net flow, monthly interest-income trend, and recent failure-rate questions have real seeded rows for February, March, and April 2026.

Includes:

- Schema DataPoints for customers, accounts, transactions, cards, loans, and FX rates
- Business metric DataPoints for deposits, interest income, default rate, and failed transactions
- Process DataPoints for daily transaction rollups and nightly risk snapshots
- Query DataPoints for common finance workflows:
  - weekly deposits / withdrawals / net flow by segment
  - top segments driving week-over-week net-flow decline
  - failed transaction-rate trend and top failure transaction types
  - high-risk loan ranking and default-rate by segment
  - fee-income mix and deposit concentration

Use with:

```bash
datachat dp sync --datapoints-dir datapoints/examples/fintech_bank
```

or load demo end-to-end:

```bash
datachat demo --dataset fintech --reset
```

Recommended sync after local edits:

```bash
datachat dp sync --datapoints-dir datapoints/examples/fintech_bank --global-scope
```

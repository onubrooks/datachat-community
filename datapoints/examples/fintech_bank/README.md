# Fintech Bank DataPoints

This folder contains a realistic sample DataPoint bundle for banking/fintech analytics.

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

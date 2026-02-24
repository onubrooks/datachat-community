# Domain Question Bank (Manual UI + CLI Testing)

Use this question bank to test DataChat in both:

- CLI: `datachat ask "<question>"`
- UI: Chat page with the same prompt text

## Grocery (20 advanced business questions)

| # | Question | Expected Signal | Possible Answer Hint |
|---|---|---|---|
| 1 | What is total revenue, gross margin, and waste cost by store for the last 30 days? | Store-level aggregation over sales + product costs + waste costs. | Revenue should be highest in at least one South store; include 3 metrics per store. |
| 2 | Which 5 SKUs have the highest stockout risk this week based on on-hand, reserved, and reorder level? | Uses inventory snapshots + reorder logic and ranks top 5 risk SKUs. | Should reference `on_hand_qty - reserved_qty` vs `reorder_level` and return 5 rows. |
| 3 | Show category-level gross margin trend by week for the last 8 weeks. | Weekly time series grouped by category. | Output should include week, category, revenue, cogs, margin pct. |
| 4 | Which suppliers have the highest late-delivery rate and what is the average delay in days? | Uses purchase order expected vs received dates and status. | `in_transit` and `partial` suppliers should show higher lateness. |
| 5 | Compare weekend vs weekday sales lift by category and store. | Splits by day-of-week class and compares sales per class. | Should show weekend uplift for some perishable categories. |
| 6 | What are the top 10 products by waste cost, and what percentage of their revenue is lost to waste? | Joins waste and sales by product; ranks by waste cost. | Includes `waste_cost / product_revenue` ratio and top 10 products. |
| 7 | Which stores have the largest gap between inventory movement and recorded sales? | Compares inventory deltas vs sold quantity over date windows. | At least one store-category pair should show noticeable mismatch. |
| 8 | Forecast likely out-of-stock SKUs in the next 7 days using recent sales velocity. | Uses recent average daily sales vs net inventory. | Should output likely days-to-stockout by SKU and store. |
| 9 | What is average discount rate by category, and does higher discounting correlate with higher unit sales? | Computes discount share and compares with quantity sold. | Pantry or produce categories should appear in discount analysis. |
| 10 | For perishable products, what is estimated shelf-loss rate by store over the last 45 days? | Filters perishable SKUs and computes waste-to-available ratio. | Includes per-store shelf-loss percentages. |
| 11 | Which purchase orders were partial or late, and what downstream sales impact do we see on affected SKUs? | Finds delayed POs then evaluates following sales patterns. | Should list impacted SKUs and reduced/volatile sales windows. |
| 12 | Show daily revenue, COGS, and gross margin for each region for the last 6 weeks. | Region-day grain time series with 3 core metrics. | Output should include South vs North comparison. |
| 13 | What are the slowest-moving products with high on-hand inventory (days of inventory greater than 30)? | Uses sales velocity and current stock to compute days of inventory. | Returns a ranked list of overstocked slow movers. |
| 14 | Which combinations of store and category have the most volatile daily sales? | Calculates variability, e.g. stddev or coefficient of variation. | Should rank store-category combinations by volatility metric. |
| 15 | If we reduce waste by 15 percent for perishables, what is the projected monthly gross margin improvement? | Scenario analysis using current waste cost baseline. | Returns projected uplift amount and percent for margin. |
| 16 | Which products fell below reorder level most frequently in the last 30 days? | Counts below-threshold days per product and store. | Should return products with high breach counts, likely perishables. |
| 17 | What is gross margin by store and category, and which are the bottom 20 percent performers? | Computes margin then percentile/rank filtering. | Bottom cohort should include low-margin combinations. |
| 18 | Which days had waste spikes and what reasons dominated each spike day? | Detects high-waste outlier days and top reasons. | Common reasons should include expired, damaged shipment, quality reject. |
| 19 | What is inventory turnover by category and store for the last 60 days? | Uses COGS or units sold divided by average inventory. | Should show higher turnover for some perishable categories. |
| 20 | If supplier lead time increases by 2 days, which SKUs are most likely to stock out first? | Combines lead time sensitivity with recent demand and stock position. | Outputs prioritized SKU risk list with estimated stockout horizon. |

## Fintech (20 advanced business questions)

| # | Question | Expected Signal | Possible Answer Hint |
|---|---|---|---|
| 1 | What is the failed transaction rate by day for the last 30 days, and which txn types drive it? | Daily failed-rate time series plus breakdown by txn_type. | Failed statuses should include declined and reversed patterns. |
| 2 | Show total deposits, withdrawals, and net flow by customer segment for the last 8 weeks. | Segment-week aggregation with net flow calculation. | Retail and SME segments should both appear with different net trends. |
| 3 | What is loan default rate (90 plus DPD) by segment and loan type? | Uses loans table with DPD thresholds and grouping dimensions. | Non-performing or delinquent segments should have non-zero rates. |
| 4 | Which accounts show unusual transaction spikes versus their trailing 14-day baseline? | Baseline vs current anomaly detection by account. | Returns accounts with spike ratio or z-score style metric. |
| 5 | Compare posted vs reversed vs declined transaction volumes and value by day. | Status-level daily metrics with volume and amount. | Includes status counts and value totals, not just one metric. |
| 6 | What is net interest income trend by month based on loan payments and interest credits? | Monthly NII estimate from interest components and credits. | Should show month trend with at least two input sources. |
| 7 | Which customers have the highest fee burden (fees as percent of debit volume)? | Customer-level fee ratio ranking. | Top results should show fee pct, total fees, debit volume. |
| 8 | Show concentration risk: top 10 customers by total balance and percent of total deposits. | Balance concentration table and cumulative share. | Includes total balance and share of total book. |
| 9 | Which loans are most at risk of becoming non-performing in the next cycle? | Risk ranking from DPD, payment status history, and segment clues. | High DPD loans and recent missed/partial payments should rank highest. |
| 10 | What is card block rate by segment and kyc_status? | Blocked cards over issued cards by dimensions. | Restricted and blocked KYC statuses should show higher block rates. |
| 11 | For FX activity, show daily USD equivalent volume by currency pair and trend. | Joins transactions and FX rates with USD normalization. | Should include time series for USD-NGN, USD-EUR, USD-GBP, USD-GHS. |
| 12 | Which transaction types are most associated with declines for restricted or pending-review users? | Filters by KYC status and computes decline affinity by txn_type. | Transfer/card types may dominate decline contribution. |
| 13 | What is average account balance and active account count by country and segment? | Country-segment balance and account activity aggregates. | Includes average balance and active account counts in same output. |
| 14 | Show repayment performance cohorts by disbursement quarter (posted, partial, missed). | Loan cohort analysis with payment outcome split. | Should output disbursement quarter and payment-status distribution. |
| 15 | If declined transfers were recovered at 30 percent, how much incremental transaction value would that add monthly? | Scenario conversion from declined transfer value. | Returns projected incremental monthly value figure. |
| 16 | Which countries have the highest decline rate after adjusting for transaction volume? | Weighted decline metric by country and volume floor. | Should avoid tiny-volume bias and return ranked countries. |
| 17 | What share of fee income comes from card purchases versus transfers versus cash withdrawals? | Fee composition across txn types with percentages. | Output should sum close to 100 percent across major fee drivers. |
| 18 | Which customers show frequent reversals and potential fraud indicators? | Customer risk flags using reversal frequency and value patterns. | Returns customers with reversal counts, amounts, and risk notes. |
| 19 | What is delinquency migration month over month from current to 30 plus to 90 plus DPD buckets? | Transition-style delinquency movement summary by month. | Should show worsening/improving bucket transitions. |
| 20 | If pending transactions clear at historical rates, what is projected net flow over the next 7 days? | Uses historical pending-to-posted conversion and current pending volume. | Returns projected net inflow/outflow with assumptions stated. |

## Suggested test pass sequence

1. Start with table-shape questions (counts, show tables, show columns).
2. Run the full 20-question set in CLI.
3. Run the same 20-question set in UI on the same selected database.
4. Compare differences in:
   - SQL generated
   - answer source (`sql` vs `context` vs `clarification`)
   - clarity and confidence
   - whether expected signal appears
5. Capture any mismatches where UI and CLI behavior diverges.

## Finance Query Datapoints (Ready-to-use)

The following query datapoints are available in `datapoints/examples/fintech_bank` and are designed to match core finance prompts quickly:

- `query_bank_weekly_net_flow_by_segment_001`
- `query_bank_top_segments_wow_net_flow_decline_001`
- `query_bank_daily_failed_transaction_rate_001`
- `query_bank_top_failure_transaction_types_001`
- `query_bank_high_risk_loans_001`
- `query_bank_loan_default_rate_by_segment_001`
- `query_bank_fee_income_mix_001`
- `query_bank_deposit_concentration_top_customers_001`

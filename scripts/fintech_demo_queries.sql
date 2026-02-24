-- Fintech demo query pack (manual validation).
-- Run with:
-- psql "postgresql://postgres:@localhost:5432/datachat_fintech" -f scripts/fintech_demo_queries.sql

-- 1) Data volume checks (sanity: should be in thousands for key tables)
SELECT 'bank_customers' AS table_name, COUNT(*) AS row_count FROM public.bank_customers
UNION ALL
SELECT 'bank_accounts', COUNT(*) FROM public.bank_accounts
UNION ALL
SELECT 'bank_transactions', COUNT(*) FROM public.bank_transactions
UNION ALL
SELECT 'bank_loans', COUNT(*) FROM public.bank_loans
UNION ALL
SELECT 'bank_loan_payments', COUNT(*) FROM public.bank_loan_payments
UNION ALL
SELECT 'bank_fx_rates', COUNT(*) FROM public.bank_fx_rates
ORDER BY table_name;

-- 2) Weekly deposits, withdrawals, and net flow by segment (last 8 weeks)
WITH weekly_flows AS (
    SELECT
        DATE_TRUNC('week', bt.business_date)::date AS week_start,
        bc.segment,
        ROUND(SUM(CASE WHEN bt.direction = 'credit' THEN bt.amount ELSE 0 END)::numeric, 2) AS total_deposits,
        ROUND(SUM(CASE WHEN bt.direction = 'debit' THEN bt.amount ELSE 0 END)::numeric, 2) AS total_withdrawals,
        ROUND(SUM(CASE WHEN bt.direction = 'credit' THEN bt.amount ELSE -bt.amount END)::numeric, 2) AS net_flow
    FROM public.bank_transactions bt
    JOIN public.bank_accounts ba ON bt.account_id = ba.account_id
    JOIN public.bank_customers bc ON ba.customer_id = bc.customer_id
    WHERE bt.status = 'posted'
      AND bt.business_date >= CURRENT_DATE - (56 * INTERVAL '1 day')
    GROUP BY 1, 2
)
SELECT
    week_start,
    segment,
    total_deposits,
    total_withdrawals,
    net_flow
FROM weekly_flows
ORDER BY week_start DESC, segment;

-- 3) Top 2 segments driving WoW net-flow decline
WITH weekly_flows AS (
    SELECT
        DATE_TRUNC('week', bt.business_date)::date AS week_start,
        bc.segment,
        SUM(CASE WHEN bt.direction = 'credit' THEN bt.amount ELSE -bt.amount END) AS net_flow
    FROM public.bank_transactions bt
    JOIN public.bank_accounts ba ON bt.account_id = ba.account_id
    JOIN public.bank_customers bc ON ba.customer_id = bc.customer_id
    WHERE bt.status = 'posted'
      AND bt.business_date >= CURRENT_DATE - (56 * INTERVAL '1 day')
    GROUP BY 1, 2
), ranked_weeks AS (
    SELECT
        week_start,
        segment,
        net_flow,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY week_start DESC) AS rn
    FROM weekly_flows
), latest AS (
    SELECT segment, net_flow AS current_week_net_flow
    FROM ranked_weeks
    WHERE rn = 1
), previous AS (
    SELECT segment, net_flow AS previous_week_net_flow
    FROM ranked_weeks
    WHERE rn = 2
)
SELECT
    l.segment,
    ROUND(p.previous_week_net_flow::numeric, 2) AS previous_week_net_flow,
    ROUND(l.current_week_net_flow::numeric, 2) AS current_week_net_flow,
    ROUND((p.previous_week_net_flow - l.current_week_net_flow)::numeric, 2) AS decline_amount
FROM latest l
JOIN previous p ON p.segment = l.segment
WHERE l.current_week_net_flow < p.previous_week_net_flow
ORDER BY decline_amount DESC
LIMIT 2;

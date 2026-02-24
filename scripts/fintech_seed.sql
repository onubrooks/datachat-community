-- Fintech bank sample schema + large seed data for DataPoint-driven testing.

DROP TABLE IF EXISTS public.bank_loan_payments CASCADE;
DROP TABLE IF EXISTS public.bank_cards CASCADE;
DROP TABLE IF EXISTS public.bank_transactions CASCADE;
DROP TABLE IF EXISTS public.bank_loans CASCADE;
DROP TABLE IF EXISTS public.bank_accounts CASCADE;
DROP TABLE IF EXISTS public.bank_fx_rates CASCADE;
DROP TABLE IF EXISTS public.bank_customers CASCADE;

CREATE TABLE public.bank_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL,
    country TEXT NOT NULL,
    segment TEXT NOT NULL,
    kyc_status TEXT NOT NULL,
    created_at DATE NOT NULL
);

CREATE TABLE public.bank_accounts (
    account_id SERIAL PRIMARY KEY,
    account_number TEXT NOT NULL UNIQUE,
    customer_id INTEGER NOT NULL REFERENCES public.bank_customers(customer_id),
    account_type TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at DATE NOT NULL,
    current_balance NUMERIC(14,2) NOT NULL
);

CREATE TABLE public.bank_transactions (
    txn_id SERIAL PRIMARY KEY,
    posted_at TIMESTAMP NOT NULL,
    business_date DATE NOT NULL,
    account_id INTEGER NOT NULL REFERENCES public.bank_accounts(account_id),
    counterparty_account TEXT,
    txn_type TEXT NOT NULL,
    direction TEXT NOT NULL,
    amount NUMERIC(14,2) NOT NULL,
    fee_amount NUMERIC(10,2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    reference_text TEXT
);

CREATE TABLE public.bank_cards (
    card_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES public.bank_customers(customer_id),
    account_id INTEGER NOT NULL REFERENCES public.bank_accounts(account_id),
    card_type TEXT NOT NULL,
    network TEXT NOT NULL,
    status TEXT NOT NULL,
    issued_at DATE NOT NULL,
    blocked_at DATE
);

CREATE TABLE public.bank_loans (
    loan_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES public.bank_customers(customer_id),
    repayment_account_id INTEGER REFERENCES public.bank_accounts(account_id),
    loan_type TEXT NOT NULL,
    principal_amount NUMERIC(14,2) NOT NULL,
    interest_rate NUMERIC(5,4) NOT NULL,
    disbursed_at DATE NOT NULL,
    maturity_date DATE NOT NULL,
    status TEXT NOT NULL,
    days_past_due INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE public.bank_loan_payments (
    payment_id SERIAL PRIMARY KEY,
    loan_id INTEGER NOT NULL REFERENCES public.bank_loans(loan_id),
    payment_date DATE NOT NULL,
    amount NUMERIC(14,2) NOT NULL,
    principal_component NUMERIC(14,2) NOT NULL,
    interest_component NUMERIC(14,2) NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE public.bank_fx_rates (
    rate_date DATE NOT NULL,
    base_currency TEXT NOT NULL,
    quote_currency TEXT NOT NULL,
    rate NUMERIC(12,6) NOT NULL,
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);

INSERT INTO public.bank_customers
(customer_code, full_name, email, country, segment, kyc_status, created_at)
VALUES
('CUST001', 'Ada Okafor', 'ada.okafor@example.com', 'NG', 'retail', 'verified', '2023-05-10'),
('CUST002', 'Noah Mensah', 'noah.mensah@example.com', 'GH', 'retail', 'verified', '2022-11-18'),
('CUST003', 'Luna Ibrahim', 'luna.ibrahim@example.com', 'NG', 'sme', 'verified', '2021-08-02'),
('CUST004', 'Kai Daniels', 'kai.daniels@example.com', 'US', 'sme', 'pending_review', '2024-01-22'),
('CUST005', 'Mia Alvarez', 'mia.alvarez@example.com', 'US', 'retail', 'verified', '2022-09-07'),
('CUST006', 'Leo Boateng', 'leo.boateng@example.com', 'GH', 'sme', 'verified', '2021-12-14'),
('CUST007', 'Sara Kimani', 'sara.kimani@example.com', 'KE', 'retail', 'verified', '2023-02-01'),
('CUST008', 'Ibrahim Yusuf', 'ibrahim.yusuf@example.com', 'NG', 'corporate', 'verified', '2020-06-19'),
('CUST009', 'Nora Smith', 'nora.smith@example.com', 'US', 'corporate', 'restricted', '2024-04-04'),
('CUST010', 'Grace Adu', 'grace.adu@example.com', 'GH', 'retail', 'pending_review', '2023-10-30'),
('CUST011', 'Tariq Bello', 'tariq.bello@example.com', 'NG', 'sme', 'verified', '2022-03-11'),
('CUST012', 'Emily Carter', 'emily.carter@example.com', 'US', 'retail', 'verified', '2021-01-27');

INSERT INTO public.bank_customers
(customer_code, full_name, email, country, segment, kyc_status, created_at)
SELECT
    'CUST' || LPAD(gs::text, 3, '0') AS customer_code,
    'Customer ' || gs AS full_name,
    'customer' || gs || '@example.com' AS email,
    CASE gs % 6
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'NG'
        WHEN 2 THEN 'GH'
        WHEN 3 THEN 'KE'
        WHEN 4 THEN 'ZA'
        ELSE 'GB'
    END AS country,
    CASE gs % 5
        WHEN 0 THEN 'corporate'
        WHEN 1 THEN 'sme'
        ELSE 'retail'
    END AS segment,
    CASE
        WHEN gs % 19 = 0 THEN 'blocked'
        WHEN gs % 11 = 0 THEN 'restricted'
        WHEN gs % 7 = 0 THEN 'pending_review'
        ELSE 'verified'
    END AS kyc_status,
    ('2021-01-01'::date + ((gs * 13) % 1200))::date AS created_at
FROM generate_series(13, 420) gs;

-- Accounts: checking for all, savings for many, plus business/fx accounts for SME/corporate.
INSERT INTO public.bank_accounts
(account_number, customer_id, account_type, currency_code, status, opened_at, current_balance)
SELECT
    LPAD((1000000000 + customer_id * 10 + 1)::text, 10, '0') AS account_number,
    customer_id,
    'checking' AS account_type,
    'USD' AS currency_code,
    CASE
        WHEN kyc_status = 'blocked' THEN 'frozen'
        WHEN kyc_status IN ('restricted', 'pending_review') THEN 'restricted'
        ELSE 'active'
    END AS status,
    (created_at + ((customer_id * 3) % 45))::date AS opened_at,
    ROUND((1200 + customer_id * 215 + (customer_id % 7) * 125)::numeric, 2) AS current_balance
FROM public.bank_customers;

INSERT INTO public.bank_accounts
(account_number, customer_id, account_type, currency_code, status, opened_at, current_balance)
SELECT
    LPAD((1000000000 + customer_id * 10 + 2)::text, 10, '0') AS account_number,
    customer_id,
    'savings' AS account_type,
    CASE WHEN customer_id % 4 = 0 THEN 'EUR' ELSE 'USD' END AS currency_code,
    CASE
        WHEN kyc_status = 'blocked' THEN 'frozen'
        WHEN kyc_status IN ('restricted', 'pending_review') THEN 'restricted'
        ELSE 'active'
    END AS status,
    (created_at + ((customer_id * 5) % 65))::date AS opened_at,
    ROUND((3800 + customer_id * 340 + (customer_id % 9) * 210)::numeric, 2) AS current_balance
FROM public.bank_customers
WHERE customer_id % 2 = 0;

INSERT INTO public.bank_accounts
(account_number, customer_id, account_type, currency_code, status, opened_at, current_balance)
SELECT
    LPAD((1000000000 + customer_id * 10 + 3)::text, 10, '0') AS account_number,
    customer_id,
    'business' AS account_type,
    CASE WHEN customer_id % 3 = 0 THEN 'EUR' ELSE 'USD' END AS currency_code,
    CASE
        WHEN kyc_status = 'blocked' THEN 'frozen'
        WHEN kyc_status IN ('restricted', 'pending_review') THEN 'restricted'
        ELSE 'active'
    END AS status,
    (created_at + ((customer_id * 7) % 90))::date AS opened_at,
    ROUND((15500 + customer_id * 620 + (customer_id % 11) * 330)::numeric, 2) AS current_balance
FROM public.bank_customers
WHERE segment IN ('sme', 'corporate');

-- Daily transactional activity over ~5.5 months with deterministic status/failure patterns.
WITH days AS (
    SELECT gs::date AS business_date
    FROM generate_series('2025-09-01'::date, '2026-02-14'::date, interval '1 day') gs
),
active_accounts AS (
    SELECT account_id, account_type, currency_code, status
    FROM public.bank_accounts
),
txn_base AS (
    SELECT
        d.business_date,
        a.account_id,
        a.account_type,
        a.currency_code,
        EXTRACT(DOY FROM d.business_date)::int AS doy,
        ((a.account_id * 37 + EXTRACT(DOY FROM d.business_date)::int * 13) % 100) AS selector
    FROM days d
    CROSS JOIN active_accounts a
),
txn_shape AS (
    SELECT
        business_date,
        account_id,
        account_type,
        currency_code,
        doy,
        selector,
        CASE
            WHEN selector < 18 THEN 'card_purchase'
            WHEN selector < 34 THEN 'transfer'
            WHEN selector < 44 THEN 'bill_payment'
            WHEN selector < 53 THEN 'cash_withdrawal'
            WHEN selector < 61 THEN 'salary_credit'
            WHEN selector < 70 THEN 'interest_credit'
            WHEN selector < 84 THEN 'fx_transfer'
            ELSE 'direct_debit'
        END AS txn_type
    FROM txn_base
),
txn_amounts AS (
    SELECT
        business_date,
        account_id,
        account_type,
        currency_code,
        doy,
        selector,
        txn_type,
        CASE
            WHEN txn_type = 'card_purchase' THEN ROUND((8 + selector * 1.6 + (account_id % 9) * 0.9)::numeric, 2)
            WHEN txn_type = 'transfer' THEN ROUND((120 + selector * 7.5 + (account_id % 5) * 25)::numeric, 2)
            WHEN txn_type = 'bill_payment' THEN ROUND((30 + selector * 4.2 + (account_id % 7) * 11)::numeric, 2)
            WHEN txn_type = 'cash_withdrawal' THEN ROUND((40 + selector * 3.4 + (account_id % 6) * 15)::numeric, 2)
            WHEN txn_type = 'salary_credit' THEN ROUND((900 + (selector % 12) * 145 + (account_id % 4) * 220)::numeric, 2)
            WHEN txn_type = 'interest_credit' THEN ROUND((4 + (selector % 10) * 1.5 + (account_id % 3) * 0.7)::numeric, 2)
            WHEN txn_type = 'fx_transfer' THEN ROUND((160 + selector * 5.4 + (account_id % 8) * 20)::numeric, 2)
            ELSE ROUND((60 + selector * 2.8 + (account_id % 10) * 9)::numeric, 2)
        END AS amount
    FROM txn_shape
)
INSERT INTO public.bank_transactions
(posted_at, business_date, account_id, counterparty_account, txn_type, direction, amount, fee_amount, status, reference_text)
SELECT
    business_date::timestamp
    + (((account_id * 11 + selector * 3 + doy) % 13) + 7) * interval '1 hour'
    + (((account_id * 17 + selector + doy) % 60)) * interval '1 minute' AS posted_at,
    business_date,
    account_id,
    'EXT-' || LPAD(((account_id * 97 + selector * 17 + doy) % 100000)::text, 5, '0') AS counterparty_account,
    txn_type,
    CASE
        WHEN txn_type IN ('salary_credit', 'interest_credit') THEN 'credit'
        WHEN txn_type = 'transfer' AND selector % 2 = 0 THEN 'credit'
        ELSE 'debit'
    END AS direction,
    amount,
    CASE
        WHEN txn_type IN ('card_purchase', 'cash_withdrawal', 'fx_transfer', 'bill_payment', 'direct_debit')
            THEN ROUND((amount * 0.0035 + 0.12)::numeric, 2)
        ELSE 0::numeric
    END AS fee_amount,
    CASE
        WHEN selector % 29 = 0 THEN 'declined'
        WHEN selector % 41 = 0 THEN 'reversed'
        WHEN selector % 19 = 0 THEN 'pending'
        ELSE 'posted'
    END AS status,
    CASE txn_type
        WHEN 'card_purchase' THEN 'Card spend'
        WHEN 'transfer' THEN 'Account transfer'
        WHEN 'bill_payment' THEN 'Bill payment'
        WHEN 'cash_withdrawal' THEN 'ATM withdrawal'
        WHEN 'salary_credit' THEN 'Payroll credit'
        WHEN 'interest_credit' THEN 'Interest payout'
        WHEN 'fx_transfer' THEN 'FX settlement'
        ELSE 'Mandate debit'
    END AS reference_text
FROM txn_amounts;

-- Explicit high-value edge cases for risk/compliance testing.
INSERT INTO public.bank_transactions
(posted_at, business_date, account_id, counterparty_account, txn_type, direction, amount, fee_amount, status, reference_text)
SELECT
    '2026-02-10 09:42:00'::timestamp,
    '2026-02-10'::date,
    account_id,
    'EXT-HVR01',
    'transfer',
    'debit',
    48000.00,
    4.25,
    'declined',
    'High-value transfer declined'
FROM public.bank_accounts
WHERE account_type = 'business'
ORDER BY account_id
LIMIT 1;

INSERT INTO public.bank_cards
(customer_id, account_id, card_type, network, status, issued_at, blocked_at)
SELECT
    a.customer_id,
    a.account_id,
    CASE WHEN a.customer_id % 3 = 0 THEN 'credit' ELSE 'debit' END AS card_type,
    CASE WHEN a.customer_id % 2 = 0 THEN 'mastercard' ELSE 'visa' END AS network,
    CASE
        WHEN c.kyc_status = 'blocked' THEN 'blocked'
        WHEN c.kyc_status IN ('restricted', 'pending_review') AND a.customer_id % 5 = 0 THEN 'blocked'
        ELSE 'active'
    END AS status,
    (a.opened_at + ((a.customer_id * 2) % 45))::date AS issued_at,
    CASE
        WHEN c.kyc_status = 'blocked' THEN (a.opened_at + ((a.customer_id * 2) % 45) + 180)::date
        WHEN c.kyc_status IN ('restricted', 'pending_review') AND a.customer_id % 5 = 0
            THEN (a.opened_at + ((a.customer_id * 2) % 45) + 120)::date
        ELSE NULL
    END AS blocked_at
FROM public.bank_accounts a
JOIN public.bank_customers c
  ON c.customer_id = a.customer_id
WHERE a.account_type = 'checking';

INSERT INTO public.bank_cards
(customer_id, account_id, card_type, network, status, issued_at, blocked_at)
SELECT
    a.customer_id,
    a.account_id,
    'virtual_debit' AS card_type,
    CASE WHEN a.customer_id % 2 = 0 THEN 'mastercard' ELSE 'visa' END AS network,
    CASE
        WHEN c.kyc_status = 'blocked' THEN 'blocked'
        WHEN c.kyc_status IN ('restricted', 'pending_review') AND a.customer_id % 4 = 0 THEN 'blocked'
        ELSE 'active'
    END AS status,
    (a.opened_at + ((a.customer_id * 3) % 35))::date AS issued_at,
    CASE
        WHEN c.kyc_status = 'blocked' THEN (a.opened_at + ((a.customer_id * 3) % 35) + 150)::date
        WHEN c.kyc_status IN ('restricted', 'pending_review') AND a.customer_id % 4 = 0
            THEN (a.opened_at + ((a.customer_id * 3) % 35) + 110)::date
        ELSE NULL
    END AS blocked_at
FROM public.bank_accounts a
JOIN public.bank_customers c
  ON c.customer_id = a.customer_id
WHERE a.account_type = 'savings'
  AND a.customer_id % 2 = 0;

INSERT INTO public.bank_loans
(customer_id, repayment_account_id, loan_type, principal_amount, interest_rate, disbursed_at, maturity_date, status, days_past_due)
SELECT
    li.customer_id,
    li.repayment_account_id,
    CASE
        WHEN li.segment = 'corporate' THEN 'trade_finance'
        WHEN li.segment = 'sme' THEN 'working_capital'
        WHEN li.loan_sequence % 3 = 0 THEN 'mortgage'
        WHEN li.customer_id % 2 = 0 THEN 'auto'
        ELSE 'personal'
    END AS loan_type,
    ROUND(
        (
            9000
            + li.customer_id * 820
            + (li.customer_id % 6) * 1500
            + li.loan_sequence * 2400
        )::numeric,
        2
    ) AS principal_amount,
    ROUND((0.065 + (li.customer_id % 8) * 0.006 + li.loan_sequence * 0.0015)::numeric, 4) AS interest_rate,
    ('2022-01-15'::date + ((li.customer_id * 31 + li.loan_sequence * 19) % 1100))::date AS disbursed_at,
    (
        '2022-01-15'::date
        + ((li.customer_id * 31 + li.loan_sequence * 19) % 1100)
        + (900 + (li.customer_id % 6) * 120 + li.loan_sequence * 45)
    )::date AS maturity_date,
    CASE
        WHEN li.kyc_status = 'blocked' THEN 'non_performing'
        WHEN (li.customer_id + li.loan_sequence) % 13 = 0 THEN 'delinquent'
        ELSE 'active'
    END AS status,
    CASE
        WHEN li.kyc_status = 'blocked' THEN 140
        WHEN (li.customer_id + li.loan_sequence) % 13 = 0 THEN 95
        WHEN (li.customer_id + li.loan_sequence) % 7 = 0 THEN 38
        ELSE 0
    END AS days_past_due
FROM (
    SELECT
        c.customer_id,
        c.segment,
        c.kyc_status,
        a.account_id AS repayment_account_id,
        gs AS loan_sequence
    FROM public.bank_customers c
    JOIN public.bank_accounts a
      ON a.customer_id = c.customer_id
     AND a.account_type = 'checking'
    JOIN LATERAL generate_series(
        1,
        CASE
            WHEN c.segment IN ('sme', 'corporate') THEN 3
            ELSE 2
        END
    ) gs ON TRUE
    WHERE c.customer_id % 2 = 0
       OR c.segment IN ('sme', 'corporate')
) li;

WITH payment_months AS (
    SELECT gs::date AS payment_date
    FROM generate_series('2025-01-15'::date, '2026-02-15'::date, interval '1 month') gs
),
loan_schedule AS (
    SELECT
        l.loan_id,
        l.days_past_due,
        l.principal_amount,
        l.interest_rate,
        p.payment_date,
        ROUND((l.principal_amount * 0.012 + l.principal_amount * l.interest_rate / 12)::numeric, 2) AS scheduled_amount
    FROM public.bank_loans l
    CROSS JOIN payment_months p
)
INSERT INTO public.bank_loan_payments
(loan_id, payment_date, amount, principal_component, interest_component, status)
SELECT
    loan_id,
    payment_date,
    CASE
        WHEN days_past_due >= 90 AND payment_date >= '2025-12-15'::date THEN 0::numeric
        WHEN days_past_due BETWEEN 30 AND 89 AND EXTRACT(MONTH FROM payment_date)::int % 2 = 0
            THEN ROUND((scheduled_amount * 0.6)::numeric, 2)
        ELSE scheduled_amount
    END AS amount,
    CASE
        WHEN days_past_due >= 90 AND payment_date >= '2025-12-15'::date THEN 0::numeric
        WHEN days_past_due BETWEEN 30 AND 89 AND EXTRACT(MONTH FROM payment_date)::int % 2 = 0
            THEN ROUND((scheduled_amount * 0.45)::numeric, 2)
        ELSE ROUND((scheduled_amount * 0.72)::numeric, 2)
    END AS principal_component,
    CASE
        WHEN days_past_due >= 90 AND payment_date >= '2025-12-15'::date THEN 0::numeric
        WHEN days_past_due BETWEEN 30 AND 89 AND EXTRACT(MONTH FROM payment_date)::int % 2 = 0
            THEN ROUND((scheduled_amount * 0.15)::numeric, 2)
        ELSE ROUND((scheduled_amount * 0.28)::numeric, 2)
    END AS interest_component,
    CASE
        WHEN days_past_due >= 90 AND payment_date >= '2025-12-15'::date THEN 'missed'
        WHEN days_past_due BETWEEN 30 AND 89 AND EXTRACT(MONTH FROM payment_date)::int % 2 = 0 THEN 'partial'
        ELSE 'posted'
    END AS status
FROM loan_schedule;

-- Daily FX rates for multiple pairs over the same ~5.5 month period.
WITH days AS (
    SELECT gs::date AS rate_date
    FROM generate_series('2025-09-01'::date, '2026-02-14'::date, interval '1 day') gs
),
quotes AS (
    SELECT * FROM (VALUES
        ('NGN', 1490.000000::numeric, 0.92::numeric),
        ('EUR', 0.905000::numeric, 0.0032::numeric),
        ('GBP', 0.776000::numeric, 0.0027::numeric),
        ('GHS', 14.680000::numeric, 0.0450::numeric)
    ) AS q(quote_currency, base_rate, step_size)
)
INSERT INTO public.bank_fx_rates
(rate_date, base_currency, quote_currency, rate)
SELECT
    d.rate_date,
    'USD' AS base_currency,
    q.quote_currency,
    ROUND((q.base_rate + ((EXTRACT(DOY FROM d.rate_date)::int % 17) - 8) * q.step_size)::numeric, 6) AS rate
FROM days d
CROSS JOIN quotes q;

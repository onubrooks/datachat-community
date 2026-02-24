-- Grocery store sample schema + expanded seed data for DataPoint-driven testing.

DROP TABLE IF EXISTS public.grocery_waste_events CASCADE;
DROP TABLE IF EXISTS public.grocery_purchase_orders CASCADE;
DROP TABLE IF EXISTS public.grocery_sales_transactions CASCADE;
DROP TABLE IF EXISTS public.grocery_inventory_snapshots CASCADE;
DROP TABLE IF EXISTS public.grocery_products CASCADE;
DROP TABLE IF EXISTS public.grocery_suppliers CASCADE;
DROP TABLE IF EXISTS public.grocery_stores CASCADE;

CREATE TABLE public.grocery_stores (
    store_id SERIAL PRIMARY KEY,
    store_code TEXT NOT NULL UNIQUE,
    store_name TEXT NOT NULL,
    city TEXT NOT NULL,
    region TEXT NOT NULL,
    opened_at DATE NOT NULL
);

CREATE TABLE public.grocery_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name TEXT NOT NULL,
    lead_time_days INTEGER NOT NULL,
    contact_email TEXT NOT NULL
);

CREATE TABLE public.grocery_products (
    product_id SERIAL PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_cost NUMERIC(10,2) NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    is_perishable BOOLEAN NOT NULL DEFAULT true,
    reorder_level INTEGER NOT NULL,
    supplier_id INTEGER NOT NULL REFERENCES public.grocery_suppliers(supplier_id)
);

CREATE TABLE public.grocery_inventory_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    store_id INTEGER NOT NULL REFERENCES public.grocery_stores(store_id),
    product_id INTEGER NOT NULL REFERENCES public.grocery_products(product_id),
    on_hand_qty INTEGER NOT NULL,
    reserved_qty INTEGER NOT NULL DEFAULT 0,
    UNIQUE(snapshot_date, store_id, product_id)
);

CREATE TABLE public.grocery_sales_transactions (
    txn_id SERIAL PRIMARY KEY,
    sold_at TIMESTAMP NOT NULL,
    business_date DATE NOT NULL,
    store_id INTEGER NOT NULL REFERENCES public.grocery_stores(store_id),
    product_id INTEGER NOT NULL REFERENCES public.grocery_products(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    discount_amount NUMERIC(10,2) NOT NULL DEFAULT 0,
    total_amount NUMERIC(12,2) NOT NULL
);

CREATE TABLE public.grocery_purchase_orders (
    po_id SERIAL PRIMARY KEY,
    ordered_at TIMESTAMP NOT NULL,
    expected_at TIMESTAMP NOT NULL,
    received_at TIMESTAMP,
    supplier_id INTEGER NOT NULL REFERENCES public.grocery_suppliers(supplier_id),
    store_id INTEGER NOT NULL REFERENCES public.grocery_stores(store_id),
    product_id INTEGER NOT NULL REFERENCES public.grocery_products(product_id),
    ordered_qty INTEGER NOT NULL,
    received_qty INTEGER,
    unit_cost NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE public.grocery_waste_events (
    waste_id SERIAL PRIMARY KEY,
    event_date DATE NOT NULL,
    store_id INTEGER NOT NULL REFERENCES public.grocery_stores(store_id),
    product_id INTEGER NOT NULL REFERENCES public.grocery_products(product_id),
    quantity INTEGER NOT NULL,
    reason TEXT NOT NULL,
    estimated_cost NUMERIC(12,2) NOT NULL
);

INSERT INTO public.grocery_stores (store_code, store_name, city, region, opened_at) VALUES
('ST001', 'Downtown Fresh', 'Austin', 'South', '2020-03-01'),
('ST002', 'Midtown Market', 'Austin', 'South', '2021-07-15'),
('ST003', 'Lakeside Grocers', 'Dallas', 'North', '2019-11-20'),
('ST004', 'Riverside Basket', 'Houston', 'South', '2022-04-10'),
('ST005', 'Uptown Foods', 'Plano', 'North', '2023-09-18');

INSERT INTO public.grocery_suppliers (supplier_name, lead_time_days, contact_email) VALUES
('FarmLine Produce', 2, 'ops@farmline.example.com'),
('Texas Dairy Co', 1, 'supply@texasdairy.example.com'),
('Pantry Wholesale', 4, 'logistics@pantrywholesale.example.com'),
('FreshCatch Seafood', 2, 'support@freshcatch.example.com'),
('Baker''s Field', 1, 'dispatch@bakersfield.example.com'),
('Spark Beverages', 3, 'fulfillment@sparkbev.example.com');

INSERT INTO public.grocery_products (
    sku, product_name, category, unit_cost, unit_price, is_perishable, reorder_level, supplier_id
) VALUES
('APL-01', 'Apple Gala 1lb', 'produce', 1.20, 2.49, true, 40, 1),
('BAN-01', 'Banana Bunch', 'produce', 0.90, 1.99, true, 50, 1),
('ORG-01', 'Orange Bag 2lb', 'produce', 1.65, 3.49, true, 35, 1),
('TMT-01', 'Tomato 1lb', 'produce', 1.05, 2.29, true, 45, 1),
('POT-01', 'Potato 5lb', 'produce', 2.10, 4.99, false, 30, 1),
('MLK-01', 'Whole Milk 1L', 'dairy', 1.05, 2.39, true, 60, 2),
('MLK-02', 'Skim Milk 1L', 'dairy', 1.00, 2.29, true, 55, 2),
('EGG-12', 'Eggs 12ct', 'dairy', 1.80, 3.79, true, 45, 2),
('YGT-01', 'Greek Yogurt Cup', 'dairy', 0.75, 1.89, true, 65, 2),
('CHS-01', 'Cheddar Cheese 200g', 'dairy', 2.20, 4.69, true, 25, 2),
('BRD-01', 'Wheat Bread', 'bakery', 1.10, 2.99, true, 35, 5),
('BRD-02', 'Sourdough Loaf', 'bakery', 1.45, 3.49, true, 30, 5),
('MUF-06', 'Blueberry Muffin 6ct', 'bakery', 1.90, 4.29, true, 28, 5),
('PST-01', 'Pasta 500g', 'pantry', 0.70, 1.89, false, 80, 3),
('RCE-01', 'Rice 1kg', 'pantry', 1.40, 3.49, false, 70, 3),
('OIL-01', 'Olive Oil 500ml', 'pantry', 3.20, 6.99, false, 25, 3),
('CNL-01', 'Canned Beans 400g', 'pantry', 0.60, 1.49, false, 90, 3),
('SUG-01', 'Brown Sugar 1kg', 'pantry', 1.10, 2.79, false, 75, 3),
('SDA-01', 'Spark Soda 330ml', 'beverage', 0.35, 0.99, false, 120, 6),
('JCE-01', 'Orange Juice 1L', 'beverage', 1.25, 2.99, true, 50, 6),
('WTR-12', 'Mineral Water 12pk', 'beverage', 2.40, 5.99, false, 60, 6),
('ICE-01', 'Vanilla Ice Cream 500ml', 'frozen', 1.80, 4.49, true, 40, 3),
('PZA-01', 'Frozen Pizza', 'frozen', 2.70, 6.49, true, 38, 3),
('FSH-01', 'Salmon Fillet 300g', 'seafood', 4.80, 9.99, true, 20, 4);

-- Add synthetic catalog depth so product-heavy tables exceed 150+ rows.
INSERT INTO public.grocery_products (
    sku, product_name, category, unit_cost, unit_price, is_perishable, reorder_level, supplier_id
)
SELECT
    'GEN-' || LPAD(seq::text, 3, '0') AS sku,
    'Generated Product ' || seq::text AS product_name,
    CASE
        WHEN seq % 7 = 0 THEN 'seafood'
        WHEN seq % 7 = 1 THEN 'produce'
        WHEN seq % 7 = 2 THEN 'dairy'
        WHEN seq % 7 = 3 THEN 'bakery'
        WHEN seq % 7 = 4 THEN 'pantry'
        WHEN seq % 7 = 5 THEN 'beverage'
        ELSE 'frozen'
    END AS category,
    ROUND((0.65 + (seq % 35) * 0.18)::numeric, 2) AS unit_cost,
    ROUND((0.65 + (seq % 35) * 0.18 + 0.90 + (seq % 6) * 0.22)::numeric, 2) AS unit_price,
    CASE WHEN seq % 7 IN (4, 5) THEN false ELSE true END AS is_perishable,
    35 + (seq % 70) AS reorder_level,
    ((seq % 6) + 1) AS supplier_id
FROM generate_series(25, 220) AS seq;

-- 62-day inventory spine with deterministic variation.
WITH days AS (
    SELECT gs::date AS snapshot_date
    FROM generate_series('2025-12-15'::date, '2026-02-14'::date, interval '1 day') gs
),
base AS (
    SELECT
        d.snapshot_date,
        s.store_id,
        p.product_id,
        p.reorder_level,
        p.is_perishable,
        EXTRACT(DOY FROM d.snapshot_date)::int AS doy
    FROM days d
    CROSS JOIN public.grocery_stores s
    CROSS JOIN public.grocery_products p
)
INSERT INTO public.grocery_inventory_snapshots
(snapshot_date, store_id, product_id, on_hand_qty, reserved_qty)
SELECT
    snapshot_date,
    store_id,
    product_id,
    GREATEST(
        0,
        reorder_level + 30 + ((store_id * 17 + product_id * 9 + doy) % 95)
        - CASE WHEN is_perishable AND (doy % 11 = 0) THEN 20 ELSE 0 END
    ) AS on_hand_qty,
    ((store_id + product_id + doy) % 9) AS reserved_qty
FROM base;

-- Baseline sales: one transaction per store/product/day.
WITH days AS (
    SELECT gs::date AS business_date
    FROM generate_series('2025-12-15'::date, '2026-02-14'::date, interval '1 day') gs
),
base AS (
    SELECT
        d.business_date,
        s.store_id,
        p.product_id,
        p.unit_price,
        p.is_perishable,
        EXTRACT(DOY FROM d.business_date)::int AS doy,
        1 + ((s.store_id * 5 + p.product_id * 3 + EXTRACT(DOY FROM d.business_date)::int) % 6) AS quantity,
        CASE
            WHEN (s.store_id + p.product_id + EXTRACT(DOY FROM d.business_date)::int) % 5 = 0
                THEN ROUND((p.unit_price * 0.12)::numeric, 2)
            ELSE 0::numeric
        END AS discount_amount
    FROM days d
    CROSS JOIN public.grocery_stores s
    CROSS JOIN public.grocery_products p
)
INSERT INTO public.grocery_sales_transactions
(sold_at, business_date, store_id, product_id, quantity, unit_price, discount_amount, total_amount)
SELECT
    (
        business_date::timestamp
        + (((store_id * 37 + product_id * 11 + doy) % 12) + 8) * interval '1 hour'
        + (((store_id * 19 + product_id * 7 + doy) % 60)) * interval '1 minute'
    ) AS sold_at,
    business_date,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    ROUND((quantity * unit_price - discount_amount)::numeric, 2) AS total_amount
FROM base;

-- Promotional weekend uplift for selected products/stores.
WITH promo_days AS (
    SELECT gs::date AS business_date
    FROM generate_series('2025-12-15'::date, '2026-02-14'::date, interval '1 day') gs
    WHERE EXTRACT(DOW FROM gs) IN (5, 6)
),
promo_base AS (
    SELECT
        d.business_date,
        s.store_id,
        p.product_id,
        p.unit_price,
        EXTRACT(DOY FROM d.business_date)::int AS doy,
        3 + ((s.store_id + p.product_id + EXTRACT(DOY FROM d.business_date)::int) % 5) AS quantity,
        ROUND((p.unit_price * 0.20)::numeric, 2) AS discount_amount
    FROM promo_days d
    JOIN public.grocery_stores s
      ON (s.store_id + EXTRACT(DOY FROM d.business_date)::int) % 2 = 0
    JOIN public.grocery_products p
      ON p.is_perishable = true
     AND (p.product_id + s.store_id) % 4 = 0
)
INSERT INTO public.grocery_sales_transactions
(sold_at, business_date, store_id, product_id, quantity, unit_price, discount_amount, total_amount)
SELECT
    business_date::timestamp + interval '18 hours' + ((store_id + product_id) % 45) * interval '1 minute',
    business_date,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    ROUND((quantity * unit_price - discount_amount)::numeric, 2)
FROM promo_base;

-- Purchase order history with received/partial/in_transit states.
WITH cycle_days AS (
    SELECT gs::date AS ordered_day
    FROM generate_series('2025-12-10'::date, '2026-02-14'::date, interval '7 day') gs
),
po_base AS (
    SELECT
        c.ordered_day,
        s.store_id,
        p.product_id,
        p.supplier_id,
        sup.lead_time_days,
        p.unit_cost,
        p.reorder_level,
        EXTRACT(DOY FROM c.ordered_day)::int AS doy,
        ROW_NUMBER() OVER (ORDER BY c.ordered_day, s.store_id, p.product_id) AS seq
    FROM cycle_days c
    CROSS JOIN public.grocery_stores s
    JOIN public.grocery_products p
      ON (p.product_id + s.store_id + EXTRACT(DOY FROM c.ordered_day)::int) % 3 = 0
    JOIN public.grocery_suppliers sup
      ON sup.supplier_id = p.supplier_id
)
INSERT INTO public.grocery_purchase_orders
(ordered_at, expected_at, received_at, supplier_id, store_id, product_id, ordered_qty, received_qty, unit_cost, status)
SELECT
    ordered_day::timestamp + interval '6 hours' + (store_id % 3) * interval '30 minutes' AS ordered_at,
    ordered_day::timestamp + lead_time_days * interval '1 day' + interval '10 hours' AS expected_at,
    CASE
        WHEN ordered_day + lead_time_days <= '2026-02-10'::date
            THEN ordered_day::timestamp + lead_time_days * interval '1 day' + interval '11 hours'
                 + (seq % 25) * interval '1 minute'
        ELSE NULL
    END AS received_at,
    supplier_id,
    store_id,
    product_id,
    reorder_level + 25 + (doy % 40) AS ordered_qty,
    CASE
        WHEN ordered_day + lead_time_days > '2026-02-10'::date THEN NULL
        WHEN seq % 5 = 0 THEN (reorder_level + 25 + (doy % 40)) - (3 + (seq % 7))
        ELSE reorder_level + 25 + (doy % 40)
    END AS received_qty,
    unit_cost,
    CASE
        WHEN ordered_day + lead_time_days > '2026-02-10'::date THEN 'in_transit'
        WHEN seq % 5 = 0 THEN 'partial'
        ELSE 'received'
    END AS status
FROM po_base;

-- Waste events concentrated on perishables with reason distribution.
WITH waste_days AS (
    SELECT gs::date AS event_date
    FROM generate_series('2025-12-15'::date, '2026-02-14'::date, interval '3 day') gs
),
waste_base AS (
    SELECT
        d.event_date,
        s.store_id,
        p.product_id,
        p.unit_cost,
        EXTRACT(DOY FROM d.event_date)::int AS doy,
        1 + ((s.store_id + p.product_id + EXTRACT(DOY FROM d.event_date)::int) % 6) AS quantity,
        CASE
            WHEN (s.store_id + p.product_id + EXTRACT(DOY FROM d.event_date)::int) % 4 = 0 THEN 'expired'
            WHEN (s.store_id + p.product_id + EXTRACT(DOY FROM d.event_date)::int) % 4 = 1 THEN 'damaged shipment'
            WHEN (s.store_id + p.product_id + EXTRACT(DOY FROM d.event_date)::int) % 4 = 2 THEN 'temperature excursion'
            ELSE 'quality reject'
        END AS reason
    FROM waste_days d
    CROSS JOIN public.grocery_stores s
    JOIN public.grocery_products p
      ON p.is_perishable = true
     AND (s.store_id + p.product_id + EXTRACT(DOY FROM d.event_date)::int) % 5 = 0
)
INSERT INTO public.grocery_waste_events
(event_date, store_id, product_id, quantity, reason, estimated_cost)
SELECT
    event_date,
    store_id,
    product_id,
    quantity,
    reason,
    ROUND((quantity * unit_cost * 1.08)::numeric, 2) AS estimated_cost
FROM waste_base;

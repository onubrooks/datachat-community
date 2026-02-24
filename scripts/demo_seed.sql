-- Demo seed data for DataChat onboarding
-- Usage: psql "$DATABASE_URL" -f scripts/demo_seed.sql

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    amount NUMERIC(12,2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    order_date DATE NOT NULL DEFAULT CURRENT_DATE
);

INSERT INTO users (email, is_active)
VALUES
    ('alice@example.com', TRUE),
    ('bob@example.com', TRUE),
    ('charlie@example.com', FALSE)
ON CONFLICT DO NOTHING;

INSERT INTO orders (user_id, amount, status, order_date)
VALUES
    (1, 120.50, 'completed', CURRENT_DATE - INTERVAL '12 days'),
    (1, 75.00, 'completed', CURRENT_DATE - INTERVAL '6 days'),
    (2, 200.00, 'completed', CURRENT_DATE - INTERVAL '2 days'),
    (3, 15.00, 'refunded', CURRENT_DATE - INTERVAL '20 days')
ON CONFLICT DO NOTHING;

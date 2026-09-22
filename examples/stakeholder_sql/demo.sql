CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    region TEXT NOT NULL,
    segment TEXT NOT NULL,
    email TEXT NOT NULL
);
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL
);
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    order_date TEXT NOT NULL,
    status TEXT NOT NULL,
    gross_amount_usd REAL NOT NULL,
    refund_amount_usd REAL NOT NULL,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(product_id) REFERENCES products(product_id)
);
INSERT INTO customers VALUES
  (1, 'North', 'Small business', 'fictional1@example.invalid'),
  (2, 'South', 'Enterprise', 'fictional2@example.invalid');
INSERT INTO products VALUES (10, 'Notebook'), (20, 'Pen');
INSERT INTO orders VALUES
  (100, 1, 10, '2026-01-05', 'paid', 100.00, 0.00),
  (101, 1, 20, '2026-01-20', 'paid', 50.00, 10.00),
  (102, 2, 10, '2026-02-04', 'paid', 80.00, 20.00),
  (103, 2, 20, '2026-02-08', 'cancelled', 70.00, 0.00);

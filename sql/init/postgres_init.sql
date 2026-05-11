CREATE TABLE IF NOT EXISTS dbserver1_inventory_customers (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    gender TEXT,
    date_of_birth DATE,
    status TEXT,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS etl_job_log (
    id BIGSERIAL PRIMARY KEY,
    topic_name TEXT NOT NULL,
    operation CHAR(1) NOT NULL,
    customer_id INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL,
    error_message TEXT
);

DROP VIEW IF EXISTS dw_customers;

CREATE VIEW dw_customers AS
SELECT
    id AS customer_id,
    name AS full_name,
    email,
    phone,
    address,
    gender,
    date_of_birth,
    status,
    created_at AS source_created_at,
    updated_at AS source_updated_at,
    updated_at AS last_event_at
FROM dbserver1_inventory_customers;

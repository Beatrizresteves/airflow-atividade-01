CREATE TABLE IF NOT EXISTS category_metrics (
    category VARCHAR(255) PRIMARY KEY,
    avg_price NUMERIC(12, 2) NOT NULL,
    min_price NUMERIC(12, 2) NOT NULL,
    max_price NUMERIC(12, 2) NOT NULL,
    product_count INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS category_metrics_history (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    category VARCHAR(255) NOT NULL,
    avg_price NUMERIC(12, 2) NOT NULL,
    min_price NUMERIC(12, 2) NOT NULL,
    max_price NUMERIC(12, 2) NOT NULL,
    product_count INTEGER NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,

    UNIQUE (run_id, category)
);
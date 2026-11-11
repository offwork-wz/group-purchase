CREATE TABLE stock_restore_history (
    order_id VARCHAR(36) PRIMARY KEY,
    product_id VARCHAR(36) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

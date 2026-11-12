CREATE TABLE stock_deduction_history (
    participation_id VARCHAR(36) PRIMARY KEY,
    groupbuy_id VARCHAR(36) NOT NULL,
    product_id VARCHAR(36) NOT NULL,
    user_id VARCHAR(36) NOT NULL,
    quantity INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PROCESSING', 'SUCCESS', 'FAILED')),
    error_code VARCHAR(50),
    error_message TEXT,
    result_event_id VARCHAR(36) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (status = 'FAILED' AND error_code IS NOT NULL AND error_message IS NOT NULL)
        OR (status IN ('PROCESSING', 'SUCCESS') AND error_code IS NULL AND error_message IS NULL)
    ),
    CHECK (status <> 'SUCCESS' OR quantity > 0)
);

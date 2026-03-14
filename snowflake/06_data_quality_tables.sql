-- ============================================================
-- 06 — DATA_QUALITY Schema Tables
-- ============================================================
USE DATABASE ZIM_PROPERTY_DB;
USE SCHEMA DATA_QUALITY;

CREATE TABLE IF NOT EXISTS CHECK_RESULTS (
    check_id        INTEGER AUTOINCREMENT PRIMARY KEY,
    check_name      VARCHAR(100)    NOT NULL,
    description     VARCHAR(500),
    metric          FLOAT,
    threshold_json  VARCHAR(200),
    status          VARCHAR(20),    -- PASS | WARNING | CRITICAL
    run_date        DATE,
    checked_at      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Historical log of all data quality check results';

CREATE TABLE IF NOT EXISTS ANOMALY_FLAGS (
    flag_id         INTEGER AUTOINCREMENT PRIMARY KEY,
    listing_id      VARCHAR(16),
    flag_type       VARCHAR(50),    -- e.g. suspicious_price, invalid_suburb
    flag_detail     VARCHAR(500),
    flagged_at      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    resolved        BOOLEAN         DEFAULT FALSE
);

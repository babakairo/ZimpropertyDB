-- ============================================================
-- 11 — Price History Tracking Tables
-- Schema: ZIM_PROPERTY_DB.MASTER (append) + ANALYTICS (views)
-- Purpose: Capture every price change for each physical property
--          over time, enabling price-trend analysis, ROI modelling,
--          and anomaly alerts.
--
-- How it works:
--   1. After each nightly scrape, dbt model `int_price_changes`
--      compares current FACT_LISTINGS price against the last known
--      price in PROPERTY_PRICE_HISTORY.
--   2. Only rows where the price changed (or first appearance) are
--      inserted → append-only audit log.
--   3. Analytics views build time-series on top.
--
-- Tables created here:
--   MASTER.PROPERTY_PRICE_HISTORY     — one row per price event
--   MASTER.PRICE_ALERT_RULES          — threshold config for alerts
--   MASTER.PRICE_ALERTS               — triggered alert log
--
-- Views created here (in ANALYTICS):
--   ANALYTICS.V_PROPERTY_PRICE_TIMELINE
--   ANALYTICS.V_PRICE_DROP_OPPORTUNITIES
--   ANALYTICS.V_PRICE_CHANGE_VELOCITY
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE WAREHOUSE ZIM_PROPERTY_WH;
USE SCHEMA MASTER;


-- ════════════════════════════════════════════════════════════
-- A. PROPERTY_PRICE_HISTORY
-- ════════════════════════════════════════════════════════════
-- Append-only: one row inserted whenever price changes or
-- a property first appears.

CREATE TABLE IF NOT EXISTS PROPERTY_PRICE_HISTORY (

    -- Surrogate PK
    price_event_id          INTEGER         NOT NULL AUTOINCREMENT,

    -- Identity
    property_id             VARCHAR(36)     NOT NULL,   -- FK → PROPERTY_MASTER
    listing_id              VARCHAR(16),                -- the specific listing that reported this price

    -- Source context
    source                  VARCHAR(100),               -- which site reported this price
    listing_url             VARCHAR(2000),

    -- Price observation
    observed_date           DATE            NOT NULL,   -- date this price was scraped
    price_usd               NUMBER(18, 2),
    price_zwl               NUMBER(24, 2),
    currency_reported       VARCHAR(10),
    listing_type            VARCHAR(10),                -- sale | rent

    -- Change details (populated from dbt int_price_changes)
    prev_price_usd          NUMBER(18, 2),              -- last known price before this event
    prev_observed_date      DATE,
    price_change_usd        NUMBER(18, 2),              -- current - previous (signed)
    price_change_pct        FLOAT,                      -- signed % change
    days_since_prev         INTEGER,                    -- days between observations
    change_type             VARCHAR(20),                -- first_listing | price_increase | price_decrease | unchanged | relisted

    -- Property context at time of event (denormalised for fast history queries)
    suburb                  VARCHAR(100),
    city                    VARCHAR(100),
    property_type           VARCHAR(50),
    bedrooms                INTEGER,

    -- Audit
    inserted_at             TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    inserted_by             VARCHAR(100)    DEFAULT 'dbt:int_price_changes',

    CONSTRAINT pk_property_price_history PRIMARY KEY (price_event_id)
)
CLUSTER BY (property_id, observed_date)
DATA_RETENTION_TIME_IN_DAYS = 365
COMMENT = 'Append-only price change log — one row per price event per property';


-- ════════════════════════════════════════════════════════════
-- B. PRICE_ALERT_RULES
-- ════════════════════════════════════════════════════════════
-- User-configured price watching.  Dagster checks this table
-- nightly and inserts into PRICE_ALERTS when triggered.

CREATE TABLE IF NOT EXISTS PRICE_ALERT_RULES (

    rule_id                 INTEGER         NOT NULL AUTOINCREMENT,

    -- Scope: watch one property, a suburb, or a city-wide filter
    scope_type              VARCHAR(20)     NOT NULL,   -- property | suburb | city
    property_id             VARCHAR(36),                -- FK → PROPERTY_MASTER (if scope=property)
    suburb                  VARCHAR(100),
    city                    VARCHAR(100),
    property_type           VARCHAR(50),
    listing_type            VARCHAR(10),

    -- Trigger condition
    trigger_type            VARCHAR(30)     NOT NULL,
    -- price_drop_pct: alert when price drops ≥ N%
    -- price_drop_abs: alert when price drops ≥ $N
    -- new_listing: alert when any new listing matches scope
    -- price_above: alert when price rises above threshold
    -- price_below: alert when price falls below threshold
    threshold_value         FLOAT,                      -- % or $ depending on trigger_type

    -- Delivery
    notify_channel          VARCHAR(50)     DEFAULT 'log',  -- log | email | webhook
    notify_target           VARCHAR(500),               -- email address or webhook URL

    -- State
    is_active               BOOLEAN         DEFAULT TRUE,
    created_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    last_triggered_at       TIMESTAMP_TZ,

    CONSTRAINT pk_price_alert_rules PRIMARY KEY (rule_id)
)
COMMENT = 'User-configured price watch rules';


-- ════════════════════════════════════════════════════════════
-- C. PRICE_ALERTS  (triggered alerts log)
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS PRICE_ALERTS (

    alert_id                INTEGER         NOT NULL AUTOINCREMENT,
    rule_id                 INTEGER         NOT NULL,   -- FK → PRICE_ALERT_RULES

    -- Event that triggered the alert
    price_event_id          INTEGER,                    -- FK → PROPERTY_PRICE_HISTORY
    property_id             VARCHAR(36),
    property_address        VARCHAR(500),
    city                    VARCHAR(100),
    suburb                  VARCHAR(100),

    -- Values at trigger time
    previous_price_usd      NUMBER(18, 2),
    new_price_usd           NUMBER(18, 2),
    price_change_pct        FLOAT,
    listing_url             VARCHAR(2000),

    -- Delivery status
    triggered_at            TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    is_sent                 BOOLEAN         DEFAULT FALSE,
    sent_at                 TIMESTAMP_TZ,
    send_error              VARCHAR(2000),

    CONSTRAINT pk_price_alerts PRIMARY KEY (alert_id)
)
COMMENT = 'Log of triggered price alerts';


-- ════════════════════════════════════════════════════════════
-- D. ANALYTICS VIEWS — price timeline and opportunities
-- ════════════════════════════════════════════════════════════

USE SCHEMA ANALYTICS;

-- ── V_PROPERTY_PRICE_TIMELINE ─────────────────────────────────
-- Full price timeline for any property, ordered by date.
CREATE OR REPLACE VIEW V_PROPERTY_PRICE_TIMELINE AS
    SELECT
        pph.property_id,
        pm.canonical_address,
        pm.suburb,
        pm.city,
        pm.property_type,
        pph.source,
        pph.listing_type,
        pph.observed_date,
        pph.price_usd,
        pph.prev_price_usd,
        pph.price_change_usd,
        pph.price_change_pct,
        pph.days_since_prev,
        pph.change_type,
        pph.bedrooms,

        -- Running stats
        MIN(pph.price_usd) OVER (PARTITION BY pph.property_id)   AS all_time_low_usd,
        MAX(pph.price_usd) OVER (PARTITION BY pph.property_id)   AS all_time_high_usd,
        FIRST_VALUE(pph.price_usd) OVER (
            PARTITION BY pph.property_id ORDER BY pph.observed_date
        )                                                         AS original_price_usd,
        LAST_VALUE(pph.price_usd) OVER (
            PARTITION BY pph.property_id ORDER BY pph.observed_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                                         AS current_price_usd
    FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_PRICE_HISTORY pph
    JOIN ZIM_PROPERTY_DB.MASTER.PROPERTY_MASTER pm
        ON pm.property_id = pph.property_id
    WHERE pph.price_usd IS NOT NULL
    ORDER BY pph.property_id, pph.observed_date;


-- ── V_PRICE_DROP_OPPORTUNITIES ────────────────────────────────
-- Properties that have dropped ≥ 10% from their peak price
-- and are still actively listed — potential buying opportunities.
CREATE OR REPLACE VIEW V_PRICE_DROP_OPPORTUNITIES AS
WITH latest AS (
    SELECT DISTINCT ON (property_id)
        property_id,
        price_usd           AS current_price,
        observed_date       AS last_seen_date,
        listing_type,
        source,
        listing_url
    FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_PRICE_HISTORY
    ORDER BY property_id, observed_date DESC
),
peaks AS (
    SELECT
        property_id,
        MAX(price_usd)      AS peak_price,
        MIN(observed_date)  AS first_listed_date,
        COUNT(*)            AS price_observations
    FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_PRICE_HISTORY
    GROUP BY property_id
)
SELECT
    l.property_id,
    pm.canonical_address,
    pm.suburb,
    pm.city,
    pm.property_type,
    pm.bedrooms_canonical   AS bedrooms,
    l.listing_type,
    l.source,
    l.listing_url,
    p.peak_price            AS peak_price_usd,
    l.current_price         AS current_price_usd,
    ROUND((l.current_price - p.peak_price) / p.peak_price * 100, 1) AS drop_from_peak_pct,
    p.first_listed_date,
    l.last_seen_date,
    DATEDIFF('day', p.first_listed_date, l.last_seen_date)  AS days_on_market,
    p.price_observations
FROM latest l
JOIN peaks p  ON p.property_id = l.property_id
JOIN ZIM_PROPERTY_DB.MASTER.PROPERTY_MASTER pm ON pm.property_id = l.property_id
WHERE l.current_price < p.peak_price * 0.90     -- ≥ 10% below peak
  AND pm.is_currently_active = TRUE
  AND l.listing_type = 'sale'
ORDER BY drop_from_peak_pct ASC;               -- biggest drops first


-- ── V_PRICE_CHANGE_VELOCITY ───────────────────────────────────
-- Suburbs ranked by average price change over the past 90 days.
CREATE OR REPLACE VIEW V_PRICE_CHANGE_VELOCITY AS
WITH recent_changes AS (
    SELECT
        pm.suburb,
        pm.city,
        pm.property_type,
        pph.listing_type,
        pph.price_change_pct,
        pph.observed_date
    FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_PRICE_HISTORY pph
    JOIN ZIM_PROPERTY_DB.MASTER.PROPERTY_MASTER pm ON pm.property_id = pph.property_id
    WHERE pph.observed_date >= DATEADD('day', -90, CURRENT_DATE())
      AND pph.change_type IN ('price_increase', 'price_decrease')
      AND pm.suburb IS NOT NULL
)
SELECT
    suburb,
    city,
    property_type,
    listing_type,
    COUNT(*)                        AS price_changes_90d,
    AVG(price_change_pct)           AS avg_change_pct_90d,
    SUM(CASE WHEN price_change_pct > 0 THEN 1 ELSE 0 END) AS increases,
    SUM(CASE WHEN price_change_pct < 0 THEN 1 ELSE 0 END) AS decreases,
    CURRENT_DATE()                  AS as_of_date
FROM recent_changes
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) >= 3               -- at least 3 price events for meaningful signal
ORDER BY avg_change_pct_90d DESC;


-- ════════════════════════════════════════════════════════════
-- E. GRANTS
-- ════════════════════════════════════════════════════════════
USE SCHEMA MASTER;
GRANT SELECT ON ALL TABLES IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_DBT_ROLE;

USE SCHEMA ANALYTICS;
GRANT SELECT ON ALL VIEWS IN SCHEMA ZIM_PROPERTY_DB.ANALYTICS TO ROLE ZIM_ANALYST_ROLE;


-- ════════════════════════════════════════════════════════════
-- VERIFICATION
-- ════════════════════════════════════════════════════════════
/*
-- Check table creation
SELECT table_name, row_count
FROM information_schema.tables
WHERE table_schema = 'MASTER'
  AND table_name IN ('PROPERTY_PRICE_HISTORY', 'PRICE_ALERT_RULES', 'PRICE_ALERTS')
ORDER BY table_name;

-- After first dbt run, check price events
SELECT change_type, COUNT(*) FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_PRICE_HISTORY
GROUP BY 1 ORDER BY 2 DESC;
*/

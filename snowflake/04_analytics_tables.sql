-- ============================================================
-- 04 — ANALYTICS Layer Table / View Definitions
-- Schema: ZIM_PROPERTY_DB.ANALYTICS
-- Purpose: Pre-aggregated marts for BI tools and dashboards.
--          All managed by dbt.
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ZIM_PROPERTY_WH;

-- ──────────────────────────────────────────────────────────
-- MART 1: Property Price by Suburb
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS PROPERTY_PRICE_BY_SUBURB (
    suburb_clean            VARCHAR(100)    NOT NULL,
    city_clean              VARCHAR(100)    NOT NULL,
    property_type           VARCHAR(50),
    listing_type            VARCHAR(10),

    listing_count           INTEGER,
    avg_price_usd           FLOAT,
    median_price_usd        FLOAT,
    min_price_usd           FLOAT,
    max_price_usd           FLOAT,
    stddev_price_usd        FLOAT,

    avg_price_per_sqm_usd   FLOAT,
    avg_bedrooms            FLOAT,
    avg_property_size_sqm   FLOAT,

    snapshot_month          DATE,           -- first day of the month
    dbt_updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_price_suburb PRIMARY KEY (suburb_clean, city_clean, property_type, listing_type, snapshot_month)
)
COMMENT = 'Monthly average property prices aggregated by suburb';

-- ──────────────────────────────────────────────────────────
-- MART 2: Property Price by City
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS PROPERTY_PRICE_BY_CITY (
    city_clean              VARCHAR(100)    NOT NULL,
    property_type           VARCHAR(50),
    listing_type            VARCHAR(10),

    listing_count           INTEGER,
    avg_price_usd           FLOAT,
    median_price_usd        FLOAT,
    avg_rent_usd            FLOAT,          -- listing_type = 'rent'
    avg_sale_price_usd      FLOAT,          -- listing_type = 'sale'

    avg_price_per_sqm_usd   FLOAT,
    avg_bedrooms            FLOAT,

    snapshot_month          DATE,
    dbt_updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_price_city PRIMARY KEY (city_clean, property_type, listing_type, snapshot_month)
)
COMMENT = 'Monthly property price aggregates by city';

-- ──────────────────────────────────────────────────────────
-- MART 3: Average Price by Bedroom Count
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS AVERAGE_PRICE_BY_BEDROOM (
    city_clean              VARCHAR(100)    NOT NULL,
    suburb_clean            VARCHAR(100),
    number_of_bedrooms      INTEGER         NOT NULL,
    property_type           VARCHAR(50),
    listing_type            VARCHAR(10),

    listing_count           INTEGER,
    avg_price_usd           FLOAT,
    median_price_usd        FLOAT,
    avg_price_per_sqm_usd   FLOAT,

    snapshot_month          DATE,
    dbt_updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_price_bedroom PRIMARY KEY (city_clean, number_of_bedrooms, property_type, listing_type, snapshot_month)
)
COMMENT = 'Price analysis broken down by number of bedrooms';

-- ──────────────────────────────────────────────────────────
-- MART 4: Monthly Price Trends
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS MONTHLY_PRICE_TRENDS (
    trend_month             DATE            NOT NULL,
    city_clean              VARCHAR(100)    NOT NULL,
    property_type           VARCHAR(50),
    listing_type            VARCHAR(10),

    listing_count           INTEGER,
    avg_price_usd           FLOAT,
    mom_price_change_pct    FLOAT,          -- month-over-month % change
    yoy_price_change_pct    FLOAT,          -- year-over-year % change
    rolling_3m_avg_usd      FLOAT,
    rolling_6m_avg_usd      FLOAT,

    new_listings_count      INTEGER,
    total_active_listings   INTEGER,

    dbt_updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_monthly_trends PRIMARY KEY (trend_month, city_clean, property_type, listing_type)
)
COMMENT = 'Monthly price trend with MoM and YoY change metrics';

-- ──────────────────────────────────────────────────────────
-- MART 5: Top Suburbs by Price Growth
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUBURB_PRICE_GROWTH (
    suburb_clean            VARCHAR(100)    NOT NULL,
    city_clean              VARCHAR(100)    NOT NULL,
    property_type           VARCHAR(50),

    avg_price_current_month_usd     FLOAT,
    avg_price_3m_ago_usd            FLOAT,
    avg_price_6m_ago_usd            FLOAT,
    avg_price_12m_ago_usd           FLOAT,

    growth_3m_pct           FLOAT,
    growth_6m_pct           FLOAT,
    growth_12m_pct          FLOAT,

    listing_count_current   INTEGER,
    rank_by_growth_12m      INTEGER,        -- 1 = fastest growing suburb

    snapshot_date           DATE            NOT NULL,
    dbt_updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_suburb_growth PRIMARY KEY (suburb_clean, city_clean, property_type, snapshot_date)
)
COMMENT = 'Suburb-level price growth rankings';

-- ──────────────────────────────────────────────────────────
-- VIEW: Investment Dashboard Summary
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW INVESTMENT_DASHBOARD_SUMMARY AS
    SELECT
        s.suburb_clean,
        s.city_clean,
        s.property_type,
        s.avg_price_usd                 AS current_avg_price,
        s.listing_count,
        g.growth_12m_pct,
        g.rank_by_growth_12m            AS growth_rank,
        s.avg_price_per_sqm_usd,
        s.avg_bedrooms,
        s.snapshot_month
    FROM PROPERTY_PRICE_BY_SUBURB s
    LEFT JOIN SUBURB_PRICE_GROWTH g
        ON s.suburb_clean = g.suburb_clean
        AND s.city_clean  = g.city_clean
        AND s.property_type = g.property_type
        AND s.snapshot_month = DATE_TRUNC('MONTH', g.snapshot_date)
    WHERE s.snapshot_month = DATE_TRUNC('MONTH', CURRENT_DATE())
    ORDER BY g.growth_12m_pct DESC NULLS LAST;

-- ──────────────────────────────────────────────────────────
-- VIEW: Rental Yield Estimate
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW RENTAL_YIELD_BY_SUBURB AS
    SELECT
        sale.suburb_clean,
        sale.city_clean,
        sale.property_type,
        sale.avg_price_usd              AS avg_sale_price,
        rent.avg_price_usd              AS avg_monthly_rent,
        rent.avg_price_usd * 12         AS annual_rent,
        CASE
            WHEN sale.avg_price_usd > 0
            THEN ROUND((rent.avg_price_usd * 12 / sale.avg_price_usd) * 100, 2)
        END                             AS gross_rental_yield_pct,
        sale.snapshot_month
    FROM PROPERTY_PRICE_BY_SUBURB sale
    JOIN PROPERTY_PRICE_BY_SUBURB rent
        ON  sale.suburb_clean   = rent.suburb_clean
        AND sale.city_clean     = rent.city_clean
        AND sale.property_type  = rent.property_type
        AND sale.snapshot_month = rent.snapshot_month
    WHERE sale.listing_type = 'sale'
      AND rent.listing_type = 'rent'
      AND sale.snapshot_month = DATE_TRUNC('MONTH', CURRENT_DATE());

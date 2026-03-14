-- ============================================================
-- 03 — STAGING Layer Table Definitions
-- Schema: ZIM_PROPERTY_DB.STAGING
-- Purpose: Cleaned, validated, and standardised data.
--          Managed by dbt — do not modify manually.
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE SCHEMA STAGING;
USE WAREHOUSE ZIM_PROPERTY_WH;

-- ── Cleaned listings (dbt-managed, defined here as reference) ─────────────
-- NOTE: dbt creates this via `dbt run`. This DDL is for documentation.
CREATE TABLE IF NOT EXISTS CLEANED_PROPERTY_LISTINGS (

    listing_id              VARCHAR(16)  NOT NULL,
    source                  VARCHAR(100) NOT NULL,
    listing_url             VARCHAR(2000),

    property_title          VARCHAR(500),
    property_price_usd      NUMBER(18, 2),          -- normalised to USD
    property_price_zwl      NUMBER(24, 2),          -- original ZWL if available
    currency_original       VARCHAR(10),
    exchange_rate_used      FLOAT,                  -- rate applied to convert to USD

    property_type           VARCHAR(50),
    listing_type            VARCHAR(10),

    city_clean              VARCHAR(100),
    suburb_clean            VARCHAR(100),
    address_raw             VARCHAR(500),
    latitude                FLOAT,
    longitude               FLOAT,

    number_of_bedrooms      INTEGER,
    number_of_bathrooms     INTEGER,
    number_of_garages       INTEGER,
    property_size_sqm       FLOAT,
    stand_size_sqm          FLOAT,
    price_per_sqm_usd       FLOAT,                  -- derived

    features                VARIANT,
    feature_count           INTEGER,
    has_pool                BOOLEAN,
    has_borehole            BOOLEAN,
    has_solar               BOOLEAN,
    has_garage              BOOLEAN,

    agent_name              VARCHAR(200),
    agent_phone             VARCHAR(50),
    agent_email             VARCHAR(200),
    agency_name             VARCHAR(200),

    image_count             INTEGER,
    image_urls              VARIANT,

    listing_date            DATE,
    listing_year            INTEGER,
    listing_month           INTEGER,
    listing_quarter         INTEGER,

    scraped_at              TIMESTAMP_TZ,
    dbt_updated_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),

    is_price_valid          BOOLEAN,
    is_location_valid       BOOLEAN,
    data_quality_score      FLOAT,                  -- 0–1 completeness score

    CONSTRAINT pk_cleaned_property_listings PRIMARY KEY (listing_id)
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Cleaned and enriched property listings managed by dbt';

-- ── Exchange rates reference table ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ZWL_USD_EXCHANGE_RATES (
    rate_date           DATE            NOT NULL,
    zwl_per_usd         FLOAT           NOT NULL,
    source              VARCHAR(100),
    loaded_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_exchange_rates PRIMARY KEY (rate_date)
)
COMMENT = 'Daily ZWL/USD exchange rates for currency normalisation';

-- ── Seed data: known Zimbabwe suburbs ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS ZW_SUBURBS_REFERENCE (
    suburb_id           INTEGER         AUTOINCREMENT PRIMARY KEY,
    suburb_name         VARCHAR(100)    NOT NULL,
    suburb_name_clean   VARCHAR(100)    NOT NULL,
    city                VARCHAR(100)    NOT NULL,
    province            VARCHAR(100),
    latitude_approx     FLOAT,
    longitude_approx    FLOAT,
    CONSTRAINT uq_suburb_city UNIQUE (suburb_name_clean, city)
)
COMMENT = 'Reference table of known Zimbabwe suburbs for location validation';

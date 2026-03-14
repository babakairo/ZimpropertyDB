-- ============================================================
-- 02 — RAW Layer Table Definitions
-- Schema: ZIM_PROPERTY_DB.RAW
-- Purpose: Immutable landing zone. Data is NEVER updated here
--          except by the MERGE dedup key on listing_id.
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE SCHEMA RAW;
USE WAREHOUSE ZIM_PROPERTY_WH;

-- ── Main listings table ────────────────────────────────────
CREATE TABLE IF NOT EXISTS ZW_PROPERTY_LISTINGS (

    -- Surrogate key
    listing_id          VARCHAR(16)     NOT NULL,   -- SHA-256 truncated to 16 hex chars

    -- Source tracking
    source              VARCHAR(100)    NOT NULL,   -- e.g. "property.co.zw"
    listing_url         VARCHAR(2000)   NOT NULL,

    -- Core listing fields
    property_title      VARCHAR(500),
    property_price      NUMBER(18, 2),
    currency            VARCHAR(10),                -- USD | ZWL | ZIG
    property_type       VARCHAR(50),                -- house | flat | land | commercial | farm
    listing_type        VARCHAR(10),                -- sale | rent

    -- Location
    city                VARCHAR(100),
    suburb              VARCHAR(100),
    address_raw         VARCHAR(500),
    latitude            FLOAT,
    longitude           FLOAT,

    -- Attributes
    number_of_bedrooms  INTEGER,
    number_of_bathrooms INTEGER,
    number_of_garages   INTEGER,
    property_size_sqm   FLOAT,
    property_size_raw   VARCHAR(100),
    stand_size_sqm      FLOAT,
    features            VARIANT,                    -- JSON array of strings

    -- Agent
    agent_name          VARCHAR(200),
    agent_phone         VARCHAR(50),
    agent_email         VARCHAR(200),
    agency_name         VARCHAR(200),

    -- Media
    image_urls          VARIANT,                    -- JSON array of URLs

    -- Timestamps
    listing_date        DATE,
    scraped_at          TIMESTAMP_TZ,
    loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT pk_zw_property_listings PRIMARY KEY (listing_id)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw scraped property listings from Zimbabwe property portals';

-- ── Scrape run audit table ─────────────────────────────────
CREATE TABLE IF NOT EXISTS SCRAPE_RUNS (
    run_id              VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    spider_name         VARCHAR(100),
    source              VARCHAR(100),
    started_at          TIMESTAMP_TZ,
    finished_at         TIMESTAMP_TZ,
    items_scraped       INTEGER         DEFAULT 0,
    items_dropped       INTEGER         DEFAULT 0,
    items_loaded        INTEGER         DEFAULT 0,
    status              VARCHAR(20),    -- running | success | failed
    error_message       VARCHAR(2000),
    CONSTRAINT pk_scrape_runs PRIMARY KEY (run_id)
)
COMMENT = 'Audit trail for every spider run';

-- ── Indexes via clustering key (Snowflake uses clustering, not indexes) ────
ALTER TABLE ZW_PROPERTY_LISTINGS CLUSTER BY (source, city, scraped_at::DATE);

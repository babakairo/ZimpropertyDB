-- ============================================================
-- 09 — WAREHOUSE Star Schema
-- Schema: ZIM_PROPERTY_DB.WAREHOUSE
-- Purpose: Kimball-style dimensional model for BI queries.
--          Dimension tables are slowly-changing (SCD Type 1).
--          FACT_LISTINGS is the central fact table.
--
-- Run order: 01 → 02 → 03 → ... → 09 (this file)
-- Prerequisites: STAGING.CLEANED_PROPERTY_LISTINGS must exist
--
-- Sections:
--   A. Schema + role setup
--   B. Dimension tables (DIM_DATE, DIM_LOCATION, DIM_PROPERTY_TYPE,
--                        DIM_SOURCE, DIM_PROPERTY)
--   C. Fact table (FACT_LISTINGS)
--   D. Seed data for static dimensions
--   E. ETL: populate dimensions from STAGING
--   F. ETL: populate FACT_LISTINGS from STAGING
--   G. Grants
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE WAREHOUSE ZIM_PROPERTY_WH;

-- ── A. Schema + role ─────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ZIM_PROPERTY_DB.WAREHOUSE
    COMMENT = 'Kimball star schema — dimensional model for BI and analytics';

USE SCHEMA WAREHOUSE;

-- Grant dbt role full access
GRANT ALL ON SCHEMA ZIM_PROPERTY_DB.WAREHOUSE TO ROLE ZIM_DBT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ZIM_PROPERTY_DB.WAREHOUSE TO ROLE ZIM_DBT_ROLE;

-- Grant analyst read access
GRANT USAGE  ON SCHEMA ZIM_PROPERTY_DB.WAREHOUSE TO ROLE ZIM_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ZIM_PROPERTY_DB.WAREHOUSE TO ROLE ZIM_ANALYST_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA ZIM_PROPERTY_DB.WAREHOUSE TO ROLE ZIM_ANALYST_ROLE;


-- ════════════════════════════════════════════════════════════
-- B. DIMENSION TABLES
-- ════════════════════════════════════════════════════════════

-- ── DIM_DATE ─────────────────────────────────────────────────
-- Pre-populated calendar dimension (2020-01-01 → 2030-12-31).
-- No FK from fact — joins on CALENDAR_DATE directly (faster).
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key            INTEGER         NOT NULL,   -- YYYYMMDD integer, e.g. 20240315
    calendar_date       DATE            NOT NULL,

    -- Calendar attributes
    day_of_week         INTEGER,                    -- 0=Sun … 6=Sat
    day_name            VARCHAR(10),                -- Monday … Sunday
    day_of_month        INTEGER,
    day_of_year         INTEGER,
    week_of_year        INTEGER,
    month_number        INTEGER,
    month_name          VARCHAR(10),
    quarter_number      INTEGER,                    -- 1–4
    year_number         INTEGER,

    -- Business flags
    is_weekend          BOOLEAN,
    is_month_start      BOOLEAN,
    is_month_end        BOOLEAN,
    is_quarter_start    BOOLEAN,
    is_quarter_end      BOOLEAN,
    is_year_start       BOOLEAN,
    is_year_end         BOOLEAN,

    -- Derived period keys (for easy aggregation)
    year_month          VARCHAR(7),                 -- '2024-03'
    year_quarter        VARCHAR(7),                 -- '2024-Q1'

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
COMMENT = 'Pre-populated calendar dimension 2020–2030';


-- ── DIM_LOCATION ─────────────────────────────────────────────
-- One row per unique (suburb, city) combination.
-- Updated nightly by dbt from STAGING.CLEANED_PROPERTY_LISTINGS.
CREATE TABLE IF NOT EXISTS DIM_LOCATION (
    location_key        INTEGER         NOT NULL AUTOINCREMENT,
    suburb_clean        VARCHAR(100),
    city_clean          VARCHAR(100)    NOT NULL,
    province            VARCHAR(100),               -- Harare Metro, Matebeleland, etc.
    country             VARCHAR(10)     DEFAULT 'ZW',

    -- Geo centroid (approx, from STAGING.ZW_SUBURBS_REFERENCE if available)
    latitude_approx     FLOAT,
    longitude_approx    FLOAT,
    geohash_5           VARCHAR(10),                -- ~4.9km city-level block
    geohash_7           VARCHAR(10),                -- ~153m suburb-level block

    -- Audit
    first_seen_date     DATE,
    dbt_updated_at      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_dim_location PRIMARY KEY (location_key),
    CONSTRAINT uq_dim_location UNIQUE (suburb_clean, city_clean)
)
COMMENT = 'Location dimension: suburb + city combinations';


-- ── DIM_PROPERTY_TYPE ────────────────────────────────────────
-- Static lookup seeded below. Rarely changes.
CREATE TABLE IF NOT EXISTS DIM_PROPERTY_TYPE (
    property_type_key   INTEGER         NOT NULL AUTOINCREMENT,
    property_type       VARCHAR(50)     NOT NULL,   -- canonical code: house, flat, etc.
    property_type_label VARCHAR(100),               -- display label
    is_residential      BOOLEAN         DEFAULT TRUE,
    is_commercial       BOOLEAN         DEFAULT FALSE,
    is_land             BOOLEAN         DEFAULT FALSE,
    sort_order          INTEGER,

    CONSTRAINT pk_dim_property_type PRIMARY KEY (property_type_key),
    CONSTRAINT uq_dim_property_type UNIQUE (property_type)
)
COMMENT = 'Property type lookup — seeded with canonical types';


-- ── DIM_SOURCE ───────────────────────────────────────────────
-- One row per scraper source domain. Seeded below.
CREATE TABLE IF NOT EXISTS DIM_SOURCE (
    source_key          INTEGER         NOT NULL AUTOINCREMENT,
    source              VARCHAR(100)    NOT NULL,   -- e.g. "property.co.zw"
    source_label        VARCHAR(200),               -- display name
    source_url          VARCHAR(500),               -- homepage
    is_active           BOOLEAN         DEFAULT TRUE,
    first_scraped_date  DATE,
    last_scraped_date   DATE,

    CONSTRAINT pk_dim_source PRIMARY KEY (source_key),
    CONSTRAINT uq_dim_source UNIQUE (source)
)
COMMENT = 'Data source dimension — one row per scraper domain';


-- ── DIM_PROPERTY ─────────────────────────────────────────────
-- One row per resolved physical property (from PROPERTY_MASTER).
-- Populated by the matching engine; linked to FACT_LISTINGS via property_key.
-- This is a "degenerate" dimension — it holds identity not attributes.
CREATE TABLE IF NOT EXISTS DIM_PROPERTY (
    property_key        INTEGER         NOT NULL AUTOINCREMENT,
    master_property_id  VARCHAR(36),                -- FK → MASTER.PROPERTY_MASTER.property_id
    canonical_address   VARCHAR(500),
    address_fingerprint VARCHAR(500),               -- sorted-token address for fuzzy matching
    address_hash        VARCHAR(16),                -- first 16 chars of SHA-256 of fingerprint
    geohash_8           VARCHAR(10),                -- ~38m building-level block
    location_key        INTEGER,                    -- FK → DIM_LOCATION
    first_listing_date  DATE,
    last_listing_date   DATE,
    total_listing_count INTEGER         DEFAULT 0,

    CONSTRAINT pk_dim_property PRIMARY KEY (property_key)
)
COMMENT = 'Physical property identity dimension — one row per real-world property';


-- ════════════════════════════════════════════════════════════
-- C. FACT TABLE — FACT_LISTINGS
-- ════════════════════════════════════════════════════════════

-- Central fact table: one row per scraped listing observation.
-- Grain: one listing_id on one scraped_at date.
CREATE TABLE IF NOT EXISTS FACT_LISTINGS (
    -- Surrogate fact key
    fact_id             INTEGER         NOT NULL AUTOINCREMENT,

    -- Natural / business key (from RAW + STAGING)
    listing_id          VARCHAR(16)     NOT NULL,   -- SHA-256[:16] of source+url

    -- Dimension foreign keys
    date_key            INTEGER,                    -- FK → DIM_DATE (scraped date)
    listing_date_key    INTEGER,                    -- FK → DIM_DATE (listing_date)
    location_key        INTEGER,                    -- FK → DIM_LOCATION
    property_type_key   INTEGER,                    -- FK → DIM_PROPERTY_TYPE
    source_key          INTEGER,                    -- FK → DIM_SOURCE
    property_key        INTEGER,                    -- FK → DIM_PROPERTY (nullable until matched)

    -- Degenerate dimensions (low cardinality, stored inline)
    listing_type        VARCHAR(10),                -- sale | rent
    currency_original   VARCHAR(10),

    -- Measures — price
    property_price_usd  NUMBER(18, 2),
    property_price_zwl  NUMBER(24, 2),
    exchange_rate_used  FLOAT,
    price_per_sqm_usd   FLOAT,

    -- Measures — size
    property_size_sqm   FLOAT,
    stand_size_sqm      FLOAT,

    -- Measures — attributes (semi-additive)
    number_of_bedrooms  INTEGER,
    number_of_bathrooms INTEGER,
    number_of_garages   INTEGER,
    feature_count       INTEGER,

    -- Feature flags
    has_pool            BOOLEAN,
    has_borehole        BOOLEAN,
    has_solar           BOOLEAN,
    has_garage          BOOLEAN,

    -- Media
    image_count         INTEGER,

    -- Data quality score (0–1)
    data_quality_score  FLOAT,
    is_price_valid      BOOLEAN,
    is_location_valid   BOOLEAN,

    -- Audit timestamps
    scraped_at          TIMESTAMP_TZ,
    loaded_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_fact_listings PRIMARY KEY (fact_id),
    CONSTRAINT uq_fact_listing_id UNIQUE (listing_id)
)
CLUSTER BY (date_key, location_key, property_type_key)
COMMENT = 'Central fact table — one row per scraped listing observation';


-- ════════════════════════════════════════════════════════════
-- D. SEED DATA — static dimensions
-- ════════════════════════════════════════════════════════════

-- ── DIM_PROPERTY_TYPE seed ───────────────────────────────────
INSERT INTO DIM_PROPERTY_TYPE (property_type, property_type_label, is_residential, is_commercial, is_land, sort_order)
SELECT * FROM VALUES
    ('house',      'House / Residential',    TRUE,  FALSE, FALSE, 1),
    ('flat',       'Flat / Apartment',        TRUE,  FALSE, FALSE, 2),
    ('townhouse',  'Townhouse / Cluster',     TRUE,  FALSE, FALSE, 3),
    ('commercial', 'Commercial Property',     FALSE, TRUE,  FALSE, 4),
    ('land',       'Land / Stand / Plot',     FALSE, FALSE, TRUE,  5),
    ('farm',       'Farm / Agricultural',     FALSE, FALSE, TRUE,  6),
    ('other',      'Other',                   FALSE, FALSE, FALSE, 99)
AS v(property_type, property_type_label, is_residential, is_commercial, is_land, sort_order)
WHERE NOT EXISTS (SELECT 1 FROM DIM_PROPERTY_TYPE WHERE property_type = v.property_type);


-- ── DIM_SOURCE seed ──────────────────────────────────────────
INSERT INTO DIM_SOURCE (source, source_label, source_url, is_active)
SELECT * FROM VALUES
    -- Major portals
    ('property.co.zw',              'Property.co.zw',               'https://www.property.co.zw',               TRUE),
    ('classifieds.co.zw',           'Classifieds.co.zw',            'https://www.classifieds.co.zw',            TRUE),
    ('propertybook.co.zw',          'PropertyBook',                 'https://www.propertybook.co.zw',           TRUE),
    ('realtorville.co.zw',          'Realtorville',                 'https://www.realtorville.co.zw',           TRUE),
    -- Secondary portals
    ('propsearch.co.zw',            'PropSearch Zimbabwe',          'https://www.propsearch.co.zw',             TRUE),
    ('stands.co.zw',                'Stands.co.zw',                 'https://www.stands.co.zw',                 TRUE),
    ('shonahome.com',               'ShonaHome',                    'https://www.shonahome.com',                TRUE),
    ('privatepropertyzimbabwe.com', 'Private Property Zimbabwe',    'https://www.privatepropertyzimbabwe.com',  TRUE),
    -- PropData platform agencies
    ('guestandtanner.co.zw',        'Guest & Tanner',               'https://www.guestandtanner.co.zw',         TRUE),
    ('seeff.co.zw',                 'Seeff Zimbabwe',               'https://www.seeff.co.zw',                  TRUE),
    ('kennanproperties.co.zw',      'Kennan Properties',            'https://www.kennanproperties.co.zw',       TRUE),
    ('zimproperties.com',           'Zim Properties',               'https://www.zimproperties.com',            TRUE),
    ('faranani.co.zw',              'Faranani Properties',          'https://www.faranani.co.zw',               TRUE),
    ('harareproperties.co.zw',      'Harare Properties',            'https://www.harareproperties.co.zw',       TRUE),
    -- International / franchise agencies
    ('knightfrank.co.zw',           'Knight Frank Zimbabwe',        'https://www.knightfrank.co.zw',            TRUE),
    ('pamgolding.co.zw',            'Pam Golding Zimbabwe',         'https://www.pamgolding.co.zw',             TRUE),
    ('pamgoldingzimbabwe.co.zw',    'Pam Golding Zimbabwe (ZW)',    'https://www.pamgoldingzimbabwe.co.zw',     TRUE),
    ('api.co.zw',                   'API Zimbabwe',                 'https://www.api.co.zw',                    TRUE),
    ('fineandcountry.co.zw',        'Fine & Country Zimbabwe',      'https://www.fineandcountry.co.zw',         TRUE),
    ('rawsonproperties.com',        'Rawson Properties Zimbabwe',   'https://www.rawsonproperties.com',         TRUE),
    ('rawson.co.zw',                'Rawson Zimbabwe',              'https://www.rawson.co.zw',                 TRUE),
    ('century21.co.zw',             'Century 21 Zimbabwe',          'https://www.century21.co.zw',              TRUE),
    ('integratedproperties.co.zw',  'Integrated Properties',        'https://www.integratedproperties.co.zw',   TRUE),
    ('remax.co.zw',                 'RE/MAX Zimbabwe',              'https://www.remax.co.zw',                  TRUE),
    -- Local agencies
    ('robertroot.co.zw',            'Robert Root',                  'https://www.robertroot.co.zw',             TRUE),
    ('stonebridge.co.zw',           'Stonebridge Real Estate',      'https://www.stonebridge.co.zw',            TRUE),
    ('johnpocock.co.zw',            'John Pocock',                  'https://www.johnpocock.co.zw',             TRUE),
    ('trevordollar.co.zw',          'Trevor Dollar',                'https://www.trevordollar.co.zw',           TRUE),
    ('newageproperties.co.zw',      'New Age Properties',           'https://www.newageproperties.co.zw',       TRUE),
    ('bridgesrealestate.co.zw',     'Bridges Real Estate',          'https://www.bridgesrealestate.co.zw',      TRUE),
    ('terezim.co.zw',               'Tere Zim',                     'https://www.terezim.co.zw',                TRUE),
    ('legacyrealestate.co.zw',      'Legacy Real Estate',           'https://www.legacyrealestate.co.zw',       TRUE),
    ('exodusandcompany.com',        'Exodus & Company',             'https://www.exodusandcompany.com',         TRUE),
    ('leengate.co.zw',              'Leengate',                     'https://www.leengate.co.zw',               TRUE),
    ('lucilerealeastate.co.zw',     'Lucile Real Estate',           'https://www.lucilerealeastate.co.zw',      TRUE),
    -- Major portal
    ('property24.co.zw',            'Property24 Zimbabwe',          'https://www.property24.co.zw',             TRUE),
    -- Developer / listed companies
    ('westprop.com',                'WestProp Holdings',            'https://www.westprop.com',                 TRUE),
    ('zimreproperties.co.zw',       'ZIMRE Property Investments',   'https://www.zimreproperties.co.zw',        TRUE),
    ('mashonalandholdings.co.zw',   'Mashonaland Holdings',         'https://www.mashonalandholdings.co.zw',    TRUE),
    -- Legacy / other
    ('zimagents.co.zw',             'ZimAgents',                    'https://www.zimagents.co.zw',              TRUE),
    -- Auctions
    ('abcauctions.co.zw',           'ABC Auctions Zimbabwe',        'https://www.abcauctions.co.zw',            TRUE)
AS v(source, source_label, source_url, is_active)
WHERE NOT EXISTS (SELECT 1 FROM DIM_SOURCE WHERE source = v.source);


-- ════════════════════════════════════════════════════════════
-- E. DIM_DATE population (2020-01-01 → 2030-12-31)
-- ════════════════════════════════════════════════════════════
-- Uses a Snowflake GENERATOR to produce 4018 rows without a helper table.

INSERT INTO DIM_DATE
WITH date_spine AS (
    SELECT
        DATEADD('day', SEQ4(), '2020-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 4019))   -- 2020-01-01 to 2030-12-31 = 4018 days + 1 buffer
    WHERE DATEADD('day', SEQ4(), '2020-01-01'::DATE) <= '2030-12-31'::DATE
)
SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD'))           AS date_key,
    calendar_date,
    DAYOFWEEK(calendar_date)                                AS day_of_week,
    DAYNAME(calendar_date)                                  AS day_name,
    DAY(calendar_date)                                      AS day_of_month,
    DAYOFYEAR(calendar_date)                                AS day_of_year,
    WEEKOFYEAR(calendar_date)                               AS week_of_year,
    MONTH(calendar_date)                                    AS month_number,
    MONTHNAME(calendar_date)                                AS month_name,
    QUARTER(calendar_date)                                  AS quarter_number,
    YEAR(calendar_date)                                     AS year_number,
    DAYOFWEEK(calendar_date) IN (0, 6)                      AS is_weekend,
    DAY(calendar_date) = 1                                  AS is_month_start,
    calendar_date = LAST_DAY(calendar_date)                 AS is_month_end,
    calendar_date = DATE_TRUNC('QUARTER', calendar_date)    AS is_quarter_start,
    calendar_date = LAST_DAY(DATE_TRUNC('QUARTER', calendar_date), 'QUARTER') AS is_quarter_end,
    (MONTH(calendar_date) = 1  AND DAY(calendar_date) = 1)  AS is_year_start,
    (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 31) AS is_year_end,
    TO_CHAR(calendar_date, 'YYYY-MM')                       AS year_month,
    YEAR(calendar_date)::VARCHAR || '-Q' || QUARTER(calendar_date)::VARCHAR AS year_quarter
FROM date_spine
WHERE NOT EXISTS (SELECT 1 FROM DIM_DATE WHERE date_key = TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD')));


-- ════════════════════════════════════════════════════════════
-- F. ETL — populate dimensions from STAGING
-- ════════════════════════════════════════════════════════════
-- Run nightly after dbt staging models complete.
-- Safe to re-run: INSERT … WHERE NOT EXISTS pattern.

-- ── F1. Populate DIM_LOCATION from STAGING ───────────────────
INSERT INTO DIM_LOCATION (suburb_clean, city_clean, province, first_seen_date)
SELECT DISTINCT
    cl.suburb_clean,
    cl.city_clean,
    CASE cl.city_clean
        WHEN 'Harare'      THEN 'Harare Metropolitan'
        WHEN 'Chitungwiza' THEN 'Harare Metropolitan'
        WHEN 'Ruwa'        THEN 'Harare Metropolitan'
        WHEN 'Norton'      THEN 'Mashonaland West'
        WHEN 'Bulawayo'    THEN 'Bulawayo Metropolitan'
        WHEN 'Mutare'      THEN 'Manicaland'
        WHEN 'Gweru'       THEN 'Midlands'
        WHEN 'Kwekwe'      THEN 'Midlands'
        WHEN 'Masvingo'    THEN 'Masvingo'
        WHEN 'Chinhoyi'    THEN 'Mashonaland West'
        WHEN 'Marondera'   THEN 'Mashonaland East'
        ELSE 'Other'
    END                                                     AS province,
    MIN(cl.listing_date)                                    AS first_seen_date
FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
WHERE cl.city_clean IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM DIM_LOCATION dl
      WHERE COALESCE(dl.suburb_clean, '') = COALESCE(cl.suburb_clean, '')
        AND dl.city_clean = cl.city_clean
  )
GROUP BY 1, 2, 3;

-- Backfill approx coordinates from STAGING suburb reference
UPDATE DIM_LOCATION dl
SET
    latitude_approx  = ref.latitude_approx,
    longitude_approx = ref.longitude_approx
FROM ZIM_PROPERTY_DB.STAGING.ZW_SUBURBS_REFERENCE ref
WHERE LOWER(dl.suburb_clean) = LOWER(ref.suburb_name_clean)
  AND LOWER(dl.city_clean)   = LOWER(ref.city)
  AND dl.latitude_approx IS NULL;


-- ── F2. Refresh DIM_SOURCE last_scraped_date ─────────────────
UPDATE DIM_SOURCE ds
SET
    last_scraped_date = latest.last_scraped,
    first_scraped_date = COALESCE(ds.first_scraped_date, latest.first_scraped)
FROM (
    SELECT
        source,
        MIN(scraped_at::DATE) AS first_scraped,
        MAX(scraped_at::DATE) AS last_scraped
    FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS
    GROUP BY source
) latest
WHERE ds.source = latest.source;


-- ════════════════════════════════════════════════════════════
-- F3. ETL — FACT_LISTINGS from STAGING
-- ════════════════════════════════════════════════════════════
-- Inserts new listings only (UNIQUE constraint on listing_id prevents dupes).
-- property_key is left NULL until the matching engine resolves identity.

INSERT INTO FACT_LISTINGS (
    listing_id,
    date_key,
    listing_date_key,
    location_key,
    property_type_key,
    source_key,
    property_key,
    listing_type,
    currency_original,
    property_price_usd,
    property_price_zwl,
    exchange_rate_used,
    price_per_sqm_usd,
    property_size_sqm,
    stand_size_sqm,
    number_of_bedrooms,
    number_of_bathrooms,
    number_of_garages,
    feature_count,
    has_pool,
    has_borehole,
    has_solar,
    has_garage,
    image_count,
    data_quality_score,
    is_price_valid,
    is_location_valid,
    scraped_at
)
SELECT
    cl.listing_id,

    -- date_key (scraped date)
    TO_NUMBER(TO_CHAR(cl.scraped_at::DATE, 'YYYYMMDD'))         AS date_key,

    -- listing_date_key (when the listing was posted — may be NULL)
    CASE
        WHEN cl.listing_date IS NOT NULL
        THEN TO_NUMBER(TO_CHAR(cl.listing_date, 'YYYYMMDD'))
    END                                                          AS listing_date_key,

    -- location FK
    dl.location_key,

    -- property type FK
    dpt.property_type_key,

    -- source FK
    ds.source_key,

    -- property FK — NULL until matching engine runs
    NULL                                                         AS property_key,

    cl.listing_type,
    cl.currency_original,
    cl.property_price_usd,
    cl.property_price_zwl,
    cl.exchange_rate_used,
    cl.price_per_sqm_usd,
    cl.property_size_sqm,
    cl.stand_size_sqm,
    cl.number_of_bedrooms,
    cl.number_of_bathrooms,
    cl.number_of_garages,
    cl.feature_count,
    cl.has_pool,
    cl.has_borehole,
    cl.has_solar,
    cl.has_garage,
    cl.image_count,
    cl.data_quality_score,
    cl.is_price_valid,
    cl.is_location_valid,
    cl.scraped_at

FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl

-- Resolve location FK
LEFT JOIN DIM_LOCATION dl
    ON COALESCE(dl.suburb_clean, '') = COALESCE(cl.suburb_clean, '')
    AND dl.city_clean = cl.city_clean

-- Resolve property type FK
LEFT JOIN DIM_PROPERTY_TYPE dpt
    ON dpt.property_type = cl.property_type

-- Resolve source FK
LEFT JOIN DIM_SOURCE ds
    ON ds.source = cl.source

-- Skip listings already in FACT
WHERE NOT EXISTS (
    SELECT 1 FROM FACT_LISTINGS fl WHERE fl.listing_id = cl.listing_id
);


-- ════════════════════════════════════════════════════════════
-- G. GRANTS — ensure analyst role can read new tables
-- ════════════════════════════════════════════════════════════
GRANT SELECT ON ALL TABLES IN SCHEMA ZIM_PROPERTY_DB.WAREHOUSE TO ROLE ZIM_ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS  IN SCHEMA ZIM_PROPERTY_DB.WAREHOUSE TO ROLE ZIM_ANALYST_ROLE;


-- ════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES (run after ETL to check row counts)
-- ════════════════════════════════════════════════════════════
/*
SELECT 'DIM_DATE'          AS tbl, COUNT(*) AS rows FROM DIM_DATE
UNION ALL SELECT 'DIM_LOCATION',      COUNT(*) FROM DIM_LOCATION
UNION ALL SELECT 'DIM_PROPERTY_TYPE', COUNT(*) FROM DIM_PROPERTY_TYPE
UNION ALL SELECT 'DIM_SOURCE',        COUNT(*) FROM DIM_SOURCE
UNION ALL SELECT 'DIM_PROPERTY',      COUNT(*) FROM DIM_PROPERTY
UNION ALL SELECT 'FACT_LISTINGS',     COUNT(*) FROM FACT_LISTINGS
ORDER BY 1;
*/

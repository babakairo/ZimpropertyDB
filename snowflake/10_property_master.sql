-- ============================================================
-- 10 — MASTER Schema: Property Identity Tables
-- Schema: ZIM_PROPERTY_DB.MASTER
-- Purpose: Central entity-resolution store.
--          One PROPERTY_MASTER row per physical property in the
--          real world, regardless of how many times or on how
--          many sites it appears.
--          LISTING_PROPERTY_LINK resolves many listings → one master.
--
-- Populated by:  matching/linker.py  (Python matching engine)
-- Read by:       dbt WAREHOUSE.DIM_PROPERTY, FACT_LISTINGS (property_key)
--                Dagster asset: property_master_asset
--
-- Sections:
--   A. Schema + role setup
--   B. PROPERTY_MASTER — one row per physical property
--   C. LISTING_PROPERTY_LINK — many-to-one: listing → master
--   D. MATCH_CANDIDATE_QUEUE — review queue for uncertain matches
--   E. MATCH_DECISION_LOG — audit trail of every linking decision
--   F. Indexes (Snowflake clustering keys)
--   G. Views
--   H. Grants
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE WAREHOUSE ZIM_PROPERTY_WH;

-- ── A. Schema + roles ─────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ZIM_PROPERTY_DB.MASTER
    COMMENT = 'Property identity resolution — one row per real-world property';

USE SCHEMA MASTER;

-- Matching engine (Python) writes here — use SCRAPER role or a dedicated MATCHER role
GRANT ALL ON SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_SCRAPER_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_SCRAPER_ROLE;

GRANT ALL ON SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_DBT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_DBT_ROLE;

GRANT USAGE  ON SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_ANALYST_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_ANALYST_ROLE;


-- ════════════════════════════════════════════════════════════
-- B. PROPERTY_MASTER
-- ════════════════════════════════════════════════════════════
-- One row per distinct physical property identified by the
-- matching engine.  Created when:
--   1. A listing has no near-duplicate → new master record
--   2. A match is confirmed (auto or manual) → existing master

CREATE TABLE IF NOT EXISTS PROPERTY_MASTER (

    -- Primary identifier (UUID v4, assigned by linker.py)
    property_id             VARCHAR(36)     NOT NULL,

    -- ── Canonical address ─────────────────────────────────
    canonical_address       VARCHAR(500),               -- best address string seen
    address_normalised      VARCHAR(500),               -- normalise_address() output
    address_fingerprint     VARCHAR(500),               -- sorted-token fingerprint
    address_hash            VARCHAR(16),                -- SHA-256[:16] of fingerprint

    -- ── Location ─────────────────────────────────────────
    suburb                  VARCHAR(100),
    city                    VARCHAR(100),
    province                VARCHAR(100),
    country                 VARCHAR(5)      DEFAULT 'ZW',

    -- Best-known geo coordinates (from most-recent listing or geocoder)
    latitude                FLOAT,
    longitude               FLOAT,
    geohash_8               VARCHAR(10),                -- ~38m precision

    -- ── Property attributes (consensus from linked listings) ─
    property_type           VARCHAR(50),                -- house | flat | land | commercial | farm
    bedrooms_min            INTEGER,                    -- min across all linked listings
    bedrooms_max            INTEGER,                    -- max (allows for renovations)
    bedrooms_canonical      INTEGER,                    -- modal / best-guess
    bathrooms_canonical     INTEGER,
    garages_canonical       INTEGER,
    size_sqm_canonical      FLOAT,
    stand_size_sqm_canonical FLOAT,

    -- ── Listing summary stats ────────────────────────────
    first_listed_date       DATE,                       -- earliest scraped_at across all links
    last_listed_date        DATE,                       -- most recent scraped_at
    total_listings_count    INTEGER         DEFAULT 0,  -- total times appeared across all sites
    active_source_count     INTEGER         DEFAULT 0,  -- number of distinct sources today
    is_currently_active     BOOLEAN         DEFAULT TRUE,

    -- ── Price history summary ────────────────────────────
    first_price_usd         NUMBER(18, 2),              -- price at first listing
    current_price_usd       NUMBER(18, 2),              -- most recent confirmed price
    min_price_usd           NUMBER(18, 2),
    max_price_usd           NUMBER(18, 2),
    price_change_pct        FLOAT,                      -- (current - first) / first * 100

    -- ── Match confidence ─────────────────────────────────
    match_confidence        FLOAT,                      -- 0.0–1.0; 1.0 = manually confirmed
    match_method            VARCHAR(50),                -- exact | fuzzy_auto | manual

    -- ── Audit ────────────────────────────────────────────
    created_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    created_by              VARCHAR(100)    DEFAULT 'linker.py',

    CONSTRAINT pk_property_master PRIMARY KEY (property_id)
)
CLUSTER BY (city, suburb, address_hash)
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Master record for each distinct physical property — entity resolution output';


-- ════════════════════════════════════════════════════════════
-- C. LISTING_PROPERTY_LINK
-- ════════════════════════════════════════════════════════════
-- Many-to-one: every listing_id that has been matched to a
-- PROPERTY_MASTER record.  Unmatched listings are absent.

CREATE TABLE IF NOT EXISTS LISTING_PROPERTY_LINK (

    -- Composite PK — one link per listing
    link_id                 INTEGER         NOT NULL AUTOINCREMENT,
    listing_id              VARCHAR(16)     NOT NULL,   -- FK → RAW.ZW_PROPERTY_LISTINGS
    property_id             VARCHAR(36)     NOT NULL,   -- FK → PROPERTY_MASTER

    -- Source context
    source                  VARCHAR(100),               -- which site this listing came from
    listing_url             VARCHAR(2000),
    scraped_at              TIMESTAMP_TZ,

    -- Match details recorded at link time
    match_score             FLOAT,                      -- 0.0–1.0 composite score
    match_method            VARCHAR(50),                -- exact | fuzzy_auto | manual_confirm | manual_reject
    match_signals           VARIANT,                    -- JSON: {address_sim:0.95, geo_dist_m:12, ...}

    -- Status
    is_active               BOOLEAN         DEFAULT TRUE,  -- FALSE if listing was taken down
    is_canonical            BOOLEAN         DEFAULT FALSE, -- TRUE for the "best" listing of this property

    -- Audit
    linked_at               TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    linked_by               VARCHAR(100)    DEFAULT 'linker.py',
    reviewed_at             TIMESTAMP_TZ,               -- populated when a human reviews
    reviewed_by             VARCHAR(200),

    CONSTRAINT pk_listing_property_link PRIMARY KEY (link_id),
    CONSTRAINT uq_listing_link          UNIQUE (listing_id),
    CONSTRAINT fk_link_property         FOREIGN KEY (property_id) REFERENCES PROPERTY_MASTER(property_id)
)
CLUSTER BY (property_id, source)
COMMENT = 'Many-to-one link table: listing_id → property_id (entity resolution output)';


-- ════════════════════════════════════════════════════════════
-- D. MATCH_CANDIDATE_QUEUE
-- ════════════════════════════════════════════════════════════
-- Listings that the scoring engine is unsure about (0.60–0.84 score).
-- Placed here for human review via a simple web UI / spreadsheet export.

CREATE TABLE IF NOT EXISTS MATCH_CANDIDATE_QUEUE (

    queue_id                INTEGER         NOT NULL AUTOINCREMENT,

    -- Candidate pair
    listing_id_a            VARCHAR(16)     NOT NULL,   -- new listing being checked
    listing_id_b            VARCHAR(16)     NOT NULL,   -- existing candidate it may match

    -- Score breakdown
    composite_score         FLOAT,                      -- overall 0.0–1.0
    address_similarity      FLOAT,                      -- token overlap ratio
    geo_distance_m          FLOAT,                      -- metres between coordinates
    price_diff_pct          FLOAT,                      -- abs % price difference
    bedroom_match           BOOLEAN,
    phone_match             BOOLEAN,

    -- Reviewer decision
    status                  VARCHAR(20)     DEFAULT 'pending',
    -- pending | confirmed_match | confirmed_no_match | auto_expired
    decision                VARCHAR(20),                -- match | no_match
    reviewed_by             VARCHAR(200),
    reviewed_at             TIMESTAMP_TZ,
    reviewer_notes          VARCHAR(2000),

    -- Audit
    queued_at               TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    expires_at              TIMESTAMP_TZ    DEFAULT DATEADD('day', 30, CURRENT_TIMESTAMP()),

    CONSTRAINT pk_match_queue PRIMARY KEY (queue_id),
    CONSTRAINT uq_match_pair  UNIQUE (listing_id_a, listing_id_b)
)
COMMENT = 'Review queue for uncertain property matches (score 0.60–0.84)';


-- ════════════════════════════════════════════════════════════
-- E. MATCH_DECISION_LOG
-- ════════════════════════════════════════════════════════════
-- Immutable audit trail — every time two listings are linked or
-- rejected.  Used for retraining the scorer.

CREATE TABLE IF NOT EXISTS MATCH_DECISION_LOG (

    log_id                  INTEGER         NOT NULL AUTOINCREMENT,
    listing_id_a            VARCHAR(16)     NOT NULL,
    listing_id_b            VARCHAR(16)     NOT NULL,
    property_id             VARCHAR(36),                -- NULL for rejected pairs

    composite_score         FLOAT,
    decision                VARCHAR(20)     NOT NULL,   -- match | no_match | manual_confirm | manual_reject
    decided_by              VARCHAR(100),               -- linker.py | reviewer@email.com
    decided_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    -- Full signal dump for ML retraining
    signals_json            VARIANT,

    CONSTRAINT pk_match_log PRIMARY KEY (log_id)
)
DATA_RETENTION_TIME_IN_DAYS = 365
COMMENT = 'Immutable audit log of every match decision — used for scorer retraining';


-- ════════════════════════════════════════════════════════════
-- F. Clustering and search optimisation
-- ════════════════════════════════════════════════════════════
-- Snowflake does not support traditional indexes; clustering keys
-- guide micro-partition pruning for the most common query patterns.

-- Already set at creation time:
--   PROPERTY_MASTER  → CLUSTER BY (city, suburb, address_hash)
--   LISTING_PROPERTY_LINK → CLUSTER BY (property_id, source)

-- Search path index for address_hash lookups (blocking pass)
-- Maintained by linker.py INSERT pattern; no extra DDL needed.


-- ════════════════════════════════════════════════════════════
-- G. VIEWS
-- ════════════════════════════════════════════════════════════

-- ── View: Active multi-site properties ───────────────────────
-- Properties currently listed on more than one source simultaneously.
CREATE OR REPLACE VIEW V_MULTI_SOURCE_PROPERTIES AS
    SELECT
        pm.property_id,
        pm.canonical_address,
        pm.suburb,
        pm.city,
        pm.property_type,
        pm.current_price_usd,
        pm.active_source_count,
        pm.last_listed_date,
        ARRAY_AGG(DISTINCT lpl.source) WITHIN GROUP (ORDER BY lpl.source) AS sources
    FROM PROPERTY_MASTER pm
    JOIN LISTING_PROPERTY_LINK lpl ON lpl.property_id = pm.property_id
    WHERE pm.is_currently_active = TRUE
      AND pm.active_source_count > 1
      AND lpl.is_active = TRUE
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ORDER BY pm.active_source_count DESC, pm.last_listed_date DESC;


-- ── View: Unmatched listings (no property_key in FACT) ───────
-- Used by the matching engine to find listings it still needs to process.
CREATE OR REPLACE VIEW V_UNMATCHED_LISTINGS AS
    SELECT
        fl.listing_id,
        fl.scraped_at,
        fl.listing_type,
        fl.property_price_usd,
        fl.location_key,
        dl.suburb_clean,
        dl.city_clean
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    LEFT JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl ON dl.location_key = fl.location_key
    WHERE fl.property_key IS NULL
    ORDER BY fl.scraped_at DESC;


-- ── View: Price history per master property ───────────────────
-- Chronological price observations across all linked listings.
CREATE OR REPLACE VIEW V_PROPERTY_PRICE_HISTORY AS
    SELECT
        lpl.property_id,
        pm.canonical_address,
        pm.suburb,
        pm.city,
        pm.property_type,
        lpl.source,
        fl.property_price_usd,
        fl.listing_type,
        fl.scraped_at::DATE         AS observed_date,
        LAG(fl.property_price_usd) OVER (
            PARTITION BY lpl.property_id, lpl.source
            ORDER BY fl.scraped_at
        )                           AS prev_price_usd,
        fl.property_price_usd -
            LAG(fl.property_price_usd) OVER (
                PARTITION BY lpl.property_id, lpl.source
                ORDER BY fl.scraped_at
            )                       AS price_change_usd
    FROM LISTING_PROPERTY_LINK lpl
    JOIN PROPERTY_MASTER pm ON pm.property_id = lpl.property_id
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl ON fl.listing_id = lpl.listing_id
    WHERE fl.is_price_valid = TRUE
    ORDER BY lpl.property_id, fl.scraped_at;


-- ════════════════════════════════════════════════════════════
-- H. GRANTS
-- ════════════════════════════════════════════════════════════
GRANT SELECT ON ALL TABLES IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS  IN SCHEMA ZIM_PROPERTY_DB.MASTER TO ROLE ZIM_ANALYST_ROLE;


-- ════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- ════════════════════════════════════════════════════════════
/*
SELECT 'PROPERTY_MASTER'        AS tbl, COUNT(*) AS rows FROM PROPERTY_MASTER
UNION ALL SELECT 'LISTING_PROPERTY_LINK', COUNT(*) FROM LISTING_PROPERTY_LINK
UNION ALL SELECT 'MATCH_CANDIDATE_QUEUE', COUNT(*) FROM MATCH_CANDIDATE_QUEUE
UNION ALL SELECT 'MATCH_DECISION_LOG',    COUNT(*) FROM MATCH_DECISION_LOG
ORDER BY 1;

-- Check unmatched listings
SELECT COUNT(*) AS unmatched FROM V_UNMATCHED_LISTINGS;

-- Check multi-site coverage
SELECT city, COUNT(*) AS multi_source_properties
FROM V_MULTI_SOURCE_PROPERTIES
GROUP BY city ORDER BY 2 DESC;
*/

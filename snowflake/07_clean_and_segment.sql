-- ============================================================
-- 07 — Clean, Deduplicate & Segment Listings
-- Schema: STAGING (clean table) + ANALYTICS (segment views)
--
-- Run order:
--   Step 1 – Populate STAGING.CLEANED_PROPERTY_LISTINGS
--   Step 2 – Create ANALYTICS views for Land / Rentals / Houses for Sale
--
-- SOURCE-AGNOSTIC DESIGN — IMPORTANT
-- ─────────────────────────────────────────────────────────────
-- This script accepts ALL sources from RAW.ZW_PROPERTY_LISTINGS
-- without any hardcoded WHERE source IN (...) filter.
-- New data sources are automatically included on the next run.
--
-- The only record-level filters applied (in the `filtered` CTE) are:
--   • listing_url must be non-null (minimum identification)
--   • property_price must be > 0 or NULL (drops data-entry zeros)
--   • listing_type must be 'sale' or 'rent' (drops unclassified records)
--   • web.archive.org records are excluded (see Fix 2 comment below)
--
-- DO NOT add WHERE source IN (...) clauses here. If a source needs
-- special handling, add a CASE branch inside the cleaned CTE instead.
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE WAREHOUSE ZIM_PROPERTY_WH;

-- ============================================================
-- STEP 1: Populate STAGING.CLEANED_PROPERTY_LISTINGS
-- ============================================================
USE SCHEMA STAGING;

INSERT OVERWRITE INTO CLEANED_PROPERTY_LISTINGS (
    listing_id,
    source,
    listing_url,
    property_title,
    property_price_usd,
    property_price_zwl,
    currency_original,
    exchange_rate_used,
    property_type,
    listing_type,
    city_clean,
    suburb_clean,
    address_raw,
    latitude,
    longitude,
    number_of_bedrooms,
    number_of_bathrooms,
    number_of_garages,
    property_size_sqm,
    stand_size_sqm,
    price_per_sqm_usd,
    features,
    feature_count,
    has_pool,
    has_borehole,
    has_solar,
    has_garage,
    agent_name,
    agent_phone,
    agent_email,
    agency_name,
    image_count,
    image_urls,
    listing_date,
    listing_year,
    listing_month,
    listing_quarter,
    scraped_at,
    is_price_valid,
    is_location_valid,
    data_quality_score
)
WITH

-- ── 1a. Deduplicate: keep the most recently scraped version of each listing ──
deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY listing_id
               ORDER BY scraped_at DESC NULLS LAST
           ) AS rn
    FROM RAW.ZW_PROPERTY_LISTINGS
    WHERE listing_id IS NOT NULL
      AND listing_id != ''
),

-- ── 1b. Must-have fields: drop rows with no usable data ──
filtered AS (
    SELECT *
    FROM deduped
    WHERE rn = 1
      -- At minimum we need a URL to identify the listing
      AND listing_url IS NOT NULL
      AND listing_url != ''
      -- Drop obvious junk prices (zero or negative)
      AND (property_price IS NULL OR property_price > 0)
      -- Keep only known listing types
      AND LOWER(listing_type) IN ('sale', 'rent')
      -- P0 Fix 2: exclude Wayback Machine / web.archive.org records.
      -- These are historical crawls with 99.9% null price/location — they
      -- drag the overall DQ score down and pollute every analytics query.
      -- Raw records are preserved in RAW.ZW_PROPERTY_LISTINGS for reference.
      -- See: snowflake/stg_wayback_archive.sql for the isolated view.
      AND source NOT LIKE '%archive.org%'
      AND source NOT LIKE '%wayback%'
),

-- ── 1c. Normalise & derive fields ──
cleaned AS (
    SELECT
        listing_id,
        source,
        listing_url,
        TRIM(property_title)                                    AS property_title,

        -- Currency normalisation: treat everything without a clear ZWL tag as USD
        -- (Zimbabwean portals price in USD by default since 2009 dollarisation)
        CASE
            WHEN UPPER(currency) IN ('ZWL', 'ZIG', 'RTGS', 'BOND')
            THEN NULL           -- ZWL price; USD unknown without exchange rate
            ELSE property_price
        END                                                     AS property_price_usd,

        CASE
            WHEN UPPER(currency) IN ('ZWL', 'ZIG', 'RTGS', 'BOND')
            THEN property_price
            ELSE NULL
        END                                                     AS property_price_zwl,

        COALESCE(UPPER(currency), 'USD')                        AS currency_original,
        NULL::FLOAT                                             AS exchange_rate_used,  -- filled by dbt later

        -- Standardise property type (P0 Fix 4: expanded mappings, unknown → NULL)
        CASE
            WHEN LOWER(property_type) IN ('house', 'home', 'residential',
                                           'cottage', 'cottages',
                                           'cluster', 'villa')                  THEN 'house'
            WHEN LOWER(property_type) IN ('flat', 'apartment', 'studio',
                                           'apartments', 'bachelor', 'lodger')  THEN 'flat'
            WHEN LOWER(property_type) IN ('room', 'rooms', 'bedsitter',
                                           'student accommodation')             THEN 'flat'
            WHEN LOWER(property_type) IN ('townhouse', 'town house',
                                           'townhouse complex', 'townhouses')   THEN 'townhouse'
            WHEN LOWER(property_type) IN ('land', 'stand', 'plot', 'vacant land',
                                           'vacant stand', 'erf', 'stands',
                                           'plots')                             THEN 'land'
            WHEN LOWER(property_type) IN ('farm', 'smallholding', 'small holding',
                                           'agricultural', 'farms')             THEN 'farm'
            WHEN LOWER(property_type) IN ('commercial', 'office', 'shop',
                                           'retail', 'warehouse', 'industrial',
                                           'shops', 'offices', 'warehouses',
                                           'factory', 'factories', 'workshop',
                                           'property development')              THEN 'commercial'
            WHEN LOWER(property_type) LIKE '%hospitality%'                      THEN 'commercial'
            WHEN LOWER(property_type) LIKE '%workshop%'                         THEN 'commercial'
            -- unknown / invalid → NULL (do not store as a category)
            WHEN LOWER(TRIM(COALESCE(property_type, 'unknown'))) IN
                 ('unknown', 'unk', 'n/a', 'na', 'none', 'null', 'other', '')  THEN NULL
            -- Genuinely unmapped: store as-is (visible for future mapping)
            ELSE LEFT(LOWER(property_type), 50)
        END                                                     AS property_type,

        LOWER(listing_type)                                     AS listing_type,

        -- Clean city & suburb: trim, title-case, collapse whitespace
        TRIM(INITCAP(REGEXP_REPLACE(city,   '\\s+', ' ')))     AS city_clean,
        TRIM(INITCAP(REGEXP_REPLACE(suburb, '\\s+', ' ')))     AS suburb_clean,
        address_raw,
        latitude,
        longitude,

        -- Attributes (keep nulls — don't default to 0)
        number_of_bedrooms,
        number_of_bathrooms,
        number_of_garages,
        property_size_sqm,
        stand_size_sqm,

        -- Price per sqm — only when both price and size are known and non-zero
        CASE
            WHEN property_price > 0
             AND UPPER(COALESCE(currency, 'USD')) NOT IN ('ZWL', 'ZIG', 'RTGS', 'BOND')
             AND COALESCE(property_size_sqm, stand_size_sqm) > 0
            THEN ROUND(
                    property_price / COALESCE(property_size_sqm, stand_size_sqm),
                    2
                )
        END                                                     AS price_per_sqm_usd,

        -- Features JSON (already VARIANT in target)
        features,
        COALESCE(ARRAY_SIZE(features), 0)                       AS feature_count,

        -- Derived feature flags from the JSON array
        ARRAY_CONTAINS('pool'::VARIANT,     features)
            OR ARRAY_CONTAINS('swimming pool'::VARIANT, features)
                                                                AS has_pool,
        ARRAY_CONTAINS('borehole'::VARIANT, features)           AS has_borehole,
        ARRAY_CONTAINS('solar'::VARIANT,    features)
            OR ARRAY_CONTAINS('solar panels'::VARIANT, features)
                                                                AS has_solar,
        (number_of_garages > 0)
            OR ARRAY_CONTAINS('garage'::VARIANT, features)      AS has_garage,

        agent_name,
        agent_phone,
        agent_email,
        agency_name,

        COALESCE(ARRAY_SIZE(image_urls), 0)                     AS image_count,
        image_urls,

        listing_date,
        YEAR(listing_date)                                      AS listing_year,
        MONTH(listing_date)                                     AS listing_month,
        QUARTER(listing_date)                                   AS listing_quarter,

        scraped_at,

        -- Validity flags
        (property_price > 0
            AND UPPER(COALESCE(currency, 'USD')) NOT IN ('ZWL', 'ZIG', 'RTGS', 'BOND'))
                                                                AS is_price_valid,
        (city IS NOT NULL AND city != '')                       AS is_location_valid,

        -- Data quality score: fraction of key fields that are non-null (0–1)
        (
            IFF(listing_url       IS NOT NULL, 1, 0) +
            IFF(property_title    IS NOT NULL, 1, 0) +
            IFF(property_price    IS NOT NULL, 1, 0) +
            IFF(property_type     IS NOT NULL, 1, 0) +
            IFF(listing_type      IS NOT NULL, 1, 0) +
            IFF(city              IS NOT NULL, 1, 0) +
            IFF(suburb            IS NOT NULL, 1, 0) +
            IFF(number_of_bedrooms IS NOT NULL, 1, 0) +
            IFF(property_size_sqm IS NOT NULL, 1, 0) +
            IFF(agent_name        IS NOT NULL, 1, 0)
        ) / 10.0                                                AS data_quality_score

    FROM filtered
)

SELECT
    listing_id,
    source,
    listing_url,
    property_title,
    property_price_usd,
    property_price_zwl,
    currency_original,
    exchange_rate_used,
    property_type,
    listing_type,
    city_clean,
    suburb_clean,
    address_raw,
    latitude,
    longitude,
    number_of_bedrooms,
    number_of_bathrooms,
    number_of_garages,
    property_size_sqm,
    stand_size_sqm,
    price_per_sqm_usd,
    features,
    feature_count,
    has_pool,
    has_borehole,
    has_solar,
    has_garage,
    agent_name,
    agent_phone,
    agent_email,
    agency_name,
    image_count,
    image_urls,
    listing_date,
    listing_year,
    listing_month,
    listing_quarter,
    scraped_at,
    is_price_valid,
    is_location_valid,
    data_quality_score
FROM cleaned;


-- ============================================================
-- STEP 2: Segment views in ANALYTICS
-- ============================================================
USE SCHEMA ANALYTICS;

-- ── View 1: Land for sale (stands, plots, farms) ──────────────────────────
CREATE OR REPLACE VIEW LAND_LISTINGS AS
SELECT
    listing_id,
    source,
    listing_url,
    property_title,
    property_price_usd,
    currency_original,
    property_type,           -- land | farm
    city_clean,
    suburb_clean,
    address_raw,
    latitude,
    longitude,
    property_size_sqm,
    stand_size_sqm,
    price_per_sqm_usd,
    agent_name,
    agent_phone,
    agent_email,
    agency_name,
    image_count,
    image_urls,
    listing_date,
    scraped_at,
    data_quality_score
FROM STAGING.CLEANED_PROPERTY_LISTINGS
WHERE listing_type = 'sale'
  AND property_type IN ('land', 'farm')
ORDER BY city_clean, suburb_clean, property_price_usd;

-- ── View 2: Rentals (all property types) ──────────────────────────────────
CREATE OR REPLACE VIEW RENTAL_LISTINGS AS
SELECT
    listing_id,
    source,
    listing_url,
    property_title,
    property_price_usd       AS monthly_rent_usd,
    currency_original,
    property_type,
    city_clean,
    suburb_clean,
    address_raw,
    latitude,
    longitude,
    number_of_bedrooms,
    number_of_bathrooms,
    number_of_garages,
    property_size_sqm,
    features,
    feature_count,
    has_pool,
    has_borehole,
    has_solar,
    has_garage,
    agent_name,
    agent_phone,
    agent_email,
    agency_name,
    image_count,
    image_urls,
    listing_date,
    scraped_at,
    data_quality_score
FROM STAGING.CLEANED_PROPERTY_LISTINGS
WHERE listing_type = 'rent'
ORDER BY city_clean, suburb_clean, number_of_bedrooms, property_price_usd;

-- ── View 3: Residential houses for sale ──────────────────────────────────
CREATE OR REPLACE VIEW HOUSE_SALE_LISTINGS AS
SELECT
    listing_id,
    source,
    listing_url,
    property_title,
    property_price_usd,
    currency_original,
    property_type,           -- house | flat | townhouse | room
    city_clean,
    suburb_clean,
    address_raw,
    latitude,
    longitude,
    number_of_bedrooms,
    number_of_bathrooms,
    number_of_garages,
    property_size_sqm,
    stand_size_sqm,
    price_per_sqm_usd,
    features,
    feature_count,
    has_pool,
    has_borehole,
    has_solar,
    has_garage,
    agent_name,
    agent_phone,
    agent_email,
    agency_name,
    image_count,
    image_urls,
    listing_date,
    scraped_at,
    data_quality_score
FROM STAGING.CLEANED_PROPERTY_LISTINGS
WHERE listing_type = 'sale'
  AND property_type NOT IN ('land', 'farm', 'commercial')
ORDER BY city_clean, suburb_clean, number_of_bedrooms, property_price_usd;

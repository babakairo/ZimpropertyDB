{{
    config(
        materialized = 'table',
        tags         = ['intermediate', 'daily']
    )
}}

/*
  int_cleaned_property_listings
  ──────────────────────────────
  Full cleaning and enrichment pass:
    • Currency normalisation (all prices to USD)
    • Location validation against reference table
    • Data quality scoring
    • Feature extraction from JSON array
    • Price per sqm calculation
*/

WITH listings AS (
    SELECT * FROM {{ ref('stg_property_listings') }}
),

rates AS (
    SELECT * FROM {{ ref('stg_exchange_rates') }}
),

suburbs_ref AS (
    SELECT * FROM {{ source('staging', 'zw_suburbs_reference') }}
),

suburb_aliases AS (
    SELECT * FROM VALUES
        ('borrowdaleharare', 'borrowdale'),
        ('borrowdalehararezimbabwe', 'borrowdale'),
        ('mtpleasant', 'mountpleasant'),
        ('mountpleasantharare', 'mountpleasant'),
        ('hararecbd', 'hararecentral'),
        ('cbdharare', 'hararecentral'),
        ('bulawayocbd', 'bulawayocentral'),
        ('mutarecbd', 'mutarecentral')
    AS t(alias_key, canonical_key)
),

suburb_city_default AS (
    SELECT
        REGEXP_REPLACE(LOWER(TRIM(suburb_name_clean)), '[^a-z0-9]', '') AS suburb_key,
        MIN(city) AS city,
        MIN(suburb_name_clean) AS suburb_name_clean,
        MIN(latitude_approx) AS latitude_approx,
        MIN(longitude_approx) AS longitude_approx
    FROM suburbs_ref
    GROUP BY 1
),

-- ── Currency normalisation ──────────────────────────────────────────────────
with_usd_price AS (
    SELECT
        l.*,

        -- Match exchange rate to listing date (fallback to today's rate)
        COALESCE(
            r_on_date.zwl_per_usd,
            r_latest.zwl_per_usd,
            {{ var('zig_per_usd') }}
        ) AS exchange_rate_used,

        CASE
            WHEN l.currency = 'USD' THEN l.property_price
            WHEN l.currency IN ('ZWL', 'ZIG') AND COALESCE(r_on_date.zwl_per_usd, r_latest.zwl_per_usd) > 0
                THEN ROUND(
                    l.property_price / COALESCE(r_on_date.zwl_per_usd, r_latest.zwl_per_usd),
                    2
                )
            ELSE NULL
        END AS property_price_usd,

        CASE WHEN l.currency IN ('ZWL', 'ZIG') THEN l.property_price ELSE NULL END AS property_price_zwl,
        CASE WHEN l.currency = 'ZIG' THEN TRUE ELSE FALSE END AS is_zig_price

    FROM listings l
    LEFT JOIN rates r_on_date ON r_on_date.rate_date = l.listing_date
    LEFT JOIN (
        SELECT zwl_per_usd FROM rates ORDER BY rate_date DESC LIMIT 1
    ) r_latest ON TRUE
),

pre_location AS (
    SELECT
        p.*,
        REGEXP_REPLACE(LOWER(TRIM(COALESCE(p.city_raw, ''))), '[^a-z0-9]', '') AS city_key_raw,
        REGEXP_REPLACE(
            LOWER(TRIM(
                CASE
                    WHEN p.city_raw IS NOT NULL
                    THEN REGEXP_REPLACE(
                        COALESCE(p.suburb_raw, ''),
                        '(?i)\\b' || REGEXP_REPLACE(p.city_raw, '[^A-Za-z0-9 ]', '') || '\\b',
                        ''
                    )
                    ELSE COALESCE(p.suburb_raw, '')
                END
            )),
            '[^a-z0-9]',
            ''
        ) AS suburb_key_raw,
        LOWER(
            TRIM(
                CONCAT(
                    COALESCE(p.property_title, ''), ' ',
                    COALESCE(p.address_raw, ''), ' ',
                    COALESCE(p.suburb_raw, ''), ' ',
                    COALESCE(p.city_raw, '')
                )
            )
        ) AS location_blob
    FROM with_usd_price p
),

-- ── Location enrichment ─────────────────────────────────────────────────────
with_location AS (
    SELECT
        p.*,
        COALESCE(s_exact.suburb_name_clean, s_fallback.suburb_name_clean) AS suburb_clean,
        COALESCE(s_exact.city, s_fallback.city)  AS city_clean_ref,
        COALESCE(
            s_exact.city,
            s_fallback.city,
            CASE
                WHEN p.location_blob RLIKE '\\bharare\\b' THEN 'Harare'
                WHEN p.location_blob RLIKE '\\bbulawayo\\b' THEN 'Bulawayo'
                WHEN p.location_blob RLIKE '\\bmutare\\b' THEN 'Mutare'
                WHEN p.location_blob RLIKE '\\bgweru\\b' THEN 'Gweru'
                WHEN p.location_blob RLIKE '\\bmasvingo\\b' THEN 'Masvingo'
                WHEN p.location_blob RLIKE '\\bkwekwe\\b' THEN 'Kwekwe'
                WHEN p.location_blob RLIKE '\\bchinhoyi\\b' THEN 'Chinhoyi'
                WHEN p.location_blob RLIKE '\\bmarondera\\b' THEN 'Marondera'
                WHEN p.location_blob RLIKE '\\bbindura\\b' THEN 'Bindura'
                WHEN p.location_blob RLIKE '\\bkariba\\b' THEN 'Kariba'
                WHEN p.location_blob RLIKE '\\brusape\\b' THEN 'Rusape'
                WHEN p.location_blob RLIKE '\\bchegutu\\b' THEN 'Chegutu'
                WHEN p.location_blob RLIKE '\\bhwange\\b' THEN 'Hwange'
                WHEN p.location_blob RLIKE '\\bbeitbridge\\b' THEN 'Beitbridge'
                WHEN p.location_blob RLIKE 'victoria\\s*falls' THEN 'Victoria Falls'
            END,
            p.city_raw
        ) AS city_clean,
        COALESCE(
            s_exact.latitude_approx, s_fallback.latitude_approx, p.latitude
        )                       AS latitude_enriched,
        COALESCE(
            s_exact.longitude_approx, s_fallback.longitude_approx, p.longitude
        )                       AS longitude_enriched,
        CASE
            WHEN s_exact.suburb_name_clean IS NOT NULL OR s_fallback.suburb_name_clean IS NOT NULL
            THEN TRUE ELSE FALSE
        END AS suburb_matched
    FROM pre_location p
    LEFT JOIN suburb_aliases sa
        ON p.suburb_key_raw = sa.alias_key
    LEFT JOIN suburbs_ref s_exact
        ON COALESCE(sa.canonical_key, p.suburb_key_raw)
           = REGEXP_REPLACE(LOWER(TRIM(s_exact.suburb_name_clean)), '[^a-z0-9]', '')
        AND p.city_key_raw = REGEXP_REPLACE(LOWER(TRIM(COALESCE(s_exact.city, ''))), '[^a-z0-9]', '')
    LEFT JOIN suburb_city_default s_fallback
        ON COALESCE(sa.canonical_key, p.suburb_key_raw) = s_fallback.suburb_key
),

-- ── Feature extraction ──────────────────────────────────────────────────────
with_features AS (
    SELECT
        *,
        ARRAY_SIZE(features)                    AS feature_count,

        -- Boolean feature flags
        CASE WHEN
            TO_ARRAY(features)::VARCHAR ILIKE '%pool%' OR
            TO_ARRAY(features)::VARCHAR ILIKE '%swimming%'
            THEN TRUE ELSE FALSE END             AS has_pool,

        CASE WHEN
            TO_ARRAY(features)::VARCHAR ILIKE '%borehole%' OR
            TO_ARRAY(features)::VARCHAR ILIKE '%well%'
            THEN TRUE ELSE FALSE END             AS has_borehole,

        CASE WHEN
            TO_ARRAY(features)::VARCHAR ILIKE '%solar%' OR
            TO_ARRAY(features)::VARCHAR ILIKE '%photovoltaic%'
            THEN TRUE ELSE FALSE END             AS has_solar,

        CASE WHEN
            TO_ARRAY(features)::VARCHAR ILIKE '%garage%' OR
            TO_ARRAY(features)::VARCHAR ILIKE '%carport%' OR
            number_of_garages > 0
            THEN TRUE ELSE FALSE END             AS has_garage

    FROM with_location
),

with_agent_contact AS (
    SELECT
        *,
        COALESCE(NULLIF(TRIM(agent_name), ''), NULLIF(TRIM(agency_name), '')) AS agent_name_clean,
        CASE
            WHEN agent_phone IS NOT NULL
            THEN REGEXP_REPLACE(agent_phone, '[^0-9+]', '')
        END AS agent_phone_clean,
        CASE
            WHEN agent_email IS NOT NULL
                 AND REGEXP_LIKE(LOWER(TRIM(agent_email)), '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
            THEN LOWER(TRIM(agent_email))
        END AS agent_email_clean,
        CASE WHEN COALESCE(NULLIF(TRIM(agent_name), ''), NULLIF(TRIM(agency_name), '')) IS NOT NULL THEN TRUE ELSE FALSE END AS has_agent_name,
        CASE
            WHEN REGEXP_REPLACE(COALESCE(agent_phone, ''), '[^0-9+]', '') <> ''
              OR (
                    agent_email IS NOT NULL
                AND REGEXP_LIKE(LOWER(TRIM(agent_email)), '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
              )
            THEN TRUE ELSE FALSE
        END AS has_agent_contact
    FROM with_features
),

-- ── Derived metrics ─────────────────────────────────────────────────────────
with_metrics AS (
    SELECT
        *,
        -- Price per sqm
        CASE
            WHEN property_price_usd > 0 AND property_size_sqm > 10
            THEN ROUND(property_price_usd / property_size_sqm, 2)
        END AS price_per_sqm_usd,

        CASE
            WHEN property_price_usd > 0 AND number_of_bedrooms > 0
            THEN ROUND(property_price_usd / number_of_bedrooms, 2)
        END AS price_per_bedroom_usd,

        -- Date parts
        YEAR(listing_date)                      AS listing_year,
        MONTH(listing_date)                     AS listing_month,
        QUARTER(listing_date)                   AS listing_quarter,

        -- Completeness score (count of key fields filled / total key fields)
        (
            (CASE WHEN property_price_usd IS NOT NULL  THEN 1 ELSE 0 END) +
            (CASE WHEN city_clean IS NOT NULL           THEN 1 ELSE 0 END) +
            (CASE WHEN suburb_raw IS NOT NULL           THEN 1 ELSE 0 END) +
            (CASE WHEN property_type IS NOT NULL        THEN 1 ELSE 0 END) +
            (CASE WHEN number_of_bedrooms IS NOT NULL   THEN 1 ELSE 0 END) +
            (CASE WHEN property_size_sqm IS NOT NULL    THEN 1 ELSE 0 END) +
            (CASE WHEN listing_date IS NOT NULL         THEN 1 ELSE 0 END) +
            (CASE WHEN has_agent_name THEN 1 ELSE 0 END) +
            (CASE WHEN has_agent_contact THEN 1 ELSE 0 END)
        ) / 9.0                                 AS data_quality_score,

        -- Validity flags
        CASE
            WHEN property_price_usd BETWEEN 100 AND 50000000 THEN TRUE
            WHEN listing_type = 'rent' AND property_price_usd BETWEEN 10 AND 50000 THEN TRUE
            ELSE FALSE
        END AS is_price_valid,

        CASE
            WHEN city_clean IS NOT NULL AND COALESCE(suburb_clean, suburb_raw) IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_location_valid

    FROM with_agent_contact
)

SELECT
    listing_id,
    data_source,
    listing_url,
    property_title,
    property_price_usd,
    property_price_zwl,
    is_zig_price,
    currency                AS currency_original,
    exchange_rate_used,
    property_type,
    listing_type,
    city_clean,
    COALESCE(suburb_clean, suburb_raw) AS suburb_clean,
    address_raw,
    latitude_enriched       AS latitude,
    longitude_enriched      AS longitude,
    number_of_bedrooms,
    number_of_bathrooms,
    number_of_garages,
    property_size_sqm,
    stand_size_sqm,
    price_per_sqm_usd,
    price_per_bedroom_usd,
    features,
    feature_count,
    has_pool,
    has_borehole,
    has_solar,
    has_garage,
    agent_name_clean        AS agent_name,
    agent_phone_clean       AS agent_phone,
    agent_email_clean       AS agent_email,
    agency_name,
    has_agent_name,
    has_agent_contact,
    ARRAY_SIZE(image_urls)  AS image_count,
    image_urls,
    listing_date,
    listing_year,
    listing_month,
    listing_quarter,
    scraped_at,
    is_price_valid,
    is_location_valid,
    data_quality_score
FROM with_metrics
WHERE (is_price_valid = TRUE OR property_price_usd IS NULL) -- keep sanity-checked prices and optional null-price records
    AND city_clean IS NOT NULL
    AND COALESCE(suburb_clean, suburb_raw) IS NOT NULL
    AND NOT REGEXP_LIKE(LOWER(TRIM(city_clean)), '^(unknown|unk|n/?a|na|none|null|other|-+)$')
    AND NOT REGEXP_LIKE(LOWER(TRIM(COALESCE(suburb_clean, suburb_raw))), '^(unknown|unk|n/?a|na|none|null|other|-+)$')

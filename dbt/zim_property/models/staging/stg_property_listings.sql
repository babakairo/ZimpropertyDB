{{
    config(
        materialized = 'view',
        tags         = ['staging', 'daily']
    )
}}

/*
  stg_property_listings
  ─────────────────────
  Lightly transforms RAW.ZW_PROPERTY_LISTINGS:
    • Casts types
    • Normalises null-ish strings to NULL
    • Parses listing_date
    • Standardises currency codes

  This view is intentionally thin — heavy logic belongs in intermediate/.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'zw_property_listings') }}
),

renamed AS (
    SELECT
        -- Keys
        listing_id,
        source                                      AS data_source,
        listing_url,

        -- Listing core
        NULLIF(TRIM(property_title), '')            AS property_title,
        property_price,
        UPPER(NULLIF(TRIM(currency), ''))           AS currency,
        LOWER(NULLIF(TRIM(property_type), ''))      AS property_type,
        LOWER(NULLIF(TRIM(listing_type), ''))       AS listing_type,

        -- Location
        CASE
          WHEN city IS NULL THEN NULL
          WHEN REGEXP_LIKE(
            LOWER(TRIM(city)),
            '^(unknown|unk|n/?a|na|none|null|not\\s*specified|not\\s*available|other|-+)$'
          ) THEN NULL
          ELSE INITCAP(NULLIF(TRIM(city), ''))
        END                                         AS city_raw,
        CASE
          WHEN suburb IS NULL THEN NULL
          WHEN REGEXP_LIKE(
            LOWER(TRIM(suburb)),
            '^(unknown|unk|n/?a|na|none|null|not\\s*specified|not\\s*available|other|-+)$'
          ) THEN NULL
          ELSE INITCAP(NULLIF(TRIM(suburb), ''))
        END                                         AS suburb_raw,
        CASE
          WHEN address_raw IS NULL THEN NULL
          WHEN REGEXP_LIKE(
            LOWER(TRIM(address_raw)),
            '^(unknown|unk|n/?a|na|none|null|not\\s*specified|not\\s*available|other|-+)$'
          ) THEN NULL
          ELSE NULLIF(TRIM(address_raw), '')
        END                                         AS address_raw,
        latitude,
        longitude,

        -- Attributes
        number_of_bedrooms,
        number_of_bathrooms,
        number_of_garages,
        property_size_sqm,
        NULLIF(TRIM(property_size_raw), '')         AS property_size_raw,
        stand_size_sqm,
        features,
        image_urls,

        -- Agent
        CASE
          WHEN agent_name IS NULL THEN NULL
          WHEN REGEXP_LIKE(
            LOWER(TRIM(agent_name)),
            '^(unknown|unk|n/?a|na|none|null|not\\s*specified|not\\s*available|other|-+)$'
          ) THEN NULL
          ELSE NULLIF(TRIM(agent_name), '')
        END                                         AS agent_name,
        CASE
          WHEN agent_phone IS NULL THEN NULL
          WHEN REGEXP_LIKE(
            LOWER(TRIM(agent_phone)),
            '^(unknown|unk|n/?a|na|none|null|not\\s*specified|not\\s*available|other|-+)$'
          ) THEN NULL
          ELSE NULLIF(REGEXP_REPLACE(TRIM(agent_phone), '^(tel:|phone:)', '', 1, 1, 'i'), '')
        END                                         AS agent_phone,
        CASE
          WHEN agent_email IS NULL THEN NULL
          WHEN REGEXP_LIKE(
            LOWER(TRIM(agent_email)),
            '^(unknown|unk|n/?a|na|none|null|not\\s*specified|not\\s*available|other|-+)$'
          ) THEN NULL
          ELSE NULLIF(LOWER(REGEXP_REPLACE(TRIM(agent_email), '^mailto:', '', 1, 1, 'i')), '')
        END                                         AS agent_email,
        CASE
          WHEN agency_name IS NULL THEN NULL
          WHEN REGEXP_LIKE(
            LOWER(TRIM(agency_name)),
            '^(unknown|unk|n/?a|na|none|null|not\\s*specified|not\\s*available|other|-+)$'
          ) THEN NULL
          ELSE NULLIF(TRIM(agency_name), '')
        END                                         AS agency_name,

        -- Dates
        TRY_TO_DATE(listing_date)                   AS listing_date,
        scraped_at,
        loaded_at

    FROM source
    WHERE listing_url IS NOT NULL
      AND listing_id  IS NOT NULL
)

SELECT * FROM renamed

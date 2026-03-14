{{
    config(
        materialized = 'view',
        tags         = ['staging', 'reference', 'excluded']
    )
}}

/*
  stg_wayback_archive
  ───────────────────
  Isolation view for web.archive.org (Wayback Machine) records.

  WHY THESE ARE EXCLUDED FROM ANALYTICS
  ──────────────────────────────────────
  web.archive.org records are historical crawls scraped from the Wayback
  Machine. They have:
    • 99.9% null property_price
    • 99.9% null city / suburb
    • 100% null number_of_bedrooms
    • Average DQ score: 0.3 / 1.0

  Including them in STAGING.CLEANED_PROPERTY_LISTINGS dragged the overall
  DQ score from ~0.75 to 0.561 and polluted every analytics query.

  WHAT THIS VIEW IS FOR
  ─────────────────────
  This view exists to:
    1. Document exactly what was excluded and why (auditable)
    2. Allow future revisiting if Wayback data is re-processed with better
       parsing logic to extract price/location from archived HTML
    3. Enable row-count checks: RAW total = STAGING total + wayback total

  DO NOT JOIN THIS VIEW INTO ANALYTICS MODELS.
*/

SELECT
    listing_id,
    source,
    listing_url,
    property_title,
    property_price,
    currency,
    property_type,
    listing_type,
    city,
    suburb,
    address_raw,
    number_of_bedrooms,
    number_of_bathrooms,
    scraped_at,
    -- Reason for exclusion (static label for lineage tracking)
    'wayback_machine_crawl' AS exclusion_reason
FROM {{ source('raw', 'zw_property_listings') }}
WHERE source LIKE '%archive.org%'
   OR source LIKE '%wayback%'

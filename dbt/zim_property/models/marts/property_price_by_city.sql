{{
    config(
        materialized = 'table',
        tags         = ['marts', 'daily']
    )
}}

/*
  property_price_by_city
  ──────────────────────
  City-level aggregation for country-wide price comparisons.
  Answers: "How does Harare compare to Bulawayo for 3-bed houses?"
*/

WITH base AS (
    SELECT * FROM {{ ref('int_cleaned_property_listings') }}
    WHERE is_price_valid = TRUE
      AND city_clean IS NOT NULL
      AND property_price_usd IS NOT NULL
),

aggregated AS (
    SELECT
        city_clean,
        COALESCE(property_type, 'unknown')  AS property_type,
        COALESCE(listing_type, 'unknown')   AS listing_type,

        DATE_TRUNC('MONTH', listing_date)   AS snapshot_month,

        COUNT(*)                            AS listing_count,
        ROUND(AVG(property_price_usd), 2)   AS avg_price_usd,
        ROUND(MEDIAN(property_price_usd), 2)AS median_price_usd,

        -- Separate sale vs rent in same row for easy comparison
        ROUND(AVG(CASE WHEN listing_type = 'sale' THEN property_price_usd END), 2)  AS avg_sale_price_usd,
        ROUND(AVG(CASE WHEN listing_type = 'rent' THEN property_price_usd END), 2)  AS avg_rent_usd,

        ROUND(AVG(price_per_sqm_usd), 2)    AS avg_price_per_sqm_usd,
        ROUND(AVG(number_of_bedrooms), 1)   AS avg_bedrooms

    FROM base
    WHERE listing_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    city_clean,
    property_type,
    listing_type,
    snapshot_month,
    listing_count,
    avg_price_usd,
    median_price_usd,
    avg_sale_price_usd,
    avg_rent_usd,
    avg_price_per_sqm_usd,
    avg_bedrooms,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM aggregated

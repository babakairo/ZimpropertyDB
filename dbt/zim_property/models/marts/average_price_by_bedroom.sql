{{
    config(
        materialized = 'table',
        tags         = ['marts', 'daily']
    )
}}

/*
  average_price_by_bedroom
  ────────────────────────
  Price breakdown by bedroom count per city.
  Answers: "What does a 4-bed house cost in Borrowdale vs Highlands?"
*/

WITH base AS (
    SELECT * FROM {{ ref('int_cleaned_property_listings') }}
    WHERE is_price_valid = TRUE
      AND number_of_bedrooms IS NOT NULL
      AND number_of_bedrooms BETWEEN 1 AND 10   -- sanity filter
      AND city_clean IS NOT NULL
      AND property_price_usd IS NOT NULL
),

aggregated AS (
    SELECT
        city_clean,
        suburb_clean,
        number_of_bedrooms,
        COALESCE(property_type, 'unknown')  AS property_type,
        COALESCE(listing_type, 'unknown')   AS listing_type,
        DATE_TRUNC('MONTH', listing_date)   AS snapshot_month,

        COUNT(*)                            AS listing_count,
        ROUND(AVG(property_price_usd), 2)   AS avg_price_usd,
        ROUND(MEDIAN(property_price_usd), 2)AS median_price_usd,
        ROUND(AVG(price_per_sqm_usd), 2)    AS avg_price_per_sqm_usd

    FROM base
    WHERE listing_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING COUNT(*) >= 2
)

SELECT
    city_clean,
    suburb_clean,
    number_of_bedrooms,
    property_type,
    listing_type,
    snapshot_month,
    listing_count,
    avg_price_usd,
    median_price_usd,
    avg_price_per_sqm_usd,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM aggregated
ORDER BY city_clean, suburb_clean, number_of_bedrooms

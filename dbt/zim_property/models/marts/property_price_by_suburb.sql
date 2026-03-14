{{
    config(
        materialized = 'table',
        tags         = ['marts', 'daily'],
        post_hook    = "COMMENT ON TABLE {{ this }} IS 'Monthly avg property price by suburb — refreshed daily'"
    )
}}

/*
  property_price_by_suburb
  ────────────────────────
  Pre-aggregated price metrics per suburb / month for BI dashboards.
  Answers: "What is the average 3-bed house price in Borrowdale this month?"
*/

WITH base AS (
    SELECT * FROM {{ ref('int_cleaned_property_listings') }}
    WHERE is_price_valid = TRUE
      AND suburb_clean IS NOT NULL
      AND city_clean IS NOT NULL
      AND property_price_usd IS NOT NULL
),

aggregated AS (
    SELECT
        suburb_clean,
        city_clean,
        COALESCE(property_type, 'unknown')  AS property_type,
        COALESCE(listing_type, 'unknown')   AS listing_type,

        DATE_TRUNC('MONTH', listing_date)   AS snapshot_month,

        COUNT(*)                            AS listing_count,
        ROUND(AVG(property_price_usd), 2)   AS avg_price_usd,
        ROUND(MEDIAN(property_price_usd), 2)AS median_price_usd,
        ROUND(MIN(property_price_usd), 2)   AS min_price_usd,
        ROUND(MAX(property_price_usd), 2)   AS max_price_usd,
        ROUND(STDDEV(property_price_usd), 2)AS stddev_price_usd,

        ROUND(AVG(price_per_sqm_usd), 2)    AS avg_price_per_sqm_usd,
        ROUND(AVG(number_of_bedrooms), 1)   AS avg_bedrooms,
        ROUND(AVG(property_size_sqm), 1)    AS avg_property_size_sqm

    FROM base
    WHERE listing_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
    HAVING COUNT(*) >= 3   -- Require at least 3 listings for a statistically meaningful average
)

SELECT
    suburb_clean,
    city_clean,
    property_type,
    listing_type,
    snapshot_month,
    listing_count,
    avg_price_usd,
    median_price_usd,
    min_price_usd,
    max_price_usd,
    stddev_price_usd,
    avg_price_per_sqm_usd,
    avg_bedrooms,
    avg_property_size_sqm,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM aggregated

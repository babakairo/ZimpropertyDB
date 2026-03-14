{{
    config(
        materialized  = 'incremental',
        unique_key    = ['trend_month', 'city_clean', 'property_type', 'listing_type'],
        incremental_strategy = 'merge',
        tags          = ['marts', 'daily', 'incremental']
    )
}}

/*
  monthly_price_trends
  ─────────────────────
  MoM and YoY price trends with rolling averages.
  Incremental model — only processes new months on each run.
*/

WITH monthly AS (
    SELECT
        DATE_TRUNC('MONTH', listing_date)   AS trend_month,
        city_clean,
        COALESCE(property_type, 'unknown')  AS property_type,
        COALESCE(listing_type, 'unknown')   AS listing_type,

        COUNT(*)                            AS listing_count,
        ROUND(AVG(property_price_usd), 2)   AS avg_price_usd,

        -- New listings vs total seen
        COUNT(DISTINCT listing_id)          AS new_listings_count,
        COUNT(DISTINCT listing_id)          AS total_active_listings

    FROM {{ ref('int_cleaned_property_listings') }}
    WHERE is_price_valid = TRUE
      AND property_price_usd IS NOT NULL
      AND listing_date IS NOT NULL
      AND city_clean IS NOT NULL

    {% if is_incremental() %}
    -- Incremental: only refresh last 3 months to catch late-arriving data
    AND DATE_TRUNC('MONTH', listing_date) >= DATE_TRUNC('MONTH', DATEADD('month', -3, CURRENT_DATE()))
    {% endif %}

    GROUP BY 1, 2, 3, 4
),

with_lag AS (
    SELECT
        *,
        LAG(avg_price_usd, 1)  OVER w AS prev_month_price,
        LAG(avg_price_usd, 12) OVER w AS prev_year_price,

        -- Rolling averages
        AVG(avg_price_usd) OVER (
            PARTITION BY city_clean, property_type, listing_type
            ORDER BY trend_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_avg_usd,

        AVG(avg_price_usd) OVER (
            PARTITION BY city_clean, property_type, listing_type
            ORDER BY trend_month
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS rolling_6m_avg_usd

    FROM monthly
    WINDOW w AS (
        PARTITION BY city_clean, property_type, listing_type
        ORDER BY trend_month
    )
)

SELECT
    trend_month,
    city_clean,
    property_type,
    listing_type,
    listing_count,
    avg_price_usd,

    CASE
        WHEN prev_month_price > 0
        THEN ROUND(((avg_price_usd - prev_month_price) / prev_month_price) * 100, 2)
    END AS mom_price_change_pct,

    CASE
        WHEN prev_year_price > 0
        THEN ROUND(((avg_price_usd - prev_year_price) / prev_year_price) * 100, 2)
    END AS yoy_price_change_pct,

    ROUND(rolling_3m_avg_usd, 2) AS rolling_3m_avg_usd,
    ROUND(rolling_6m_avg_usd, 2) AS rolling_6m_avg_usd,
    new_listings_count,
    total_active_listings,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM with_lag

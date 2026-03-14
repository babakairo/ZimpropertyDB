{{
    config(
        materialized = 'table',
        tags         = ['marts', 'daily']
    )
}}

/*
  suburb_price_growth
  ────────────────────
  Ranks suburbs by price growth over 3, 6, and 12 months.
  Answers: "Which suburbs have appreciated the most in the last year?"
  Used for the Investment Dashboard and Diaspora Analytics.
*/

WITH suburb_monthly AS (
    SELECT * FROM {{ ref('property_price_by_suburb') }}
    WHERE listing_count >= 3
),

current_month AS (
    SELECT * FROM suburb_monthly
    WHERE snapshot_month = DATE_TRUNC('MONTH', CURRENT_DATE())
),

-- Self-join to get historical snapshots for each suburb
with_history AS (
    SELECT
        c.suburb_clean,
        c.city_clean,
        c.property_type,
        c.listing_type,
        c.avg_price_usd                                     AS avg_price_current,
        c.listing_count                                     AS listing_count_current,

        m3.avg_price_usd                                    AS avg_price_3m_ago,
        m6.avg_price_usd                                    AS avg_price_6m_ago,
        m12.avg_price_usd                                   AS avg_price_12m_ago

    FROM current_month c

    LEFT JOIN suburb_monthly m3
        ON  c.suburb_clean   = m3.suburb_clean
        AND c.city_clean     = m3.city_clean
        AND c.property_type  = m3.property_type
        AND c.listing_type   = m3.listing_type
        AND m3.snapshot_month = DATEADD('MONTH', -3, DATE_TRUNC('MONTH', CURRENT_DATE()))

    LEFT JOIN suburb_monthly m6
        ON  c.suburb_clean   = m6.suburb_clean
        AND c.city_clean     = m6.city_clean
        AND c.property_type  = m6.property_type
        AND c.listing_type   = m6.listing_type
        AND m6.snapshot_month = DATEADD('MONTH', -6, DATE_TRUNC('MONTH', CURRENT_DATE()))

    LEFT JOIN suburb_monthly m12
        ON  c.suburb_clean   = m12.suburb_clean
        AND c.city_clean     = m12.city_clean
        AND c.property_type  = m12.property_type
        AND c.listing_type   = m12.listing_type
        AND m12.snapshot_month = DATEADD('MONTH', -12, DATE_TRUNC('MONTH', CURRENT_DATE()))
),

with_growth AS (
    SELECT
        *,
        CASE WHEN avg_price_3m_ago > 0
            THEN ROUND(((avg_price_current - avg_price_3m_ago)  / avg_price_3m_ago)  * 100, 2)
        END AS growth_3m_pct,

        CASE WHEN avg_price_6m_ago > 0
            THEN ROUND(((avg_price_current - avg_price_6m_ago)  / avg_price_6m_ago)  * 100, 2)
        END AS growth_6m_pct,

        CASE WHEN avg_price_12m_ago > 0
            THEN ROUND(((avg_price_current - avg_price_12m_ago) / avg_price_12m_ago) * 100, 2)
        END AS growth_12m_pct

    FROM with_history
)

SELECT
    suburb_clean,
    city_clean,
    property_type,
    listing_type,
    avg_price_current               AS avg_price_current_month_usd,
    avg_price_3m_ago                AS avg_price_3m_ago_usd,
    avg_price_6m_ago                AS avg_price_6m_ago_usd,
    avg_price_12m_ago               AS avg_price_12m_ago_usd,
    growth_3m_pct,
    growth_6m_pct,
    growth_12m_pct,
    listing_count_current,
    RANK() OVER (
        PARTITION BY property_type, listing_type
        ORDER BY COALESCE(growth_12m_pct, -9999) DESC
    )                               AS rank_by_growth_12m,
    CURRENT_DATE()                  AS snapshot_date,
    CURRENT_TIMESTAMP()             AS dbt_updated_at
FROM with_growth
WHERE listing_type = 'sale'   -- Growth analysis is most meaningful for sales

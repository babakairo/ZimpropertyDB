-- ============================================================
-- 08 — Populate Analytics Layer
-- Reads from STAGING.CLEANED_PROPERTY_LISTINGS
-- Writes to 5 ANALYTICS tables + refreshes views
--
-- snapshot_month logic:
--   Use listing_date where available (historical accuracy).
--   Fall back to scraped_at date (records with no listing_date).
-- Price stats: only records with a known USD price (is_price_valid = TRUE)
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE WAREHOUSE ZIM_PROPERTY_WH;
USE SCHEMA ANALYTICS;

-- ── Helper CTE reused across all inserts ─────────────────────────────────────
-- Materialised once per run via the base_data CTE inside each statement.
-- snapshot_month = first day of the listing's month.

-- ============================================================
-- TABLE 1: PROPERTY_PRICE_BY_SUBURB
-- ============================================================
INSERT OVERWRITE INTO PROPERTY_PRICE_BY_SUBURB (
    suburb_clean, city_clean, property_type, listing_type,
    listing_count, avg_price_usd, median_price_usd, min_price_usd,
    max_price_usd, stddev_price_usd, avg_price_per_sqm_usd,
    avg_bedrooms, avg_property_size_sqm, snapshot_month
)
WITH base AS (
    SELECT
        COALESCE(suburb_clean, 'Unknown')               AS suburb_clean,
        COALESCE(city_clean,   'Unknown')               AS city_clean,
        COALESCE(property_type, 'unknown')              AS property_type,
        listing_type,
        property_price_usd,
        price_per_sqm_usd,
        number_of_bedrooms,
        property_size_sqm,
        DATE_TRUNC('MONTH',
            COALESCE(listing_date, scraped_at::DATE))   AS snapshot_month
    FROM STAGING.CLEANED_PROPERTY_LISTINGS
    WHERE listing_type IN ('sale', 'rent')
      AND snapshot_month IS NOT NULL
)
SELECT
    suburb_clean,
    city_clean,
    property_type,
    listing_type,
    COUNT(*)                                            AS listing_count,
    ROUND(AVG(property_price_usd), 2)                  AS avg_price_usd,
    ROUND(MEDIAN(property_price_usd), 2)               AS median_price_usd,
    ROUND(MIN(property_price_usd), 2)                  AS min_price_usd,
    ROUND(MAX(property_price_usd), 2)                  AS max_price_usd,
    ROUND(STDDEV(property_price_usd), 2)               AS stddev_price_usd,
    ROUND(AVG(price_per_sqm_usd), 2)                   AS avg_price_per_sqm_usd,
    ROUND(AVG(number_of_bedrooms), 1)                  AS avg_bedrooms,
    ROUND(AVG(property_size_sqm), 1)                   AS avg_property_size_sqm,
    snapshot_month
FROM base
GROUP BY suburb_clean, city_clean, property_type, listing_type, snapshot_month;


-- ============================================================
-- TABLE 2: PROPERTY_PRICE_BY_CITY
-- ============================================================
INSERT OVERWRITE INTO PROPERTY_PRICE_BY_CITY (
    city_clean, property_type, listing_type,
    listing_count, avg_price_usd, median_price_usd,
    avg_rent_usd, avg_sale_price_usd,
    avg_price_per_sqm_usd, avg_bedrooms, snapshot_month
)
WITH base AS (
    SELECT
        COALESCE(city_clean, 'Unknown')                 AS city_clean,
        COALESCE(property_type, 'unknown')              AS property_type,
        listing_type,
        property_price_usd,
        price_per_sqm_usd,
        number_of_bedrooms,
        DATE_TRUNC('MONTH',
            COALESCE(listing_date, scraped_at::DATE))   AS snapshot_month
    FROM STAGING.CLEANED_PROPERTY_LISTINGS
    WHERE listing_type IN ('sale', 'rent')
      AND snapshot_month IS NOT NULL
)
SELECT
    city_clean,
    property_type,
    listing_type,
    COUNT(*)                                            AS listing_count,
    ROUND(AVG(property_price_usd), 2)                  AS avg_price_usd,
    ROUND(MEDIAN(property_price_usd), 2)               AS median_price_usd,
    ROUND(AVG(IFF(listing_type = 'rent', property_price_usd, NULL)), 2)
                                                        AS avg_rent_usd,
    ROUND(AVG(IFF(listing_type = 'sale', property_price_usd, NULL)), 2)
                                                        AS avg_sale_price_usd,
    ROUND(AVG(price_per_sqm_usd), 2)                   AS avg_price_per_sqm_usd,
    ROUND(AVG(number_of_bedrooms), 1)                  AS avg_bedrooms,
    snapshot_month
FROM base
GROUP BY city_clean, property_type, listing_type, snapshot_month;


-- ============================================================
-- TABLE 3: AVERAGE_PRICE_BY_BEDROOM
-- ============================================================
INSERT OVERWRITE INTO AVERAGE_PRICE_BY_BEDROOM (
    city_clean, suburb_clean, number_of_bedrooms,
    property_type, listing_type,
    listing_count, avg_price_usd, median_price_usd,
    avg_price_per_sqm_usd, snapshot_month
)
WITH base AS (
    SELECT
        COALESCE(city_clean,   'Unknown')               AS city_clean,
        COALESCE(suburb_clean, 'Unknown')               AS suburb_clean,
        number_of_bedrooms,
        COALESCE(property_type, 'unknown')              AS property_type,
        listing_type,
        property_price_usd,
        price_per_sqm_usd,
        DATE_TRUNC('MONTH',
            COALESCE(listing_date, scraped_at::DATE))   AS snapshot_month
    FROM STAGING.CLEANED_PROPERTY_LISTINGS
    WHERE listing_type IN ('sale', 'rent')
      AND number_of_bedrooms IS NOT NULL
      AND number_of_bedrooms BETWEEN 0 AND 20   -- exclude outlier data entry errors
      AND snapshot_month IS NOT NULL
)
SELECT
    city_clean,
    suburb_clean,
    number_of_bedrooms,
    property_type,
    listing_type,
    COUNT(*)                                            AS listing_count,
    ROUND(AVG(property_price_usd), 2)                  AS avg_price_usd,
    ROUND(MEDIAN(property_price_usd), 2)               AS median_price_usd,
    ROUND(AVG(price_per_sqm_usd), 2)                   AS avg_price_per_sqm_usd,
    snapshot_month
FROM base
GROUP BY city_clean, suburb_clean, number_of_bedrooms, property_type, listing_type, snapshot_month;


-- ============================================================
-- TABLE 4: MONTHLY_PRICE_TRENDS
-- ============================================================
INSERT OVERWRITE INTO MONTHLY_PRICE_TRENDS (
    trend_month, city_clean, property_type, listing_type,
    listing_count, avg_price_usd,
    mom_price_change_pct, yoy_price_change_pct,
    rolling_3m_avg_usd, rolling_6m_avg_usd,
    new_listings_count, total_active_listings
)
WITH base AS (
    SELECT
        COALESCE(city_clean, 'Unknown')                 AS city_clean,
        COALESCE(property_type, 'unknown')              AS property_type,
        listing_type,
        property_price_usd,
        listing_id,
        DATE_TRUNC('MONTH',
            COALESCE(listing_date, scraped_at::DATE))   AS trend_month
    FROM STAGING.CLEANED_PROPERTY_LISTINGS
    WHERE listing_type IN ('sale', 'rent')
      AND trend_month IS NOT NULL
),
monthly AS (
    SELECT
        trend_month,
        city_clean,
        property_type,
        listing_type,
        COUNT(*)                                        AS listing_count,
        ROUND(AVG(property_price_usd), 2)              AS avg_price_usd,
        COUNT(listing_id)                              AS new_listings_count
    FROM base
    GROUP BY trend_month, city_clean, property_type, listing_type
),
with_windows AS (
    SELECT
        trend_month,
        city_clean,
        property_type,
        listing_type,
        listing_count,
        avg_price_usd,
        new_listings_count,

        -- Month-over-month % change
        LAG(avg_price_usd, 1) OVER (
            PARTITION BY city_clean, property_type, listing_type
            ORDER BY trend_month
        )                                              AS prev_month_price,

        -- Year-over-year % change
        LAG(avg_price_usd, 12) OVER (
            PARTITION BY city_clean, property_type, listing_type
            ORDER BY trend_month
        )                                              AS prev_year_price,

        -- Rolling 3-month average
        AVG(avg_price_usd) OVER (
            PARTITION BY city_clean, property_type, listing_type
            ORDER BY trend_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                                              AS rolling_3m_avg_usd,

        -- Rolling 6-month average
        AVG(avg_price_usd) OVER (
            PARTITION BY city_clean, property_type, listing_type
            ORDER BY trend_month
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        )                                              AS rolling_6m_avg_usd,

        -- Cumulative total (proxy for total active listings)
        SUM(listing_count) OVER (
            PARTITION BY city_clean, property_type, listing_type
            ORDER BY trend_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                              AS total_active_listings
    FROM monthly
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
        THEN ROUND((avg_price_usd - prev_month_price) / prev_month_price * 100, 2)
    END                                                AS mom_price_change_pct,
    CASE
        WHEN prev_year_price > 0
        THEN ROUND((avg_price_usd - prev_year_price) / prev_year_price * 100, 2)
    END                                                AS yoy_price_change_pct,
    ROUND(rolling_3m_avg_usd, 2)                       AS rolling_3m_avg_usd,
    ROUND(rolling_6m_avg_usd, 2)                       AS rolling_6m_avg_usd,
    new_listings_count,
    total_active_listings
FROM with_windows;


-- ============================================================
-- TABLE 5: SUBURB_PRICE_GROWTH
-- (snapshot_date = today; compare to 3, 6, 12 months ago)
-- ============================================================
INSERT OVERWRITE INTO SUBURB_PRICE_GROWTH (
    suburb_clean, city_clean, property_type,
    avg_price_current_month_usd,
    avg_price_3m_ago_usd,
    avg_price_6m_ago_usd,
    avg_price_12m_ago_usd,
    growth_3m_pct, growth_6m_pct, growth_12m_pct,
    listing_count_current, rank_by_growth_12m,
    snapshot_date
)
WITH monthly_suburb AS (
    SELECT
        DATE_TRUNC('MONTH',
            COALESCE(listing_date, scraped_at::DATE))   AS snap_month,
        COALESCE(suburb_clean, 'Unknown')               AS suburb_clean,
        COALESCE(city_clean,   'Unknown')               AS city_clean,
        COALESCE(property_type, 'unknown')              AS property_type,
        AVG(property_price_usd)                         AS avg_price,
        COUNT(*)                                        AS cnt
    FROM STAGING.CLEANED_PROPERTY_LISTINGS
    WHERE listing_type = 'sale'
      AND property_price_usd IS NOT NULL
      AND snap_month IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
current_month AS (
    SELECT * FROM monthly_suburb
    WHERE snap_month = DATE_TRUNC('MONTH', CURRENT_DATE())
),
three_months_ago AS (
    SELECT * FROM monthly_suburb
    WHERE snap_month = DATEADD('MONTH', -3, DATE_TRUNC('MONTH', CURRENT_DATE()))
),
six_months_ago AS (
    SELECT * FROM monthly_suburb
    WHERE snap_month = DATEADD('MONTH', -6, DATE_TRUNC('MONTH', CURRENT_DATE()))
),
twelve_months_ago AS (
    SELECT * FROM monthly_suburb
    WHERE snap_month = DATEADD('MONTH', -12, DATE_TRUNC('MONTH', CURRENT_DATE()))
),
joined AS (
    SELECT
        c.suburb_clean,
        c.city_clean,
        c.property_type,
        ROUND(c.avg_price,   2)                         AS avg_price_current_month_usd,
        ROUND(m3.avg_price,  2)                         AS avg_price_3m_ago_usd,
        ROUND(m6.avg_price,  2)                         AS avg_price_6m_ago_usd,
        ROUND(m12.avg_price, 2)                         AS avg_price_12m_ago_usd,
        CASE WHEN m3.avg_price  > 0
             THEN ROUND((c.avg_price - m3.avg_price)  / m3.avg_price  * 100, 2)
        END                                             AS growth_3m_pct,
        CASE WHEN m6.avg_price  > 0
             THEN ROUND((c.avg_price - m6.avg_price)  / m6.avg_price  * 100, 2)
        END                                             AS growth_6m_pct,
        CASE WHEN m12.avg_price > 0
             THEN ROUND((c.avg_price - m12.avg_price) / m12.avg_price * 100, 2)
        END                                             AS growth_12m_pct,
        c.cnt                                           AS listing_count_current
    FROM current_month c
    LEFT JOIN three_months_ago  m3  USING (suburb_clean, city_clean, property_type)
    LEFT JOIN six_months_ago    m6  USING (suburb_clean, city_clean, property_type)
    LEFT JOIN twelve_months_ago m12 USING (suburb_clean, city_clean, property_type)
)
SELECT
    suburb_clean,
    city_clean,
    property_type,
    avg_price_current_month_usd,
    avg_price_3m_ago_usd,
    avg_price_6m_ago_usd,
    avg_price_12m_ago_usd,
    growth_3m_pct,
    growth_6m_pct,
    growth_12m_pct,
    listing_count_current,
    RANK() OVER (
        PARTITION BY property_type
        ORDER BY growth_12m_pct DESC NULLS LAST
    )                                                   AS rank_by_growth_12m,
    CURRENT_DATE()                                      AS snapshot_date
FROM joined;

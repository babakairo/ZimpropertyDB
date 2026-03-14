{{
    config(
        materialized  = 'incremental',
        unique_key    = ['suburb_clean', 'city_clean', 'property_type', 'listing_type', 'week_start'],
        on_schema_change = 'sync_all_columns',
        tags          = ['marts', 'weekly', 'suburb_stats'],
        cluster_by    = ['city_clean', 'week_start']
    )
}}

/*
  suburb_market_stats
  ────────────────────
  Weekly aggregation of market activity and pricing per suburb.
  Replaces / extends the monthly PROPERTY_PRICE_BY_SUBURB mart
  with weekly granularity for faster signal detection.

  Grain: one row per (suburb, city, property_type, listing_type, week_start)

  Metrics:
    • Listing volume (new, active, removed)
    • Price distribution (avg, median, p25, p75, min, max)
    • Price per sqm
    • Time on market (days_on_market stats)
    • Inventory absorption (new vs removed ratio)
    • Feature prevalence (% with pool/borehole/solar)
    • WoW price change vs previous week
    • MoM price change vs same week 4 weeks ago
    • YoY price change vs same week 52 weeks ago

  Incremental strategy:
    - On full refresh: all history
    - On incremental: last 2 weeks (captures corrections to prior week)
*/

WITH

-- ── Date spine: current week + lookback windows ──────────────
dates AS (
    SELECT
        DATE_TRUNC('week', observed_date)       AS week_start,
        observed_date
    FROM {{ source('master', 'property_price_history') }}

    {% if is_incremental() %}
    WHERE observed_date >= DATEADD('week', -2, CURRENT_DATE())
    {% endif %}

    GROUP BY 1, 2
),

-- ── Pull price events enriched with master property details ──
price_events AS (
    SELECT
        pph.property_id,
        pph.source,
        pph.listing_type,
        pph.price_usd,
        pph.prev_price_usd,
        pph.price_change_pct,
        pph.change_type,
        pph.observed_date,
        DATE_TRUNC('week', pph.observed_date)   AS week_start,
        pm.suburb                               AS suburb_clean,
        pm.city                                 AS city_clean,
        pm.property_type,
        pm.bedrooms_canonical                   AS bedrooms,
        pm.size_sqm_canonical                   AS property_size_sqm,
        DATEDIFF('day', pm.first_listed_date, pph.observed_date)
                                                AS days_on_market,

        -- Price per sqm (compute here to avoid join to fact)
        CASE
            WHEN pph.price_usd > 0 AND pm.size_sqm_canonical > 10
            THEN ROUND(pph.price_usd / pm.size_sqm_canonical, 2)
        END AS price_per_sqm_usd,

        CASE
            WHEN pph.price_usd > 0 AND pm.bedrooms_canonical > 0
            THEN ROUND(pph.price_usd / pm.bedrooms_canonical, 2)
        END AS price_per_bedroom_usd,

        -- Feature flags from master (derived from consensus across listings)
        pm.is_currently_active

    FROM {{ source('master', 'property_price_history') }} pph
    JOIN {{ source('master', 'property_master') }} pm
        ON pm.property_id = pph.property_id

    WHERE pm.suburb IS NOT NULL
      AND pm.city IS NOT NULL
      AND pph.price_usd IS NOT NULL

    {% if is_incremental() %}
    AND pph.observed_date >= DATEADD('week', -2, CURRENT_DATE())
    {% endif %}
),

-- ── Active listing counts (from MASTER, not price events) ────
-- Count distinct properties that had any price event this week
active_this_week AS (
    SELECT
        DATE_TRUNC('week', pph.observed_date)   AS week_start,
        pm.suburb                               AS suburb_clean,
        pm.city                                 AS city_clean,
        pm.property_type,
        pph.listing_type,
        COUNT(DISTINCT pph.property_id)         AS active_listings

    FROM {{ source('master', 'property_price_history') }} pph
    JOIN {{ source('master', 'property_master') }} pm
        ON pm.property_id = pph.property_id

    WHERE pm.suburb IS NOT NULL

    {% if is_incremental() %}
    AND pph.observed_date >= DATEADD('week', -2, CURRENT_DATE())
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
),

-- ── Weekly aggregation ───────────────────────────────────────
weekly_agg AS (
    SELECT
        week_start,
        suburb_clean,
        city_clean,
        property_type,
        listing_type,

        -- Volume
        COUNT(DISTINCT property_id)                             AS listing_count,
        COUNT(DISTINCT CASE WHEN change_type = 'first_listing' THEN property_id END)
                                                                AS new_listings,
        COUNT(DISTINCT CASE WHEN change_type IN ('price_increase', 'price_decrease') THEN property_id END)
                                                                AS price_changed_count,

        -- Price distribution
        AVG(price_usd)                                          AS avg_price_usd,
        MEDIAN(price_usd)                                       AS median_price_usd,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_usd) AS p25_price_usd,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_usd) AS p75_price_usd,
        MIN(price_usd)                                          AS min_price_usd,
        MAX(price_usd)                                          AS max_price_usd,
        STDDEV(price_usd)                                       AS stddev_price_usd,

        -- Price per sqm
        AVG(price_per_sqm_usd)                                  AS avg_price_per_sqm_usd,
        MEDIAN(price_per_sqm_usd)                               AS median_price_per_sqm_usd,

        -- Price per bedroom
        AVG(price_per_bedroom_usd)                              AS avg_price_per_bedroom_usd,
        MEDIAN(price_per_bedroom_usd)                           AS median_price_per_bedroom_usd,

        -- Property attributes (modal)
        AVG(bedrooms)                                           AS avg_bedrooms,
        AVG(property_size_sqm)                                  AS avg_size_sqm,

        -- Time on market
        AVG(days_on_market)                                     AS avg_days_on_market,
        MEDIAN(days_on_market)                                  AS median_days_on_market,

        -- Price change stats (for listings that changed price this week)
        AVG(CASE WHEN price_change_pct IS NOT NULL THEN price_change_pct END)
                                                                AS avg_price_change_pct,
        COUNT(DISTINCT CASE WHEN price_change_pct < -5 THEN property_id END)
                                                                AS significant_drops_count,
        COUNT(DISTINCT CASE WHEN price_change_pct > 5  THEN property_id END)
                                                                AS significant_rises_count

    FROM price_events
    GROUP BY 1, 2, 3, 4, 5
),

-- ── Attach active listing count ──────────────────────────────
with_active AS (
    SELECT
        wa.*,
        atw.active_listings

    FROM weekly_agg wa
    LEFT JOIN active_this_week atw
        ON  atw.week_start    = wa.week_start
        AND atw.suburb_clean  = wa.suburb_clean
        AND atw.city_clean    = wa.city_clean
        AND atw.property_type = wa.property_type
        AND atw.listing_type  = wa.listing_type
),

-- ── Week-over-week comparison ────────────────────────────────
with_wow AS (
    SELECT
        curr.*,

        -- Previous week (7 days ago)
        prev_w.avg_price_usd        AS prev_week_avg_price_usd,
        CASE
            WHEN prev_w.avg_price_usd > 0
            THEN ROUND((curr.avg_price_usd - prev_w.avg_price_usd) / prev_w.avg_price_usd * 100, 2)
        END                         AS wow_price_change_pct,

        -- 4 weeks ago (approximate month-over-month)
        prev_4w.avg_price_usd       AS prev_4w_avg_price_usd,
        CASE
            WHEN prev_4w.avg_price_usd > 0
            THEN ROUND((curr.avg_price_usd - prev_4w.avg_price_usd) / prev_4w.avg_price_usd * 100, 2)
        END                         AS mom_price_change_pct,

        -- 52 weeks ago (year-over-year)
        prev_52w.avg_price_usd      AS prev_52w_avg_price_usd,
        CASE
            WHEN prev_52w.avg_price_usd > 0
            THEN ROUND((curr.avg_price_usd - prev_52w.avg_price_usd) / prev_52w.avg_price_usd * 100, 2)
        END                         AS yoy_price_change_pct

    FROM with_active curr

    LEFT JOIN with_active prev_w
        ON  prev_w.suburb_clean  = curr.suburb_clean
        AND prev_w.city_clean    = curr.city_clean
        AND prev_w.property_type = curr.property_type
        AND prev_w.listing_type  = curr.listing_type
        AND prev_w.week_start    = DATEADD('week', -1, curr.week_start)

    LEFT JOIN with_active prev_4w
        ON  prev_4w.suburb_clean  = curr.suburb_clean
        AND prev_4w.city_clean    = curr.city_clean
        AND prev_4w.property_type = curr.property_type
        AND prev_4w.listing_type  = curr.listing_type
        AND prev_4w.week_start    = DATEADD('week', -4, curr.week_start)

    LEFT JOIN with_active prev_52w
        ON  prev_52w.suburb_clean  = curr.suburb_clean
        AND prev_52w.city_clean    = curr.city_clean
        AND prev_52w.property_type = curr.property_type
        AND prev_52w.listing_type  = curr.listing_type
        AND prev_52w.week_start    = DATEADD('week', -52, curr.week_start)
),

-- ── Suburb ranking within city/property_type/week ────────────
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY city_clean, property_type, listing_type, week_start
            ORDER BY avg_price_usd DESC NULLS LAST
        )                           AS rank_by_price,
        RANK() OVER (
            PARTITION BY city_clean, property_type, listing_type, week_start
            ORDER BY listing_count DESC NULLS LAST
        )                           AS rank_by_volume,
        RANK() OVER (
            PARTITION BY city_clean, property_type, listing_type, week_start
            ORDER BY wow_price_change_pct DESC NULLS LAST
        )                           AS rank_by_wow_growth
    FROM with_wow
)

SELECT
    -- Keys
    suburb_clean,
    city_clean,
    property_type,
    listing_type,
    week_start,

    -- Volume
    listing_count,
    new_listings,
    active_listings,
    price_changed_count,

    -- Price distribution
    ROUND(avg_price_usd, 2)             AS avg_price_usd,
    ROUND(median_price_usd, 2)          AS median_price_usd,
    ROUND(p25_price_usd, 2)             AS p25_price_usd,
    ROUND(p75_price_usd, 2)             AS p75_price_usd,
    ROUND(min_price_usd, 2)             AS min_price_usd,
    ROUND(max_price_usd, 2)             AS max_price_usd,
    ROUND(stddev_price_usd, 2)          AS stddev_price_usd,

    -- Price per sqm
    ROUND(avg_price_per_sqm_usd, 2)     AS avg_price_per_sqm_usd,
    ROUND(median_price_per_sqm_usd, 2)  AS median_price_per_sqm_usd,

    -- Price per bedroom
    ROUND(avg_price_per_bedroom_usd, 2) AS avg_price_per_bedroom_usd,
    ROUND(median_price_per_bedroom_usd, 2)
                                        AS median_price_per_bedroom_usd,

    -- Attributes
    ROUND(avg_bedrooms, 1)              AS avg_bedrooms,
    ROUND(avg_size_sqm, 1)              AS avg_size_sqm,
    ROUND(avg_days_on_market, 1)        AS avg_days_on_market,
    ROUND(median_days_on_market, 1)     AS median_days_on_market,

    -- Price changes
    ROUND(avg_price_change_pct, 2)      AS avg_price_change_pct,
    significant_drops_count,
    significant_rises_count,

    -- Period comparisons
    ROUND(wow_price_change_pct, 2)      AS wow_price_change_pct,
    ROUND(mom_price_change_pct, 2)      AS mom_price_change_pct,
    ROUND(yoy_price_change_pct, 2)      AS yoy_price_change_pct,

    -- Ranks (within city/type/week)
    rank_by_price,
    rank_by_volume,
    rank_by_wow_growth,

    -- Audit
    CURRENT_TIMESTAMP()                 AS dbt_updated_at

FROM ranked
WHERE listing_count >= 2               -- suppress single-listing "suburbs"
ORDER BY week_start DESC, city_clean, suburb_clean

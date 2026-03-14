{{
    config(
        materialized  = 'incremental',
        unique_key    = 'price_event_id_natural',
        on_schema_change = 'sync_all_columns',
        tags          = ['intermediate', 'price_history', 'daily'],
        post_hook     = [
            "DELETE FROM {{ source('master', 'property_price_history') }}
             WHERE inserted_by = 'dbt:int_price_changes'
               AND inserted_at < DATEADD('day', -3, CURRENT_TIMESTAMP())",
        ]
    )
}}

/*
  int_price_changes
  ─────────────────
  Incremental model: compares today's prices against the last known price
  per property_id and emits one row only when the price has changed
  (or when a property appears for the first time).

  Output is written to MASTER.PROPERTY_PRICE_HISTORY via a post-hook
  INSERT in the Dagster pipeline (or can be materialised as a table here
  and synced via the loader).

  Grain: one row per (property_id, source, observed_date) where
         a price change occurred.

  Incremental strategy:
    - On full refresh: processes all FACT_LISTINGS records
    - On incremental: processes only listings scraped in the last 3 days
      (3-day lookback handles weekend gaps and late-arriving data)
*/

WITH fact AS (
    SELECT
        fl.listing_id,
        fl.property_key,
        fl.source_key,
        fl.location_key,
        fl.property_type_key,
        fl.listing_type,
        fl.property_price_usd,
        fl.property_price_zwl,
        fl.currency_original,
        fl.number_of_bedrooms,
        fl.scraped_at::DATE         AS observed_date,
        fl.is_price_valid

    FROM {{ source('warehouse', 'fact_listings') }} fl

    WHERE fl.is_price_valid = TRUE
      AND fl.property_key IS NOT NULL        -- only matched listings
      AND fl.property_price_usd IS NOT NULL

    {% if is_incremental() %}
      -- Incremental: only new scrapes from the last 3 days
      AND fl.scraped_at >= DATEADD('day', -3, CURRENT_TIMESTAMP())
    {% endif %}
),

-- Resolve dimension labels for denormalisation
dim_prop AS (
    SELECT property_key, master_property_id FROM {{ source('warehouse', 'dim_property') }}
),

dim_loc AS (
    SELECT location_key, suburb_clean, city_clean FROM {{ source('warehouse', 'dim_location') }}
),

dim_src AS (
    SELECT source_key, source FROM {{ source('warehouse', 'dim_source') }}
),

dim_ptype AS (
    SELECT property_type_key, property_type FROM {{ source('warehouse', 'dim_property_type') }}
),

-- Enrich fact with dim labels
enriched AS (
    SELECT
        f.listing_id,
        dp.master_property_id       AS property_id,
        ds.source,
        f.listing_type,
        f.property_price_usd,
        f.property_price_zwl,
        f.currency_original,
        f.number_of_bedrooms,
        f.observed_date,
        dl.suburb_clean             AS suburb,
        dl.city_clean               AS city,
        dpt.property_type

    FROM fact f
    JOIN dim_prop   dp  ON dp.property_key      = f.property_key
    JOIN dim_loc    dl  ON dl.location_key       = f.location_key
    JOIN dim_src    ds  ON ds.source_key         = f.source_key
    JOIN dim_ptype  dpt ON dpt.property_type_key = f.property_type_key
),

-- Last known price per property+source from the price history table
last_known AS (
    SELECT
        property_id,
        source,
        price_usd                   AS last_price_usd,
        observed_date               AS last_observed_date
    FROM {{ source('master', 'property_price_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id, source ORDER BY observed_date DESC) = 1
),

-- Join current prices against last known — detect changes
compared AS (
    SELECT
        e.listing_id,
        e.property_id,
        e.source,
        e.listing_type,
        e.property_price_usd        AS price_usd,
        e.property_price_zwl        AS price_zwl,
        e.currency_original,
        e.observed_date,
        e.suburb,
        e.city,
        e.property_type,
        e.number_of_bedrooms        AS bedrooms,

        lk.last_price_usd           AS prev_price_usd,
        lk.last_observed_date       AS prev_observed_date,

        -- Change metrics
        e.property_price_usd - COALESCE(lk.last_price_usd, 0)  AS price_change_usd,

        CASE
            WHEN lk.last_price_usd IS NULL OR lk.last_price_usd = 0
            THEN NULL
            ELSE ROUND(
                (e.property_price_usd - lk.last_price_usd) / lk.last_price_usd * 100,
                2
            )
        END                         AS price_change_pct,

        CASE
            WHEN lk.last_observed_date IS NOT NULL
            THEN DATEDIFF('day', lk.last_observed_date, e.observed_date)
            ELSE NULL
        END                         AS days_since_prev,

        -- Change type classification
        CASE
            WHEN lk.last_price_usd IS NULL                             THEN 'first_listing'
            WHEN e.property_price_usd > lk.last_price_usd * 1.001     THEN 'price_increase'
            WHEN e.property_price_usd < lk.last_price_usd * 0.999     THEN 'price_decrease'
            WHEN DATEDIFF('day', lk.last_observed_date, e.observed_date) > 30
            THEN 'relisted'                                            -- gone then came back
            ELSE 'unchanged'
        END                         AS change_type

    FROM enriched e
    LEFT JOIN last_known lk
        ON lk.property_id = e.property_id
        AND lk.source     = e.source
),

-- Only keep rows where something changed (or first appearance)
price_events AS (
    SELECT
        -- Natural key for incremental dedup
        {{ dbt_utils.generate_surrogate_key(['property_id', 'source', 'observed_date']) }}
                                    AS price_event_id_natural,
        *
    FROM compared
    WHERE change_type != 'unchanged'
)

SELECT * FROM price_events

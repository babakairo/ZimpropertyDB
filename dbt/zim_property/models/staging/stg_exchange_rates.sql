{{
    config(
        materialized = 'view',
        tags         = ['staging', 'reference']
    )
}}

/*
  stg_exchange_rates
  ──────────────────
  Staging view for ZWL/USD exchange rates.
  Fills forward-fill gaps using LAST_VALUE window function.
*/

WITH source AS (
    SELECT * FROM {{ source('staging', 'zwl_usd_exchange_rates') }}
),

spine AS (
    -- Generate a date spine from first rate date to today
    SELECT DATEADD('day', SEQ4(), '2024-01-01'::DATE) AS rate_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1000))
    WHERE DATEADD('day', SEQ4(), '2024-01-01'::DATE) <= CURRENT_DATE()
),

joined AS (
    SELECT
        s.rate_date,
        r.zwl_per_usd,
        r.source
    FROM spine s
    LEFT JOIN source r ON r.rate_date = s.rate_date
),

filled AS (
    SELECT
        rate_date,
        LAST_VALUE(zwl_per_usd IGNORE NULLS)
            OVER (ORDER BY rate_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS zwl_per_usd,
        LAST_VALUE(source IGNORE NULLS)
            OVER (ORDER BY rate_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS rate_source
    FROM joined
)

SELECT
    rate_date,
    zwl_per_usd,
    1.0 / NULLIF(zwl_per_usd, 0) AS usd_per_zwl,
    rate_source
FROM filled
WHERE zwl_per_usd IS NOT NULL

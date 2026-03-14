-- Fail when any recent listing in analytics-ready layer has missing/unknown suburb or city
WITH stats AS (
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN city_clean IS NULL THEN 1 ELSE 0 END) AS missing_city,
        SUM(CASE WHEN suburb_clean IS NULL THEN 1 ELSE 0 END) AS missing_suburb,
        SUM(CASE
            WHEN city_clean IS NOT NULL
             AND REGEXP_LIKE(LOWER(TRIM(city_clean)), '^(unknown|unk|n/?a|na|none|null|other|-+)$')
            THEN 1 ELSE 0 END
        ) AS unknown_city,
        SUM(CASE
            WHEN suburb_clean IS NOT NULL
             AND REGEXP_LIKE(LOWER(TRIM(suburb_clean)), '^(unknown|unk|n/?a|na|none|null|other|-+)$')
            THEN 1 ELSE 0 END
        ) AS unknown_suburb
    FROM {{ ref('int_cleaned_property_listings') }}
    WHERE scraped_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT
    total,
    missing_city,
    missing_suburb,
    unknown_city,
    unknown_suburb
FROM stats
WHERE (missing_city + missing_suburb + unknown_city + unknown_suburb) > 0

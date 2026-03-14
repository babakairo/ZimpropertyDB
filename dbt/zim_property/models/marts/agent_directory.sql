{{
    config(
        materialized = 'view',
        tags         = ['mart', 'reporting', 'agents']
    )
}}

SELECT
    agent_name,
    agency_name,
    agent_phone,
    agent_email,
    COUNT(*) AS active_listing_count,
    COUNT(DISTINCT city_clean) AS cities_covered,
    COUNT(DISTINCT suburb_clean) AS suburbs_covered,
    MIN(listing_date) AS earliest_listing_date,
    MAX(scraped_at) AS last_seen_at,
    ROUND(AVG(property_price_usd), 0) AS avg_listing_price_usd
FROM {{ ref('current_agent_listings') }}
GROUP BY 1, 2, 3, 4

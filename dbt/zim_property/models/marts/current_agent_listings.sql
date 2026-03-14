{{
    config(
        materialized = 'view',
        tags         = ['mart', 'reporting', 'agents']
    )
}}

WITH ranked AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.listing_id
            ORDER BY c.scraped_at DESC, c.listing_date DESC NULLS LAST
        ) AS row_num
    FROM {{ ref('int_cleaned_property_listings') }} c
)

SELECT
    listing_id,
    data_source,
    listing_url,
    property_title,
    property_type,
    listing_type,
    city_clean,
    suburb_clean,
    property_price_usd,
    number_of_bedrooms,
    number_of_bathrooms,
    number_of_garages,
    property_size_sqm,
    stand_size_sqm,
    agent_name,
    agent_phone,
    agent_email,
    agency_name,
    listing_date,
    scraped_at
FROM ranked
WHERE row_num = 1
  AND has_agent_name = TRUE
  AND has_agent_contact = TRUE

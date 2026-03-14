-- Listings with a date in the future are scraping artifacts
SELECT listing_id, listing_url, listing_date, scraped_at
FROM {{ ref('stg_property_listings') }}
WHERE listing_date > CURRENT_DATE()

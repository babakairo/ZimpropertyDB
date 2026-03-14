-- Custom dbt test: no USD prices above $50M or below $50
-- Returns rows that FAIL the test (i.e. rows = test failures)
SELECT
    listing_id,
    listing_url,
    property_price_usd,
    currency_original,
    listing_type
FROM {{ ref('int_cleaned_property_listings') }}
WHERE property_price_usd IS NOT NULL
  AND (
        property_price_usd > 50000000
    OR (listing_type = 'sale' AND property_price_usd < 50)
    OR (listing_type = 'rent' AND property_price_usd > 50000)
  )

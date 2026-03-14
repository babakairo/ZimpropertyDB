"""
Scrapy Item definitions for Zimbabwe property listings.
All fields are intentionally Optional at scrape time — validation happens
in the ValidationPipeline so we capture whatever the site provides.
"""
import scrapy


class PropertyListingItem(scrapy.Item):
    # ── Identification ──────────────────────────────────────────────────────
    listing_id          = scrapy.Field()   # hash(source + listing_url)
    source              = scrapy.Field()   # e.g. "property.co.zw"

    # ── Listing details ─────────────────────────────────────────────────────
    property_title      = scrapy.Field()
    property_price      = scrapy.Field()   # numeric, cleaned
    currency            = scrapy.Field()   # "USD" | "ZWL" | "ZWIG"
    property_type       = scrapy.Field()   # house | flat | land | commercial | farm
    listing_type        = scrapy.Field()   # sale | rent

    # ── Location ────────────────────────────────────────────────────────────
    city                = scrapy.Field()
    suburb              = scrapy.Field()
    address_raw         = scrapy.Field()   # full raw address string
    latitude            = scrapy.Field()
    longitude           = scrapy.Field()

    # ── Property attributes ──────────────────────────────────────────────────
    number_of_bedrooms  = scrapy.Field()
    number_of_bathrooms = scrapy.Field()
    number_of_garages   = scrapy.Field()
    property_size_sqm   = scrapy.Field()   # normalised to sqm
    property_size_raw   = scrapy.Field()   # original string e.g. "1 500 m²"
    stand_size_sqm      = scrapy.Field()   # for land / stands
    features            = scrapy.Field()   # list: ["pool", "borehole", "solar"]

    # ── Agent / seller ───────────────────────────────────────────────────────
    agent_name          = scrapy.Field()
    agent_phone         = scrapy.Field()
    agent_email         = scrapy.Field()
    agency_name         = scrapy.Field()

    # ── Media ────────────────────────────────────────────────────────────────
    image_urls          = scrapy.Field()   # list of image URLs
    images              = scrapy.Field()   # populated by ImagesPipeline (optional)

    # ── Metadata ─────────────────────────────────────────────────────────────
    listing_url         = scrapy.Field()
    listing_date        = scrapy.Field()   # ISO date string
    scraped_at          = scrapy.Field()   # ISO datetime string
    is_new_listing      = scrapy.Field()   # bool — first time we see this URL

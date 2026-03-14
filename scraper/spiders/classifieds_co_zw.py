"""
Spider for classifieds.co.zw — Zimbabwe's second-largest property portal.

Verified against live site 2026-03-09:
  - Property hub:   /zimbabwe-property  (8 036 listings)
  - Sub-categories: /zimbabwe-houses-for-sale, /zimbabwe-houses-to-rent,
                    /zimbabwe-land-for-sale, /zimbabwe-commercial-property, etc.
  - Listing URL:    /listings/{suburb}-{type}-{id}
  - Pagination:     <link rel="next" href="...?page=N"> in <head>
  - Detail page:    JSON-LD <script type="application/ld+json"> — @type=Product
                    has price, currency, beds, baths, floor size, amenities, address
  - Cloudflare:     Requires Playwright (set CLASSIFIEDS_USE_PLAYWRIGHT=true in .env)

Install once:
    pip install scrapy-playwright
    playwright install chromium

Run:
    scrapy crawl classifieds_co_zw
"""
import re
import json
import scrapy
from urllib.parse import urljoin, urlencode, urlparse, parse_qs, urlunsplit

from scraper.spiders.base_spider import BasePropertySpider
from scraper.items import PropertyListingItem
from scraper.utils.helpers import (
    parse_price,
    parse_size,
    parse_int,
    normalise_city,
    normalise_property_type,
    normalise_listing_type,
)

import os

USE_PLAYWRIGHT = os.getenv("CLASSIFIEDS_USE_PLAYWRIGHT", "false").lower() == "true"

# No PageMethods needed — classifieds.co.zw is server-rendered;
# Playwright just needs to pass the Cloudflare JS challenge, not wait for JS rendering.
_LIST_METHODS = []
_DETAIL_METHODS = []


def _pw_meta(methods):
    if not USE_PLAYWRIGHT:
        return {}
    return {
        "playwright": True,
        "playwright_include_page": False,
        "playwright_page_methods": methods,
    }


class ClassifiedsCoZwSpider(BasePropertySpider):
    name = "classifieds_co_zw"
    source = "classifieds.co.zw"
    allowed_domains = ["www.classifieds.co.zw", "classifieds.co.zw"]
    BASE_URL = "https://www.classifieds.co.zw"

    # Verified category URLs (2026-03-09)
    # Main hub + all confirmed sub-categories for full coverage
    START_PATHS = [
        # ── For sale ────────────────────────────────────────────────────────
        # /zimbabwe-property is intentionally excluded — it is the site-wide hub
        # and includes vehicles, electronics, and other non-property categories.
        ("/zimbabwe-houses-for-sale",                 "sale",  "house"),
        ("/zimbabwe-flats-for-sale",                  "sale",  "flat"),
        ("/zimbabwe-townhouses-for-sale",             "sale",  "townhouse"),
        ("/zimbabwe-land-for-sale",                   "sale",  "land"),
        ("/zimbabwe-commercial-property-for-sale",    "sale",  "commercial"),
        ("/zimbabwe-offices-for-sale",                "sale",  "office"),
        ("/zimbabwe-shops-for-sale",                  "sale",  "shop"),
        ("/zimbabwe-warehouses-for-sale",             "sale",  "warehouse"),
        ("/zimbabwe-farms-for-sale",                  "sale",  "farm"),
        # ── To rent ─────────────────────────────────────────────────────────
        ("/zimbabwe-houses-to-rent",                  "rent",  "house"),
        ("/zimbabwe-flats-to-rent",                   "rent",  "flat"),
        ("/zimbabwe-townhouses-to-rent",              "rent",  "townhouse"),
        ("/zimbabwe-land-to-rent",                    "rent",  "land"),
        ("/zimbabwe-commercial-property-to-rent",     "rent",  "commercial"),
        ("/zimbabwe-offices-to-rent",                 "rent",  "office"),
        ("/zimbabwe-shops-to-rent",                   "rent",  "shop"),
        ("/zimbabwe-warehouses-to-rent",              "rent",  "warehouse"),
        ("/zimbabwe-rooms-to-rent",                   "rent",  "room"),
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 3.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
    }

    def start_requests(self):
        if not USE_PLAYWRIGHT:
            self.logger.warning(
                "[classifieds_co_zw] CLASSIFIEDS_USE_PLAYWRIGHT=false — "
                "this site requires Playwright. Set it to 'true' in configs/.env."
            )

        for path, lt, pt in self.START_PATHS:
            yield scrapy.Request(
                url=self.BASE_URL + path,
                callback=self.parse_list_page,
                meta={
                    "listing_type": lt,
                    "property_type_hint": pt,
                    **_pw_meta(_LIST_METHODS),
                },
                errback=self.handle_error,
            )

    # ── List page ────────────────────────────────────────────────────────────

    def parse_list_page(self, response):
        # Detect Cloudflare block (plain HTTP path)
        if "just a moment" in response.text.lower() and response.status in (200, 403):
            self.logger.warning(
                f"[{self.name}] Cloudflare challenge on {response.url}. "
                "Enable CLASSIFIEDS_USE_PLAYWRIGHT=true."
            )
            return

        listing_type = response.meta.get("listing_type", "sale")
        type_hint = response.meta.get("property_type_hint", "")

        # Listing links — site uses div.listing-simple > .title > a
        # Restrict to the main content area to avoid sidebar/related links
        links = (
            response.css("div.listing-simple .title a::attr(href)").getall()
            or response.css("div.listing-simple a[href*='/listings/']::attr(href)").getall()
        )

        abs_links = list(dict.fromkeys(urljoin(self.BASE_URL, h) for h in links))

        self.logger.info(
            f"[{self.name}] {len(abs_links)} listings on {response.url}"
        )

        new_links, stop = self._filter_new_hrefs(abs_links)

        for url in new_links:
            yield scrapy.Request(
                url=url,
                callback=self._safe_parse_listing,
                meta={
                    "listing_type": listing_type,
                    "property_type_hint": type_hint,
                    **_pw_meta(_DETAIL_METHODS),
                },
                errback=self.handle_error,
            )

        # ── Pagination: stop early if whole page already scraped ──────────
        if stop:
            return

        next_url = response.css("link[rel='next']::attr(href)").get()
        if not next_url:
            next_url = response.css(
                "a[rel='next']::attr(href), "
                "a[aria-label='Next']::attr(href), "
                "a.next::attr(href)"
            ).get()
        if next_url:
            yield scrapy.Request(
                url=urljoin(self.BASE_URL, next_url),
                callback=self.parse_list_page,
                meta={**response.meta, **_pw_meta(_LIST_METHODS)},
                errback=self.handle_error,
            )

    # ── Detail page ──────────────────────────────────────────────────────────

    def parse_listing(self, response) -> PropertyListingItem:
        if "just a moment" in response.text.lower():
            self.logger.warning(f"[{self.name}] CF block on {response.url}")
            return PropertyListingItem()

        item = PropertyListingItem()

        # ── JSON-LD (primary source — has almost everything) ─────────────
        ld = self._extract_jsonld(response)
        offers = ld.get("offers", {})
        address = ld.get("address", {})
        floor_size = ld.get("floorSize", {})
        amenities = ld.get("amenityFeature", [])

        # Title
        item["property_title"] = (
            ld.get("name")
            or response.css("h1.page-header::text").get("").strip()
            or None
        )

        # Price / currency
        raw_price = offers.get("price")
        item["property_price"] = float(raw_price) if raw_price is not None else None
        item["currency"] = offers.get("priceCurrency", "USD")

        # Listing type from spider meta
        listing_type = response.meta.get("listing_type", "sale")
        # Confirm from breadcrumb JSON-LD if possible
        graph = ld.get("@graph", [])
        breadcrumb = next(
            (g for g in graph if g.get("@type") == "BreadcrumbList"), {}
        )
        crumb_names = " ".join(
            item_.get("name", "").lower()
            for item_ in breadcrumb.get("itemListElement", [])
        )
        if "rent" in crumb_names or "to-rent" in response.url:
            listing_type = "rent"
        elif "sale" in crumb_names or "for-sale" in response.url:
            listing_type = "sale"
        item["listing_type"] = normalise_listing_type(listing_type)

        # Property type from breadcrumb or meta
        type_hint = response.meta.get("property_type_hint", "")
        if not type_hint and crumb_names:
            type_hint = crumb_names
        item["property_type"] = normalise_property_type(type_hint)

        # Location — JSON-LD address + city normalisation
        locality = address.get("addressLocality", "").strip()
        region = address.get("addressRegion", "").strip()
        item["suburb"] = locality.title() if locality else None
        item["city"] = normalise_city(region or locality)
        item["address_raw"] = (
            f"{locality}, {region}".strip(", ") if locality or region else None
        )

        # Coordinates — classifieds doesn't expose in JSON-LD
        item["latitude"] = None
        item["longitude"] = None

        # Beds / baths / size — JSON-LD Product schema
        item["number_of_bedrooms"] = ld.get("numberOfBedrooms")
        item["number_of_bathrooms"] = ld.get("numberOfBathroomsTotal")
        item["number_of_garages"] = None

        size_val = floor_size.get("value")
        item["property_size_sqm"] = float(size_val) if size_val else None
        item["property_size_raw"] = (
            f"{size_val} m²" if size_val else None
        )
        item["stand_size_sqm"] = None

        # Amenities from amenityFeature array
        item["features"] = [
            a["name"] for a in amenities
            if isinstance(a, dict) and a.get("value") == "on"
        ]

        # Agent / contact
        item["agent_name"] = None
        phone_text = response.css("a[href^='tel:']::text").get("").strip()
        phone_href = response.css("a[href^='tel:']::attr(href)").get("").replace("tel:", "").strip()
        item["agent_phone"] = phone_href or phone_text or None
        item["agent_email"] = response.css("a[href^='mailto:']::attr(href)").get("")\
            .replace("mailto:", "").strip() or None
        item["agency_name"] = None

        # Images — JSON-LD image field (single URL or list)
        ld_image = ld.get("image")
        if isinstance(ld_image, list):
            item["image_urls"] = ld_image
        elif isinstance(ld_image, str):
            item["image_urls"] = [ld_image]
        else:
            item["image_urls"] = response.css(
                ".gallery img::attr(src), .photos img::attr(src), "
                "img[class*='listing']::attr(src)"
            ).getall()

        # Listing date
        item["listing_date"] = (
            response.css("time::attr(datetime)").get()
            or response.css("[class*='date']::text").get()
        )

        return item

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _extract_jsonld(self, response) -> dict:
        """Return the Product JSON-LD block, or empty dict."""
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                # May be a single object or a @graph array
                if isinstance(data, dict):
                    graph = data.get("@graph", [])
                    # Try top-level first
                    if data.get("@type") == "Product":
                        return data
                    # Merge graph into top-level for BreadcrumbList access
                    product = next((g for g in graph if g.get("@type") == "Product"), {})
                    if product:
                        product["@graph"] = graph  # carry breadcrumb data
                        return product
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "Product":
                            return item
            except (json.JSONDecodeError, AttributeError):
                continue
        return {}

    def handle_error(self, failure):
        self.logger.error(
            f"[{self.name}] Request failed: {failure.request.url} — {failure.value}"
        )
        self._failed_count += 1

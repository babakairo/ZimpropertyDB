"""
Spider for propertybook.co.zw — Zimbabwe's second-largest property portal.

JavaScript-rendered (React SPA), uses scrapy-playwright for full rendering.

Verified selectors (2026-03-10):
  List page : a[href*='/for-sale/'] and a[href*='/to-rent/'] → /listings/for-sale/slug-ref
  Pagination: a[href*='?page='] (next page link)
  Detail     : h1 (title), .listing-price or text containing USD, .row.features text
               (.agent-details → agent name + phone), a[href^='tel:']

Run:
    scrapy crawl propertybook_co_zw
    scrapy crawl propertybook_co_zw -a listing_type=rent
"""
import re
import scrapy
from urllib.parse import urljoin

from scrapy_playwright.page import PageMethod

from scraper.spiders.base_spider import BasePropertySpider
from scraper.items import PropertyListingItem
from scraper.utils.helpers import (
    parse_price, parse_size, normalise_city, normalise_listing_type
)

_WAIT = [PageMethod("wait_for_timeout", 3000)]


class PropertybookCoZwSpider(BasePropertySpider):
    name   = "propertybook_co_zw"
    source = "propertybook.co.zw"
    allowed_domains = ["www.propertybook.co.zw", "propertybook.co.zw"]
    BASE_URL = "https://www.propertybook.co.zw"

    # Category start URLs — matches sitemap structure
    SALE_PATHS = [
        "/houses/for-sale",
        "/flats-apartments/for-sale",
        "/townhouses-clusters/for-sale",
        "/residential-stands/for-sale",
        "/land/for-sale",
        "/farms/for-sale",
        "/commercial/for-sale",
        "/warehouses/for-sale",
        "/offices/for-sale",
        "/retail-shops/for-sale",
    ]
    RENT_PATHS = [
        "/houses/to-rent",
        "/flats-apartments/to-rent",
        "/townhouses-clusters/to-rent",
        "/cottages/to-rent",
        "/rooms/to-rent",
        "/student-accommodation/to-rent",
        "/commercial/to-rent",
        "/offices/to-rent",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    def __init__(self, listing_type: str = "all", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.listing_type = listing_type

    def start_requests(self):
        paths = []
        if self.listing_type in ("all", "sale"):
            paths += [(p, "sale") for p in self.SALE_PATHS]
        if self.listing_type in ("all", "rent"):
            paths += [(p, "rent") for p in self.RENT_PATHS]

        for path, lt in paths:
            yield scrapy.Request(
                url=self.BASE_URL + path,
                callback=self.parse_list_page,
                meta={
                    "listing_type": lt,
                    "playwright": True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

    # ── List page ─────────────────────────────────────────────────────────────

    def parse_list_page(self, response):
        listing_type = response.meta.get("listing_type", "sale")
        slug = "for-sale" if listing_type == "sale" else "to-rent"

        # Listing links: /listings/for-sale/4-bedroom-house-hatfield-harare-pzp0527
        hrefs = list(dict.fromkeys(
            urljoin(self.BASE_URL, h)
            for h in response.css(f"a[href*='/{slug}/']::attr(href)").getall()
            if re.search(r"/listings/(?:for-sale|to-rent)/[a-z0-9-]+-[a-z0-9]+$", h)
        ))

        self.logger.info(f"[{self.name}] {len(hrefs)} listings on {response.url}")

        new_hrefs, stop = self._filter_new_hrefs(hrefs)
        for url in new_hrefs:
            yield scrapy.Request(
                url=url,
                callback=self._safe_parse_listing,
                meta={
                    "listing_type": listing_type,
                    "playwright": True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

        if stop:
            return

        next_url = response.css("a[aria-label='Next Page']::attr(href), a[href*='?page=']::attr(href)").get()
        # Take the highest page number link as "next"
        page_links = response.css("a[href*='?page=']::attr(href)").getall()
        if page_links:
            # Find next page: current page + 1
            current_page = int(re.search(r"page=(\d+)", response.url).group(1)) if "page=" in response.url else 1
            next_page_href = next(
                (h for h in page_links if f"page={current_page + 1}" in h), None
            )
            if next_page_href:
                yield scrapy.Request(
                    url=urljoin(self.BASE_URL, next_page_href),
                    callback=self.parse_list_page,
                    meta={
                        "listing_type": listing_type,
                        "playwright": True,
                        "playwright_page_methods": _WAIT,
                    },
                    errback=self.handle_error,
                )

    # ── Detail page ───────────────────────────────────────────────────────────

    def parse_listing(self, response) -> PropertyListingItem:
        item = PropertyListingItem()

        # Title
        item["property_title"] = response.css("h1::text").get("").strip() or None

        # Listing type from URL
        url_lower = response.url.lower()
        if "to-rent" in url_lower or "for-rent" in url_lower:
            item["listing_type"] = "rent"
        else:
            item["listing_type"] = normalise_listing_type(
                response.meta.get("listing_type", "sale")
            )

        # Price — appears as "USD 190,000" in page text
        price_text = response.css(
            "[class*='price']::text, [class*='Price']::text"
        ).get("").strip()
        if not price_text:
            # Fallback: scan all text for "USD NNN,NNN"
            full_text = " ".join(response.css("*::text").getall())
            m = re.search(r"USD\s+([\d,]+)", full_text, re.I)
            price_text = m.group(0) if m else ""
        item["property_price"], item["currency"] = parse_price(price_text)

        # Features row: "4 Beds   2 Baths   2 Lounges   3,866m2"
        features_text = ""
        for el_text in response.css("[class*='feature']::text, .row *::text").getall():
            el_text = el_text.strip()
            if re.search(r"\d+\s*Bed", el_text, re.I):
                features_text = el_text
                break
        # Alternatively, look for the compact summary line
        if not features_text:
            for line in " ".join(response.css("*::text").getall()).split("\n"):
                if re.search(r"\d+\s*Bed.*\d+\s*Bath", line, re.I):
                    features_text = line.strip()
                    break

        item["number_of_bedrooms"]  = self._parse_feat(features_text, r"(\d+)\s*Bed")
        item["number_of_bathrooms"] = self._parse_feat(features_text, r"(\d+)\s*Bath")
        item["number_of_garages"]   = self._parse_feat(features_text, r"(\d+)\s*Garage")

        # Size: "3,866m2" or "1.5Ha" in the features line or page
        size_m = re.search(r"([\d,]+)\s*m2", features_text, re.I)
        if size_m:
            item["property_size_sqm"] = float(size_m.group(1).replace(",", ""))
            item["property_size_raw"] = size_m.group(0)
        else:
            ha_m = re.search(r"([\d.]+)\s*Ha", features_text, re.I)
            if ha_m:
                item["property_size_sqm"] = round(float(ha_m.group(1)) * 10000, 1)
                item["property_size_raw"] = ha_m.group(0)
            else:
                item["property_size_sqm"] = None
                item["property_size_raw"] = None
        item["stand_size_sqm"] = None

        # Property type from URL slug
        type_match = re.search(
            r"/listings/(?:for-sale|to-rent)/(\d+-bedroom-)?([a-z-]+?)-[a-z]{2,}-[a-z]{2,}-[a-z0-9]+$",
            response.url.lower(),
        )
        raw_type = type_match.group(2).replace("-", " ") if type_match else ""
        item["property_type"] = self._normalise_type(raw_type)

        # Location — extract from title: "X Bedroom House for Sale in Hatfield, Harare"
        loc_m = re.search(r"in\s+([^,]+),\s*(.+)$", item.get("property_title", ""), re.I)
        if loc_m:
            item["suburb"] = loc_m.group(1).strip()
            item["city"]   = normalise_city(loc_m.group(2).strip())
        else:
            item["suburb"] = None
            item["city"]   = None
        item["address_raw"] = item.get("property_title")
        item["latitude"]    = None
        item["longitude"]   = None

        # Features list
        item["features"] = [
            f.strip() for f in response.css(
                "[class*='feature-item']::text, [class*='amenity']::text, "
                ".property-features .feature::text"
            ).getall()
            if f.strip()
        ]

        # Agent — ".agent-details" contains "Name\n+263 XXX Phone\nEmail..."
        agent_block = response.css(".agent-details, [class*='agent-detail']").get("")
        agent_text  = re.sub(r"<[^>]+>", " ", agent_block)
        name_m  = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+)", agent_text)
        phone_m = re.search(r"\+?263[\s\d]+", agent_text)
        email_m = re.search(r"[\w.+-]+@[\w.-]+\.\w{2,}", agent_text)

        item["agent_name"]  = name_m.group(1) if name_m else None
        item["agent_phone"] = response.css("a[href^='tel:']::attr(href)").get(
            "").replace("tel:", "").strip() or (phone_m.group(0).strip() if phone_m else None)
        item["agent_email"] = email_m.group(0) if email_m else None
        item["agency_name"] = response.css(
            "[class*='agency'] h2::text, [class*='agency'] h3::text, "
            "[class*='branch'] h2::text"
        ).get(""). strip() or None

        # Images
        item["image_urls"] = response.css(
            "img[src*='propertybook']::attr(src), "
            "img[data-src*='propertybook']::attr(data-src)"
        ).getall()

        # Listing date — not visible on page; use None (scraped_at added by base)
        item["listing_date"] = None

        return item

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_feat(text: str, pattern: str):
        m = re.search(pattern, text, re.I)
        return int(m.group(1)) if m else None

    @staticmethod
    def _normalise_type(raw: str) -> str:
        raw = raw.lower().strip()
        mapping = {
            "house": "house", "home": "house", "cottage": "house",
            "flat": "flat", "apartment": "flat", "studio": "flat",
            "townhouse": "townhouse", "cluster": "townhouse",
            "stand": "land", "residential stand": "land", "plot": "land",
            "land": "land", "farm": "farm",
            "commercial": "commercial", "office": "commercial",
            "warehouse": "commercial", "shop": "commercial", "retail": "commercial",
            "room": "room",
        }
        for key, val in mapping.items():
            if key in raw:
                return val
        return raw or "unknown"

    def handle_error(self, failure):
        self.logger.error(f"[{self.name}] {failure.request.url}: {failure.value}")
        self._failed_count += 1

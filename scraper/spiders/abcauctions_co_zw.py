"""
Spider for abcauctions.co.zw — Zimbabwe's primary property auction house.

Scrapes all property auction lots including:
  - Residential properties
  - Commercial properties
  - Land / stands
  - Farms

ABC Auctions lists properties in numbered lots with a title, description,
starting bid / reserve price, and auction date.

Note: Auction prices are typically "reserve price" or "starting bid" —
      these are mapped to property_price with listing_type="auction".

Run:
    scrapy crawl abcauctions_co_zw
    scrapy crawl abcauctions_co_zw -a category=residential
"""
import re
import json
import scrapy
from urllib.parse import urljoin

from scrapy_playwright.page import PageMethod

from scraper.spiders.base_spider import BasePropertySpider
from scraper.items import PropertyListingItem
from scraper.utils.helpers import (
    parse_price, parse_size, parse_int,
    normalise_city, normalise_property_type,
)

BASE_URL = "https://www.abcauctions.co.zw"

_WAIT = [PageMethod("wait_for_timeout", 3000)]

# Property auction categories on the site
CATEGORY_PATHS = [
    ("/auctions/residential-property", "sale"),
    ("/auctions/commercial-property",  "sale"),
    ("/auctions/land-stands",          "sale"),
    ("/auctions/farms-smallholdings",  "sale"),
    # General property lots (catches anything not in above categories)
    ("/auctions/property",             "sale"),
]


class AbcAuctionsCoZwSpider(BasePropertySpider):
    name   = "abcauctions_co_zw"
    source = "abcauctions.co.zw"
    allowed_domains = ["www.abcauctions.co.zw", "abcauctions.co.zw"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    def __init__(self, category: str = "all", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category = category

    def start_requests(self):
        paths = CATEGORY_PATHS if self.category == "all" else [
            (p, lt) for p, lt in CATEGORY_PATHS if self.category in p
        ]

        for path, lt in paths:
            yield scrapy.Request(
                url=BASE_URL + path,
                callback=self.parse_list_page,
                meta={
                    "listing_type": "auction",   # auctions are always "for sale"
                    "playwright":   True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

    # ── List page ─────────────────────────────────────────────────────────────

    def parse_list_page(self, response):
        listing_type = response.meta.get("listing_type", "auction")

        # ABC Auctions — try common auction lot card patterns
        hrefs = (
            response.css("a.lot-card::attr(href)").getall()
            or response.css("a.auction-lot::attr(href)").getall()
            or response.css(".lot-listing a::attr(href)").getall()
            or response.css("a[href*='/lot/']::attr(href)").getall()
            or response.css("a[href*='/auction-lot/']::attr(href)").getall()
            or response.css("a[href*='/property/']::attr(href)").getall()
            or [
                h for h in response.css("a::attr(href)").getall()
                if re.search(r"/(?:lot|listing|property|auction)/[a-z0-9-]{4,}", h, re.I)
            ]
        )

        abs_hrefs = list(dict.fromkeys(
            urljoin(BASE_URL, h) for h in hrefs
            if h and not h.startswith("javascript")
        ))

        self.logger.info(f"[{self.name}] {len(abs_hrefs)} lots on {response.url}")

        new_hrefs, stop = self._filter_new_hrefs(abs_hrefs)
        for url in new_hrefs:
            yield scrapy.Request(
                url=url,
                callback=self._safe_parse_listing,
                meta={
                    "listing_type": listing_type,
                    "playwright":   True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

        if stop:
            return

        # Pagination
        next_url = (
            response.css("a[rel='next']::attr(href)").get()
            or response.css("a[aria-label='Next']::attr(href)").get()
            or response.css("a.pagination-next::attr(href)").get()
        )
        if not next_url and abs_hrefs:
            base_path = response.url.split("?")[0]
            current   = int(re.search(r"[?&]page=(\d+)", response.url).group(1)) \
                if "page=" in response.url else 1
            next_url  = f"{base_path}?page={current + 1}"

        if next_url and "javascript" not in next_url.lower():
            yield scrapy.Request(
                url=urljoin(BASE_URL, next_url),
                callback=self.parse_list_page,
                meta={
                    "listing_type": listing_type,
                    "playwright":   True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

    # ── Detail page ───────────────────────────────────────────────────────────

    def parse_listing(self, response) -> PropertyListingItem:
        item = PropertyListingItem()

        # JSON-LD (if present)
        ld = self._extract_jsonld(response)

        # Title
        item["property_title"] = (
            ld.get("name")
            or self.css_first(
                response,
                "h1.lot-title::text",
                "h1.auction-title::text",
                "h1.property-title::text",
                "h1::text",
            )
            or ""
        ).strip() or None

        # Price (reserve price / starting bid)
        # Auction sites often show "Reserve: USD 120,000" or "Starting Bid: $50,000"
        full_text = " ".join(t.strip() for t in response.css("*::text").getall() if t.strip())

        raw_price = (
            ld.get("offers", {}).get("price")
            or self.css_first(
                response,
                ".reserve-price::text",
                ".starting-bid::text",
                ".lot-price::text",
                ".auction-price::text",
                "[class*='price']::text",
            )
            or ""
        )
        # Fallback: scan text for "Reserve: USD NNN,NNN" or "Starting Bid: USD NNN,NNN"
        if not raw_price or not re.search(r"\d{3,}", str(raw_price)):
            m = re.search(
                r"(?:reserve|starting bid|guide price)[:\s]+(?:USD|US\$|\$)?\s*([\d,]+)",
                full_text, re.I
            )
            if not m:
                m = re.search(r"(?:USD|US\$|\$)\s*([\d][,\d ]{2,})", full_text, re.I)
            raw_price = m.group(0) if m else ""

        item["property_price"], item["currency"] = parse_price(str(raw_price))

        # Auction listings are always "sale" type
        item["listing_type"] = "sale"

        # Property type
        type_raw = (
            self.css_first(
                response,
                ".lot-category::text",
                ".property-type::text",
                ".category::text",
            )
            or ""
        )
        # Infer from URL path if CSS fails
        url_lower = response.url.lower()
        if not type_raw:
            if "commercial" in url_lower:
                type_raw = "commercial"
            elif any(k in url_lower for k in ("land", "stand", "plot")):
                type_raw = "land"
            elif any(k in url_lower for k in ("farm", "smallholding")):
                type_raw = "farm"
            elif "residential" in url_lower or "house" in url_lower:
                type_raw = "house"
        item["property_type"] = normalise_property_type(type_raw)

        # Location
        suburb_raw = self.css_first(
            response,
            ".lot-suburb::text", ".suburb::text",
            "[class*='suburb']::text", "[class*='location']::text",
        ) or ""
        city_raw = self.css_first(
            response,
            ".lot-city::text", ".city::text",
            "[class*='city']::text", "[class*='region']::text",
        ) or ""

        # Fallback: parse title for location
        if not suburb_raw or not city_raw:
            title = item.get("property_title", "") or ""
            loc_m = re.search(r"\bin\s+([^,]+),\s*(.+?)(?:\s*[-|]|$)", title, re.I)
            if loc_m:
                suburb_raw = suburb_raw or loc_m.group(1).strip()
                city_raw   = city_raw   or loc_m.group(2).strip()
            else:
                # Scan description text for "Harare" / "Bulawayo" city names
                known_cities = [
                    "harare", "bulawayo", "mutare", "gweru", "kwekwe",
                    "kadoma", "masvingo", "chinhoyi", "bindura",
                ]
                for city in known_cities:
                    if city in full_text.lower():
                        city_raw = city_raw or city.title()
                        break

        item["suburb"]      = suburb_raw.title() if suburb_raw else None
        item["city"]        = normalise_city(city_raw) if city_raw else None
        item["address_raw"] = (
            self.css_first(response, ".lot-address::text", ".address::text")
            or item.get("property_title")
        )
        item["latitude"]  = None
        item["longitude"] = None

        # Attributes
        beds_raw  = self.css_first(
            response, ".bedrooms::text", "[class*='bed'] span::text"
        )
        baths_raw = self.css_first(
            response, ".bathrooms::text", "[class*='bath'] span::text"
        )
        size_raw = self.css_first(
            response,
            ".lot-size::text", ".stand-size::text",
            ".floor-area::text", "[class*='size']::text",
        )
        # Fallback: scan text for "NNN m²" or "NNN sqm"
        if not size_raw:
            m = re.search(r"([\d,]+)\s*(?:m2|m²|sqm|square metres?)", full_text, re.I)
            if m:
                size_raw = m.group(0)

        item["number_of_bedrooms"]  = parse_int(beds_raw)
        item["number_of_bathrooms"] = parse_int(baths_raw)
        item["number_of_garages"]   = None
        item["property_size_sqm"]   = parse_size(size_raw)
        item["property_size_raw"]   = size_raw
        item["stand_size_sqm"]      = (
            item["property_size_sqm"] if item["property_type"] == "land" else None
        )

        # Features / auction description snippets
        item["features"] = [
            t.strip()
            for t in response.css(
                ".lot-features li::text, "
                ".auction-description li::text, "
                "[class*='feature']::text"
            ).getall()
            if t.strip() and len(t.strip()) > 2
        ]

        # Agent / auctioneer details
        item["agency_name"] = "ABC Auctions"
        item["agent_name"]  = self.css_first(
            response, ".auctioneer-name::text", ".contact-name::text"
        )
        phone_href = response.css("a[href^='tel:']::attr(href)").get("") or ""
        item["agent_phone"] = phone_href.replace("tel:", "").strip() or None
        item["agent_email"] = (
            response.css("a[href^='mailto:']::attr(href)").get("") or ""
        ).replace("mailto:", "").strip() or None

        # Images
        item["image_urls"] = (
            response.css(
                ".lot-gallery img::attr(src), "
                ".property-images img::attr(src), "
                ".auction-images img::attr(src), "
                "img[class*='gallery']::attr(src)"
            ).getall()
            or ld.get("image", [])
        )

        # Auction/listing date
        item["listing_date"] = (
            response.css("time::attr(datetime)").get()
            or self.css_first(
                response, ".auction-date::text", ".lot-date::text", ".date::text"
            )
        )

        return item

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_jsonld(self, response) -> dict:
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                if isinstance(data, list):
                    data = data[0]
                if isinstance(data, dict) and data.get("@type") in (
                    "RealEstateListing", "Product", "Offer",
                    "Event",   # some auctions use Event schema
                ):
                    return data
            except (json.JSONDecodeError, AttributeError):
                continue
        return {}

    def handle_error(self, failure):
        self.logger.error(f"[{self.name}] {failure.request.url}: {failure.value}")
        self._failed_count += 1

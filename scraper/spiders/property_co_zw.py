"""
Spider for property.co.zw — Zimbabwe's largest property portal.

Scrapes ALL live listings across every property category and type.
Verified against live site 2026-03-09:
  - Results page: /property-for-sale (329 pages, 6 569 listings)
                  /property-for-rent (paginated)
                  + 14 sub-category pages for full coverage
  - Listing cards:  div[data-carousel="result"] with data-href attribute
  - Pagination:     a[aria-label="Next"] href (no page cap — follows until last)
  - Detail page:    JSON-LD <script type="application/ld+json"> contains all
                    structured data (price, beds, baths, size, coords, agent)

Run:
    scrapy crawl property_co_zw                     # all listings, unlimited
    scrapy crawl property_co_zw -a listing_type=rent
"""
import re
import json
import scrapy
from urllib.parse import urljoin

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


class PropertyCoZwSpider(BasePropertySpider):
    name = "property_co_zw"
    source = "property.co.zw"
    allowed_domains = ["www.property.co.zw", "property.co.zw"]
    BASE_URL = "https://www.property.co.zw"

    # ── All category start URLs (verified 2026-03-09) ───────────────────────
    # Main aggregated pages (cover everything) + specific sub-categories
    # to ensure no listings are missed by deduplication.
    SALE_PATHS = [
        "/property-for-sale",           # main sale hub (329 pages)
        "/houses-for-sale",
        "/flats-apartments-for-sale",
        "/townhouses-for-sale",
        "/garden-flats-for-sale",
        "/commercial-property-for-sale",
        "/offices-for-sale",
        "/shops-for-sale",
        "/warehouses-for-sale",
        "/land-for-sale",
        "/residential-land-stands-for-sale",
        "/commercial-land-for-sale",
        "/agricultural-land-farms-for-sale",
        "/development-projects",
    ]
    RENT_PATHS = [
        "/property-for-rent",           # main rent hub
        "/houses-for-rent",
        "/flats-apartments-for-rent",
        "/townhouses-for-rent",
        "/garden-flats-for-rent",
        "/commercial-property-for-rent",
        "/offices-for-rent",
        "/shops-for-rent",
        "/warehouses-for-rent",
        "/land-for-rent",
        "/cottages-for-rent",
        "/rooms-for-rent",
        "/student-accommodation-for-rent",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        },
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
                meta={"listing_type": lt},
                errback=self.handle_error,
                dont_filter=False,
            )

    # ── List page (results grid) ─────────────────────────────────────────────

    def parse_list_page(self, response):
        listing_type = response.meta.get("listing_type", "sale")

        # Each listing card is a swiper-container with data-carousel="result"
        # and a data-href pointing to the detail page path.
        hrefs = response.css(
            'div[data-carousel="result"]::attr(data-href)'
        ).getall()

        # Fallback: direct anchor links used on some card layouts
        if not hrefs:
            hrefs = response.css(
                "a.gold-grid-thumbnails::attr(href)"
            ).getall()

        # Resolve to absolute URLs for dedup check
        abs_hrefs = list(dict.fromkeys(
            urljoin(self.BASE_URL, h) for h in hrefs
        ))

        self.logger.info(
            f"[{self.name}] {len(abs_hrefs)} listings on {response.url}"
        )

        new_hrefs, stop = self._filter_new_hrefs(abs_hrefs)

        for url in new_hrefs:
            yield scrapy.Request(
                url=url,
                callback=self._safe_parse_listing,
                meta={"listing_type": listing_type},
                errback=self.handle_error,
            )

        # ── Pagination ──────────────────────────────────────────────────────
        # Stop early if every listing on this page was already scraped.
        if stop:
            return

        next_url = response.css("a[aria-label='Next']::attr(href)").get()
        if next_url:
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_list_page,
                meta={"listing_type": listing_type},
                errback=self.handle_error,
            )

    # ── Detail page ──────────────────────────────────────────────────────────

    def parse_listing(self, response) -> PropertyListingItem:
        item = PropertyListingItem()

        # ── JSON-LD extraction (primary — contains almost everything) ───────
        ld = self._extract_jsonld(response)
        about = ld.get("about", {})
        offers = ld.get("offers", {})
        address = about.get("address", {})
        geo = about.get("geo", {})
        floor_size = about.get("floorSize", {})

        # Title
        item["property_title"] = (
            ld.get("name")
            or response.css("h1#ListingTitle::text").get("").strip()
        )

        # Price / currency
        raw_price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
        item["property_price"] = float(raw_price) if raw_price else None
        item["currency"] = offers.get("priceCurrency", "USD")

        # Listing & property type
        listing_type_meta = response.meta.get("listing_type", "")
        # Infer from URL path: /for-sale/houses-xxx → sale, /for-rent/xxx → rent
        url_path = response.url.lower()
        if "for-rent" in url_path or "to-rent" in url_path:
            listing_type_meta = "rent"
        elif "for-sale" in url_path:
            listing_type_meta = "sale"
        item["listing_type"] = normalise_listing_type(listing_type_meta)

        # Property type from URL segment (e.g. /for-sale/houses-xxx → "house")
        type_slug = re.search(r"/for-(?:sale|rent)/([a-z-]+?)-[a-z]{2,6}\d", url_path)
        type_hint = type_slug.group(1).replace("-", " ") if type_slug else ""
        item["property_type"] = normalise_property_type(type_hint)

        # Location
        item["suburb"] = address.get("addressLocality", "").strip() or None
        item["city"] = normalise_city(address.get("addressRegion", ""))
        # Full address shown as "Suburb, City, Country" in div.address
        addr_full = response.css("div.address::text").getall()
        item["address_raw"] = " ".join(t.strip() for t in addr_full if t.strip()) or None

        # Coordinates
        item["latitude"] = geo.get("latitude") or None
        item["longitude"] = geo.get("longitude") or None

        # Bedrooms / bathrooms
        item["number_of_bedrooms"] = about.get("numberOfBedrooms") or self._parse_stat(
            response.css("div.bed::text").get("")
        )
        item["number_of_bathrooms"] = about.get("numberOfBathroomsTotal") or self._parse_stat(
            response.css("div.bath::text").get("")
        )
        item["number_of_garages"] = self._parse_stat(
            response.css("div.garage::text").get("")
        )

        # Floor size (first .area div) and stand/land size (second .area div)
        area_texts = response.css("div.area::text").getall()
        area_texts = [t.strip() for t in area_texts if t.strip()]
        item["property_size_sqm"] = (
            floor_size.get("value")
            or parse_size(area_texts[0] if area_texts else None)
        )
        item["property_size_raw"] = area_texts[0] if area_texts else None
        item["stand_size_sqm"] = parse_size(area_texts[1] if len(area_texts) > 1 else None)

        # Features / amenities — extracted from alt text on amenity icons
        item["features"] = response.css(
            "div.grid img[alt]::attr(alt)"
        ).getall()

        # Agent / agency
        item["agency_name"] = (
            ld.get("author", {}).get("name")
            or response.css("a[href*='/estate-agents/'] h3::text").get("").strip()
        )
        # Phone: hidden behind a click-to-reveal; the tel: href is in the DOM
        phone_href = response.css("a[href^='tel:']::attr(href)").get("")
        item["agent_phone"] = phone_href.replace("tel:", "").strip() or None
        item["agent_name"] = (
            response.css("span.mainAgentNumber::text").get("").strip() or None
        )
        item["agent_email"] = None  # behind contact form, not in HTML

        # Images — JSON-LD image array is most complete
        item["image_urls"] = ld.get("image") or response.css(
            "img.swiper-lazy::attr(src), img.swiper-lazy::attr(data-src)"
        ).getall()

        # Listing date
        item["listing_date"] = ld.get("datePosted") or None

        return item

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _extract_jsonld(self, response) -> dict:
        """Return the first RealEstateListing JSON-LD block, or empty dict."""
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                if isinstance(data, list):
                    data = data[0]
                if data.get("@type") in ("RealEstateListing", "Product", "Offer"):
                    return data
            except (json.JSONDecodeError, AttributeError):
                continue
        return {}

    def _parse_stat(self, text: str):
        """Extract leading integer from strings like '4 Bedrooms'."""
        m = re.search(r"\d+", text)
        return int(m.group()) if m else None

    def handle_error(self, failure):
        self.logger.error(
            f"[{self.name}] Request failed: {failure.request.url} — {failure.value}"
        )
        self._failed_count += 1

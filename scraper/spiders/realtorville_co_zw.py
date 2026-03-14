"""
Spider for realtorville.co.zw — Zimbabwe property portal.

The site is a JavaScript SPA (client-side rendered). This spider uses
scrapy-playwright to fully render each page before scraping.

It covers:
  - Residential houses for sale/rent
  - Flats & apartments for sale/rent
  - Townhouses for sale/rent
  - Land / stands / agricultural / commercial land for sale
  - Commercial, offices, warehouses, shops for sale/rent

Pagination: ?page=N appended to each category URL.

Run:
    scrapy crawl realtorville_co_zw
    scrapy crawl realtorville_co_zw -a listing_type=sale
    scrapy crawl realtorville_co_zw -a listing_type=rent
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
    normalise_city, normalise_property_type, normalise_listing_type,
)

BASE_URL = "https://www.realtorville.co.zw"

# Wait for any listing card container to appear, or fall back after 5s
_WAIT = [
    PageMethod("wait_for_load_state", "networkidle"),
    PageMethod("wait_for_timeout", 3000),
]

SALE_PATHS = [
    "/houses-for-sale",
    "/flats-apartments-for-sale",
    "/townhouses-for-sale",
    "/residential-land-stands-for-sale",
    "/land-for-sale",
    "/agricultural-land-farms-for-sale",
    "/commercial-land-for-sale",
    "/commercial-property-for-sale",
    "/offices-for-sale",
    "/warehouses-for-sale",
    "/shops-for-sale",
    "/lodges-hotels-for-sale",
]

RENT_PATHS = [
    "/houses-to-rent",
    "/flats-apartments-to-rent",
    "/townhouses-to-rent",
    "/commercial-property-to-rent",
    "/offices-to-rent",
]


class RealtorvilleCoZwSpider(BasePropertySpider):
    name   = "realtorville_co_zw"
    source = "realtorville.co.zw"
    allowed_domains = ["www.realtorville.co.zw", "realtorville.co.zw"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        # Playwright must be enabled in settings.py DOWNLOAD_HANDLERS
    }

    def __init__(self, listing_type: str = "all", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.listing_type = listing_type

    def start_requests(self):
        paths = []
        if self.listing_type in ("all", "sale"):
            paths += [(p, "sale") for p in SALE_PATHS]
        if self.listing_type in ("all", "rent"):
            paths += [(p, "rent") for p in RENT_PATHS]

        for path, lt in paths:
            yield scrapy.Request(
                url=BASE_URL + path,
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

        # Collect all listing detail hrefs from rendered page.
        # We try a broad set of selectors since we can't pre-inspect the DOM.
        hrefs = (
            # Common: card anchor wrapping the whole listing thumbnail
            response.css("a.listing-card::attr(href)").getall()
            or response.css("a.property-card::attr(href)").getall()
            or response.css("a.result-card::attr(href)").getall()
            or response.css("[class*='listing-item'] a::attr(href)").getall()
            or response.css("[class*='property-item'] a::attr(href)").getall()
            or response.css("[class*='listing-thumb'] a::attr(href)").getall()
            or response.css("[class*='result-item'] a::attr(href)").getall()
            # Generic: any link whose href looks like a property detail slug
            or [
                h for h in response.css("a::attr(href)").getall()
                if re.search(
                    r"/(property|listing|for-sale|to-rent|for-rent)/[a-z0-9-]{10,}",
                    h, re.I
                )
            ]
        )

        # Deduplicate while preserving order; make absolute
        seen = set()
        abs_hrefs = []
        for h in hrefs:
            full = urljoin(BASE_URL, h)
            if full not in seen and full != BASE_URL and BASE_URL in full:
                seen.add(full)
                abs_hrefs.append(full)

        self.logger.info(f"[{self.name}] {len(abs_hrefs)} listings on {response.url}")

        new_hrefs, stop = self._filter_new_hrefs(abs_hrefs)
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

        # Pagination: try an explicit Next link, then fall back to ?page=N+1
        next_url = next(
            (
                urljoin(BASE_URL, h)
                for h in response.css(
                    "a[aria-label='Next']::attr(href), "
                    "a[aria-label='Next Page']::attr(href), "
                    "a.next::attr(href), "
                    "[class*='pagination'] a[href*='page=']::attr(href)"
                ).getall()
                if h and "javascript" not in h.lower()
            ),
            None,
        )

        if not next_url and abs_hrefs:
            # Construct next page URL
            base_path = response.url.split("?")[0]
            current = int(re.search(r"[?&]page=(\d+)", response.url).group(1)) \
                if "page=" in response.url else 1
            next_url = f"{base_path}?page={current + 1}"

        if next_url:
            yield scrapy.Request(
                url=next_url,
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

        # ── JSON-LD (richest data source if present) ─────────────────────────
        ld = self._extract_jsonld(response)

        # Title
        item["property_title"] = (
            ld.get("name")
            or self.css_first(
                response,
                "h1.listing-title::text",
                "h1.property-title::text",
                "h1[class*='title']::text",
                "h1::text",
            )
            or ""
        ).strip() or None

        # Price — CSS selectors on this site return placeholder values ("$9"),
        # so we scan the full rendered text for realistic property prices (≥1,000).
        full_text = " ".join(t.strip() for t in response.css("*::text").getall() if t.strip())
        raw_price = ld.get("offers", {}).get("price") or ""
        if not raw_price:
            # Match "$10,000" / "USD 250,000" / "$1 500 000" — require at least 4 digits
            m = re.search(
                r"(?:USD|US\$|\$)\s*([\d][,\d ]{3,})",
                full_text, re.I
            )
            if m:
                # Validate extracted number is a plausible property price
                digits = re.sub(r"[^\d]", "", m.group(1))
                if digits and int(digits) >= 1000:
                    raw_price = m.group(0)
        item["property_price"], item["currency"] = parse_price(str(raw_price))

        # Listing type
        url_lower = response.url.lower()
        if "to-rent" in url_lower or "for-rent" in url_lower:
            item["listing_type"] = "rent"
        elif "for-sale" in url_lower:
            item["listing_type"] = "sale"
        else:
            item["listing_type"] = normalise_listing_type(
                response.meta.get("listing_type", "sale")
            )

        # Property type — infer from the detail page URL slug (category is embedded)
        path = url_lower
        if any(k in path for k in ("agricultural-land", "commercial-land", "land", "stand", "plot", "erf")):
            item["property_type"] = "land"
        elif any(k in path for k in ("flat", "apartment")):
            item["property_type"] = "flat"
        elif any(k in path for k in ("townhouse", "cluster")):
            item["property_type"] = "townhouse"
        elif any(k in path for k in ("farm", "smallholding")):
            item["property_type"] = "farm"
        elif any(k in path for k in ("lodge", "hotel", "commercial", "office", "warehouse", "shop")):
            item["property_type"] = "commercial"
        elif "house" in path:
            item["property_type"] = "house"
        else:
            item["property_type"] = normalise_property_type(
                item.get("property_title", "")
            )

        # ── Location ─────────────────────────────────────────────────────────
        addr_ld = ld.get("about", {}).get("address", {}) or ld.get("address", {})
        suburb_raw = addr_ld.get("addressLocality", "").strip()
        city_raw   = addr_ld.get("addressRegion",   "").strip()

        if not suburb_raw or not city_raw:
            # Try page elements
            suburb_raw = suburb_raw or self.css_first(
                response,
                "[class*='suburb']::text",
                "[class*='location'] span:first-child::text",
            ) or ""
            city_raw = city_raw or self.css_first(
                response,
                "[class*='city']::text",
                "[class*='location'] span:last-child::text",
            ) or ""

        if not suburb_raw or not city_raw:
            # Parse "in Suburb, City" from title
            title = item.get("property_title", "") or ""
            loc_m = re.search(r"\bin\s+([^,]+),\s*(.+)$", title, re.I)
            if loc_m:
                suburb_raw = suburb_raw or loc_m.group(1).strip()
                city_raw   = city_raw   or loc_m.group(2).strip()

        item["suburb"]  = suburb_raw or None
        item["city"]    = normalise_city(city_raw) if city_raw else None
        item["address_raw"] = self.css_first(
            response,
            "[class*='address']::text",
            "[class*='location']::text",
        ) or item.get("property_title")

        geo = (
            ld.get("about", {}).get("geo", {})
            or ld.get("geo", {})
        )
        item["latitude"]  = geo.get("latitude")  or None
        item["longitude"] = geo.get("longitude") or None

        # ── Attributes ───────────────────────────────────────────────────────
        item["number_of_bedrooms"]  = (
            ld.get("about", {}).get("numberOfBedrooms")
            or self._parse_stat(
                self.css_first(response, "[class*='bed']::text", "[data-beds]::attr(data-beds)") or ""
            )
            or self._extract_count(full_text, r"(\d+)\s*(?:bed(?:room)?s?)", )
        )
        item["number_of_bathrooms"] = (
            ld.get("about", {}).get("numberOfBathroomsTotal")
            or self._parse_stat(
                self.css_first(response, "[class*='bath']::text", "[data-baths]::attr(data-baths)") or ""
            )
            or self._extract_count(full_text, r"(\d+)\s*(?:bath(?:room)?s?)")
        )
        item["number_of_garages"] = (
            self._parse_stat(
                self.css_first(response, "[class*='garage']::text", "[data-garages]::attr(data-garages)") or ""
            )
            or self._extract_count(full_text, r"(\d+)\s*(?:garage|parking)s?")
        )

        # Size
        floor_ld = ld.get("about", {}).get("floorSize", {})
        size_raw = self.css_first(
            response,
            "[class*='size']::text",
            "[class*='area']::text",
            "[class*='sqm']::text",
            "[data-size]::attr(data-size)",
        ) or ""
        item["property_size_sqm"] = (
            floor_ld.get("value")
            or parse_size(size_raw)
            or self._extract_size(full_text)
        )
        item["property_size_raw"] = size_raw or None
        item["stand_size_sqm"]    = None   # will match property_size_sqm for land

        if item["property_type"] == "land" and item["property_size_sqm"]:
            item["stand_size_sqm"] = item["property_size_sqm"]

        # Features
        item["features"] = [
            f.strip()
            for f in response.css(
                "[class*='feature']::text, [class*='amenity']::text, "
                "[class*='tag']::text, li::text"
            ).getall()
            if f.strip() and len(f.strip()) > 2
        ]

        # ── Agent ─────────────────────────────────────────────────────────────
        item["agency_name"] = (
            ld.get("author", {}).get("name")
            or self.css_first(
                response,
                "[class*='agency-name']::text",
                "[class*='agent-company']::text",
                "[class*='agency'] h2::text",
                "[class*='agency'] h3::text",
            )
        )
        phone_href = response.css("a[href^='tel:']::attr(href)").get("") or ""
        item["agent_phone"] = (
            phone_href.replace("tel:", "").strip()
            or self.css_first(response, "[class*='phone']::text", "[class*='tel']::text")
        ) or None
        item["agent_name"] = self.css_first(
            response,
            "[class*='agent-name']::text",
            "[class*='agent'] h3::text",
            "[class*='agent'] h4::text",
        )
        email_text = self.css_first(
            response, "a[href^='mailto:']::attr(href)", "[class*='email']::text"
        )
        item["agent_email"] = (
            email_text.replace("mailto:", "") if email_text and "@" in email_text else None
        )

        # ── Images ────────────────────────────────────────────────────────────
        raw_imgs = (
            ld.get("image")
            or response.css(
                "img[class*='gallery']::attr(src), "
                "img[class*='listing']::attr(src), "
                "img[class*='property']::attr(src), "
                "[class*='slider'] img::attr(src), "
                "[class*='carousel'] img::attr(src), "
                "img[src*='uploadedfiles']::attr(src), "
                "img[data-src]::attr(data-src)"
            ).getall()
        ) or []
        item["image_urls"] = [
            urljoin(BASE_URL, u)
            for u in raw_imgs
            if u and not re.search(r"(\.svg$|logo|icon|thumb-xs|avatar|amenities)", u, re.I)
        ]

        item["listing_date"] = ld.get("datePosted") or None

        return item

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_jsonld(self, response) -> dict:
        """Extract the first RealEstateListing / Product JSON-LD block."""
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                if isinstance(data, list):
                    data = data[0]
                if isinstance(data, dict) and data.get("@type") in (
                    "RealEstateListing", "Product", "Offer", "Apartment",
                    "House", "Residence", "LodgingBusiness",
                ):
                    return data
            except (json.JSONDecodeError, AttributeError):
                continue
        return {}

    @staticmethod
    def _parse_stat(text: str):
        m = re.search(r"\d+", text or "")
        return int(m.group()) if m else None

    @staticmethod
    def _extract_count(text: str, pattern: str):
        m = re.search(pattern, text or "", re.I)
        return int(m.group(1)) if m else None

    @staticmethod
    def _extract_size(text: str):
        """Pull first sqm / ha measurement from a blob of text."""
        m = re.search(r"([\d,]+)\s*(?:m2|m²|sqm)", text or "", re.I)
        if m:
            return float(m.group(1).replace(",", ""))
        ha = re.search(r"([\d.]+)\s*(?:ha|hectare)", text or "", re.I)
        if ha:
            return round(float(ha.group(1)) * 10_000, 1)
        return None

    def handle_error(self, failure):
        self.logger.error(f"[{self.name}] {failure.request.url}: {failure.value}")
        self._failed_count += 1

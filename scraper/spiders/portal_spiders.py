"""
Spider for Zimbabwe secondary property portals:
  - propsearch.co.zw     (~1,000 listings — residential + commercial)
  - stands.co.zw         (stand / land / plot specialists)
  - shonahome.com        (residential, Harare-focused)
  - privatepropertyzimbabwe.com (private seller listings)

All follow the same scraping pattern: list page → detail page → ?page=N pagination.
Playwright is used for JS-rendered pages.

Run a single portal:
    scrapy crawl portal_zw -a site=propsearch
    scrapy crawl portal_zw -a site=stands
    scrapy crawl portal_zw -a site=shonahome
    scrapy crawl portal_zw -a site=privateproperty

Run all:
    scrapy crawl portal_zw
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
    utc_now_iso, make_listing_id, enrich_location_fields, coalesce_agent_fields,
)

_WAIT = [PageMethod("wait_for_timeout", 3000)]


# ── Portal registry ────────────────────────────────────────────────────────────
PORTALS = {
    "propsearch": {
        "base_url": "https://propsearch.co.zw",
        "source":   "propsearch.co.zw",
        "api_mode": True,
        "api_endpoint": "/api/properties",
        "api_per_page": 50,
        "sale_paths": [
            "/property-for-sale/houses",
            "/property-for-sale/flats-apartments",
            "/property-for-sale/townhouses",
            "/property-for-sale/land-stands",
            "/property-for-sale/commercial",
            "/property-for-sale/farms",
        ],
        "rent_paths": [
            "/property-to-rent/houses",
            "/property-to-rent/flats-apartments",
            "/property-to-rent/townhouses",
            "/property-to-rent/commercial",
        ],
        # CSS selectors for list page
        "list_links": [
            "a.listing-card::attr(href)",
            "a.property-card::attr(href)",
            ".listing-title a::attr(href)",
            "div.result-item a::attr(href)",
        ],
        "list_next": [
            "a[rel='next']::attr(href)",
            "a[aria-label='Next']::attr(href)",
            "a.next::attr(href)",
        ],
        # CSS selectors for detail page
        "detail": {
            "title":  ["h1.listing-title::text", "h1::text"],
            "price":  [".listing-price::text", ".price::text", "[class*='price']::text"],
            "type":   [".property-type::text", ".listing-type::text"],
            "suburb": [".suburb::text", "[class*='suburb']::text"],
            "city":   [".city::text", "[class*='city']::text"],
            "beds":   [".beds::text", "[class*='bed'] span::text", "span.bedrooms::text"],
            "baths":  [".baths::text", "[class*='bath'] span::text", "span.bathrooms::text"],
            "size":   [".floor-size::text", "[class*='size'] span::text", ".area::text"],
            "agent":  [".agent-name::text", "[class*='agent'] h3::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": [
                ".gallery img::attr(src)",
                ".carousel img::attr(src)",
                "img[class*='property']::attr(src)",
            ],
            "date": ["time::attr(datetime)", ".date-listed::text"],
        },
    },

    "stands": {
        "base_url": "https://www.stands.co.zw",
        "source":   "stands.co.zw",
        "sale_paths": [
            "/stands-for-sale",
            "/residential-stands-for-sale",
            "/commercial-stands-for-sale",
            "/agricultural-land-for-sale",
            "/farms-for-sale",
            "/houses-for-sale",
        ],
        "rent_paths": [
            "/stands-to-rent",
            "/houses-to-rent",
        ],
        "list_links": [
            "a[href*='/listing/']::attr(href)",
            "a[href*='/stand/']::attr(href)",
            ".listing-card a::attr(href)",
            ".property-card a::attr(href)",
        ],
        "list_next": [
            "a[rel='next']::attr(href)",
            "a[aria-label='Next']::attr(href)",
            ".pagination a.active + a::attr(href)",
        ],
        "detail": {
            "title":  ["h1::text", "h1.listing-title::text"],
            "price":  [".price::text", ".listing-price::text", "[class*='price']::text"],
            "type":   [".category::text", ".property-type::text"],
            "suburb": [".suburb::text", ".location::text"],
            "city":   [".city::text", ".region::text"],
            "beds":   ["[class*='bed']::text"],
            "baths":  ["[class*='bath']::text"],
            "size":   [".stand-size::text", ".area::text", "[class*='size']::text"],
            "agent":  [".agent-name::text", ".seller-name::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": [".gallery img::attr(src)", "img[class*='listing']::attr(src)"],
            "date":   ["time::attr(datetime)", ".posted-date::text"],
        },
    },

    "shonahome": {
        "base_url": "https://www.shonahome.com",
        "source":   "shonahome.com",
        "sale_paths": [
            "/property-for-sale",
            "/houses-for-sale",
            "/flats-for-sale",
            "/land-for-sale",
        ],
        "rent_paths": [
            "/property-to-rent",
            "/houses-to-rent",
            "/flats-to-rent",
        ],
        "list_links": [
            "a.property-link::attr(href)",
            ".listing-grid a::attr(href)",
            ".property-card a::attr(href)",
            "a[href*='/property/']::attr(href)",
        ],
        "list_next": [
            "a[rel='next']::attr(href)",
            "a.next-page::attr(href)",
            "a[aria-label='Next']::attr(href)",
        ],
        "detail": {
            "title":  ["h1::text", "h1.entry-title::text"],
            "price":  [".property-price::text", ".price::text", "[class*='price']::text"],
            "type":   [".property-type::text", ".category::text"],
            "suburb": [".suburb::text", ".neighborhood::text"],
            "city":   [".city::text", ".town::text"],
            "beds":   [".bedrooms::text", "[class*='bedroom'] span::text"],
            "baths":  [".bathrooms::text", "[class*='bathroom'] span::text"],
            "size":   [".property-size::text", ".floor-area::text"],
            "agent":  [".agent-name::text", ".contact-name::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": ["img.property-image::attr(src)", ".gallery img::attr(src)"],
            "date":   ["time::attr(datetime)", ".listing-date::text"],
        },
    },

    "privateproperty": {
        "base_url": "https://www.privatepropertyzimbabwe.com",
        "source":   "privatepropertyzimbabwe.com",
        "sale_paths": [
            "/for-sale",
            "/houses-for-sale",
            "/apartments-for-sale",
            "/land-for-sale",
            "/commercial-for-sale",
        ],
        "rent_paths": [
            "/to-rent",
            "/houses-to-rent",
            "/apartments-to-rent",
        ],
        "list_links": [
            "a.listing-card::attr(href)",
            "a[href*='/listing/']::attr(href)",
            "a[href*='/property/']::attr(href)",
            ".property-list a::attr(href)",
        ],
        "list_next": [
            "a[rel='next']::attr(href)",
            "a[aria-label='Next']::attr(href)",
            ".pagination .next a::attr(href)",
        ],
        "detail": {
            "title":  ["h1::text", "h1.listing-title::text"],
            "price":  [".listing-price::text", ".price-tag::text"],
            "type":   [".listing-category::text", ".property-type::text"],
            "suburb": [".suburb::text", "[class*='suburb']::text"],
            "city":   [".city::text", "[class*='city']::text"],
            "beds":   ["[class*='bed'] span::text"],
            "baths":  ["[class*='bath'] span::text"],
            "size":   ["[class*='size'] span::text", ".floor-area::text"],
            "agent":  [".agent-name::text", ".lister-name::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": [".gallery img::attr(src)", "img[class*='main']::attr(src)"],
            "date":   ["time::attr(datetime)", ".posted::text"],
        },
    },

    # ── Property24 Zimbabwe ───────────────────────────────────────────────────
    # South African portal with a dedicated Zimbabwe section.
    # Uses standardised Property24 platform HTML.
    "property24": {
        "base_url": "https://www.property24.co.zw",
        "source":   "property24.co.zw",
        "sale_paths": [
            "/for-sale/harare",
            "/for-sale/bulawayo",
            "/for-sale/mutare",
            "/for-sale/gweru",
            "/for-sale/zimbabwe",
        ],
        "rent_paths": [
            "/to-rent/harare",
            "/to-rent/bulawayo",
            "/to-rent/zimbabwe",
        ],
        "list_links": [
            "a[data-listing-number]::attr(href)",
            "a.p24_regularTile::attr(href)",
            "a[href*='/for-sale/']::attr(href)",
            "a[href*='/to-rent/']::attr(href)",
        ],
        "list_next": [
            "a[title='Go to next page']::attr(href)",
            "a[aria-label='Next page']::attr(href)",
            "a.p24_pagination_next::attr(href)",
        ],
        "detail": {
            "title":  ["h1.p24_title::text", "h1::text"],
            "price":  [".p24_price::text", "span[class*='Price']::text", ".price::text"],
            "type":   [".p24_propertyType::text", "span[class*='PropertyType']::text"],
            "suburb": [".p24_suburb::text", "span[class*='Suburb']::text"],
            "city":   [".p24_city::text", "span[class*='City']::text"],
            "beds":   [".p24_beds::text", "span[class*='Bed']::text"],
            "baths":  [".p24_baths::text", "span[class*='Bath']::text"],
            "size":   [".p24_floorSize::text", "span[class*='FloorSize']::text"],
            "agent":  [".p24_agentName::text", "[class*='agentName']::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": ["img[class*='gallery']::attr(src)", ".p24_imageGallery img::attr(src)"],
            "date":   ["span[class*='ListedDate']::text", "time::attr(datetime)"],
        },
    },

    # ── WestProp Zimbabwe ─────────────────────────────────────────────────────
    # Major listed developer (Harare Stock Exchange). Off-plan and completed
    # developments. Treats development units as property listings.
    "westprop": {
        "base_url": "https://www.westprop.com",
        "source":   "westprop.com",
        "sale_paths": [
            "/developments",
            "/properties-for-sale",
            "/buy",
        ],
        "rent_paths": [
            "/properties-to-rent",
        ],
        "list_links": [
            "a.development-card::attr(href)",
            "a.property-card::attr(href)",
            "a[href*='/development/']::attr(href)",
            "a[href*='/property/']::attr(href)",
        ],
        "list_next": [
            "a[rel='next']::attr(href)",
            "a[aria-label='Next']::attr(href)",
        ],
        "detail": {
            "title":  ["h1.development-title::text", "h1::text"],
            "price":  [".development-price::text", ".price::text", "[class*='price']::text"],
            "type":   [".development-type::text", ".property-type::text"],
            "suburb": [".suburb::text", ".location::text"],
            "city":   [".city::text", ".region::text"],
            "beds":   [".beds::text", "[class*='bed'] span::text"],
            "baths":  [".baths::text", "[class*='bath'] span::text"],
            "size":   [".size::text", ".floor-area::text", "[class*='size']::text"],
            "agent":  [".contact-name::text", ".agent-name::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": [".gallery img::attr(src)", ".development-image img::attr(src)"],
            "date":   ["time::attr(datetime)", ".date::text"],
        },
    },

    # ── ZIMRE Property Investments ────────────────────────────────────────────
    # ZIMRE Group subsidiary — investment properties and commercial.
    "zimre": {
        "base_url": "https://www.zimreproperties.co.zw",
        "source":   "zimreproperties.co.zw",
        "sale_paths": [
            "/properties-for-sale",
            "/commercial-for-sale",
        ],
        "rent_paths": [
            "/properties-to-rent",
            "/commercial-to-rent",
        ],
        "list_links": [
            "a.property-card::attr(href)",
            "a.listing-card::attr(href)",
            "a[href*='/property/']::attr(href)",
        ],
        "list_next": [
            "a[rel='next']::attr(href)",
            "a[aria-label='Next']::attr(href)",
            "a.next::attr(href)",
        ],
        "detail": {
            "title":  ["h1::text"],
            "price":  [".price::text", ".listing-price::text"],
            "type":   [".property-type::text", ".category::text"],
            "suburb": [".suburb::text"],
            "city":   [".city::text"],
            "beds":   [".beds::text", "[class*='bed'] span::text"],
            "baths":  [".baths::text", "[class*='bath'] span::text"],
            "size":   [".area::text", ".floor-size::text"],
            "agent":  [".agent-name::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": [".gallery img::attr(src)"],
            "date":   ["time::attr(datetime)", ".date-listed::text"],
        },
    },

    # ── Mashonaland Holdings ──────────────────────────────────────────────────
    # ZSE-listed property company. Commercial, industrial, and residential
    # investment properties in Harare.
    "mashonaland": {
        "base_url": "https://www.mashonalandholdings.co.zw",
        "source":   "mashonalandholdings.co.zw",
        "sale_paths": [
            "/properties-for-sale",
            "/developments",
        ],
        "rent_paths": [
            "/properties-to-rent",
            "/commercial-to-rent",
        ],
        "list_links": [
            "a.property-card::attr(href)",
            "a[href*='/property/']::attr(href)",
            "a[href*='/listing/']::attr(href)",
        ],
        "list_next": [
            "a[rel='next']::attr(href)",
            "a[aria-label='Next']::attr(href)",
        ],
        "detail": {
            "title":  ["h1::text"],
            "price":  [".price::text", ".rental-amount::text"],
            "type":   [".property-type::text", ".category::text"],
            "suburb": [".suburb::text", ".area::text"],
            "city":   [".city::text"],
            "beds":   [".beds::text"],
            "baths":  [".baths::text"],
            "size":   [".floor-area::text", ".size::text"],
            "agent":  [".contact-person::text", ".agent-name::text"],
            "phone":  ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": [".gallery img::attr(src)", ".property-image img::attr(src)"],
            "date":   ["time::attr(datetime)", ".date::text"],
        },
    },
}


class PortalZwSpider(BasePropertySpider):
    """
    Multi-portal spider for secondary Zimbabwe property portals.
    Pass -a site=<key> to scrape one portal, or omit for all.
    """
    name   = "portal_zw"
    source = "portal_zw"   # overridden per request

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    def __init__(self, site: str = "all", listing_type: str = "all", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.listing_type = listing_type

        if site == "all":
            self._active_portals = list(PORTALS.values())
        elif site in PORTALS:
            self._active_portals = [PORTALS[site]]
        else:
            raise ValueError(
                f"Unknown site '{site}'. Choose from: {list(PORTALS.keys())} or 'all'"
            )

        self.allowed_domains = [
            d for cfg in self._active_portals
            for d in [
                cfg["base_url"].replace("https://www.", ""),
                cfg["base_url"].replace("https://", ""),
            ]
        ]

    def start_requests(self):
        for cfg in self._active_portals:
            if cfg.get("api_mode"):
                endpoint = cfg.get("api_endpoint", "/api/properties")
                per_page = cfg.get("api_per_page", 50)
                yield scrapy.Request(
                    url=f"{cfg['base_url']}{endpoint}?currentPage=1&perPage={per_page}",
                    callback=self.parse_list_page,
                    meta={
                        "listing_type": self.listing_type,
                        "portal_cfg": cfg,
                        "api_page": 1,
                    },
                    headers={
                        "Accept": "application/json, */*",
                        "Accept-Encoding": "gzip, deflate",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Referer": f"{cfg['base_url']}/for-sale",
                    },
                    errback=self.handle_error,
                )
                continue

            paths = []
            if self.listing_type in ("all", "sale"):
                paths += [(p, "sale") for p in cfg["sale_paths"]]
            if self.listing_type in ("all", "rent"):
                paths += [(p, "rent") for p in cfg["rent_paths"]]

            for path, lt in paths:
                yield scrapy.Request(
                    url=cfg["base_url"] + path,
                    callback=self.parse_list_page,
                    meta={
                        "listing_type": lt,
                        "portal_cfg":   cfg,
                        "playwright":   True,
                        "playwright_page_methods": _WAIT,
                    },
                    errback=self.handle_error,
                )

    # ── List page ─────────────────────────────────────────────────────────────

    def parse_list_page(self, response):
        cfg          = response.meta["portal_cfg"]
        listing_type = response.meta["listing_type"]
        base_url     = cfg["base_url"]

        if cfg.get("api_mode"):
            yield from self._parse_propsearch_api_page(response, cfg, listing_type)
            return

        # Try each link selector until we find results
        hrefs = []
        for selector in cfg["list_links"]:
            hrefs = response.css(selector).getall()
            if hrefs:
                break

        # Fallback: generic property-detail-looking href patterns
        if not hrefs:
            hrefs = [
                h for h in response.css("a::attr(href)").getall()
                if re.search(r"/(listing|property|for-sale|to-rent|stand)/[a-z0-9-]{6,}", h, re.I)
            ]

        abs_hrefs = list(dict.fromkeys(urljoin(base_url, h) for h in hrefs))
        self.logger.info(
            f"[{self.name}:{cfg['source']}] {len(abs_hrefs)} listings on {response.url}"
        )

        new_hrefs, stop = self._filter_new_hrefs(abs_hrefs)
        for url in new_hrefs:
            yield scrapy.Request(
                url=url,
                callback=self._safe_parse_listing,
                meta={
                    "listing_type": listing_type,
                    "portal_cfg":   cfg,
                    "playwright":   True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

        if stop:
            return

        # Pagination: try explicit selectors, then construct ?page=N
        next_url = None
        for selector in cfg["list_next"]:
            next_url = response.css(selector).get()
            if next_url and "javascript" not in next_url.lower():
                next_url = urljoin(base_url, next_url)
                break

        if not next_url and abs_hrefs:
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
                    "portal_cfg":   cfg,
                    "playwright":   True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

    def _parse_propsearch_api_page(self, response, cfg: dict, listing_type: str):
        """Parse one JSON page from PropSearch API and follow pagination."""
        try:
            payload = response.json()
        except Exception:
            self.logger.error(
                f"[{self.name}:{cfg['source']}] PropSearch API non-JSON response on {response.url}"
            )
            return

        page = int(payload.get("currentPage") or response.meta.get("api_page", 1))
        total_pages = int(payload.get("totalPages") or 1)
        total_items = int(payload.get("totalItems") or 0)
        rows = payload.get("data") or []

        yielded = 0
        for raw in rows:
            item = self._propsearch_item_from_api(raw, cfg, listing_type)
            if item is None:
                continue

            canonical_url = item["listing_url"]
            if self._seen.is_seen(canonical_url):
                self._skipped_count += 1
                continue

            self._seen.mark_seen(canonical_url)
            self._scraped_count += 1
            yielded += 1
            yield item

        self.logger.info(
            f"[{self.name}:{cfg['source']}] API page {page}/{total_pages}: "
            f"rows={len(rows)}, yielded={yielded}, total={total_items}"
        )

        if page < total_pages:
            per_page = cfg.get("api_per_page", 50)
            next_page = page + 1
            endpoint = cfg.get("api_endpoint", "/api/properties")
            next_url = f"{cfg['base_url']}{endpoint}?currentPage={next_page}&perPage={per_page}"
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_list_page,
                meta={
                    "listing_type": listing_type,
                    "portal_cfg": cfg,
                    "api_page": next_page,
                },
                headers={
                    "Accept": "application/json, */*",
                    "Accept-Encoding": "gzip, deflate",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": f"{cfg['base_url']}/for-sale",
                },
                errback=self.handle_error,
            )

    @staticmethod
    def _propsearch_room_count(rooms: list, bucket: str) -> int | None:
        total = 0
        b = bucket.lower()
        for room in rooms or []:
            parent = (room.get("parentRoomName") or "").lower()
            name = (room.get("roomTypeName") or "").lower()
            if b in parent or b in name:
                total += int(room.get("numberOfRooms") or 0)
        return total or None

    def _propsearch_item_from_api(self, raw: dict, cfg: dict, listing_type: str) -> PropertyListingItem | None:
        """Map one PropSearch API record to PropertyListingItem."""
        def _loc_name(v):
            if isinstance(v, dict):
                return (v.get("name") or "").strip()
            return (v or "").strip()

        price_obj = raw.get("price") or {}
        mandate = (raw.get("status") or {}).get("mandate") or {}
        mandate_tag = (mandate.get("tag") or "for-sale").lower()

        item_listing_type = "rent" if "rent" in mandate_tag else "sale"
        if listing_type in ("sale", "rent") and item_listing_type != listing_type:
            return None

        prop_ref = raw.get("propDeskRef") or raw.get("internalRef") or str(raw.get("listingId") or "")
        if not prop_ref:
            return None

        listing_url = f"{cfg['base_url']}/property/{prop_ref}"

        loc = raw.get("location") or {}
        suburb = _loc_name(loc.get("suburb")) or None
        city_raw = _loc_name(loc.get("city"))
        city = normalise_city(city_raw) if city_raw else None
        province = _loc_name(loc.get("province")) or None
        street = (loc.get("streetName") or "").strip()
        building = (loc.get("buildingName") or "").strip()
        address_parts = [p for p in [street, building, suburb, city or province] if p]
        address_raw = ", ".join(address_parts) if address_parts else None

        type_raw = None
        for t in raw.get("types") or []:
            type_raw = (
                (t.get("propertyType") or {}).get("name")
                or (t.get("category") or {}).get("name")
            )
            if type_raw:
                break

        buildings = raw.get("buildings") or []
        rooms = (buildings[0].get("rooms") or []) if buildings else []
        floor_size = buildings[0].get("floorSize") if buildings else None

        land = raw.get("land") or {}
        stand_size = land.get("totalLandSize")
        stand_unit = (land.get("totalLandSizePostfix") or "").lower()
        if stand_size is not None:
            try:
                stand_size = float(stand_size)
                if "ha" in stand_unit:
                    stand_size = stand_size * 10000
            except (TypeError, ValueError):
                stand_size = None

        image_urls = []
        for img in sorted(raw.get("images") or [], key=lambda x: x.get("position", 999)):
            img_url = img.get("processedUrl") or img.get("originalUrl")
            if img_url:
                image_urls.append(img_url)

        agency = raw.get("agency") or {}
        agency_name = agency.get("publicName") or agency.get("name")
        agency_phone = (
            agency.get("phone")
            or agency.get("phoneNumber")
            or agency.get("telephone")
            or agency.get("mobile")
        )
        agency_email = agency.get("email") or agency.get("emailAddress")
        agent_name = agency.get("contactName") or agency.get("agentName") or agency_name

        created_at = raw.get("createdAt")
        listing_date = created_at[:10] if isinstance(created_at, str) and len(created_at) >= 10 else None

        item = PropertyListingItem()
        item["listing_id"] = make_listing_id(cfg["source"], listing_url)
        item["source"] = cfg["source"]
        item["listing_url"] = listing_url
        item["property_title"] = (raw.get("title") or "").strip() or None

        if price_obj.get("poa"):
            item["property_price"] = None
            item["currency"] = None
        else:
            p = price_obj.get("price")
            item["property_price"] = float(p) if p is not None else None
            item["currency"] = (price_obj.get("currency") or {}).get("abbreviation")

        item["property_type"] = normalise_property_type(type_raw or "")
        item["listing_type"] = normalise_listing_type(item_listing_type)

        item["suburb"], item["city"], item["address_raw"] = enrich_location_fields(
            suburb,
            city,
            title=(raw.get("title") or "").strip() or None,
            address=address_raw,
            listing_url=listing_url,
        )
        item["latitude"] = None
        item["longitude"] = None

        item["number_of_bedrooms"] = self._propsearch_room_count(rooms, "bedroom")
        item["number_of_bathrooms"] = self._propsearch_room_count(rooms, "bathroom")
        item["number_of_garages"] = self._propsearch_room_count(rooms, "garage")
        item["property_size_sqm"] = float(floor_size) if floor_size is not None else None
        item["property_size_raw"] = f"{floor_size} m²" if floor_size is not None else None
        item["stand_size_sqm"] = stand_size

        item["features"] = [
            (f.get("name") or "").strip()
            for f in raw.get("propertyFeatures") or []
            if (f.get("name") or "").strip()
        ]
        item["agent_name"], item["agent_phone"], item["agent_email"], item["agency_name"] = coalesce_agent_fields(
            agent_name,
            agency_phone,
            agency_email,
            agency_name,
            fallback_text=(raw.get("title") or ""),
        )

        item["image_urls"] = image_urls
        item["listing_date"] = listing_date
        item["scraped_at"] = utc_now_iso()
        item["is_new_listing"] = True
        return item

    # ── Detail page ───────────────────────────────────────────────────────────

    def parse_listing(self, response) -> PropertyListingItem:
        cfg  = response.meta.get("portal_cfg", {})
        self.source = cfg.get("source", "portal_zw")

        item = PropertyListingItem()
        d    = cfg.get("detail", {})

        # ── JSON-LD (use if present) ──────────────────────────────────────────
        ld = self._extract_jsonld(response)

        # Title
        item["property_title"] = (
            ld.get("name")
            or self._try_selectors(response, d.get("title", []))
            or ""
        ).strip() or None

        # Price
        raw_price = (
            ld.get("offers", {}).get("price")
            or self._try_selectors(response, d.get("price", []))
            or ""
        )
        item["property_price"], item["currency"] = parse_price(str(raw_price))

        # Listing type from URL or meta
        url_lower = response.url.lower()
        if any(k in url_lower for k in ("to-rent", "for-rent", "to-let")):
            item["listing_type"] = "rent"
        elif "for-sale" in url_lower:
            item["listing_type"] = "sale"
        else:
            item["listing_type"] = normalise_listing_type(
                response.meta.get("listing_type", "sale")
            )

        # Property type
        type_raw = (
            self._try_selectors(response, d.get("type", []))
            or ld.get("@type", "")
        )
        item["property_type"] = normalise_property_type(type_raw)

        # Location
        suburb_ld = (
            ld.get("address", {}).get("addressLocality", "").strip()
            or ld.get("about", {}).get("address", {}).get("addressLocality", "").strip()
        )
        city_ld = (
            ld.get("address", {}).get("addressRegion", "").strip()
            or ld.get("about", {}).get("address", {}).get("addressRegion", "").strip()
        )
        suburb_css = self._try_selectors(response, d.get("suburb", [])) or ""
        city_css   = self._try_selectors(response, d.get("city", []))   or ""

        suburb_raw = suburb_ld or suburb_css
        city_raw   = city_ld   or city_css

        # Fallback: parse title for "in Suburb, City"
        if not suburb_raw or not city_raw:
            title = item.get("property_title", "") or ""
            loc_m = re.search(r"\bin\s+([^,]+),\s*(.+?)(?:\s*[-|]|$)", title, re.I)
            if loc_m:
                suburb_raw = suburb_raw or loc_m.group(1).strip()
                city_raw   = city_raw   or loc_m.group(2).strip()

        breadcrumb_text = ", ".join(
            t.strip() for t in response.css("[class*='breadcrumb'] a::text, [class*='breadcrumbs'] a::text").getall() if t.strip()
        ) or None
        item["suburb"], item["city"], item["address_raw"] = enrich_location_fields(
            suburb_raw,
            city_raw,
            title=item.get("property_title"),
            address=breadcrumb_text or item.get("property_title"),
            listing_url=response.url,
        )
        item["latitude"]    = ld.get("geo", {}).get("latitude")  or None
        item["longitude"]   = ld.get("geo", {}).get("longitude") or None

        # Attributes
        beds_raw  = self._try_selectors(response, d.get("beds",  []))
        baths_raw = self._try_selectors(response, d.get("baths", []))
        size_raw  = self._try_selectors(response, d.get("size",  []))

        item["number_of_bedrooms"]  = parse_int(beds_raw)
        item["number_of_bathrooms"] = parse_int(baths_raw)
        item["number_of_garages"]   = None
        item["property_size_sqm"]   = (
            ld.get("about", {}).get("floorSize", {}).get("value")
            or parse_size(size_raw)
        )
        item["property_size_raw"]   = size_raw
        item["stand_size_sqm"]      = None

        # Features
        item["features"] = [
            t.strip()
            for t in response.css(
                "[class*='feature']::text, [class*='amenity']::text, li.feature::text"
            ).getall()
            if t.strip() and len(t.strip()) > 2
        ]

        # Agent
        agent_raw  = self._try_selectors(response, d.get("agent", []))
        phone_raw  = self._try_selectors(response, d.get("phone", []))
        email_raw = response.css("a[href^='mailto:']::attr(href), a[href*='mailto']::attr(href)").get("")
        agency_raw = (
            ld.get("author", {}).get("name")
            or response.css("[class*='agency'] h2::text, [class*='agency'] h3::text").get()
        )
        contact_block = " ".join(
            t.strip() for t in response.css(
                ".agent::text, .agent-name::text, .contact::text, .contact-info::text, .contact-information::text, [class*='agent']::text, [class*='contact']::text"
            ).getall() if t.strip()
        )
        item["agent_name"], item["agent_phone"], item["agent_email"], item["agency_name"] = coalesce_agent_fields(
            agent_raw,
            phone_raw,
            email_raw,
            agency_raw,
            fallback_text=contact_block,
        )

        # Images
        image_urls = ld.get("image") or []
        if not image_urls:
            for sel in d.get("images", []):
                image_urls = response.css(sel).getall()
                if image_urls:
                    break
        item["image_urls"] = (
            image_urls if isinstance(image_urls, list) else [image_urls]
        )

        # Date
        item["listing_date"] = (
            ld.get("datePosted")
            or self._try_selectors(response, d.get("date", []))
        )

        return item

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _try_selectors(self, response, selectors: list) -> str | None:
        """Try a list of CSS selectors; return first non-empty result."""
        for sel in selectors:
            val = response.css(sel).get()
            if val and val.strip():
                return val.strip()
        return None

    def _extract_jsonld(self, response) -> dict:
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                if isinstance(data, list):
                    data = data[0]
                if isinstance(data, dict) and data.get("@type") in (
                    "RealEstateListing", "Product", "Offer",
                    "House", "Apartment", "Residence",
                ):
                    return data
            except (json.JSONDecodeError, AttributeError):
                continue
        return {}

    def handle_error(self, failure):
        self.logger.error(f"[{self.name}] {failure.request.url}: {failure.value}")
        self._failed_count += 1

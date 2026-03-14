"""
Spider for PropData-platform agency sites operating in Zimbabwe.

All three agencies run on the same PropData CMS with identical HTML selectors:
  - guestandtanner.co.zw   (~417 listings)
  - seeff.co.zw             (~359 listings)
  - kennanproperties.co.zw  (~170 listings)

Selectors are identical to property.co.zw (same platform, same HTML).

Run a specific agency:
    scrapy crawl propdata_zw -a site=guestandtanner
    scrapy crawl propdata_zw -a site=seeff
    scrapy crawl propdata_zw -a site=kennan

Run all agencies:
    scrapy crawl propdata_zw
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

_WAIT = [PageMethod("wait_for_timeout", 3000)]

# ── Site registry ──────────────────────────────────────────────────────────────
SITES = {
    "realtorville": {
        "base_url": "https://www.realtorville.co.zw",
        "source":   "realtorville.co.zw",
        "sale_paths": [
            "/houses-for-sale", "/flats-apartments-for-sale",
            "/townhouses-for-sale", "/residential-land-stands-for-sale",
            "/land-for-sale", "/agricultural-land-farms-for-sale",
            "/commercial-land-for-sale", "/commercial-property-for-sale",
            "/offices-for-sale", "/warehouses-for-sale",
            "/shops-for-sale", "/lodges-hotels-for-sale",
        ],
        "rent_paths": [
            "/houses-to-rent", "/flats-apartments-to-rent",
            "/townhouses-to-rent", "/commercial-property-to-rent",
            "/offices-to-rent",
        ],
    },

    "guestandtanner": {
        "base_url": "https://www.guestandtanner.co.zw",
        "source":   "guestandtanner.co.zw",
        "sale_paths": [
            "/houses-for-sale", "/flats-apartments-for-sale",
            "/townhouses-for-sale", "/land-for-sale",
            "/residential-land-stands-for-sale", "/commercial-land-for-sale",
            "/agricultural-land-farms-for-sale", "/commercial-property-for-sale",
            "/warehouses-for-sale", "/offices-for-sale", "/shops-for-sale",
            "/garden-flats-for-sale",
        ],
        "rent_paths": [
            "/houses-for-rent", "/flats-apartments-for-rent",
            "/townhouses-for-rent", "/garden-flats-for-rent",
            "/commercial-property-for-rent", "/offices-for-rent",
            "/shops-for-rent", "/warehouses-for-rent",
            "/cottages-for-rent",
        ],
    },
    "seeff": {
        "base_url": "https://www.seeff.co.zw",
        "source":   "seeff.co.zw",
        "sale_paths": [
            "/houses-for-sale", "/flats-apartments-for-sale",
            "/townhouses-for-sale", "/land-for-sale",
            "/commercial-property-for-sale",
        ],
        "rent_paths": [
            "/houses-for-rent", "/flats-apartments-for-rent",
            "/townhouses-for-rent", "/commercial-property-for-rent",
        ],
    },
    "kennan": {
        "base_url": "https://www.kennanproperties.co.zw",
        "source":   "kennanproperties.co.zw",
        "sale_paths": [
            "/houses-for-sale", "/flats-apartments-for-sale",
            "/townhouses-for-sale", "/land-for-sale",
            "/commercial-property-for-sale",
        ],
        "rent_paths": [
            "/houses-for-rent", "/flats-apartments-for-rent",
            "/townhouses-for-rent", "/commercial-property-for-rent",
        ],
    },

    # ── Additional PropData platform sites ───────────────────────────────────
    # These agencies also run on the PropData CMS (identical HTML/JSON-LD selectors).

    "zimproperties": {
        "base_url": "https://www.zimproperties.com",
        "source":   "zimproperties.com",
        "sale_paths": [
            "/houses-for-sale", "/flats-apartments-for-sale",
            "/townhouses-for-sale", "/land-for-sale",
            "/farms-for-sale", "/commercial-property-for-sale",
        ],
        "rent_paths": [
            "/houses-for-rent", "/flats-apartments-for-rent",
            "/townhouses-for-rent", "/commercial-property-for-rent",
        ],
    },

    "faranani": {
        "base_url": "https://www.faranani.co.zw",
        "source":   "faranani.co.zw",
        "sale_paths": [
            "/houses-for-sale", "/flats-apartments-for-sale",
            "/stands-for-sale", "/land-for-sale",
            "/commercial-property-for-sale",
        ],
        "rent_paths": [
            "/houses-for-rent", "/flats-apartments-for-rent",
            "/commercial-property-for-rent",
        ],
    },

    "harare_properties": {
        "base_url": "https://www.harareproperties.co.zw",
        "source":   "harareproperties.co.zw",
        "sale_paths": [
            "/houses-for-sale", "/flats-apartments-for-sale",
            "/townhouses-for-sale", "/land-for-sale",
            "/commercial-property-for-sale",
        ],
        "rent_paths": [
            "/houses-for-rent", "/flats-apartments-for-rent",
            "/townhouses-for-rent", "/commercial-property-for-rent",
        ],
    },
}


class PropDataZwSpider(BasePropertySpider):
    name   = "propdata_zw"
    source = "propdata_zw"   # overridden per request via meta

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    def __init__(self, site: str = "all", listing_type: str = "all", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.listing_type = listing_type

        if site == "all":
            self._active_sites = list(SITES.values())
        elif site in SITES:
            self._active_sites = [SITES[site]]
        else:
            raise ValueError(f"Unknown site '{site}'. Choose from: {list(SITES.keys())} or 'all'")

        self.allowed_domains = [
            d for cfg in self._active_sites
            for d in [cfg["base_url"].replace("https://www.", ""),
                      cfg["base_url"].replace("https://", "")]
        ]

    def start_requests(self):
        for cfg in self._active_sites:
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
                        "site_cfg":     cfg,
                        "playwright":   True,
                        "playwright_page_methods": _WAIT,
                    },
                    errback=self.handle_error,
                )

    # ── List page ─────────────────────────────────────────────────────────────

    def parse_list_page(self, response):
        cfg          = response.meta["site_cfg"]
        base_url     = cfg["base_url"]
        listing_type = response.meta["listing_type"]

        # PropData: listing cards use data-carousel="result" with data-href
        hrefs = response.css('div[data-carousel="result"]::attr(data-href)').getall()

        # Fallback: anchor links in result cards
        if not hrefs:
            hrefs = response.css(
                "a.gold-grid-thumbnails::attr(href), "
                "div.listing-result a::attr(href)"
            ).getall()

        abs_hrefs = list(dict.fromkeys(urljoin(base_url, h) for h in hrefs))
        self.logger.info(f"[{self.name}:{cfg['source']}] {len(abs_hrefs)} on {response.url}")

        new_hrefs, stop = self._filter_new_hrefs(abs_hrefs)
        for url in new_hrefs:
            yield scrapy.Request(
                url=url,
                callback=self._safe_parse_listing,
                meta={
                    "listing_type": listing_type,
                    "site_cfg":     cfg,
                    "playwright":   True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

        if stop:
            return

        # Pagination: next page has ?page=N in URL — skip javascript: hrefs
        current_page = int(re.search(r"[?&]page=(\d+)", response.url).group(1)) if "page=" in response.url else 1
        next_page    = current_page + 1

        # Try explicit next-page link first
        next_url = next(
            (h for h in response.css("a[aria-label='Next']::attr(href)").getall()
             if h and not h.startswith("javascript")),
            None,
        )
        # Fall back to constructing ?page=N
        if not next_url:
            base_path = response.url.split("?")[0]
            # Only follow if the current page actually had listings (avoid infinite loops)
            if abs_hrefs:
                next_url = f"{base_path}?page={next_page}"

        if next_url:
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_list_page,
                meta={
                    "listing_type": listing_type,
                    "site_cfg":     cfg,
                    "playwright":   True,
                    "playwright_page_methods": _WAIT,
                },
                errback=self.handle_error,
            )

    # ── Detail page ───────────────────────────────────────────────────────────

    def parse_listing(self, response) -> PropertyListingItem:
        cfg  = response.meta.get("site_cfg", {})
        # Override the source field with the agency's domain
        self.source = cfg.get("source", "propdata_zw")

        item = PropertyListingItem()

        # ── JSON-LD (PropData emits structured data on detail pages) ─────────
        ld = self._extract_jsonld(response)
        about  = ld.get("about", {})
        offers = ld.get("offers", {})
        addr   = about.get("address", {})
        geo    = about.get("geo", {})
        floor  = about.get("floorSize", {})

        item["property_title"] = (
            ld.get("name")
            or response.css("h1#ListingTitle::text, h1::text").get("").strip()
        )

        # Price
        raw_price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
        item["property_price"] = float(raw_price) if raw_price else None
        item["currency"]       = offers.get("priceCurrency", "USD")

        # Listing type — infer from URL
        url_lower = response.url.lower()
        if "for-rent" in url_lower or "to-rent" in url_lower:
            item["listing_type"] = "rent"
        elif "for-sale" in url_lower:
            item["listing_type"] = "sale"
        else:
            item["listing_type"] = normalise_listing_type(
                response.meta.get("listing_type", "sale")
            )

        # Property type — infer from the category path the spider started from
        # e.g. /houses-for-sale → house, /warehouses-for-sale → commercial
        path_lower = response.url.lower()
        if any(k in path_lower for k in ("warehouse", "factory", "industrial")):
            item["property_type"] = "commercial"
        elif any(k in path_lower for k in ("shop", "office", "commercial", "retail")):
            item["property_type"] = "commercial"
        elif any(k in path_lower for k in ("farm", "agricultural", "smallholding")):
            item["property_type"] = "farm"
        elif any(k in path_lower for k in ("land", "stand", "plot", "erf")):
            item["property_type"] = "land"
        elif any(k in path_lower for k in ("flat", "apartment")):
            item["property_type"] = "flat"
        elif any(k in path_lower for k in ("townhouse", "cluster")):
            item["property_type"] = "townhouse"
        elif any(k in path_lower for k in ("garden-flat", "cottage")):
            item["property_type"] = "house"
        elif "house" in path_lower:
            item["property_type"] = "house"
        else:
            # Last resort: parse URL detail slug
            type_slug = re.search(r"/for-(?:sale|rent)/([a-z-]+?)-[a-z]{2,6}\d", url_lower)
            type_hint = type_slug.group(1).replace("-", " ") if type_slug else ""
            item["property_type"] = normalise_property_type(type_hint)

        # Location — try JSON-LD first, fall back to title parsing
        suburb_raw = addr.get("addressLocality", "").strip()
        city_raw   = addr.get("addressRegion",   "").strip()

        if not suburb_raw or not city_raw:
            # Title often ends with "... in Suburb, City" or "... in Suburb, City Area"
            title = item.get("property_title", "") or ""
            loc_m = re.search(r"\bin\s+([^,]+),\s*(.+)$", title, re.I)
            if loc_m:
                suburb_raw = suburb_raw or loc_m.group(1).strip()
                city_raw   = city_raw   or loc_m.group(2).strip()

        item["suburb"]      = suburb_raw or None
        item["city"]        = normalise_city(city_raw)
        addr_parts          = response.css("div.address::text").getall()
        item["address_raw"] = " ".join(t.strip() for t in addr_parts if t.strip()) or item.get("property_title")
        item["latitude"]    = geo.get("latitude") or None
        item["longitude"]   = geo.get("longitude") or None

        # Attributes
        item["number_of_bedrooms"]  = about.get("numberOfBedrooms") or self._parse_stat(
            response.css("div.bed::text").get("")
        )
        item["number_of_bathrooms"] = about.get("numberOfBathroomsTotal") or self._parse_stat(
            response.css("div.bath::text").get("")
        )
        item["number_of_garages"]   = self._parse_stat(
            response.css("div.garage::text").get("")
        )

        area_texts = [t.strip() for t in response.css("div.area::text").getall() if t.strip()]
        item["property_size_sqm"] = (
            floor.get("value") or parse_size(area_texts[0] if area_texts else None)
        )
        item["property_size_raw"] = area_texts[0] if area_texts else None
        item["stand_size_sqm"]    = parse_size(area_texts[1] if len(area_texts) > 1 else None)

        # Features
        item["features"] = response.css("div.grid img[alt]::attr(alt)").getall()

        # Agent
        item["agency_name"] = (
            ld.get("author", {}).get("name")
            or response.css("a[href*='/estate-agents/'] h3::text").get("").strip()
        )
        phone_href = response.css("a[href^='tel:']::attr(href)").get("")
        item["agent_phone"] = phone_href.replace("tel:", "").strip() or None
        item["agent_name"]  = response.css("span.mainAgentNumber::text").get("").strip() or None
        item["agent_email"] = None

        # Images
        item["image_urls"] = ld.get("image") or response.css(
            "img.swiper-lazy::attr(src), img.swiper-lazy::attr(data-src)"
        ).getall()

        item["listing_date"] = ld.get("datePosted") or None

        return item

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_jsonld(self, response) -> dict:
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

    @staticmethod
    def _parse_stat(text: str):
        m = re.search(r"\d+", text)
        return int(m.group()) if m else None

    def handle_error(self, failure):
        self.logger.error(f"[{self.name}] {failure.request.url}: {failure.value}")
        self._failed_count += 1

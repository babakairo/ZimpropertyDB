"""
Wayback Machine spider — scrapes ALL historical property listings archived by
the Internet Archive (web.archive.org) for property.co.zw and classifieds.co.zw.

Coverage:
  - property.co.zw:    archived since 2005; practical data from ~2009 (USD adoption)
  - classifieds.co.zw: archived since ~2023

Strategy:
  1. Query Wayback CDX API to enumerate every unique archived listing URL.
  2. For each URL, fetch the earliest useful snapshot (>=2009-01-01).
  3. Parse using the same JSON-LD extraction as the live spiders, with fallbacks
     for older page formats that predate structured data.

CDX API reference:
  http://web.archive.org/cdx/search/cdx
    ?url=property.co.zw/for-sale/*
    &output=json
    &fl=timestamp,original,statuscode
    &filter=statuscode:200
    &collapse=urlkey          ← one result per unique URL
    &from=20090101            ← USD adoption in Zimbabwe
    &to=20260101

Run:
    scrapy crawl wayback                         # both sites, all years
    scrapy crawl wayback -a site=property        # property.co.zw only
    scrapy crawl wayback -a site=classifieds     # classifieds.co.zw only
    scrapy crawl wayback -a from_year=2015       # custom start year
"""
import re
import json
import scrapy
from urllib.parse import urljoin, quote

from scraper.spiders.base_spider import BasePropertySpider
from scraper.items import PropertyListingItem
from scraper.utils.helpers import (
    parse_price,
    parse_size,
    normalise_city,
    normalise_property_type,
    normalise_listing_type,
)

# ── CDX API targets ───────────────────────────────────────────────────────────
# Each tuple: (site_key, CDX url pattern, listing_type_hint)
_CDX_TARGETS = {
    "property": [
        # Sale listings
        ("property", "property.co.zw/for-sale/*",                       "sale"),
        ("property", "property.co.zw/houses-for-sale/*",                "sale"),
        ("property", "property.co.zw/flats-apartments-for-sale/*",      "sale"),
        ("property", "property.co.zw/townhouses-for-sale/*",            "sale"),
        ("property", "property.co.zw/land-for-sale/*",                  "sale"),
        ("property", "property.co.zw/commercial-property-for-sale/*",   "sale"),
        ("property", "property.co.zw/offices-for-sale/*",               "sale"),
        ("property", "property.co.zw/shops-for-sale/*",                 "sale"),
        ("property", "property.co.zw/warehouses-for-sale/*",            "sale"),
        ("property", "property.co.zw/agricultural-land-farms-for-sale/*", "sale"),
        # Rent listings
        ("property", "property.co.zw/for-rent/*",                       "rent"),
        ("property", "property.co.zw/houses-for-rent/*",                "rent"),
        ("property", "property.co.zw/flats-apartments-for-rent/*",      "rent"),
        ("property", "property.co.zw/townhouses-for-rent/*",            "rent"),
        ("property", "property.co.zw/land-for-rent/*",                  "rent"),
        ("property", "property.co.zw/commercial-property-for-rent/*",   "rent"),
    ],
    # classifieds.co.zw — CDX for all /listings/* then filter by property slug keywords.
    # classifieds slugs follow the pattern {suburb}-{property-type}-{id}, so keywords
    # reliably identify property vs vehicle/electronics/other listings.
    "classifieds": [
        ("classifieds", "classifieds.co.zw/listings/*",     "sale"),
        ("classifieds", "www.classifieds.co.zw/listings/*", "sale"),
    ],
}

# Property-type nouns used in classifieds.co.zw listing slugs.
# IMPORTANT: Only include words that are unambiguous property identifiers.
# Avoid: "sale", "rent", "office", "industrial", "commercial", "estate"
# — these appear in non-property slugs (e.g. "invertor-for-sale", "office-chair").
_PROPERTY_SLUG_KEYWORDS = {
    # Dwelling types
    "house", "houses",
    "flat", "flats",
    "apartment", "apartments",
    "townhouse", "townhouses",
    "cottage", "cottages",
    "penthouse",
    "duplex",
    "villa",
    "cluster",
    "sectional",
    # Land / plots
    "stand", "stands",
    "land",
    "farm", "farms",
    "plot", "plots",
    # Commercial property (multi-word slugs)
    "warehouse", "warehouses",
    # Generic — only safe as whole slug word
    "property",
    "accommodation",
    "letting",
}

CDX_BASE = "http://web.archive.org/cdx/search/cdx"
WB_BASE  = "https://web.archive.org/web"


def _cdx_url(pattern: str, from_ts: str, resume_key: str = None) -> str:
    params = (
        f"?url={pattern}"
        f"&output=json"
        f"&fl=timestamp,original,statuscode,urlkey"
        f"&filter=statuscode:200"
        f"&filter=!statuscode:3"          # exclude redirects
        f"&collapse=urlkey"               # one per unique URL
        f"&from={from_ts}"
        f"&to=20260101"
        f"&limit=5000"
    )
    if resume_key:
        params += f"&resumeKey={resume_key}"
    return CDX_BASE + params


class WaybackSpider(BasePropertySpider):
    name = "wayback"
    source = "web.archive.org"
    allowed_domains = [
        "web.archive.org",
        "property.co.zw", "www.property.co.zw",
        "classifieds.co.zw", "www.classifieds.co.zw",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        # Wayback serves gzip — let Scrapy handle it
        "COMPRESSION_ENABLED": True,
        # Retry on 5xx / timeout (archive can be slow)
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 429],
    }

    def __init__(self, site: str = "all", from_year: str = "2009", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.site = site
        self.from_ts = f"{from_year}0101000000"

    # ── Bootstrap: enumerate CDX API pages ───────────────────────────────────

    def start_requests(self):
        targets = []
        if self.site in ("all", "property"):
            targets += _CDX_TARGETS["property"]
        if self.site in ("all", "classifieds"):
            targets += _CDX_TARGETS["classifieds"]

        for site_key, pattern, lt in targets:
            yield scrapy.Request(
                url=_cdx_url(pattern, self.from_ts),
                callback=self._parse_cdx,
                meta={"listing_type": lt, "site_key": site_key, "cdx_pattern": pattern},
                errback=self.handle_error,
                priority=10,
            )

    # ── CDX result page → individual snapshot requests ────────────────────────

    def _parse_cdx(self, response):
        try:
            rows = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.warning(f"[wayback] Bad CDX JSON from {response.url}")
            return

        # First row is the header ["timestamp","original","statuscode","urlkey"]
        if not rows or len(rows) < 2:
            return

        header, *data = rows

        # Check for resumption key (last item may be {"resumeKey": "..."}
        resume_key = None
        if data and isinstance(data[-1], dict) and "resumeKey" in data[-1]:
            resume_key = data[-1]["resumeKey"]
            data = data[:-1]

        listing_type = response.meta["listing_type"]
        site_key     = response.meta["site_key"]
        pattern      = response.meta["cdx_pattern"]

        self.logger.info(
            f"[wayback] CDX returned {len(data)} URLs for pattern={pattern}"
        )

        for row in data:
            try:
                ts, original_url, statuscode, urlkey = row
            except (ValueError, TypeError):
                continue

            # Skip category/list pages — only want detail pages
            if self._is_list_page(original_url, site_key):
                continue

            snapshot_url = f"{WB_BASE}/{ts}/{original_url}"

            yield scrapy.Request(
                url=snapshot_url,
                callback=self._safe_parse_listing,
                meta={
                    "listing_type": listing_type,
                    "site_key": site_key,
                    "original_url": original_url,
                    "wayback_ts": ts,
                    "dont_redirect": True,
                },
                errback=self.handle_error,
                priority=5,
            )

        # Paginate CDX results if there's a resume key
        if resume_key:
            yield scrapy.Request(
                url=_cdx_url(pattern, self.from_ts, resume_key),
                callback=self._parse_cdx,
                meta=response.meta,
                errback=self.handle_error,
                priority=10,
            )

    def _is_list_page(self, url: str, site_key: str) -> bool:
        """Return True if URL should be skipped (non-detail or non-property page)."""
        url_lower = url.lower()
        if site_key == "property":
            # Detail pages end with /for-sale/{slug}-{id} or /for-rent/{slug}-{id}
            return not re.search(r"/for-(?:sale|rent)/[a-z0-9-]+-\d+$", url_lower)
        if site_key == "classifieds":
            if "/listings/" not in url_lower:
                return True  # category page, not a detail listing
            # Extract the slug part: /listings/{slug-NNNNNN}
            slug = url_lower.split("/listings/")[-1].split("?")[0].rstrip("/")
            # Split slug into hyphen-delimited words and require a full-word keyword match.
            # Substring matching (e.g. "land" in "island") causes false positives.
            words = set(slug.split("-"))
            return not (words & _PROPERTY_SLUG_KEYWORDS)
        return False

    # ── Detail page parser (dispatch by site) ────────────────────────────────

    def parse_listing(self, response) -> PropertyListingItem:
        site_key = response.meta.get("site_key", "")

        # Wayback wraps the page — check for soft-404 / error pages
        title_text = response.css("title::text").get("").lower()
        if any(x in title_text for x in ("just a moment", "access denied", "404", "not found")):
            self.logger.debug(f"[wayback] Skipping error page: {response.url}")
            return PropertyListingItem()

        if site_key == "property":
            return self._parse_property_co_zw(response)
        elif site_key == "classifieds":
            return self._parse_classifieds_co_zw(response)
        else:
            return self._parse_generic(response)

    # ── property.co.zw parser ────────────────────────────────────────────────

    def _parse_property_co_zw(self, response) -> PropertyListingItem:
        item = PropertyListingItem()
        ld = self._extract_jsonld(response, ("RealEstateListing", "Product", "Offer"))
        about  = ld.get("about", {})
        offers = ld.get("offers", {})
        address = about.get("address", {}) or ld.get("address", {})
        geo    = about.get("geo", {})
        floor_size = about.get("floorSize", {})

        item["listing_url"] = response.meta.get("original_url", response.url)
        item["property_title"] = (
            ld.get("name")
            or response.css("h1#ListingTitle::text, h1.listing-title::text").get("").strip()
            or None
        )

        raw_price = (
            offers.get("price")
            or offers.get("priceSpecification", {}).get("price")
            or ld.get("price")
        )
        item["property_price"] = float(raw_price) if raw_price else None
        item["currency"] = offers.get("priceCurrency", "USD")

        url_path = response.meta.get("original_url", response.url).lower()
        if "for-rent" in url_path or "to-rent" in url_path:
            lt = "rent"
        elif "for-sale" in url_path:
            lt = "sale"
        else:
            lt = response.meta.get("listing_type", "sale")
        item["listing_type"] = normalise_listing_type(lt)

        type_slug = re.search(r"/for-(?:sale|rent)/([a-z-]+?)-[a-z]{2,6}\d", url_path)
        type_hint = type_slug.group(1).replace("-", " ") if type_slug else ""
        item["property_type"] = normalise_property_type(type_hint)

        item["suburb"] = address.get("addressLocality", "").strip() or None
        item["city"]   = normalise_city(address.get("addressRegion", ""))
        addr_parts = response.css("div.address::text").getall()
        item["address_raw"] = " ".join(t.strip() for t in addr_parts if t.strip()) or None

        item["latitude"]  = geo.get("latitude") or None
        item["longitude"] = geo.get("longitude") or None

        item["number_of_bedrooms"]  = about.get("numberOfBedrooms") or self._parse_stat(
            response.css("div.bed::text, span.beds::text").get(""))
        item["number_of_bathrooms"] = about.get("numberOfBathroomsTotal") or self._parse_stat(
            response.css("div.bath::text, span.baths::text").get(""))
        item["number_of_garages"]   = self._parse_stat(
            response.css("div.garage::text, span.garages::text").get(""))

        area_texts = [t.strip() for t in response.css("div.area::text").getall() if t.strip()]
        item["property_size_sqm"] = floor_size.get("value") or parse_size(area_texts[0] if area_texts else None)
        item["property_size_raw"] = area_texts[0] if area_texts else None
        item["stand_size_sqm"]    = parse_size(area_texts[1] if len(area_texts) > 1 else None)

        item["features"] = response.css("div.grid img[alt]::attr(alt)").getall()

        item["agency_name"] = (
            ld.get("author", {}).get("name")
            or response.css("a[href*='/estate-agents/'] h3::text").get("").strip()
            or None
        )
        phone_href = response.css("a[href^='tel:']::attr(href)").get("")
        item["agent_phone"] = phone_href.replace("tel:", "").strip() or None
        item["agent_name"]  = response.css("span.mainAgentNumber::text").get("").strip() or None
        item["agent_email"] = None

        item["image_urls"] = ld.get("image") or response.css(
            "img.swiper-lazy::attr(src), img.swiper-lazy::attr(data-src)"
        ).getall()

        item["listing_date"] = (
            ld.get("datePosted")
            or response.meta.get("wayback_ts", "")[:8]  # YYYYMMDD from CDX timestamp
            or None
        )

        return item

    # ── classifieds.co.zw parser ─────────────────────────────────────────────

    def _parse_classifieds_co_zw(self, response) -> PropertyListingItem:
        item = PropertyListingItem()
        ld = self._extract_jsonld(response, ("Product",))
        offers     = ld.get("offers", {})
        address    = ld.get("address", {})
        floor_size = ld.get("floorSize", {})
        amenities  = ld.get("amenityFeature", [])

        item["listing_url"] = response.meta.get("original_url", response.url)
        item["property_title"] = (
            ld.get("name")
            or response.css("h1.page-header::text").get("").strip()
            or None
        )

        raw_price = offers.get("price")
        item["property_price"] = float(raw_price) if raw_price is not None else None
        item["currency"] = offers.get("priceCurrency", "USD")

        listing_type = response.meta.get("listing_type", "sale")
        url = response.meta.get("original_url", response.url).lower()
        if "to-rent" in url or "for-rent" in url:
            listing_type = "rent"
        elif "for-sale" in url:
            listing_type = "sale"
        item["listing_type"] = normalise_listing_type(listing_type)

        type_hint = response.meta.get("property_type_hint", "")
        item["property_type"] = normalise_property_type(type_hint)

        locality = address.get("addressLocality", "").strip()
        region   = address.get("addressRegion", "").strip()
        item["suburb"]      = locality.title() if locality else None
        item["city"]        = normalise_city(region or locality)
        item["address_raw"] = f"{locality}, {region}".strip(", ") or None

        item["latitude"]  = None
        item["longitude"] = None

        item["number_of_bedrooms"]  = ld.get("numberOfBedrooms")
        item["number_of_bathrooms"] = ld.get("numberOfBathroomsTotal")
        item["number_of_garages"]   = None

        size_val = floor_size.get("value")
        item["property_size_sqm"] = float(size_val) if size_val else None
        item["property_size_raw"] = f"{size_val} m²" if size_val else None
        item["stand_size_sqm"]    = None

        item["features"] = [
            a["name"] for a in amenities
            if isinstance(a, dict) and a.get("value") == "on"
        ]

        item["agent_name"]  = None
        phone_href = response.css("a[href^='tel:']::attr(href)").get("").replace("tel:", "").strip()
        item["agent_phone"] = phone_href or None
        item["agent_email"] = (
            response.css("a[href^='mailto:']::attr(href)").get("").replace("mailto:", "").strip()
            or None
        )
        item["agency_name"] = None

        ld_image = ld.get("image")
        if isinstance(ld_image, list):
            item["image_urls"] = ld_image
        elif isinstance(ld_image, str):
            item["image_urls"] = [ld_image]
        else:
            item["image_urls"] = response.css(
                ".gallery img::attr(src), .photos img::attr(src)"
            ).getall()

        ts = response.meta.get("wayback_ts", "")
        item["listing_date"] = (
            response.css("time::attr(datetime)").get()
            or (f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}" if len(ts) >= 8 else None)
        )

        return item

    # ── Generic fallback parser ───────────────────────────────────────────────

    def _parse_generic(self, response) -> PropertyListingItem:
        item = PropertyListingItem()
        ld = self._extract_jsonld(response)
        item["listing_url"]     = response.meta.get("original_url", response.url)
        item["property_title"]  = ld.get("name") or response.css("h1::text").get("").strip() or None
        item["listing_date"]    = response.meta.get("wayback_ts", "")[:8] or None
        item["listing_type"]    = normalise_listing_type(response.meta.get("listing_type", ""))
        item["property_type"]   = None
        item["property_price"]  = None
        item["currency"]        = "USD"
        return item

    # ── Shared helpers ────────────────────────────────────────────────────────

    def _extract_jsonld(self, response, types=None) -> dict:
        types = types or ("RealEstateListing", "Product", "Offer", "Accommodation")
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                if isinstance(data, list):
                    data = data[0]
                if not isinstance(data, dict):
                    continue
                if data.get("@type") in types:
                    return data
                # Search @graph
                for node in data.get("@graph", []):
                    if isinstance(node, dict) and node.get("@type") in types:
                        node["@graph"] = data.get("@graph", [])
                        return node
            except (json.JSONDecodeError, AttributeError, IndexError):
                continue
        return {}

    def _parse_stat(self, text: str):
        m = re.search(r"\d+", text or "")
        return int(m.group()) if m else None

    def handle_error(self, failure):
        self.logger.error(
            f"[{self.name}] Request failed: {failure.request.url} — {failure.value}"
        )
        self._failed_count += 1

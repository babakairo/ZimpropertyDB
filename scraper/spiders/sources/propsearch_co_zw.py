"""
Spider for propsearch.co.zw — Zimbabwe property portal.

Uses the PropSearch JSON REST API directly:
    GET /api/properties?currentPage=N&perPage=50

No HTML scraping needed — all data is returned as structured JSON.

Run:
    scrapy crawl propsearch_co_zw
    scrapy crawl propsearch_co_zw -a listing_type=sale
    scrapy crawl propsearch_co_zw -a listing_type=rent
"""
import re
import scrapy
from scraper.spiders.base_spider import BasePropertySpider
from scraper.items import PropertyListingItem
from scraper.utils.helpers import (
    parse_price,
    parse_size,
    normalise_city,
    normalise_property_type,
    normalise_listing_type,
    make_listing_id,
    utc_now_iso,
    enrich_location_fields,
    coalesce_agent_fields,
)

BASE_URL      = "https://propsearch.co.zw"
API_ENDPOINT  = "/api/properties"
AGENT_ENDPOINT = "/api/agents"
PER_PAGE      = 50

_HEADERS = {
    "Accept": "application/json, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{BASE_URL}/property-for-sale",
}


class PropSearchCoZwSpider(BasePropertySpider):
    """
    Scrapes all sale and rental listings from propsearch.co.zw via their
    private JSON API.  Full pagination is followed automatically.
    """
    name   = "propsearch_co_zw"
    source = "propsearch.co.zw"
    allowed_domains = ["propsearch.co.zw"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def __init__(self, listing_type: str = "all", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.listing_type = listing_type   # "all" | "sale" | "rent"

    # ── Entry point ──────────────────────────────────────────────────────────

    def start_requests(self):
        # The API returns all listing types in one paginated feed.
        # We filter per-record after fetching.
        url = f"{BASE_URL}{API_ENDPOINT}?currentPage=1&perPage={PER_PAGE}"
        yield scrapy.Request(
            url=url,
            callback=self.parse_list_page,
            headers=_HEADERS,
            meta={"api_page": 1},
            errback=self.handle_error,
        )

    # ── Parse paginated API response ─────────────────────────────────────────

    def parse_list_page(self, response):
        try:
            payload = response.json()
        except Exception as exc:
            self.logger.error(
                f"[{self.name}] Non-JSON response on {response.url}: {exc}"
            )
            return

        page        = int(payload.get("currentPage") or response.meta.get("api_page", 1))
        total_pages = int(payload.get("totalPages")  or 1)
        total_items = int(payload.get("totalItems")  or 0)
        rows        = payload.get("data") or []

        yielded = 0
        for raw in rows:
            item = self._map_record(raw)
            if item is None:
                continue

            # Filter by listing_type if requested
            if self.listing_type in ("sale", "rent") and item.get("listing_type") != self.listing_type:
                continue

            url = item["listing_url"]
            if self._seen.is_seen(url):
                self._skipped_count += 1
                continue

            self._seen.mark_seen(url)
            self._scraped_count += 1
            yielded += 1

            # Fetch agent profile if userId is available.
            # The listing API only returns agents: [{"userId": N}] — actual
            # name/phone/email require a second call to /api/agents/{userId}.
            first_user_id = self._extract_first_agent_user_id(raw)
            if first_user_id:
                yield scrapy.Request(
                    url=f"{BASE_URL}{AGENT_ENDPOINT}/{first_user_id}",
                    callback=self._parse_agent_and_yield,
                    headers=_HEADERS,
                    meta={"item": item},
                    errback=self._agent_errback,
                )
            else:
                yield item

        self.logger.info(
            f"[{self.name}] Page {page}/{total_pages} — "
            f"rows={len(rows)}, yielded={yielded}, total_items={total_items}"
        )

        # Follow next page
        if page < total_pages:
            next_page = page + 1
            yield scrapy.Request(
                url=f"{BASE_URL}{API_ENDPOINT}?currentPage={next_page}&perPage={PER_PAGE}",
                callback=self.parse_list_page,
                headers=_HEADERS,
                meta={"api_page": next_page},
                errback=self.handle_error,
            )

    # ── Map raw API record to PropertyListingItem ────────────────────────────

    def _map_record(self, raw: dict) -> PropertyListingItem | None:
        def _name(v):
            if isinstance(v, dict):
                return (v.get("name") or "").strip()
            return (str(v) or "").strip()

        # Listing reference (used to construct detail URL)
        ref = (
            raw.get("propDeskRef")
            or raw.get("internalRef")
            or str(raw.get("listingId") or "")
        ).strip()
        if not ref:
            return None

        listing_url = f"{BASE_URL}/property/{ref}"

        # Listing type from status mandate
        mandate_tag = (
            ((raw.get("status") or {}).get("mandate") or {}).get("tag") or "for-sale"
        ).lower()
        lt = "rent" if "rent" in mandate_tag or "let" in mandate_tag else "sale"

        # Price
        price_obj = raw.get("price") or {}
        if price_obj.get("poa"):
            price_val  = None
            currency   = None
        else:
            p = price_obj.get("price")
            price_val  = float(p) if p is not None else None
            currency   = (_name(price_obj.get("currency", {})) or "USD") if price_val else None

        # Location
        loc    = raw.get("location") or {}
        suburb = _name(loc.get("suburb")) or None
        city_r = _name(loc.get("city"))
        city   = normalise_city(city_r) if city_r else None
        prov   = _name(loc.get("province")) or None
        street = (loc.get("streetName") or "").strip()
        bldg   = (loc.get("buildingName") or "").strip()
        addr   = ", ".join(p for p in [street, bldg, suburb, city or prov] if p) or None

        # Property type
        type_raw = None
        for t in raw.get("types") or []:
            type_raw = (
                (t.get("propertyType") or {}).get("name")
                or (t.get("category") or {}).get("name")
            )
            if type_raw:
                break

        # Rooms (from buildings[0].rooms array)
        buildings  = raw.get("buildings") or []
        rooms      = buildings[0].get("rooms", []) if buildings else []
        floor_size = buildings[0].get("floorSize") if buildings else None

        beds  = self._count_rooms(rooms, "bedroom")
        baths = self._count_rooms(rooms, "bathroom")
        garages = self._count_rooms(rooms, "garage")

        # Stand/land size
        land = raw.get("land") or {}
        stand_val  = land.get("totalLandSize")
        stand_unit = (land.get("totalLandSizePostfix") or "").lower()
        if stand_val is not None:
            try:
                stand_val = float(stand_val)
                if "ha" in stand_unit:
                    stand_val *= 10_000
            except (TypeError, ValueError):
                stand_val = None

        # Images
        images = []
        for img in sorted(raw.get("images") or [], key=lambda x: x.get("position", 999)):
            url_img = img.get("processedUrl") or img.get("originalUrl")
            if url_img:
                images.append(url_img)

        # Agency
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

        # Features
        features = [
            (f.get("name") or "").strip()
            for f in raw.get("propertyFeatures") or []
            if (f.get("name") or "").strip()
        ]

        # Date
        created = raw.get("createdAt")
        listing_date = created[:10] if isinstance(created, str) and len(created) >= 10 else None

        item = PropertyListingItem()
        item["listing_id"]          = make_listing_id(self.source, listing_url)
        item["source"]              = self.source
        item["listing_url"]         = listing_url
        item["property_title"]      = (raw.get("title") or "").strip() or None
        item["property_price"]      = price_val
        item["currency"]            = currency
        item["listing_type"]        = normalise_listing_type(lt)
        item["property_type"]       = normalise_property_type(type_raw or "")
        item["suburb"], item["city"], item["address_raw"] = enrich_location_fields(
            suburb,
            city,
            title=(raw.get("title") or "").strip() or None,
            address=addr,
            listing_url=listing_url,
        )
        item["latitude"]            = None
        item["longitude"]           = None
        item["number_of_bedrooms"]  = beds
        item["number_of_bathrooms"] = baths
        item["number_of_garages"]   = garages
        item["property_size_sqm"]   = float(floor_size) if floor_size is not None else None
        item["property_size_raw"]   = f"{floor_size} m²" if floor_size is not None else None
        item["stand_size_sqm"]      = stand_val
        item["features"]            = features
        item["agent_name"], item["agent_phone"], item["agent_email"], item["agency_name"] = coalesce_agent_fields(
            agent_name,
            agency_phone,
            agency_email,
            agency_name,
            fallback_text=(raw.get("title") or ""),
        )
        item["image_urls"]          = images
        item["listing_date"]        = listing_date
        item["scraped_at"]          = utc_now_iso()
        item["is_new_listing"]      = True
        return item

    @staticmethod
    def _extract_first_agent_user_id(raw: dict) -> int | None:
        """Return the first userId from the agents array, or None."""
        agents = raw.get("agents") or []
        if agents and isinstance(agents[0], dict):
            return agents[0].get("userId")
        return None

    def _parse_agent_and_yield(self, response):
        """Callback: receive /api/agents/{userId} and attach contact to item."""
        item = response.meta["item"]
        try:
            profile = response.json()
        except Exception:
            yield item
            return

        # Build name from firstName + lastName
        first = (profile.get("firstName") or "").strip()
        last  = (profile.get("lastName")  or "").strip()
        name  = f"{first} {last}".strip() or None

        # First business phone (prefer business, fallback to first available)
        phone = None
        for p in profile.get("userPhones") or []:
            raw_phone = f"{p.get('dialingCode', '')}{p.get('phoneNumber', '')}".strip()
            if raw_phone:
                if p.get("use", "").lower() == "business":
                    phone = raw_phone
                    break
                phone = phone or raw_phone

        # First email
        email = None
        for e in profile.get("userEmails") or []:
            addr = (e.get("email") or "").strip().lower()
            if addr and "@" in addr:
                email = addr
                break

        # Agency name stays from the listing-level mapping
        agency_name = item.get("agency_name")

        item["agent_name"],  item["agent_phone"], \
        item["agent_email"], item["agency_name"] = coalesce_agent_fields(
            name, phone, email, agency_name,
            fallback_text=(item.get("property_title") or ""),
        )
        yield item

    def _agent_errback(self, failure):
        """If the agent profile call fails, yield the item without agent data."""
        item = failure.request.meta.get("item")
        if item:
            yield item
        self.logger.warning(
            f"[{self.name}] Agent profile fetch failed: {failure.request.url}"
        )

    @staticmethod
    def _count_rooms(rooms: list, bucket: str) -> int | None:
        total = 0
        b = bucket.lower()
        for room in rooms or []:
            parent = (room.get("parentRoomName") or "").lower()
            name   = (room.get("roomTypeName")   or "").lower()
            if b in parent or b in name:
                total += int(room.get("numberOfRooms") or 0)
        return total or None

    # No parse_listing needed — all data comes from the API
    def parse_listing(self, response) -> PropertyListingItem:
        raise NotImplementedError("PropSearch uses API mode — no detail page scraping.")

    def handle_error(self, failure):
        self.logger.error(f"[{self.name}] {failure.request.url}: {failure.value}")
        self._failed_count += 1

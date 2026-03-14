"""
Generic spider template for Zimbabwe real estate agent websites.
Pre-configured for common patterns; override selectors per agency.

Run:
    scrapy crawl zim_agent -a agency=remax_zw
"""
import re
import scrapy
import requests
from urllib.parse import urljoin
from parsel import Selector

from scraper.spiders.base_spider import BasePropertySpider
from scraper.items import PropertyListingItem
from scraper.utils.helpers import (
    parse_price, parse_size, parse_int,
    normalise_city, normalise_property_type, normalise_listing_type,
    make_listing_id, utc_now_iso, enrich_location_fields, coalesce_agent_fields,
)


_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


# ── Agency config registry ───────────────────────────────────────────────────
# Add new agencies here without writing a new spider.
AGENCY_CONFIGS = {
    "knight_frank_zw": {
        "base_url": "https://www.knightfrank.co.zw",
        "start_paths": ["/properties-for-sale", "/properties-to-let"],
        "list": {
            "links": "a.property-link::attr(href)",
            "next": "a.pagination-next::attr(href)",
        },
        "detail": {
            "title": "h1.property-title::text",
            "price": "span.price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".bedrooms span::text",
            "baths": ".bathrooms span::text",
            "size": ".floor-area span::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },
    "pam_golding_zw": {
        "base_url": "https://www.pamgolding.co.zw",
        "start_paths": ["/buy", "/rent"],
        "list": {
            "links": "a.listing-card-link::attr(href)",
            "next": "a[aria-label='Next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".listing-price::text",
            "type": ".category-tag::text",
            "suburb": ".suburb-name::text",
            "city": ".city-name::text",
            "beds": "span.beds-count::text",
            "baths": "span.baths-count::text",
            "size": "span.property-size::text",
            "agent": ".agent-fullname::text",
            "phone": ".agent-contact-tel::text",
            "images": "div.gallery-slide img::attr(src)",
            "date": ".date-listed::text",
        },
    },
    "api_zw": {
        "base_url": "https://www.api.co.zw",
        "start_paths": ["/residential-for-sale", "/residential-to-let"],
        "list": {
            "links": ".property-card a::attr(href)",
            "next": ".pager-next a::attr(href)",
        },
        "detail": {
            "title": "h1.page-title::text",
            "price": ".field-name-field-price .field-item::text",
            "type": ".field-name-field-property-type .field-item::text",
            "suburb": ".field-name-field-suburb .field-item::text",
            "city": ".field-name-field-city .field-item::text",
            "beds": ".field-name-field-bedrooms .field-item::text",
            "baths": ".field-name-field-bathrooms .field-item::text",
            "size": ".field-name-field-floor-size .field-item::text",
            "agent": ".field-name-field-agent-name .field-item::text",
            "phone": ".field-name-field-agent-phone .field-item::text",
            "images": ".field-name-field-images img::attr(src)",
            "date": ".date-display-single::text",
        },
    },

    # ── Fine & Country Zimbabwe ───────────────────────────────────────────────
    # Premium international agency. Uses their global nurtur.tech platform.
    # Verified 2026-03-13: /sales/property-for-sale/zimbabwe returns 10 listings.
    # Detail URLs: /harare-estate-agents/property-sale/<slug>/<id>
    "fine_country_zw": {
        "base_url": "https://www.fineandcountry.co.zw",
        "start_paths": [
            "/sales/property-for-sale/zimbabwe",
            "/lettings/property-to-rent/zimbabwe",
        ],
        "list": {
            "links": ".card-property a[href*='/property-sale/']::attr(href)",
            "next": "a[rel='next']::attr(href)",
        },
        "detail": {
            "title": "h1 span::text",
            "price": "[class*='price__value']::text",
            "type": "[class*='property-type']::text",
            "suburb": "[class*='suburb']::text",
            "city": "[class*='town']::text",
            "beds": "[class*='bedroom'] [class*='value']::text",
            "baths": "[class*='bathroom'] [class*='value']::text",
            "size": "[class*='size'] [class*='value']::text",
            "agent": ".agent__name::text",
            "phone": "a[href^='tel:']::attr(href)",
            "images": "img[src*='cdn.members.nurtur.tech']::attr(src)",
            "date": "[class*='date']::text",
        },
    },

    # ── Rawson Properties Zimbabwe ────────────────────────────────────────────
    # South African franchise with Zimbabwe operations.
    "rawson_zw": {
        "base_url": "https://www.rawsonproperties.com",
        "start_paths": [
            "/buy/zimbabwe",
            "/rent/zimbabwe",
        ],
        "list": {
            "links": "a.listing-card::attr(href)",
            "next": "a.pagination__next::attr(href)",
        },
        "detail": {
            "title": "h1.listing-detail__title::text",
            "price": ".listing-detail__price::text",
            "type": ".listing-detail__category::text",
            "suburb": ".listing-detail__suburb::text",
            "city": ".listing-detail__city::text",
            "beds": "[data-testid='beds'] span::text",
            "baths": "[data-testid='baths'] span::text",
            "size": "[data-testid='floor-size'] span::text",
            "agent": ".agent-card__name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".listing-gallery img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Century 21 Zimbabwe ───────────────────────────────────────────────────
    # International franchise with Zimbabwe office.
    "century21_zw": {
        "base_url": "https://www.century21.co.zw",
        "start_paths": ["/properties-for-sale", "/properties-to-rent"],
        "list": {
            "links": ".property-card a::attr(href)",
            "next": "a[aria-label='Next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds span::text",
            "baths": ".baths span::text",
            "size": ".floor-size span::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Integrated Properties Zimbabwe ────────────────────────────────────────
    # Local agency active in Harare/Bulawayo commercial and residential.
    "integrated_zw": {
        "base_url": "https://www.integratedproperties.co.zw",
        "start_paths": ["/property-for-sale", "/property-to-rent"],
        "list": {
            "links": "a.listing-link::attr(href)",
            "next": "a.next-page::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".listing-price::text",
            "type": ".property-category::text",
            "suburb": ".suburb-name::text",
            "city": ".city-name::text",
            "beds": ".bedrooms::text",
            "baths": ".bathrooms::text",
            "size": ".property-size::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".property-image img::attr(src)",
            "date": ".date-listed::text",
        },
    },

    # ── Pam Golding Zimbabwe ──────────────────────────────────────────────────
    # pamgoldingzimbabwe.co.zw redirects to pamgolding.co.zw (South Africa).
    # Verified 2026-03-13: /results/residential/for-sale/ returns 16 cards.
    # Links are relative paths on property-card-sm anchors.
    "pam_golding_zimbabwe_zw": {
        "base_url": "https://www.pamgolding.co.zw",
        "start_paths": [
            "/results/residential/for-sale/",
            "/results/residential/to-rent/",
            "/results/commercial/for-sale/",
        ],
        "list": {
            "links": "a.property-card-sm::attr(href)",
            "next": "a[rel='next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type-tag::text",
            "suburb": ".property-suburb::text",
            "city": ".property-city::text",
            "beds": "[data-key='bedrooms'] .value::text",
            "baths": "[data-key='bathrooms'] .value::text",
            "size": "[data-key='floor-size'] .value::text",
            "agent": ".agent-card-name::text",
            "phone": "a[href^='tel:']::attr(href)",
            "images": "img[src*='cloudfront.net']::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Rawson Properties Zimbabwe (local .co.zw domain) ─────────────────────
    # rawson.co.zw is the dedicated Zimbabwe domain (rawsonproperties.com is
    # the parent South African site with a /zimbabwe sub-section).
    "rawson_zw_local": {
        "base_url": "https://www.rawson.co.zw",
        "start_paths": ["/buy", "/rent"],
        "list": {
            "links": "a.listing-card::attr(href)",
            "next": "a.pagination__next::attr(href)",
        },
        "detail": {
            "title": "h1.listing-detail__title::text",
            "price": ".listing-detail__price::text",
            "type": ".listing-detail__category::text",
            "suburb": ".listing-detail__suburb::text",
            "city": ".listing-detail__city::text",
            "beds": "[data-testid='beds'] span::text",
            "baths": "[data-testid='baths'] span::text",
            "size": "[data-testid='floor-size'] span::text",
            "agent": ".agent-card__name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".listing-gallery img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Robert Root ───────────────────────────────────────────────────────────
    # Established Harare agency.
    "robert_root_zw": {
        "base_url": "https://www.robertroot.co.zw",
        "start_paths": ["/properties-for-sale", "/properties-to-rent"],
        "list": {
            "links": "a.property-card::attr(href)",
            "next": "a[aria-label='Next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".bedrooms span::text",
            "baths": ".bathrooms span::text",
            "size": ".floor-area span::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Stonebridge Real Estate ────────────────────────────────────────────────
    # Harare residential and commercial agency.
    "stonebridge_zw": {
        "base_url": "https://www.stonebridge.co.zw",
        "start_paths": ["/properties-for-sale", "/properties-to-rent"],
        "list": {
            "links": "a.listing-link::attr(href)",
            "next": "a.next::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".listing-price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds::text",
            "baths": ".baths::text",
            "size": ".area::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": ".date-listed::text",
        },
    },

    # ── John Pocock ───────────────────────────────────────────────────────────
    # Established Zimbabwe auction and estate agency.
    "john_pocock_zw": {
        "base_url": "https://www.johnpocock.co.zw",
        "start_paths": ["/for-sale", "/to-rent", "/auctions"],
        "list": {
            "links": "a.property-card::attr(href)",
            "next": "a[rel='next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds::text",
            "baths": ".baths::text",
            "size": ".floor-size::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".property-images img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Trevor Dollar ─────────────────────────────────────────────────────────
    # Boutique Harare luxury property agency.
    "trevor_dollar_zw": {
        "base_url": "https://www.trevordollar.co.zw",
        "start_paths": ["/properties-for-sale", "/properties-to-rent"],
        "list": {
            "links": "a.property-link::attr(href)",
            "next": "a.next-page::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".bedrooms::text",
            "baths": ".bathrooms::text",
            "size": ".area::text",
            "agent": ".agent::text",
            "phone": "a[href^='tel:']::text",
            "images": ".slider img::attr(src)",
            "date": ".date::text",
        },
    },

    # ── New Age Properties ────────────────────────────────────────────────────
    # Residential and commercial agency in Harare.
    "newage_properties_zw": {
        "base_url": "https://www.newageproperties.co.zw",
        "start_paths": ["/properties.php"],
        "list": {
            "links": "a.property-card::attr(href)",
            "next": "a[aria-label='Next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds span::text",
            "baths": ".baths span::text",
            "size": ".size span::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── RE/MAX Zimbabwe ───────────────────────────────────────────────────────
    # International franchise with Zimbabwe offices.
    "remax_zw": {
        "base_url": "https://www.remax.co.zw",
        "start_paths": ["/buy", "/rent"],
        "list": {
            "links": "a.listing-card::attr(href)",
            "next": "a.pagination-next::attr(href)",
        },
        "detail": {
            "title": "h1.listing-title::text",
            "price": ".listing-price::text",
            "type": ".listing-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": "[data-beds]::attr(data-beds)",
            "baths": "[data-baths]::attr(data-baths)",
            "size": ".floor-area::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".property-image img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Bridges Real Estate ───────────────────────────────────────────────────
    # Harare and Bulawayo residential specialist.
    "bridges_realestate_zw": {
        "base_url": "https://www.bridgesrealestate.co.zw",
        "start_paths": ["/properties-for-sale", "/properties-to-rent"],
        "list": {
            "links": "a.listing-card::attr(href)",
            "next": "a[rel='next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds::text",
            "baths": ".baths::text",
            "size": ".area::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": ".date-listed::text",
        },
    },

    # ── Tere Zim ──────────────────────────────────────────────────────────────
    # Property listing and management company in Zimbabwe.
    # Verified 2026-03-13: /properties/ page returns 15 cards per page with
    # WordPress pagination (/properties/page/N/). Detail at /property/<slug>/.
    "terezim_zw": {
        "base_url": "https://terezim.co.zw",
        "start_paths": ["/properties/"],
        "list": {
            "links": "a[href*='/property/']::attr(href)",
            "next": "a[rel='next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".item-price::text",
            "type": ".item-meta-listing-type .item-meta-value::text",
            "suburb": ".item-meta-suburb .item-meta-value::text",
            "city": ".item-meta-city .item-meta-value::text",
            "beds": ".item-meta-bedrooms .item-meta-value::text",
            "baths": ".item-meta-bathrooms .item-meta-value::text",
            "size": ".item-meta-area .item-meta-value::text",
            "agent": ".hz-vcard-name::text",
            "phone": "a[href^='tel:']::attr(href)",
            "images": "img[src*='wp-content/uploads']::attr(src)",
            "date": ".item-date::text",
        },
    },

    # ── Legacy Real Estate ────────────────────────────────────────────────────
    # Harare-based residential and luxury properties.
    "legacy_realestate_zw": {
        "base_url": "https://www.legacyrealestate.co.zw",
        "start_paths": ["/properties-for-sale", "/properties-to-rent"],
        "list": {
            "links": "a.property-card::attr(href)",
            "next": "a[rel='next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".bedrooms span::text",
            "baths": ".bathrooms span::text",
            "size": ".floor-area span::text",
            "agent": ".agent-name::text",
            "phone": "a[href^='tel:']::text",
            "images": ".property-gallery img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Exodus & Company ──────────────────────────────────────────────────────
    # Property services company in Zimbabwe.
    "exodus_zw": {
        "base_url": "https://www.exodusandcompany.com",
        "start_paths": ["/properties-for-sale", "/properties-to-rent"],
        "list": {
            "links": "a.listing-link::attr(href)",
            "next": "a.next-page::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds::text",
            "baths": ".baths::text",
            "size": ".area::text",
            "agent": ".agent::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": ".date::text",
        },
    },

    # ── Leengate ──────────────────────────────────────────────────────────────
    # Zimbabwe property agency.
    "leengate_zw": {
        "base_url": "https://www.leengate.co.zw",
        "start_paths": ["/available-stands"],
        "list": {
            "links": ".project h5 a::attr(href)",
            "next": "a.next::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".elementor-post-info__terms-list-item::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds::text",
            "baths": ".baths::text",
            "size": ".size::text",
            "agent": ".agent::text",
            "phone": "a[href^='tel:']::text",
            "images": "img::attr(src)",
            "date": "time::attr(datetime)",
        },
    },

    # ── Lucile Real Estate ────────────────────────────────────────────────────
    # Note: domain is lucilerealeastate.co.zw (typo in site name preserved).
    "lucile_realestate_zw": {
        "base_url": "https://www.lucilerealeastate.co.zw",
        "start_paths": ["/for-sale", "/to-rent"],
        "list": {
            "links": "a.property-card::attr(href)",
            "next": "a[rel='next']::attr(href)",
        },
        "detail": {
            "title": "h1::text",
            "price": ".price::text",
            "type": ".property-type::text",
            "suburb": ".suburb::text",
            "city": ".city::text",
            "beds": ".beds::text",
            "baths": ".baths::text",
            "size": ".area::text",
            "agent": ".agent::text",
            "phone": "a[href^='tel:']::text",
            "images": ".gallery img::attr(src)",
            "date": ".date::text",
        },
    },
}


class ZimAgentSpider(BasePropertySpider):
    """
    One spider that can scrape multiple agent websites using the config registry.
    Pass -a agency=<key> to select an agency.
    """
    name = "zim_agent"
    source = "zim_agent"

    def __init__(self, agency: str = "knight_frank_zw", *args, **kwargs):
        super().__init__(*args, **kwargs)
        if agency not in AGENCY_CONFIGS:
            raise ValueError(
                f"Unknown agency '{agency}'. "
                f"Available: {list(AGENCY_CONFIGS.keys())}"
            )
        self.agency = agency
        self.config = AGENCY_CONFIGS[agency]
        self.source = agency
        self.BASE_URL = self.config["base_url"]
        self.allowed_domains = [self.BASE_URL.replace("https://", "").replace("http://", "")]

    def start_requests(self):
        for path in self.config["start_paths"]:
            lt = "rent" if any(w in path for w in ["let", "rent", "lease"]) else "sale"
            yield scrapy.Request(
                url=self.BASE_URL + path,
                callback=self.parse_list_page,
                meta={"listing_type": lt},
                errback=self.handle_error,
            )

    def parse_list_page(self, response):
        cfg = self.config["list"]
        links = response.css(cfg["links"]).getall()

        if not links and self.agency == "newage_properties_zw":
            yielded = 0
            for item in self._parse_newage_cards(response):
                yielded += 1
                yield item
            self.logger.info(f"[{self.agency}] Parsed {yielded} property cards from {response.url}")
            return

        if not links:
            links = [
                h for h in response.css("a::attr(href)").getall()
                if re.search(
                    r"/(property|properties|listing|listings|for-sale|to-rent|to-let|buy|rent|project)/",
                    (h or ""),
                    re.I,
                )
            ]

        self.logger.info(f"[{self.agency}] Found {len(links)} on {response.url}")

        for href in set(links):
            yield scrapy.Request(
                url=urljoin(self.BASE_URL, href),
                callback=self._safe_parse_listing,
                meta=response.meta,
                errback=self.handle_error,
            )

        next_page = response.css(cfg.get("next", "a.next::attr(href)")).get()
        if next_page:
            yield scrapy.Request(
                url=urljoin(self.BASE_URL, next_page),
                callback=self.parse_list_page,
                meta=response.meta,
                errback=self.handle_error,
            )

    def _parse_newage_cards(self, response):
        cards = response.css(".property-card")
        if not cards:
            try:
                r = requests.get(response.url, headers=_HTTP_HEADERS, timeout=30, allow_redirects=True)
                if r.status_code == 200 and r.text:
                    cards = Selector(text=r.text).css(".property-card")
            except Exception as exc:
                self.logger.warning(f"[{self.agency}] New Age fallback fetch failed: {exc}")

        for idx, card in enumerate(cards, start=1):
            title = (card.css(".property-title::text").get() or "").strip()
            if not title:
                continue

            card_id = (card.css(".view-details-btn::attr(data-property-id)").get() or "").strip()
            listing_url = urljoin(self.BASE_URL, f"/properties.php#property-{card_id or idx}")
            if self._seen.is_seen(listing_url):
                self._skipped_count += 1
                continue

            item = PropertyListingItem()
            item["property_title"] = title

            price_raw = (card.css(".property-price::text").get() or "").strip()
            item["property_price"], item["currency"] = parse_price(price_raw)

            type_raw = (card.css(".property-badge::text").get() or "").strip()
            item["property_type"] = normalise_property_type(type_raw)

            status_raw = (card.css(".property-status::text").get() or "").strip().lower()
            inferred_listing_type = "rent" if any(k in status_raw for k in ("rent", "let")) else "sale"
            item["listing_type"] = normalise_listing_type(inferred_listing_type)

            location = (card.css(".property-location::text").get() or "").strip()
            suburb, city, address = enrich_location_fields(
                None,
                location,
                title=title,
                address=location or title,
                listing_url=listing_url,
            )
            item["suburb"] = suburb
            item["city"] = city
            item["address_raw"] = address
            item["latitude"] = None
            item["longitude"] = None

            detail_texts = [t.strip() for t in card.css(".property-details span::text").getall() if t.strip()]
            beds_raw = next((t for t in detail_texts if "bed" in t.lower()), "")
            baths_raw = next((t for t in detail_texts if "bath" in t.lower()), "")
            size_raw = next((t for t in detail_texts if "sqm" in t.lower() or "m2" in t.lower() or "m²" in t.lower()), "")

            item["number_of_bedrooms"] = parse_int(beds_raw)
            item["number_of_bathrooms"] = parse_int(baths_raw)
            item["number_of_garages"] = None
            item["property_size_raw"] = size_raw or None
            item["property_size_sqm"] = parse_size(size_raw)
            item["stand_size_sqm"] = None
            item["features"] = []

            item["agent_name"] = None
            item["agent_phone"] = None
            item["agent_email"] = None
            item["agency_name"] = "New Age Properties"
            item["agent_name"], item["agent_phone"], item["agent_email"], item["agency_name"] = coalesce_agent_fields(
                item.get("agent_name"),
                item.get("agent_phone"),
                item.get("agent_email"),
                item.get("agency_name"),
                fallback_text=title,
            )

            image = (
                card.css("img::attr(data-src)").get()
                or card.css("img::attr(src)").get()
                or ""
            ).strip()
            item["image_urls"] = [urljoin(self.BASE_URL, image)] if image else []

            item["listing_date"] = None
            item["listing_url"] = listing_url
            item["source"] = self.source
            item["listing_id"] = make_listing_id(self.source, listing_url)
            item["scraped_at"] = utc_now_iso()
            item["is_new_listing"] = True

            self._seen.mark_seen(listing_url)
            self._scraped_count += 1
            yield item

    def parse_listing(self, response) -> PropertyListingItem:
        item = PropertyListingItem()
        d = self.config["detail"]

        item["property_title"] = response.css(d["title"]).get("").strip() or None
        price_raw = response.css(d["price"]).get("").strip()
        item["property_price"], item["currency"] = parse_price(price_raw)
        item["listing_type"] = normalise_listing_type(response.meta.get("listing_type"))
        item["property_type"] = normalise_property_type(response.css(d["type"]).get(""))
        raw_suburb = (response.css(d["suburb"]).get("") or "").strip() or None
        raw_city = response.css(d["city"]).get("")
        raw_address = (
            response.css(".address::text, .property-address::text, .item-address::text, .listing-address::text").get()
            or response.css("[class*='breadcrumb'] a::text, [class*='breadcrumbs'] a::text").getall()[-1] if response.css("[class*='breadcrumb'] a::text, [class*='breadcrumbs'] a::text").getall() else None
        )
        item["suburb"], item["city"], item["address_raw"] = enrich_location_fields(
            raw_suburb,
            raw_city,
            title=item["property_title"],
            address=raw_address,
            listing_url=response.url,
        )
        item["latitude"] = None
        item["longitude"] = None
        item["number_of_bedrooms"] = parse_int(response.css(d["beds"]).get())
        item["number_of_bathrooms"] = parse_int(response.css(d["baths"]).get())
        item["number_of_garages"] = None
        size_raw = response.css(d["size"]).get()
        item["property_size_raw"] = size_raw
        item["property_size_sqm"] = parse_size(size_raw)
        item["stand_size_sqm"] = None
        item["features"] = []
        agent_block = " ".join(
            t.strip() for t in response.css(
                ".agent::text, .agent-name::text, .contact::text, .contact-information::text, .contact-info::text, [class*='agent']::text, [class*='contact']::text"
            ).getall() if t.strip()
        )
        email_raw = (
            response.css("a[href^='mailto:']::attr(href)").get("")
            or response.css("a[href*='mailto']::attr(href)").get("")
        )
        agency_raw = (
            response.css("[class*='agency'] h1::text, [class*='agency'] h2::text, [class*='agency'] h3::text, .broker-name::text").get("").strip()
            or self.agency.replace("_", " ").title()
        )
        item["agent_name"], item["agent_phone"], item["agent_email"], item["agency_name"] = coalesce_agent_fields(
            response.css(d["agent"]).get("").strip() or None,
            response.css(d["phone"]).get("").strip() or None,
            email_raw,
            agency_raw,
            fallback_text=agent_block,
        )
        item["image_urls"] = response.css(d["images"]).getall()
        item["listing_date"] = response.css(d["date"]).get("").strip() or None

        return item

    def handle_error(self, failure):
        self.logger.error(f"[{self.agency}] Request failed: {failure.request.url}")
        self._failed_count += 1

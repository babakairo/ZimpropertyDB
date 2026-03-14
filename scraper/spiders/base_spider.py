"""
Base spider that all Zimbabwe property spiders inherit from.
Provides common scraping patterns, error handling, and item population.
"""
import scrapy
from abc import abstractmethod
from scraper.items import PropertyListingItem
from scraper.utils.helpers import utc_now_iso, make_listing_id
from scraper.utils.seen_urls import SeenUrlsStore

# Shared across all spiders in the same process (singleton per process)
_seen_store: SeenUrlsStore | None = None


def _get_store() -> SeenUrlsStore:
    global _seen_store
    if _seen_store is None:
        _seen_store = SeenUrlsStore()
    return _seen_store


class BasePropertySpider(scrapy.Spider):
    """
    Base class enforcing a consistent interface across all property spiders.

    Subclasses must define:
        - name: str
        - source: str           (e.g. "property.co.zw")
        - start_urls: list[str]
        - parse_listing(response) -> PropertyListingItem

    Incremental scraping
    --------------------
    All spiders share a persistent SQLite store (data/seen_urls.sqlite).
    Call self._filter_new_hrefs(hrefs) in parse_list_page to get only
    new listing URLs and a flag indicating whether to stop paginating:

        new_hrefs, stop = self._filter_new_hrefs(hrefs)
        for href in new_hrefs:
            yield Request(href, ...)
        if stop:
            return          # entire page already seen — no need to go further
        next_url = ...
        if next_url:
            yield Request(next_url, ...)
    """

    # Override in subclass
    source: str = "unknown"
    custom_settings: dict = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._scraped_count = 0
        self._failed_count = 0
        self._skipped_count = 0
        self._seen = _get_store()

    # ── Public entry point ────────────────────────────────────────────────────

    def parse(self, response):
        """
        Default parse dispatches to listing-page detection or pagination.
        Subclasses override parse_listing_page() and parse_list_page().
        """
        if self.is_listing_page(response):
            yield from self._safe_parse_listing(response)
        else:
            yield from self.parse_list_page(response)

    # ── Abstract / optional overrides ─────────────────────────────────────────

    def is_listing_page(self, response) -> bool:
        """Return True when the response is a single listing detail page."""
        return False

    @abstractmethod
    def parse_list_page(self, response):
        """Yield Requests to listing detail pages + follow pagination."""
        ...

    @abstractmethod
    def parse_listing(self, response) -> PropertyListingItem:
        """Extract and return a PropertyListingItem from a detail page."""
        ...

    # ── Incremental helpers ───────────────────────────────────────────────────

    def _filter_new_hrefs(self, hrefs: list[str]) -> tuple[list[str], bool]:
        """
        Filter out already-scraped URLs.

        Returns (new_hrefs, stop_paginating).
        stop_paginating is True when every href on the page is already known —
        the caller should skip following the next-page link.
        """
        new_hrefs, all_seen = self._seen.filter_new(hrefs)
        if all_seen:
            self.logger.info(
                f"[{self.name}] All {len(hrefs)} listings on page already scraped "
                f"— stopping pagination."
            )
        elif len(hrefs) != len(new_hrefs):
            self.logger.debug(
                f"[{self.name}] {len(hrefs) - len(new_hrefs)} already-seen listings "
                f"skipped on this page."
            )
        return new_hrefs, all_seen

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _safe_parse_listing(self, response):
        # Normalise URL: for Wayback snapshots use the original URL as the key
        canonical_url = response.meta.get("original_url", response.url)

        if self._seen.is_seen(canonical_url):
            self._skipped_count += 1
            return

        try:
            item = self.parse_listing(response)
            if item:
                item["scraped_at"] = utc_now_iso()
                item["source"] = self.source
                # Preserve original_url set by wayback spider; fall back to response.url
                if not item.get("listing_url"):
                    item["listing_url"] = canonical_url
                item["listing_id"] = make_listing_id(self.source, canonical_url)
                item["is_new_listing"] = True
                self._seen.mark_seen(canonical_url)
                self._scraped_count += 1
                yield item
        except Exception as exc:
            self._failed_count += 1
            self.logger.error(f"Failed to parse {response.url}: {exc}", exc_info=True)

    def closed(self, reason):
        self.logger.info(
            f"Spider closed. Scraped={self._scraped_count}, "
            f"Skipped(seen)={self._skipped_count}, "
            f"Failed={self._failed_count}, "
            f"Total seen in DB={self._seen.count()}, "
            f"Reason={reason}"
        )

    @staticmethod
    def css_first(response, *selectors, default=None) -> str | None:
        """Try multiple CSS selectors, return first non-empty text."""
        for sel in selectors:
            val = response.css(sel).get(default=None)
            if val and val.strip():
                return val.strip()
        return default

    @staticmethod
    def xpath_first(response, *xpaths, default=None) -> str | None:
        for xp in xpaths:
            val = response.xpath(xp).get(default=None)
            if val and val.strip():
                return val.strip()
        return default

"""
Custom Scrapy middlewares for Zimbabwe Property scraper.

Included:
    - RotatingUserAgentMiddleware   : rotates desktop + mobile UAs
    - ProxyMiddleware               : rotates proxy list from settings
    - DuplicateFilterMiddleware     : spider-level URL dedup using fingerprints
"""
import random
import hashlib
import logging
from scrapy import signals
from scrapy.http import Request
from scrapy.exceptions import IgnoreRequest

logger = logging.getLogger(__name__)

# ── User Agents ──────────────────────────────────────────────────────────────

DESKTOP_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
]

MOBILE_USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
]

ALL_USER_AGENTS = DESKTOP_USER_AGENTS + MOBILE_USER_AGENTS


class RotatingUserAgentMiddleware:
    """
    Assign a random User-Agent to every request.
    Biased 80% desktop, 20% mobile to mimic real traffic.
    """

    def process_request(self, request: Request, spider):
        ua = (
            random.choice(DESKTOP_USER_AGENTS)
            if random.random() < 0.8
            else random.choice(MOBILE_USER_AGENTS)
        )
        request.headers["User-Agent"] = ua


# ── Proxy Rotation ───────────────────────────────────────────────────────────

class ProxyMiddleware:
    """
    Rotate through a list of proxies defined in settings.PROXY_LIST.
    Silently skips if no proxies are configured.
    """

    def __init__(self, proxy_list: list[str]):
        self.proxies = [p.strip() for p in proxy_list if p.strip()]
        if self.proxies:
            logger.info(f"ProxyMiddleware: loaded {len(self.proxies)} proxies")
        else:
            logger.info("ProxyMiddleware: no proxies configured — direct connection")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(proxy_list=crawler.settings.getlist("PROXY_LIST", []))

    def process_request(self, request: Request, spider):
        if not self.proxies:
            return
        proxy = random.choice(self.proxies)
        request.meta["proxy"] = proxy

    def process_exception(self, request: Request, exception, spider):
        if self.proxies and "proxy" in request.meta:
            bad_proxy = request.meta.pop("proxy")
            logger.warning(f"Proxy failed ({bad_proxy}): {exception}. Retrying without proxy.")
            # Retry without that proxy this round
            return request


# ── Duplicate Filter ─────────────────────────────────────────────────────────

class DuplicateFilterMiddleware:
    """
    Spider-level in-memory URL deduplication.
    Works alongside Scrapy's built-in DUPEFILTER_CLASS for extra safety.
    Fingerprint = SHA-1 of URL stripped of query params.
    """

    def __init__(self):
        self._seen: set[str] = set()

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls()
        crawler.signals.connect(obj.spider_opened, signal=signals.spider_opened)
        return obj

    def spider_opened(self, spider):
        logger.info(f"DuplicateFilterMiddleware active for spider: {spider.name}")

    def process_spider_output(self, response, result, spider):
        for item_or_request in result:
            if isinstance(item_or_request, Request):
                fp = self._fingerprint(item_or_request.url)
                if fp in self._seen:
                    logger.debug(f"Duplicate URL skipped: {item_or_request.url}")
                    continue
                self._seen.add(fp)
            yield item_or_request

    @staticmethod
    def _fingerprint(url: str) -> str:
        # Keep query params for PropSearch API pagination so
        # /api/properties?currentPage=1 and ?currentPage=2 are treated as unique.
        if "propsearch.co.zw/api/properties" in url.lower():
            canonical = url.rstrip("/").lower()
        else:
            canonical = url.split("?")[0].rstrip("/").lower()
        return hashlib.sha1(canonical.encode()).hexdigest()

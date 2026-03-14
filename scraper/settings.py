"""
Scrapy settings for Zimbabwe Property Market Intelligence Platform.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

BOT_NAME = "zim_property_scraper"
SPIDER_MODULES = ["scraper.spiders"]
NEWSPIDER_MODULE = "scraper.spiders"

# ─── Politeness ────────────────────────────────────────────────────────────────
ROBOTSTXT_OBEY = True
DOWNLOAD_DELAY = 2          # seconds between requests per domain
RANDOMIZE_DOWNLOAD_DELAY = True  # actual delay = 0.5x–1.5x DOWNLOAD_DELAY
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 2
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.5

# ─── Retry ─────────────────────────────────────────────────────────────────────
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# ─── Cache (useful during development) ─────────────────────────────────────────
HTTPCACHE_ENABLED = os.getenv("SCRAPY_HTTPCACHE", "false").lower() == "true"
HTTPCACHE_EXPIRATION_SECS = 86400  # 24 hours
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_IGNORE_HTTP_CODES = [403, 404, 500, 503]

# ─── Middlewares ────────────────────────────────────────────────────────────────
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scraper.middlewares.RotatingUserAgentMiddleware": 400,
    "scraper.middlewares.ProxyMiddleware": 410,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,
}

SPIDER_MIDDLEWARES = {
    "scraper.middlewares.DuplicateFilterMiddleware": 100,
}

# ─── Item Pipelines ─────────────────────────────────────────────────────────────
ITEM_PIPELINES = {
    "scraper.pipelines.validation.ValidationPipeline": 100,
    "scraper.pipelines.dedup.DedupPipeline": 200,
    "scraper.pipelines.jsonl_export.JsonlExportPipeline": 300,
    "scraper.pipelines.snowflake_pipeline.SnowflakePipeline": 400,
}

# ─── Feeds ──────────────────────────────────────────────────────────────────────
FEEDS = {
    "data/listings_%(time)s.jsonl": {
        "format": "jsonlines",
        "encoding": "utf8",
        "store_empty": False,
        "overwrite": True,
    },
}

# ─── Snowflake credentials (injected from .env) ──────────────────────────────
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_RAW_TABLE", "ZW_PROPERTY_LISTINGS")

# ─── Snowflake pipeline batch size ──────────────────────────────────────────
SNOWFLAKE_BATCH_SIZE = int(os.getenv("SNOWFLAKE_BATCH_SIZE", "500"))

# ─── Proxy settings (optional) ──────────────────────────────────────────────
PROXY_LIST = os.getenv("PROXY_LIST", "").split(",")  # comma-separated proxy URLs

# ─── Request headers ─────────────────────────────────────────────────────────
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ─── Playwright (always enabled — used by propertybook_co_zw spider) ──────────
DOWNLOAD_HANDLERS = {
    "http":  "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "args": ["--no-sandbox", "--disable-dev-shm-usage"],
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 45_000  # ms
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }
}

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

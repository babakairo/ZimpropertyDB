"""
ValidationPipeline — first pipeline stage.

Drops items that are clearly unusable (no URL, no title).
Normalises fields and fills defaults.
"""
import logging
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = {"listing_url", "property_title"}
MAX_PRICE_USD = 50_000_000   # sanity cap: $50M
MIN_PRICE_USD = 10           # sanity floor: $10


class ValidationPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # ── Required fields ──────────────────────────────────────────────────
        for field in REQUIRED_FIELDS:
            if not adapter.get(field):
                raise DropItem(f"Missing required field '{field}' in {adapter.get('listing_url')}")

        # ── Price sanity check ───────────────────────────────────────────────
        price = adapter.get("property_price")
        currency = adapter.get("currency", "USD")
        if price is not None:
            # ZWL prices can be very large; only cap USD
            if currency == "USD":
                if price < MIN_PRICE_USD or price > MAX_PRICE_USD:
                    logger.warning(
                        f"Suspicious USD price {price} for {adapter['listing_url']} — keeping but flagging"
                    )

        # ── Normalise booleans / lists ───────────────────────────────────────
        if not isinstance(adapter.get("features", []), list):
            adapter["features"] = []
        if not isinstance(adapter.get("image_urls", []), list):
            adapter["image_urls"] = []

        # ── Ensure string fields are stripped ───────────────────────────────
        for str_field in ("property_title", "city", "suburb", "agent_name", "agency_name"):
            val = adapter.get(str_field)
            if val and isinstance(val, str):
                adapter[str_field] = val.strip() or None

        return item

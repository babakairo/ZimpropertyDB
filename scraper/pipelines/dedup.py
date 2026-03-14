"""
DedupPipeline — second pipeline stage.

Maintains a set of seen listing_ids per scrape session.
Items with duplicate IDs are dropped.

NOTE: Cross-session deduplication is handled by Snowflake's MERGE statement
in the SnowflakePipeline — this is an in-process filter only.
"""
import logging
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)


class DedupPipeline:
    def __init__(self):
        self._seen_ids: set[str] = set()
        self._dropped = 0

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        lid = adapter.get("listing_id")

        if not lid:
            # Can't dedup without ID — pass through
            return item

        if lid in self._seen_ids:
            self._dropped += 1
            raise DropItem(f"Duplicate listing_id {lid} — {adapter.get('listing_url')}")

        self._seen_ids.add(lid)
        return item

    def close_spider(self, spider):
        logger.info(f"DedupPipeline: dropped {self._dropped} in-session duplicates")

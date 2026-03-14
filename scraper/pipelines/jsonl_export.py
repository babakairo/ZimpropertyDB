"""
JsonlExportPipeline — third pipeline stage.

Writes items to a JSONL file for backup / replay purposes.
One file per spider run, timestamped.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)


class JsonlExportPipeline:
    def __init__(self):
        self._file = None
        self._path = None
        self._count = 0

    def open_spider(self, spider):
        Path("data").mkdir(exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._path = Path("data") / f"{spider.name}_{ts}.jsonl"
        self._file = self._path.open("w", encoding="utf-8")
        logger.info(f"JsonlExportPipeline: writing to {self._path}")

    def close_spider(self, spider):
        if self._file:
            self._file.close()
        logger.info(f"JsonlExportPipeline: wrote {self._count} records to {self._path}")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        line = json.dumps(dict(adapter), ensure_ascii=False, default=str)
        self._file.write(line + "\n")
        self._count += 1
        return item

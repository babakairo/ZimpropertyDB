"""
SnowflakePipeline — fourth pipeline stage.

Buffers scraped items and bulk-inserts them into Snowflake RAW layer
using MERGE to handle duplicates on listing_id.

Activated only when SNOWFLAKE_ACCOUNT is set in settings.
"""
import json
import logging
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)


class SnowflakePipeline:
    def __init__(self, settings):
        self.account = settings.get("SNOWFLAKE_ACCOUNT")
        self.user = settings.get("SNOWFLAKE_USER")
        self.password = settings.get("SNOWFLAKE_PASSWORD")
        self.database = settings.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB")
        self.schema = settings.get("SNOWFLAKE_SCHEMA", "RAW")
        self.warehouse = settings.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        self.role = settings.get("SNOWFLAKE_ROLE", "SYSADMIN")
        self.table = settings.get("SNOWFLAKE_TABLE", "ZW_PROPERTY_LISTINGS")
        self.batch_size = settings.getint("SNOWFLAKE_BATCH_SIZE", 500)
        self._buffer: list[dict] = []
        self._conn = None
        self._cursor = None
        self._inserted = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(settings=crawler.settings)

    def open_spider(self, spider):
        if not self.account:
            logger.warning(
                "SnowflakePipeline: SNOWFLAKE_ACCOUNT not set — Snowflake loading disabled"
            )
            return
        try:
            import snowflake.connector
            self._conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role,
                application="ZimPropertyScraper",
            )
            self._cursor = self._conn.cursor()
            logger.info(f"SnowflakePipeline: connected to {self.database}.{self.schema}")
        except Exception as exc:
            logger.error(f"SnowflakePipeline: connection failed — {exc}")
            self._conn = None

    def close_spider(self, spider):
        if self._buffer:
            self._flush()
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        logger.info(f"SnowflakePipeline: total inserted/merged = {self._inserted}")

    def process_item(self, item, spider):
        if not self._conn:
            return item
        self._buffer.append(dict(ItemAdapter(item)))
        if len(self._buffer) >= self.batch_size:
            self._flush()
        return item

    # ── Private ──────────────────────────────────────────────────────────────

    def _flush(self):
        if not self._buffer or not self._cursor:
            return

        # Stage data as a temp table then MERGE
        rows = [self._to_row(r) for r in self._buffer]
        self._cursor.executemany(self._merge_sql(), rows)
        self._conn.commit()
        self._inserted += len(rows)
        logger.info(f"SnowflakePipeline: flushed {len(rows)} rows (total={self._inserted})")
        self._buffer.clear()

    def _to_row(self, record: dict) -> tuple:
        return (
            record.get("listing_id"),
            record.get("source"),
            record.get("property_title"),
            record.get("property_price"),
            record.get("currency"),
            record.get("property_type"),
            record.get("listing_type"),
            record.get("city"),
            record.get("suburb"),
            record.get("address_raw"),
            record.get("latitude"),
            record.get("longitude"),
            record.get("number_of_bedrooms"),
            record.get("number_of_bathrooms"),
            record.get("number_of_garages"),
            record.get("property_size_sqm"),
            record.get("property_size_raw"),
            record.get("stand_size_sqm"),
            json.dumps(record.get("features") or []),
            record.get("agent_name"),
            record.get("agent_phone"),
            record.get("agent_email"),
            record.get("agency_name"),
            json.dumps(record.get("image_urls") or []),
            record.get("listing_url"),
            record.get("listing_date"),
            record.get("scraped_at"),
        )

    def _merge_sql(self) -> str:
        full_table = f"{self.database}.{self.schema}.{self.table}"
        return f"""
        MERGE INTO {full_table} AS target
        USING (
            SELECT
                %s AS listing_id, %s AS source, %s AS property_title,
                %s AS property_price, %s AS currency, %s AS property_type,
                %s AS listing_type, %s AS city, %s AS suburb,
                %s AS address_raw, %s AS latitude, %s AS longitude,
                %s AS number_of_bedrooms, %s AS number_of_bathrooms,
                %s AS number_of_garages, %s AS property_size_sqm,
                %s AS property_size_raw, %s AS stand_size_sqm,
                PARSE_JSON(%s) AS features, %s AS agent_name,
                %s AS agent_phone, %s AS agent_email, %s AS agency_name,
                PARSE_JSON(%s) AS image_urls, %s AS listing_url,
                %s AS listing_date, %s AS scraped_at
        ) AS source
        ON target.listing_id = source.listing_id
        WHEN MATCHED THEN UPDATE SET
            property_price  = source.property_price,
            currency        = source.currency,
            scraped_at      = source.scraped_at,
            features        = source.features,
            image_urls      = source.image_urls
        WHEN NOT MATCHED THEN INSERT (
            listing_id, source, property_title, property_price, currency,
            property_type, listing_type, city, suburb, address_raw,
            latitude, longitude, number_of_bedrooms, number_of_bathrooms,
            number_of_garages, property_size_sqm, property_size_raw,
            stand_size_sqm, features, agent_name, agent_phone, agent_email,
            agency_name, image_urls, listing_url, listing_date, scraped_at
        ) VALUES (
            source.listing_id, source.source, source.property_title,
            source.property_price, source.currency, source.property_type,
            source.listing_type, source.city, source.suburb, source.address_raw,
            source.latitude, source.longitude, source.number_of_bedrooms,
            source.number_of_bathrooms, source.number_of_garages,
            source.property_size_sqm, source.property_size_raw,
            source.stand_size_sqm, source.features, source.agent_name,
            source.agent_phone, source.agent_email, source.agency_name,
            source.image_urls, source.listing_url, source.listing_date,
            source.scraped_at
        )
        """

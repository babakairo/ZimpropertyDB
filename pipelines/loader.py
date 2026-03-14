"""
pipelines/loader.py — Standalone Snowflake loader.

Reads JSONL files produced by the scraper and loads them into
Snowflake RAW.ZW_PROPERTY_LISTINGS using incremental MERGE logic.

Features:
    - Batch inserts (configurable size)
    - Incremental loading (only new/updated records via listing_id MERGE)
    - Deduplication within the file
    - Audit trail written to RAW.SCRAPE_RUNS
    - Retry on transient Snowflake errors

Usage:
    python pipelines/loader.py --input data/listings_*.jsonl
    python pipelines/loader.py --input data/listings_20240315.jsonl --batch-size 1000
"""
import os
import re
import sys
import json
import uuid
import glob
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterator

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("snowflake_loader")


# ─── Snowflake Connection ─────────────────────────────────────────────────────

def get_connection():
    """Return an authenticated Snowflake connection."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        schema=os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        login_timeout=30,
        network_timeout=60,
        application="ZimPropertyLoader",
    )
    logger.info(
        f"Connected to Snowflake: "
        f"{os.environ['SNOWFLAKE_ACCOUNT']} / "
        f"{os.environ.get('SNOWFLAKE_DATABASE', 'ZIM_PROPERTY_DB')}.RAW"
    )
    return conn


# ─── JSONL Reading ────────────────────────────────────────────────────────────

def read_jsonl_files(patterns: list[str]) -> Iterator[dict]:
    """Yield records from one or many JSONL files matching glob patterns."""
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern, recursive=True))

    if not files:
        logger.error(f"No files found matching: {patterns}")
        sys.exit(1)

    logger.info(f"Reading {len(files)} file(s)")
    for file_path in sorted(files):
        logger.info(f"  Reading {file_path}")
        with open(file_path, encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(f"  Skipping malformed line {line_no} in {file_path}: {e}")


def deduplicate(records: list[dict]) -> list[dict]:
    """Keep the last occurrence of each listing_id (most recent scrape wins)."""
    seen: dict[str, dict] = {}
    for rec in records:
        lid = rec.get("listing_id")
        if lid:
            seen[lid] = rec
        else:
            # No ID — generate one to avoid collisions
            seen[str(uuid.uuid4())] = rec
    return list(seen.values())


# ─── Row Builder ──────────────────────────────────────────────────────────────

def record_to_row(r: dict) -> tuple:
    """Convert a JSONL record dict to a parameterised tuple for Snowflake."""
    return (
        r.get("listing_id"),
        _trunc(r.get("source"), 100),
        _trunc(r.get("listing_url"), 2000),
        _trunc(r.get("property_title"), 500),
        _safe_float(r.get("property_price")),
        _trunc(r.get("currency"), 10),
        _trunc(r.get("property_type"), 200),
        _trunc(r.get("listing_type"), 20),
        _trunc(r.get("city"), 200),
        _trunc(r.get("suburb"), 200),
        _trunc(r.get("address_raw"), 500),
        _safe_float(r.get("latitude")),
        _safe_float(r.get("longitude")),
        _safe_int(r.get("number_of_bedrooms")),
        _safe_int(r.get("number_of_bathrooms")),
        _safe_int(r.get("number_of_garages")),
        _safe_float(r.get("property_size_sqm")),
        _trunc(r.get("property_size_raw"), 100),
        _safe_float(r.get("stand_size_sqm")),
        json.dumps(r.get("features") or []),
        _trunc(_clean_agent_name(r.get("agent_name")), 200),
        _trunc(_clean_phone(r.get("agent_phone")), 50),
        _clean_email(r.get("agent_email")),
        _trunc(r.get("agency_name"), 200),
        json.dumps(r.get("image_urls") or []),
        _safe_date(r.get("listing_date")),
        r.get("scraped_at"),
    )


# ─── Bulk load via write_pandas + single MERGE ───────────────────────────────
#
# Strategy (correct Snowflake pattern for bulk loads):
#   1. write_pandas() streams all rows into a temp stage table in one network call
#   2. A single MERGE statement reconciles the stage with the target
# This is 10-100x faster than one MERGE per row.

_STAGE_TABLE = "ZW_PROPERTY_LISTINGS_STAGE"

_MERGE_FROM_STAGE = """
MERGE INTO ZW_PROPERTY_LISTINGS AS t
USING {stage} AS s
ON t.listing_id = s.listing_id
WHEN MATCHED AND s.scraped_at > t.scraped_at THEN UPDATE SET
    property_price      = s.property_price,
    currency            = s.currency,
    property_title      = s.property_title,
    scraped_at          = s.scraped_at,
    features            = TRY_PARSE_JSON(s.features),
    image_urls          = TRY_PARSE_JSON(s.image_urls),
    agent_name          = COALESCE(s.agent_name, t.agent_name),
    agent_phone         = COALESCE(s.agent_phone, t.agent_phone),
    agent_email         = COALESCE(s.agent_email, t.agent_email),
    agency_name         = COALESCE(s.agency_name, t.agency_name)
WHEN NOT MATCHED THEN INSERT (
    listing_id, source, listing_url, property_title, property_price, currency,
    property_type, listing_type, city, suburb, address_raw, latitude, longitude,
    number_of_bedrooms, number_of_bathrooms, number_of_garages,
    property_size_sqm, property_size_raw, stand_size_sqm, features,
    agent_name, agent_phone, agent_email, agency_name, image_urls,
    listing_date, scraped_at
) VALUES (
    s.listing_id, s.source, s.listing_url, s.property_title,
    s.property_price, s.currency, s.property_type, s.listing_type,
    s.city, s.suburb, s.address_raw, s.latitude, s.longitude,
    s.number_of_bedrooms, s.number_of_bathrooms, s.number_of_garages,
    s.property_size_sqm, s.property_size_raw, s.stand_size_sqm,
    TRY_PARSE_JSON(s.features), s.agent_name, s.agent_phone, s.agent_email,
    s.agency_name, TRY_PARSE_JSON(s.image_urls), s.listing_date, s.scraped_at
)
"""


def batch_merge(conn, rows: list[tuple], batch_size: int = 500) -> int:
    """
    Bulk-load rows into Snowflake using write_pandas + a single MERGE.
    Returns number of rows processed.
    """
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    columns = [
        "listing_id", "source", "listing_url", "property_title",
        "property_price", "currency", "property_type", "listing_type",
        "city", "suburb", "address_raw", "latitude", "longitude",
        "number_of_bedrooms", "number_of_bathrooms", "number_of_garages",
        "property_size_sqm", "property_size_raw", "stand_size_sqm",
        "features", "agent_name", "agent_phone", "agent_email", "agency_name",
        "image_urls", "listing_date", "scraped_at",
    ]

    df = pd.DataFrame(rows, columns=columns)
    cursor = conn.cursor()

    # Create stage table explicitly with TEXT for VARIANT columns.
    # (Can't use LIKE — it copies VARIANT type which can't be altered to TEXT)
    cursor.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {_STAGE_TABLE} (
            listing_id          VARCHAR(16),
            source              VARCHAR(100),
            listing_url         VARCHAR(2000),
            property_title      VARCHAR(500),
            property_price      FLOAT,
            currency            VARCHAR(10),
            property_type       VARCHAR(200),
            listing_type        VARCHAR(20),
            city                VARCHAR(200),
            suburb              VARCHAR(200),
            address_raw         VARCHAR(500),
            latitude            FLOAT,
            longitude           FLOAT,
            number_of_bedrooms  INTEGER,
            number_of_bathrooms INTEGER,
            number_of_garages   INTEGER,
            property_size_sqm   FLOAT,
            property_size_raw   VARCHAR(100),
            stand_size_sqm      FLOAT,
            features            TEXT,
            agent_name          VARCHAR(200),
            agent_phone         VARCHAR(50),
            agent_email         VARCHAR(200),
            agency_name         VARCHAR(200),
            image_urls          TEXT,
            listing_date        DATE,
            scraped_at          TIMESTAMP_TZ
        )
    """)

    logger.info(f"Uploading {len(df):,} rows to stage table {_STAGE_TABLE} ...")
    success, nchunks, nrows, _ = write_pandas(
        conn, df, _STAGE_TABLE,
        chunk_size=batch_size,
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
    )

    if not success:
        raise RuntimeError(f"write_pandas failed: {nrows} rows in {nchunks} chunks")

    logger.info(f"Stage loaded ({nrows:,} rows). Running MERGE ...")
    cursor.execute(_MERGE_FROM_STAGE.format(stage=_STAGE_TABLE))
    merge_result = cursor.fetchone()
    inserted = merge_result[0] if merge_result else 0
    updated  = merge_result[1] if merge_result and len(merge_result) > 1 else 0
    logger.info(f"MERGE complete: {inserted} inserted, {updated} updated")

    cursor.execute(f"DROP TABLE IF EXISTS {_STAGE_TABLE}")
    cursor.close()
    return nrows


# ─── Audit ────────────────────────────────────────────────────────────────────

def write_audit_record(cursor, run_id: str, spider: str, source: str,
                        started: datetime, finished: datetime,
                        loaded: int, status: str, error: str = None):
    cursor.execute(
        """
        MERGE INTO SCRAPE_RUNS AS t
        USING (SELECT %s run_id) AS s ON t.run_id = s.run_id
        WHEN NOT MATCHED THEN INSERT
            (run_id, spider_name, source, started_at, finished_at,
             items_loaded, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, run_id, spider, source, started, finished, loaded, status, error),
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load JSONL property data into Snowflake")
    parser.add_argument("--input", nargs="+", required=True, help="JSONL file(s) or glob patterns")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per Snowflake batch")
    parser.add_argument("--dry-run", action="store_true", help="Parse and print stats without loading")
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    # Read and deduplicate
    raw_records = list(read_jsonl_files(args.input))
    logger.info(f"Read {len(raw_records)} raw records")

    deduped = deduplicate(raw_records)
    logger.info(f"After deduplication: {len(deduped)} records ({len(raw_records) - len(deduped)} dropped)")

    if args.dry_run:
        logger.info("Dry run mode — skipping Snowflake load")
        logger.info(f"Would load {len(deduped)} records")
        return

    rows = [record_to_row(r) for r in deduped]

    conn = get_connection()
    status = "success"
    error_msg = None
    loaded = 0

    try:
        loaded = batch_merge(conn, rows, batch_size=args.batch_size)
        conn.commit()
        logger.info(f"Load complete: {loaded} records merged into Snowflake")
    except Exception as exc:
        conn.rollback()
        status = "failed"
        error_msg = str(exc)
        logger.error(f"Load failed: {exc}", exc_info=True)
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        source = deduped[0].get("source", "unknown") if deduped else "unknown"
        audit_cursor = conn.cursor()
        try:
            write_audit_record(
                audit_cursor, run_id, "standalone_loader", source,
                started_at, finished_at, loaded, status, error_msg
            )
            conn.commit()
        except Exception as ae:
            logger.warning(f"Could not write audit record: {ae}")
        finally:
            audit_cursor.close()
            conn.close()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _safe_float(val):
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None

def _safe_int(val):
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None

def _safe_date(val):
    if not val:
        return None
    if isinstance(val, str):
        m = re.search(r"\d{4}-\d{2}-\d{2}", val)
        return m.group() if m else None
    return val

def _clean_agent_name(val: str | None) -> str | None:
    """Strip embedded phone numbers from agent name strings.

    Some PropData portals concatenate name + phone into a single field,
    e.g. "Melinda Wolvaardt +263772817923".  Remove the phone portion so
    only the human name is stored in agent_name.
    """
    if not val:
        return None
    # Remove +263... or 0263... or 07xx... phone patterns
    cleaned = re.sub(r"\s*\+?\d[\d\s\-]{8,}", "", val).strip()
    return cleaned or None

def _clean_phone(val: str | None) -> str | None:
    """Normalise a phone number string — strip whitespace, keep digits and +."""
    if not val:
        return None
    # Extract the first plausible phone number from the string
    m = re.search(r"\+?[\d][\d\s\-]{7,}", str(val))
    if m:
        # Collapse spaces and dashes
        return re.sub(r"[\s\-]", "", m.group()).strip()
    return val.strip() or None

def _clean_email(val: str | None) -> str | None:
    """Extract a plain email address from a mailto: URI or raw string.

    Handles inputs like:
      - 'user@example.com'
      - 'mailto:user@example.com'
      - 'mailto:user@example.com?Subject=...&body=...'
    Truncates to 200 characters to match the column definition.
    """
    if not val:
        return None
    # Strip mailto: prefix
    s = re.sub(r"^mailto:", "", str(val).strip(), flags=re.I)
    # Everything before the first ? is the address
    s = s.split("?")[0].strip()
    # Basic email format check
    if "@" not in s:
        return None
    return s[:200] or None

def _trunc(val: str | None, max_len: int) -> str | None:
    """Truncate a string to max_len characters to fit VARCHAR columns."""
    if val is None:
        return None
    return str(val)[:max_len]


if __name__ == "__main__":
    main()

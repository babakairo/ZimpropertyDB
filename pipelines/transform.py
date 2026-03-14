"""
pipelines/transform.py — Clean, deduplicate & segment raw listings,
then populate all ANALYTICS aggregate tables.

Steps:
  1. 07_clean_and_segment.sql  → STAGING.CLEANED_PROPERTY_LISTINGS + segment views
  2. 08_populate_analytics.sql → 5 ANALYTICS tables (price by suburb/city,
                                  by bedroom, monthly trends, suburb growth)

Usage:
    python pipelines/transform.py
    python pipelines/transform.py --clean-only  # skip analytics tables
    python pipelines/transform.py --dry-run     # print counts only, no write
"""
import os
import sys
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("transform")

SQL_FILE   = Path(__file__).parent.parent / "snowflake" / "07_clean_and_segment.sql"
SQL_ANALYTICS = Path(__file__).parent.parent / "snowflake" / "08_populate_analytics.sql"

SEGMENT_QUERIES = {
    "Total cleaned":      "SELECT COUNT(*) FROM STAGING.CLEANED_PROPERTY_LISTINGS",
    "Land for sale":      "SELECT COUNT(*) FROM ANALYTICS.LAND_LISTINGS",
    "Rentals":            "SELECT COUNT(*) FROM ANALYTICS.RENTAL_LISTINGS",
    "Houses for sale":    "SELECT COUNT(*) FROM ANALYTICS.HOUSE_SALE_LISTINGS",
}

DRY_RUN_COUNTS = """
SELECT
    COUNT(*)                                                    AS total_raw,
    COUNT(DISTINCT listing_id)                                  AS unique_listings,
    SUM(IFF(listing_type = 'sale' AND property_type IN ('land','farm'), 1, 0))
                                                                AS land_for_sale,
    SUM(IFF(listing_type = 'rent', 1, 0))                      AS rentals,
    SUM(IFF(listing_type = 'sale'
        AND property_type IN ('house','flat','townhouse','room','unknown')
        AND property_type NOT IN ('land','farm','commercial'), 1, 0))
                                                                AS houses_for_sale,
    SUM(IFF(listing_url IS NULL OR listing_url = '', 1, 0))    AS missing_url,
    SUM(IFF(property_price IS NULL OR property_price <= 0, 1, 0))
                                                                AS invalid_price,
    SUM(IFF(LOWER(listing_type) NOT IN ('sale','rent'), 1, 0)) AS unknown_listing_type
FROM RAW.ZW_PROPERTY_LISTINGS
"""


def get_connection():
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        login_timeout=30,
    )
    return conn


def split_statements(sql: str) -> list[str]:
    """Split SQL file into individual statements (skip blank lines & comments)."""
    statements, current = [], []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
    return statements


def main():
    parser = argparse.ArgumentParser(description="Clean and segment property listings")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print RAW counts only; do not write to STAGING")
    parser.add_argument("--clean-only", action="store_true",
                        help="Run clean/segment only; skip analytics tables")
    args = parser.parse_args()

    conn = get_connection()
    cursor = conn.cursor()

    try:
        if args.dry_run:
            logger.info("Dry run — querying RAW layer only")
            cursor.execute(f"USE DATABASE ZIM_PROPERTY_DB")
            cursor.execute(DRY_RUN_COUNTS)
            row = cursor.fetchone()
            cols = [d[0] for d in cursor.description]
            for col, val in zip(cols, row):
                logger.info(f"  {col:<28} {val:>8,}")
            return

        # Run the transformation SQL
        sql = SQL_FILE.read_text(encoding="utf-8")
        statements = split_statements(sql)
        logger.info(f"Running {SQL_FILE.name}: {len(statements)} statements")

        for i, stmt in enumerate(statements, 1):
            # Label long statements by first meaningful line
            label = next((l.strip() for l in stmt.splitlines() if l.strip()), "")[:60]
            try:
                cursor.execute(stmt)
                rowcount = cursor.rowcount
                suffix = f" ({rowcount:,} rows)" if rowcount and rowcount > 0 else ""
                logger.info(f"  [{i}/{len(statements)}] OK{suffix}  — {label}")
            except Exception as e:
                logger.error(f"  [{i}/{len(statements)}] FAILED — {label}\n    {e}")
                raise

        conn.commit()

        # Print segment counts
        logger.info("\nSegment row counts:")
        for label, query in SEGMENT_QUERIES.items():
            cursor.execute(query)
            count = cursor.fetchone()[0]
            logger.info(f"  {label:<22} {count:>8,}")

        if args.clean_only:
            return

        # ── Step 2: Populate analytics tables ────────────────────────────
        analytics_sql = SQL_ANALYTICS.read_text(encoding="utf-8")
        analytics_stmts = split_statements(analytics_sql)
        logger.info(f"\nRunning {SQL_ANALYTICS.name}: {len(analytics_stmts)} statements")

        ANALYTICS_TABLES = {
            "PROPERTY_PRICE_BY_SUBURB": "SELECT COUNT(*) FROM ANALYTICS.PROPERTY_PRICE_BY_SUBURB",
            "PROPERTY_PRICE_BY_CITY":   "SELECT COUNT(*) FROM ANALYTICS.PROPERTY_PRICE_BY_CITY",
            "AVERAGE_PRICE_BY_BEDROOM": "SELECT COUNT(*) FROM ANALYTICS.AVERAGE_PRICE_BY_BEDROOM",
            "MONTHLY_PRICE_TRENDS":     "SELECT COUNT(*) FROM ANALYTICS.MONTHLY_PRICE_TRENDS",
            "SUBURB_PRICE_GROWTH":      "SELECT COUNT(*) FROM ANALYTICS.SUBURB_PRICE_GROWTH",
        }

        for i, stmt in enumerate(analytics_stmts, 1):
            label = next((l.strip() for l in stmt.splitlines() if l.strip()), "")[:60]
            try:
                cursor.execute(stmt)
                rowcount = cursor.rowcount
                suffix = f" ({rowcount:,} rows)" if rowcount and rowcount > 0 else ""
                logger.info(f"  [{i}/{len(analytics_stmts)}] OK{suffix}  — {label}")
            except Exception as e:
                logger.error(f"  [{i}/{len(analytics_stmts)}] FAILED — {label}\n    {e}")
                raise

        conn.commit()

        logger.info("\nAnalytics table row counts:")
        for label, query in ANALYTICS_TABLES.items():
            cursor.execute(query)
            count = cursor.fetchone()[0]
            logger.info(f"  {label:<30} {count:>8,}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Transform failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

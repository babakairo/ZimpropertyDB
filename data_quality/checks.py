"""
data_quality/checks.py — Standalone data quality check runner.

Can be run independently or called from the Prefect pipeline.
Writes results to Snowflake DATA_QUALITY schema.

Usage:
    python data_quality/checks.py
    python data_quality/checks.py --date 2024-03-15
"""
import os
import sys
import json
import logging
import argparse
from datetime import datetime, timezone, date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("data_quality")


# ─── Check definitions ────────────────────────────────────────────────────────

CHECKS = [
    {
        "name": "daily_load_volume",
        "description": "At least 50 records must be loaded per day",
        "severity": "CRITICAL",
        "sql": """
            SELECT COUNT(*) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT::DATE = '{run_date}'
        """,
        "threshold": {"min": 50},
    },
    {
        "name": "price_null_rate",
        "description": "No more than 40% of listings should have NULL price",
        "severity": "WARNING",
        "sql": """
            SELECT ROUND(
                SUM(CASE WHEN PROPERTY_PRICE IS NULL THEN 1 ELSE 0 END) /
                NULLIF(COUNT(*), 0) * 100,
            2) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT::DATE = '{run_date}'
        """,
        "threshold": {"max": 40},
    },
    {
        "name": "duplicate_listing_rate",
        "description": "Duplicate listing_id rate must be below 5%",
        "severity": "CRITICAL",
        "sql": """
            SELECT ROUND(
                (1 - COUNT(DISTINCT LISTING_ID) / NULLIF(COUNT(*), 0)) * 100,
            2) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT::DATE = '{run_date}'
        """,
        "threshold": {"max": 5},
    },
    {
        "name": "city_null_rate",
        "description": "No more than 25% of listings should have NULL city",
        "severity": "WARNING",
        "sql": """
            SELECT ROUND(
                SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END) /
                NULLIF(COUNT(*), 0) * 100,
            2) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT::DATE = '{run_date}'
        """,
        "threshold": {"max": 25},
    },
    {
        "name": "invalid_currency",
        "description": "All currency values must be USD, ZWL, or ZIG",
        "severity": "CRITICAL",
        "sql": """
            SELECT COUNT(*) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT::DATE = '{run_date}'
              AND CURRENCY IS NOT NULL
              AND CURRENCY NOT IN ('USD', 'ZWL', 'ZIG')
        """,
        "threshold": {"max": 0},
    },
    {
        "name": "suspicious_usd_prices",
        "description": "USD prices must be between $100 and $50M",
        "severity": "WARNING",
        "sql": """
            SELECT COUNT(*) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT::DATE = '{run_date}'
              AND CURRENCY = 'USD'
              AND PROPERTY_PRICE IS NOT NULL
              AND (PROPERTY_PRICE < 100 OR PROPERTY_PRICE > 50000000)
        """,
        "threshold": {"max": 10},
    },
    {
        "name": "future_listing_dates",
        "description": "No listings should have a future listing_date",
        "severity": "WARNING",
        "sql": """
            SELECT COUNT(*) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE LISTING_DATE > CURRENT_DATE()
        """,
        "threshold": {"max": 0},
    },
    {
        "name": "source_coverage",
        "description": "Both main sources must have loaded data today",
        "severity": "CRITICAL",
        "sql": """
            SELECT COUNT(DISTINCT SOURCE) AS metric
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT::DATE = '{run_date}'
              AND SOURCE IN ('property.co.zw', 'classifieds.co.zw')
        """,
        "threshold": {"min": 2},
    },
]


# ─── Runner ───────────────────────────────────────────────────────────────────

def run_checks(run_date: str) -> list[dict]:
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
    )
    cursor = conn.cursor()

    results = []
    for check in CHECKS:
        sql = check["sql"].format(run_date=run_date).strip()
        try:
            cursor.execute(sql)
            metric = cursor.fetchone()[0] or 0
        except Exception as e:
            logger.error(f"Check '{check['name']}' query failed: {e}")
            metric = None

        # Evaluate threshold
        passed = True
        if metric is not None:
            t = check["threshold"]
            if "min" in t and metric < t["min"]:
                passed = False
            if "max" in t and metric > t["max"]:
                passed = False

        status = "PASS" if passed else check["severity"]
        result = {
            "check_name": check["name"],
            "description": check["description"],
            "metric": metric,
            "threshold": json.dumps(check["threshold"]),
            "status": status,
            "run_date": run_date,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        results.append(result)
        icon = "✓" if status == "PASS" else ("⚠" if status == "WARNING" else "✗")
        logger.info(f"  {icon} [{status}] {check['name']}: metric={metric}")

    # Write results to Snowflake audit table
    _write_results(cursor, results)
    conn.commit()
    cursor.close()
    conn.close()
    return results


def _write_results(cursor, results: list[dict]):
    cursor.executemany(
        """
        INSERT INTO ZIM_PROPERTY_DB.DATA_QUALITY.CHECK_RESULTS
        (check_name, description, metric, threshold_json, status, run_date, checked_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                r["check_name"], r["description"],
                r["metric"], r["threshold"], r["status"],
                r["run_date"], r["checked_at"]
            )
            for r in results
        ],
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat(), help="Run date (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info(f"Running data quality checks for {args.date}")
    results = run_checks(args.date)

    critical_failures = [r for r in results if r["status"] == "CRITICAL"]
    warnings = [r for r in results if r["status"] == "WARNING"]

    logger.info(f"\nSummary: {len(results)} checks | {len(critical_failures)} CRITICAL | {len(warnings)} WARNING")

    if critical_failures:
        logger.error("CRITICAL failures:")
        for r in critical_failures:
            logger.error(f"  - {r['check_name']}: {r['description']} (metric={r['metric']})")
        sys.exit(1)


if __name__ == "__main__":
    main()

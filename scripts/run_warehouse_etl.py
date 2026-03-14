"""
scripts/run_warehouse_etl.py
Runs WAREHOUSE ETL sections F1, F2, F3 from 09_star_schema.sql.
Safe to re-run — INSERT WHERE NOT EXISTS pattern throughout.
Skips GRANT statements gracefully (roles may not exist in dev).
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

import snowflake.connector

def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        login_timeout=30,
    )

def run(cursor, sql, label):
    try:
        cursor.execute(sql)
        result = cursor.fetchone()
        print(f"  OK  {label}")
        return result
    except Exception as e:
        if "does not exist" in str(e).lower() or "insufficient privileges" in str(e).lower():
            print(f"  SKIP {label} ({e})")
        else:
            print(f"  ERROR {label}: {e}")
            raise

# ── F1: Populate DIM_LOCATION ────────────────────────────────────────────────
F1_INSERT = """
INSERT INTO ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION (suburb_clean, city_clean, province, first_seen_date)
SELECT DISTINCT
    cl.suburb_clean,
    cl.city_clean,
    CASE cl.city_clean
        WHEN 'Harare'      THEN 'Harare Metropolitan'
        WHEN 'Chitungwiza' THEN 'Harare Metropolitan'
        WHEN 'Ruwa'        THEN 'Harare Metropolitan'
        WHEN 'Norton'      THEN 'Mashonaland West'
        WHEN 'Bulawayo'    THEN 'Bulawayo Metropolitan'
        WHEN 'Mutare'      THEN 'Manicaland'
        WHEN 'Gweru'       THEN 'Midlands'
        WHEN 'Kwekwe'      THEN 'Midlands'
        WHEN 'Masvingo'    THEN 'Masvingo'
        WHEN 'Chinhoyi'    THEN 'Mashonaland West'
        WHEN 'Marondera'   THEN 'Mashonaland East'
        ELSE 'Other'
    END AS province,
    MIN(cl.listing_date) AS first_seen_date
FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
WHERE cl.city_clean IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
      WHERE COALESCE(dl.suburb_clean, '') = COALESCE(cl.suburb_clean, '')
        AND dl.city_clean = cl.city_clean
  )
GROUP BY 1, 2, 3
"""

# ── F2: Refresh DIM_SOURCE ────────────────────────────────────────────────────
F2_UPDATE = """
UPDATE ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
SET
    last_scraped_date  = latest.last_scraped,
    first_scraped_date = COALESCE(ds.first_scraped_date, latest.first_scraped)
FROM (
    SELECT
        source,
        MIN(scraped_at::DATE) AS first_scraped,
        MAX(scraped_at::DATE) AS last_scraped
    FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS
    GROUP BY source
) latest
WHERE ds.source = latest.source
"""

# Insert any sources not yet in DIM_SOURCE
F2_INSERT_MISSING = """
INSERT INTO ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE (source, source_label, is_active)
SELECT DISTINCT
    cl.source,
    cl.source,
    TRUE
FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
WHERE NOT EXISTS (
    SELECT 1 FROM ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
    WHERE ds.source = cl.source
)
"""

# ── F3: Populate FACT_LISTINGS ────────────────────────────────────────────────
F3_INSERT = """
INSERT INTO ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS (
    listing_id, date_key, listing_date_key, location_key,
    property_type_key, source_key, property_key, listing_type,
    currency_original, property_price_usd, property_price_zwl,
    exchange_rate_used, price_per_sqm_usd, property_size_sqm, stand_size_sqm,
    number_of_bedrooms, number_of_bathrooms, number_of_garages,
    feature_count, has_pool, has_borehole, has_solar, has_garage,
    image_count, data_quality_score, is_price_valid, is_location_valid, scraped_at
)
SELECT
    cl.listing_id,
    TO_NUMBER(TO_CHAR(cl.scraped_at::DATE, 'YYYYMMDD')) AS date_key,
    CASE
        WHEN cl.listing_date IS NOT NULL
        THEN TO_NUMBER(TO_CHAR(cl.listing_date, 'YYYYMMDD'))
    END AS listing_date_key,
    dl.location_key,
    dpt.property_type_key,
    ds.source_key,
    NULL AS property_key,
    cl.listing_type,
    cl.currency_original,
    cl.property_price_usd,
    cl.property_price_zwl,
    cl.exchange_rate_used,
    cl.price_per_sqm_usd,
    cl.property_size_sqm,
    cl.stand_size_sqm,
    cl.number_of_bedrooms,
    cl.number_of_bathrooms,
    cl.number_of_garages,
    cl.feature_count,
    cl.has_pool,
    cl.has_borehole,
    cl.has_solar,
    cl.has_garage,
    cl.image_count,
    cl.data_quality_score,
    cl.is_price_valid,
    cl.is_location_valid,
    cl.scraped_at
FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
LEFT JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
    ON COALESCE(dl.suburb_clean, '') = COALESCE(cl.suburb_clean, '')
    AND dl.city_clean = cl.city_clean
LEFT JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_PROPERTY_TYPE dpt
    ON dpt.property_type = cl.property_type
LEFT JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
    ON ds.source = cl.source
WHERE NOT EXISTS (
    SELECT 1 FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    WHERE fl.listing_id = cl.listing_id
)
"""

# ── Verification queries ──────────────────────────────────────────────────────
VERIFY_FACT_COUNT = "SELECT COUNT(*) FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS"
VERIFY_MASTER_COUNT = "SELECT COUNT(*) FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_MASTER"
VERIFY_SOURCES = """
SELECT ds.source, COUNT(fl.fact_id) AS listing_count
FROM ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
JOIN ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl ON fl.source_key = ds.source_key
WHERE ds.source IN ('property.co.zw', 'propsearch.co.zw')
GROUP BY ds.source
"""
VERIFY_TOP20_SUBURBS = """
SELECT
    dl.suburb_clean,
    dl.city_clean,
    COUNT(fl.fact_id) AS listing_count
FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl ON dl.location_key = fl.location_key
WHERE dl.suburb_clean IS NOT NULL
GROUP BY dl.suburb_clean, dl.city_clean
ORDER BY listing_count DESC
LIMIT 20
"""

if __name__ == "__main__":
    print("Connecting to Snowflake...")
    conn = get_conn()
    cur = conn.cursor()

    print("\n--- F1: Populate DIM_LOCATION ---")
    run(cur, F1_INSERT, "DIM_LOCATION INSERT new locations")

    print("\n--- F2: Refresh DIM_SOURCE ---")
    run(cur, F2_INSERT_MISSING, "DIM_SOURCE INSERT missing sources")
    run(cur, F2_UPDATE, "DIM_SOURCE UPDATE last_scraped_date")

    print("\n--- F3: Populate FACT_LISTINGS ---")
    run(cur, F3_INSERT, "FACT_LISTINGS INSERT new records")

    conn.commit()

    print("\n--- Verification ---")

    cur.execute(VERIFY_FACT_COUNT)
    fact_count = cur.fetchone()[0]
    print(f"\nWAREHOUSE.FACT_LISTINGS: {fact_count:,} rows")

    try:
        cur.execute(VERIFY_MASTER_COUNT)
        master_count = cur.fetchone()[0]
        print(f"MASTER.PROPERTY_MASTER: {master_count:,} rows")
    except Exception as e:
        print(f"MASTER.PROPERTY_MASTER: could not query ({e})")

    print("\nSource presence check (property.co.zw, propsearch.co.zw):")
    cur.execute(VERIFY_SOURCES)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,} listings")

    print("\nTop 20 suburbs by active listing count:")
    cur.execute(VERIFY_TOP20_SUBURBS)
    rows = cur.fetchall()
    print(f"  {'Suburb':<30} {'City':<20} {'Count':>6}")
    print(f"  {'-'*30} {'-'*20} {'-'*6}")
    for i, row in enumerate(rows, 1):
        print(f"  {(row[0] or '(no suburb)'):<30} {row[1]:<20} {row[2]:>6}")

    cur.close()
    conn.close()
    print("\nDone.")

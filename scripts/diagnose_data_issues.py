"""
scripts/diagnose_data_issues.py
Diagnostic queries for listing_type and days_on_market issues.
Uses actual column names from WAREHOUSE.FACT_LISTINGS.
"""
import os
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

conn = get_conn()
cur = conn.cursor()

# ─────────────────────────────────────────────────────────────────────────────
# LISTING_TYPE DIAGNOSTICS
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 70)
print("LISTING_TYPE DIAGNOSTICS")
print("=" * 70)

print("\n[D1] listing_type distribution with price stats:")
cur.execute("""
    SELECT
        fl.listing_type,
        COUNT(*)                            AS count,
        ROUND(AVG(fl.property_price_usd),2) AS avg_price,
        MIN(fl.property_price_usd)          AS min_price,
        MAX(fl.property_price_usd)          AS max_price
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    GROUP BY fl.listing_type
    ORDER BY count DESC
""")
rows = cur.fetchall()
print(f"  {'listing_type':<12} {'count':>8} {'avg_price':>14} {'min_price':>14} {'max_price':>14}")
print(f"  {'-'*12} {'-'*8} {'-'*14} {'-'*14} {'-'*14}")
for r in rows:
    lt = str(r[0]) if r[0] else 'NULL'
    avg = f"${r[2]:,.2f}" if r[2] else "NULL"
    mn  = f"${r[3]:,.2f}" if r[3] else "NULL"
    mx  = f"${r[4]:,.2f}" if r[4] else "NULL"
    print(f"  {lt:<12} {r[1]:>8,} {avg:>14} {mn:>14} {mx:>14}")

print("\n[D2] listing_type by source:")
cur.execute("""
    SELECT
        ds.source,
        fl.listing_type,
        COUNT(*) AS cnt
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    GROUP BY ds.source, fl.listing_type
    ORDER BY ds.source, fl.listing_type
""")
rows = cur.fetchall()
print(f"  {'source':<35} {'listing_type':<14} {'count':>8}")
print(f"  {'-'*35} {'-'*14} {'-'*8}")
for r in rows:
    src = str(r[0])[:35]
    lt  = str(r[1]) if r[1] else 'NULL'
    print(f"  {src:<35} {lt:<14} {r[2]:>8,}")

print("\n[D3] NULL / empty listing_type count:")
cur.execute("""
    SELECT COUNT(*)
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS
    WHERE listing_type IS NULL OR listing_type = ''
""")
print(f"  NULL or empty listing_type: {cur.fetchone()[0]:,}")

print("\n[D4] listing_type population rate per source (% non-null):")
cur.execute("""
    SELECT
        ds.source,
        COUNT(*)                                        AS total,
        COUNT(fl.listing_type)                          AS non_null,
        ROUND(COUNT(fl.listing_type) / COUNT(*) * 100, 1) AS pct_populated
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    GROUP BY ds.source
    ORDER BY pct_populated ASC
""")
rows = cur.fetchall()
print(f"  {'source':<35} {'total':>8} {'non_null':>10} {'% filled':>10}")
print(f"  {'-'*35} {'-'*8} {'-'*10} {'-'*10}")
for r in rows:
    src = str(r[0])[:35]
    print(f"  {src:<35} {r[1]:>8,} {r[2]:>10,} {r[3]:>9.1f}%")


# ─────────────────────────────────────────────────────────────────────────────
# DAYS_ON_MARKET DIAGNOSTICS
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("DAYS_ON_MARKET DIAGNOSTICS")
print("=" * 70)

print("\n[D5] DOM distribution for Borrowdale:")
cur.execute("""
    SELECT
        MIN(TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'))      AS oldest_listing,
        MAX(TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'))      AS newest_listing,
        ROUND(AVG(
            DATEDIFF('day',
                TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
                CURRENT_DATE())
        ), 1)                                                        AS avg_dom,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY DATEDIFF('day',
                TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
                CURRENT_DATE())
        )                                                            AS median_dom,
        COUNT(CASE WHEN DATEDIFF('day',
            TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
            CURRENT_DATE()) > 365 THEN 1 END)                       AS over_one_year,
        COUNT(CASE WHEN DATEDIFF('day',
            TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
            CURRENT_DATE()) > 90 THEN 1 END)                        AS over_90_days,
        COUNT(CASE WHEN DATEDIFF('day',
            TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
            CURRENT_DATE()) BETWEEN 0 AND 90 THEN 1 END)            AS normal_range,
        COUNT(CASE WHEN DATEDIFF('day',
            TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
            CURRENT_DATE()) < 0 THEN 1 END)                         AS negative_dom,
        COUNT(*)                                                     AS total_with_date,
        COUNT(*) - COUNT(fl.listing_date_key)                        AS null_listing_date
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
        ON dl.location_key = fl.location_key
    WHERE LOWER(dl.suburb_clean) = 'borrowdale'
      AND fl.listing_date_key IS NOT NULL
""")
r = cur.fetchone()
if r:
    print(f"  oldest listing date : {r[0]}")
    print(f"  newest listing date : {r[1]}")
    print(f"  avg DOM             : {r[2]} days")
    print(f"  median DOM          : {r[3]} days")
    print(f"  > 365 days          : {r[4]:,}")
    print(f"  > 90 days           : {r[5]:,}")
    print(f"  0-90 days (normal)  : {r[6]:,}")
    print(f"  negative DOM        : {r[7]:,}")
    print(f"  total with date     : {r[8]:,}")
    print(f"  null listing_date   : {r[9]:,}")

print("\n[D6] DOM by source (all suburbs):")
cur.execute("""
    SELECT
        ds.source,
        MIN(TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'))      AS oldest,
        MAX(TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'))      AS newest,
        ROUND(AVG(
            DATEDIFF('day',
                TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
                CURRENT_DATE())
        ), 1)                                                        AS avg_dom,
        COUNT(*)                                                     AS total,
        COUNT(fl.listing_date_key)                                   AS with_date
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    GROUP BY ds.source
    ORDER BY avg_dom DESC NULLS LAST
""")
rows = cur.fetchall()
print(f"  {'source':<35} {'oldest':<14} {'newest':<14} {'avg_dom':>9} {'total':>7} {'w/date':>7}")
print(f"  {'-'*35} {'-'*14} {'-'*14} {'-'*9} {'-'*7} {'-'*7}")
for r in rows:
    src = str(r[0])[:35]
    oldest = str(r[1]) if r[1] else "NULL"
    newest = str(r[2]) if r[2] else "NULL"
    avg    = f"{r[3]:.0f}" if r[3] else "N/A"
    print(f"  {src:<35} {oldest:<14} {newest:<14} {avg:>9} {r[4]:>7,} {r[5]:>7,}")

print("\n[D7] Extreme DOM records (>500 days, sample 20):")
cur.execute("""
    SELECT
        TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD')           AS listing_date,
        fl.scraped_at::DATE                                          AS scraped_date,
        DATEDIFF('day',
            TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
            CURRENT_DATE())                                          AS dom,
        ds.source,
        cl.listing_url
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    LEFT JOIN ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
        ON cl.listing_id = fl.listing_id
    WHERE fl.listing_date_key IS NOT NULL
      AND DATEDIFF('day',
            TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
            CURRENT_DATE()) > 500
    ORDER BY dom DESC
    LIMIT 20
""")
rows = cur.fetchall()
print(f"  {'listing_date':<14} {'scraped':<12} {'dom':>6} {'source':<30} url")
print(f"  {'-'*14} {'-'*12} {'-'*6} {'-'*30} {'-'*40}")
for r in rows:
    url = (str(r[4]) or "")[:50] if r[4] else ""
    print(f"  {str(r[0]):<14} {str(r[1]):<12} {r[2]:>6} {str(r[3]):<30} {url}")

print("\n[D8] listing_date vs scraped_at — how many are IDENTICAL (date part):")
cur.execute("""
    SELECT
        COUNT(*)    AS identical_count,
        ds.source
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    WHERE fl.listing_date_key IS NOT NULL
      AND fl.listing_date_key = TO_NUMBER(TO_CHAR(fl.scraped_at::DATE, 'YYYYMMDD'))
    GROUP BY ds.source
    ORDER BY identical_count DESC
""")
rows = cur.fetchall()
print(f"  Records where listing_date == scraped_date (by source):")
total_identical = 0
for r in rows:
    print(f"    {str(r[1]):<35} {r[0]:>6,} identical")
    total_identical += r[0]
print(f"  Total identical: {total_identical:,}")

print("\n[D9] listing_date distribution — bucket by year:")
cur.execute("""
    SELECT
        YEAR(TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'))  AS yr,
        COUNT(*)                                                   AS cnt
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    WHERE fl.listing_date_key IS NOT NULL
    GROUP BY yr
    ORDER BY yr
""")
rows = cur.fetchall()
print(f"  {'year':<8} {'count':>8}")
for r in rows:
    print(f"  {r[0]:<8} {r[1]:>8,}")

cur.close()
conn.close()
print("\nDiagnostics complete.")

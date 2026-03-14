"""
analytics/suburb_queries.py
Data access functions for property intelligence report generation.

All queries target:
  - ZIM_PROPERTY_DB.WAREHOUSE  (star schema — FACT_LISTINGS, DIM_*)
  - ZIM_PROPERTY_DB.STAGING    (CLEANED_PROPERTY_LISTINGS — agent/URL data)
  - ZIM_PROPERTY_DB.ANALYTICS  (SUBURB_MARKET_STATS — weekly aggregations)

Fix notes (2026-03-14):
  - All queries now accept listing_type parameter (default 'sale') to separate
    sale vs rental listings.
  - web.archive.org / wayback records are excluded from all queries via
    DIM_SOURCE filter (they remain in FACT_LISTINGS for reference but must
    not enter analytics or reports).
  - days_on_market is now NULL-safe: only computed for records where
    listing_date_key is present. Cap applied at display layer (999 days).

Connection is ephemeral per call (open → query → close).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

# Wayback / archive sources excluded from all analytics queries.
# Mirrors the STAGING-level filter in 07_clean_and_segment.sql.
_WAYBACK_EXCLUSION = """
  AND ds.source NOT LIKE '%%archive.org%%'
  AND ds.source NOT LIKE '%%wayback%%'
"""

# Price outlier filters — remove garbage records without deleting from DB.
# Applied to all sale and rental analytics queries.
_PRICE_RANGE = {
    "sale": "AND fl.property_price_usd >= 5000 AND fl.property_price_usd <= 5000000",
    "rent": "AND fl.property_price_usd >= 100  AND fl.property_price_usd <= 15000",
}

def _price_filter(listing_type: str) -> str:
    """Returns SQL price range filter for the given listing type."""
    return _PRICE_RANGE.get(listing_type, _PRICE_RANGE["sale"])


# ── Connection helpers ────────────────────────────────────────────────────────

def _get_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        login_timeout=30,
    )


def _fetchall(sql: str, params: tuple = ()) -> list[tuple]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def _fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchone()
    finally:
        conn.close()


# ── 1. get_suburb_snapshot ────────────────────────────────────────────────────

_SNAPSHOT_SQL = """
WITH base AS (
    SELECT
        fl.listing_id,
        fl.property_price_usd,
        fl.number_of_bedrooms,
        fl.data_quality_score,
        fl.scraped_at,
        fl.listing_date_key,
        dpt.property_type
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
        ON dl.location_key = fl.location_key
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    LEFT JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_PROPERTY_TYPE dpt
        ON dpt.property_type_key = fl.property_type_key
    WHERE LOWER(dl.suburb_clean) = LOWER(%s)
      AND fl.listing_type = %s
      AND fl.is_price_valid = TRUE
      AND fl.property_price_usd > 0
      {wayback}
      {price_filter}
      {type_filter}
      {bed_filter}
      {budget_filter}
)
SELECT
    COUNT(*)                                                AS active_listing_count,
    MEDIAN(property_price_usd)                              AS median_asking_price,
    AVG(property_price_usd)                                 AS avg_asking_price,
    MIN(property_price_usd)                                 AS min_asking_price,
    MAX(property_price_usd)                                 AS max_asking_price,
    -- avg DOM for ALL records with a date (includes zombies — shown for transparency)
    AVG(
        CASE
            WHEN listing_date_key IS NOT NULL
            THEN DATEDIFF('day',
                    TO_DATE(listing_date_key::VARCHAR, 'YYYYMMDD'),
                    CURRENT_DATE())
        END
    )                                                       AS avg_days_on_market,
    -- avg DOM excluding listings older than 365 days (typical active-market signal)
    AVG(
        CASE
            WHEN listing_date_key IS NOT NULL
             AND DATEDIFF('day',
                    TO_DATE(listing_date_key::VARCHAR, 'YYYYMMDD'),
                    CURRENT_DATE()) <= 365
            THEN DATEDIFF('day',
                    TO_DATE(listing_date_key::VARCHAR, 'YYYYMMDD'),
                    CURRENT_DATE())
        END
    )                                                       AS avg_dom_excl_stale,
    MEDIAN(
        CASE
            WHEN property_price_usd > 0 AND number_of_bedrooms > 0
            THEN property_price_usd / number_of_bedrooms
        END
    )                                                       AS price_per_bedroom_median,
    COUNT(CASE WHEN number_of_bedrooms IS NOT NULL THEN 1 END) AS beds_count,
    MAX(scraped_at::DATE)                                   AS data_freshness,
    AVG(data_quality_score)                                 AS avg_dq,
    COUNT(
        CASE
            WHEN listing_date_key IS NOT NULL
             AND DATEDIFF('day',
                    TO_DATE(listing_date_key::VARCHAR, 'YYYYMMDD'),
                    CURRENT_DATE()) > 365
            THEN 1
        END
    )                                                       AS over_one_year_count
FROM base
"""


def get_suburb_snapshot(
    suburb: str,
    property_type: Optional[str] = None,
    bedrooms: Optional[int] = None,
    budget_min: Optional[float] = None,
    budget_max: Optional[float] = None,
    listing_type: str = "sale",
) -> dict:
    """
    Returns current market snapshot for a suburb.

    Filters by listing_type (default 'sale'), property_type, bedrooms,
    and budget_min/max when provided. Excludes web.archive.org records.

    Returns dict with keys:
      suburb_name, listing_type, active_listing_count,
      median_asking_price, avg_asking_price, min_asking_price,
      max_asking_price, avg_days_on_market, price_per_bedroom_median,
      market_temperature, data_freshness, records_with_bedrooms_pct,
      over_one_year_count
    """
    params: list = [suburb, listing_type]
    type_filter = ""
    bed_filter = ""
    budget_clauses = []

    if property_type:
        type_filter = "AND dpt.property_type = %s"
        params.append(property_type)
    if bedrooms is not None:
        bed_filter = "AND fl.number_of_bedrooms = %s"
        params.append(bedrooms)
    if budget_min is not None:
        budget_clauses.append("AND fl.property_price_usd >= %s")
        params.append(budget_min)
    if budget_max is not None:
        budget_clauses.append("AND fl.property_price_usd <= %s")
        params.append(budget_max)

    sql = _SNAPSHOT_SQL.format(
        wayback=_WAYBACK_EXCLUSION,
        price_filter=_price_filter(listing_type),
        type_filter=type_filter,
        bed_filter=bed_filter,
        budget_filter=" ".join(budget_clauses),
    )

    row = _fetchone(sql, tuple(params))

    if not row or row[0] == 0:
        return {
            "suburb_name": suburb,
            "listing_type": listing_type,
            "active_listing_count": 0,
            "median_asking_price": None,
            "avg_asking_price": None,
            "min_asking_price": None,
            "max_asking_price": None,
            "avg_days_on_market": None,
            "avg_dom_excl_stale": None,
            "price_per_bedroom_median": None,
            "market_temperature": "No Data",
            "data_freshness": None,
            "records_with_bedrooms_pct": 0.0,
            "over_one_year_count": 0,
        }

    (count, median_p, avg_p, min_p, max_p, avg_dom, avg_dom_excl_stale, ppm,
     beds_count, freshness, avg_dq, over_one_year) = row

    count    = int(count)    if count    else 0
    beds_pct = round((int(beds_count) / count * 100), 1) if count > 0 and beds_count else 0.0

    if count < 15:
        temperature = "Seller's Market"
    elif count <= 30:
        temperature = "Balanced"
    else:
        temperature = "Buyer's Market"

    bedroom_records = int(beds_count) if beds_count else 0
    ppm_out = float(round(ppm, 2)) if ppm and bedroom_records >= 5 else None

    return {
        "suburb_name": suburb,
        "listing_type": listing_type,
        "active_listing_count": count,
        "median_asking_price": float(round(median_p, 2)) if median_p else None,
        "avg_asking_price": float(round(avg_p, 2)) if avg_p else None,
        "min_asking_price": float(round(min_p, 2)) if min_p else None,
        "max_asking_price": float(round(max_p, 2)) if max_p else None,
        "avg_days_on_market": float(round(avg_dom, 1)) if avg_dom else None,
        "avg_dom_excl_stale": float(round(avg_dom_excl_stale, 1)) if avg_dom_excl_stale else None,
        "price_per_bedroom_median": ppm_out,
        "market_temperature": temperature,
        "data_freshness": (
            freshness.isoformat()
            if hasattr(freshness, "isoformat") else
            str(freshness) if freshness else None
        ),
        "records_with_bedrooms_pct": beds_pct,
        "over_one_year_count": int(over_one_year) if over_one_year else 0,
    }


# ── 2. get_price_trend ────────────────────────────────────────────────────────

_TREND_FROM_STATS_SQL = """
SELECT
    TO_CHAR(week_start, 'YYYY-"W"IW')  AS week_label,
    ROUND(median_price_usd, 2)          AS median_price,
    listing_count
FROM ZIM_PROPERTY_DB.ANALYTICS.SUBURB_MARKET_STATS
WHERE LOWER(suburb_clean) = LOWER(%s)
  AND listing_type = %s
  {type_filter}
  AND week_start >= DATEADD('week', -%s, CURRENT_DATE())
  AND median_price_usd IS NOT NULL
ORDER BY week_start ASC
"""

_TREND_FALLBACK_SQL = """
SELECT
    TO_CHAR(DATE_TRUNC('week', fl.scraped_at::DATE), 'YYYY-"W"IW') AS week_label,
    ROUND(MEDIAN(fl.property_price_usd), 2)                         AS median_price,
    COUNT(DISTINCT fl.listing_id)                                    AS listing_count
FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
    ON dl.location_key = fl.location_key
JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
    ON ds.source_key = fl.source_key
LEFT JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_PROPERTY_TYPE dpt
    ON dpt.property_type_key = fl.property_type_key
WHERE LOWER(dl.suburb_clean) = LOWER(%s)
  AND fl.listing_type = %s
  AND fl.is_price_valid = TRUE
  AND fl.property_price_usd > 0
  {wayback}
  {price_filter}
  {type_filter}
  AND fl.scraped_at >= DATEADD('week', -%s, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 ASC
"""


def get_price_trend(
    suburb: str,
    weeks: int = 12,
    property_type: Optional[str] = None,
    listing_type: str = "sale",
) -> list:
    """
    Returns weekly median asking price for the last N weeks, for the given
    listing_type ('sale' or 'rent').

    Tries ANALYTICS.SUBURB_MARKET_STATS first; falls back to WAREHOUSE.

    Returns list of dicts: [{"week": "2025-W01", "median_price": 95000,
    "listing_count": 12}, ...] sorted oldest to newest.
    Returns empty list if fewer than 4 weeks of data.
    """
    type_filter = "AND dpt.property_type = %s" if property_type else ""

    try:
        sql = _TREND_FROM_STATS_SQL.format(type_filter=type_filter)
        params = (
            [suburb, listing_type]
            + ([property_type] if property_type else [])
            + [weeks]
        )
        rows = _fetchall(sql, tuple(params))
    except Exception:
        rows = []

    if len(rows) < 4:
        sql = _TREND_FALLBACK_SQL.format(
            wayback=_WAYBACK_EXCLUSION,
            price_filter=_price_filter(listing_type),
            type_filter="AND dpt.property_type = %s" if property_type else "",
        )
        params = (
            [suburb, listing_type]
            + ([property_type] if property_type else [])
            + [weeks]
        )
        rows = _fetchall(sql, tuple(params))

    if len(rows) < 4:
        return []

    return [
        {
            "week": row[0],
            "median_price": float(row[1]) if row[1] else None,
            "listing_count": int(row[2]) if row[2] else 0,
        }
        for row in rows
        if row[1] is not None
    ]


# ── 3. get_comparable_listings ────────────────────────────────────────────────

_COMPARABLE_SQL = """
WITH suburb_median AS (
    SELECT MEDIAN(fl2.property_price_usd) AS med
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl2
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl2
        ON dl2.location_key = fl2.location_key
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds2
        ON ds2.source_key = fl2.source_key
    WHERE LOWER(dl2.suburb_clean) = LOWER(%s)
      AND fl2.listing_type = %s
      AND fl2.is_price_valid = TRUE
      AND fl2.property_price_usd > 0
      AND fl2.property_price_usd >= CASE fl2.listing_type WHEN 'sale' THEN 5000 ELSE 100 END
      AND fl2.property_price_usd <= CASE fl2.listing_type WHEN 'sale' THEN 5000000 ELSE 15000 END
      AND ds2.source NOT LIKE '%%archive.org%%'
      AND ds2.source NOT LIKE '%%wayback%%'
)
SELECT
    dl.suburb_clean,
    dpt.property_type,
    fl.number_of_bedrooms,
    fl.property_price_usd,
    CASE
        WHEN fl.listing_date_key IS NOT NULL
        THEN LEAST(
            DATEDIFF('day',
                TO_DATE(fl.listing_date_key::VARCHAR, 'YYYYMMDD'),
                CURRENT_DATE()),
            9999
        )
    END                                                 AS days_on_market,
    cl.agent_name,
    cl.listing_url,
    fl.listing_id,
    ABS(fl.property_price_usd - sm.med)                 AS price_dist
FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
    ON dl.location_key = fl.location_key
JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
    ON ds.source_key = fl.source_key
LEFT JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_PROPERTY_TYPE dpt
    ON dpt.property_type_key = fl.property_type_key
LEFT JOIN ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
    ON cl.listing_id = fl.listing_id
CROSS JOIN suburb_median sm
WHERE LOWER(dl.suburb_clean) = LOWER(%s)
  AND fl.listing_type = %s
  AND fl.is_price_valid = TRUE
  AND fl.property_price_usd > 0
  AND fl.property_price_usd >= CASE fl.listing_type WHEN 'sale' THEN 5000 ELSE 100 END
  AND fl.property_price_usd <= CASE fl.listing_type WHEN 'sale' THEN 5000000 ELSE 15000 END
  AND ds.source NOT LIKE '%%archive.org%%'
  AND ds.source NOT LIKE '%%wayback%%'
  {type_filter}
  {bed_filter}
  {budget_filter}
ORDER BY price_dist ASC
LIMIT %s
"""

_PRICE_CHANGE_SQL = """
SELECT listing_id
FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_PRICE_HISTORY
WHERE listing_id IN ({placeholders})
  AND change_type = 'price_decrease'
"""


def get_comparable_listings(
    suburb: str,
    property_type: Optional[str] = None,
    bedrooms: Optional[int] = None,
    budget_min: Optional[float] = None,
    budget_max: Optional[float] = None,
    limit: int = 8,
    listing_type: str = "sale",
) -> list:
    """
    Returns up to `limit` current listings in the suburb closest to median price.
    Filters by listing_type (default 'sale'). Excludes web.archive.org records.
    days_on_market is capped at 9999 internally; display cap of 999 applied
    in the report layer.

    Returns list of dicts with keys:
      suburb, property_type, bedrooms, asking_price, days_on_market,
      agent_name, source_url, is_stale, price_reduction_detected
    """
    params: list = [suburb, listing_type, suburb, listing_type]
    type_filter = ""
    bed_filter = ""
    budget_clauses = []

    if property_type:
        type_filter = "AND dpt.property_type = %s"
        params.append(property_type)
    if bedrooms is not None:
        bed_filter = "AND fl.number_of_bedrooms = %s"
        params.append(bedrooms)
    if budget_min is not None:
        budget_clauses.append("AND fl.property_price_usd >= %s")
        params.append(budget_min)
    if budget_max is not None:
        budget_clauses.append("AND fl.property_price_usd <= %s")
        params.append(budget_max)
    params.append(limit)

    sql = _COMPARABLE_SQL.format(
        type_filter=type_filter,
        bed_filter=bed_filter,
        budget_filter=" ".join(budget_clauses),
    )

    rows = _fetchall(sql, tuple(params))
    if not rows:
        return []

    reduced_ids: set[str] = set()
    try:
        listing_ids = [r[7] for r in rows]
        placeholders = ", ".join(["%s"] * len(listing_ids))
        pr_rows = _fetchall(
            _PRICE_CHANGE_SQL.format(placeholders=placeholders),
            tuple(listing_ids),
        )
        reduced_ids = {r[0] for r in pr_rows}
    except Exception:
        pass

    result = []
    for row in rows:
        suburb_val, ptype, beds, price, dom, agent, url, lid, _ = row
        result.append({
            "suburb": suburb_val,
            "property_type": ptype,
            "bedrooms": int(beds) if beds is not None else None,
            "asking_price": float(price) if price else None,
            "days_on_market": int(dom) if dom is not None else None,
            "agent_name": agent,
            "source_url": url,
            "is_stale": (int(dom) > 90) if dom is not None else False,
            "price_reduction_detected": lid in reduced_ids,
        })

    return result


# ── 4. get_active_agents ──────────────────────────────────────────────────────

_AGENTS_SQL = """
SELECT
    cl.agent_name,
    cl.agency_name,
    COUNT(DISTINCT fl.listing_id)   AS active_listing_count,
    MAX(cl.agent_phone)             AS agent_phone,
    MAX(cl.agent_email)             AS agent_email
FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
    ON dl.location_key = fl.location_key
JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
    ON ds.source_key = fl.source_key
JOIN ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
    ON cl.listing_id = fl.listing_id
WHERE LOWER(dl.suburb_clean) = LOWER(%s)
  AND cl.agent_name IS NOT NULL
  AND cl.agent_name != ''
  AND ds.source NOT LIKE '%%archive.org%%'
  AND ds.source NOT LIKE '%%wayback%%'
GROUP BY cl.agent_name, cl.agency_name
ORDER BY active_listing_count DESC
LIMIT %s
"""


def get_active_agents(suburb: str, limit: int = 5) -> list:
    """
    Returns agents with the most active listings in this suburb.
    Excludes web.archive.org records.

    Returns list of dicts: agent_name, agency_name, active_listing_count,
    agent_phone, agent_email. Sorted by active_listing_count descending.
    """
    rows = _fetchall(_AGENTS_SQL, (suburb, limit))
    return [
        {
            "agent_name": row[0],
            "agency_name": row[1],
            "active_listing_count": int(row[2]),
            "agent_phone": row[3],
            "agent_email": row[4],
        }
        for row in rows
    ]


# ── 5. get_available_suburbs ──────────────────────────────────────────────────

_AVAILABLE_SUBURBS_SQL = """
WITH sale_counts AS (
    SELECT
        dl.suburb_clean,
        dl.city_clean,
        COUNT(DISTINCT fl.listing_id)   AS sale_listings,
        AVG(fl.data_quality_score)      AS avg_dq
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
        ON dl.location_key = fl.location_key
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    WHERE dl.suburb_clean IS NOT NULL
      AND fl.is_price_valid = TRUE
      AND fl.listing_type = 'sale'
      AND fl.property_price_usd >= 5000
      AND fl.property_price_usd <= 5000000
      AND ds.source NOT LIKE '%%archive.org%%'
      AND ds.source NOT LIKE '%%wayback%%'
    GROUP BY dl.suburb_clean, dl.city_clean
),
rent_counts AS (
    SELECT
        dl.suburb_clean,
        dl.city_clean,
        COUNT(DISTINCT fl.listing_id)   AS rental_listings
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
        ON dl.location_key = fl.location_key
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    WHERE dl.suburb_clean IS NOT NULL
      AND fl.is_price_valid = TRUE
      AND fl.listing_type = 'rent'
      AND fl.property_price_usd >= 100
      AND fl.property_price_usd <= 15000
      AND ds.source NOT LIKE '%%archive.org%%'
      AND ds.source NOT LIKE '%%wayback%%'
    GROUP BY dl.suburb_clean, dl.city_clean
),
week_counts AS (
    SELECT
        suburb_clean,
        city_clean,
        COUNT(DISTINCT week_start) AS weeks_of_data
    FROM ZIM_PROPERTY_DB.ANALYTICS.SUBURB_MARKET_STATS
    WHERE suburb_clean IS NOT NULL
    GROUP BY suburb_clean, city_clean
)
SELECT
    sc.suburb_clean,
    sc.city_clean,
    sc.sale_listings,
    COALESCE(rc.rental_listings, 0)     AS rental_listings,
    COALESCE(wc.weeks_of_data, 0)       AS weeks_of_data,
    sc.avg_dq
FROM sale_counts sc
LEFT JOIN rent_counts rc
    ON LOWER(rc.suburb_clean) = LOWER(sc.suburb_clean)
    AND LOWER(rc.city_clean) = LOWER(sc.city_clean)
LEFT JOIN week_counts wc
    ON LOWER(wc.suburb_clean) = LOWER(sc.suburb_clean)
    AND LOWER(wc.city_clean) = LOWER(sc.city_clean)
WHERE sc.sale_listings >= 8
  AND sc.avg_dq >= 0.65
ORDER BY sc.suburb_clean ASC
"""

_AVAILABLE_SUBURBS_FALLBACK_SQL = """
WITH sale_counts AS (
    SELECT
        dl.suburb_clean,
        dl.city_clean,
        COUNT(DISTINCT fl.listing_id)   AS sale_listings,
        AVG(fl.data_quality_score)      AS avg_dq
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
        ON dl.location_key = fl.location_key
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    WHERE dl.suburb_clean IS NOT NULL
      AND fl.is_price_valid = TRUE
      AND fl.listing_type = 'sale'
      AND fl.property_price_usd >= 5000
      AND fl.property_price_usd <= 5000000
      AND ds.source NOT LIKE '%%archive.org%%'
      AND ds.source NOT LIKE '%%wayback%%'
    GROUP BY dl.suburb_clean, dl.city_clean
    HAVING COUNT(DISTINCT fl.listing_id) >= 8
       AND AVG(fl.data_quality_score) >= 0.65
),
rent_counts AS (
    SELECT
        dl.suburb_clean,
        dl.city_clean,
        COUNT(DISTINCT fl.listing_id) AS rental_listings
    FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
        ON dl.location_key = fl.location_key
    JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
        ON ds.source_key = fl.source_key
    WHERE dl.suburb_clean IS NOT NULL
      AND fl.is_price_valid = TRUE
      AND fl.listing_type = 'rent'
      AND fl.property_price_usd >= 100
      AND fl.property_price_usd <= 15000
      AND ds.source NOT LIKE '%%archive.org%%'
      AND ds.source NOT LIKE '%%wayback%%'
    GROUP BY dl.suburb_clean, dl.city_clean
)
SELECT
    sc.suburb_clean,
    sc.city_clean,
    sc.sale_listings,
    COALESCE(rc.rental_listings, 0) AS rental_listings,
    0                               AS weeks_of_data,
    sc.avg_dq
FROM sale_counts sc
LEFT JOIN rent_counts rc
    ON LOWER(rc.suburb_clean) = LOWER(sc.suburb_clean)
    AND LOWER(rc.city_clean)  = LOWER(sc.city_clean)
ORDER BY sc.suburb_clean ASC
"""


def get_available_suburbs(with_counts: bool = False) -> list:
    """
    Returns suburbs with sufficient SALE listing data for a reliable report.

    Threshold: >= 8 active SALE listings AND DQ >= 0.65.
    Also returns rental_listing_count per suburb so callers can decide
    whether a rental report is viable.

    Parameters
    ----------
    with_counts : bool
        If True, returns list of dicts with suburb_name, city_clean,
        sale_listing_count, rental_listing_count, weeks_of_data.
        If False (default), returns list of suburb name strings.
    """
    try:
        rows = _fetchall(_AVAILABLE_SUBURBS_SQL)
        if not rows:
            raise ValueError("no rows from primary query")
    except Exception:
        rows = _fetchall(_AVAILABLE_SUBURBS_FALLBACK_SQL)

    if with_counts:
        return [
            {
                "suburb_name": row[0],
                "city_clean": row[1],
                "sale_listing_count": int(row[2]),
                "rental_listing_count": int(row[3]),
                "weeks_of_data": int(row[4]),
            }
            for row in rows
        ]

    return [row[0] for row in rows]


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    TEST_SUBURB = "Borrowdale"

    print("=" * 60)
    print(f"suburb_queries.py — test with suburb='{TEST_SUBURB}'")
    print("=" * 60)

    print("\n[1] get_suburb_snapshot — SALE")
    snap_sale = get_suburb_snapshot(TEST_SUBURB, listing_type="sale")
    for k, v in snap_sale.items():
        print(f"  {k}: {v}")

    print("\n[2] get_suburb_snapshot — RENT")
    snap_rent = get_suburb_snapshot(TEST_SUBURB, listing_type="rent")
    for k, v in snap_rent.items():
        print(f"  {k}: {v}")

    print("\n[3] get_price_trend — SALE (12 weeks)")
    trend_sale = get_price_trend(TEST_SUBURB, weeks=12, listing_type="sale")
    print(f"  {len(trend_sale)} weeks returned")
    for t in trend_sale:
        print(f"  {t['week']}: ${t['median_price']:,.0f} ({t['listing_count']} listings)")

    print("\n[4] get_comparable_listings — SALE, 3 bed")
    comps_sale = get_comparable_listings(TEST_SUBURB, bedrooms=3, listing_type="sale", limit=5)
    print(f"  {len(comps_sale)} listings returned")
    for c in comps_sale:
        beds  = c["bedrooms"] if c["bedrooms"] is not None else "?"
        dom   = c["days_on_market"]
        dom_s = f"{min(dom, 999)} days" if dom is not None else "N/A"
        stale = " [STALE]" if c["is_stale"] else ""
        print(f"  {c['property_type']} | {beds} bed | ${c['asking_price']:,.0f} | {dom_s}{stale}")

    print("\n[5] get_comparable_listings — RENT, 3 bed")
    comps_rent = get_comparable_listings(TEST_SUBURB, bedrooms=3, listing_type="rent", limit=5)
    print(f"  {len(comps_rent)} listings returned")
    for c in comps_rent:
        beds  = c["bedrooms"] if c["bedrooms"] is not None else "?"
        dom   = c["days_on_market"]
        dom_s = f"{min(dom, 999)} days" if dom is not None else "N/A"
        print(f"  {c['property_type']} | {beds} bed | ${c['asking_price']:,.0f} | {dom_s}")

    print("\n[6] get_available_suburbs — with counts")
    suburbs = get_available_suburbs(with_counts=True)
    print(f"  {len(suburbs)} qualifying suburbs:")
    print(f"  {'suburb':<30} {'city':<20} {'sale':>6} {'rent':>6} {'weeks':>6}")
    print(f"  {'-'*30} {'-'*20} {'-'*6} {'-'*6} {'-'*6}")
    for s in suburbs:
        print(f"  {s['suburb_name']:<30} {s['city_clean']:<20} "
              f"{s['sale_listing_count']:>6} {s['rental_listing_count']:>6} "
              f"{s['weeks_of_data']:>6}")

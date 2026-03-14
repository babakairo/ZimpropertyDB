"""
matching/linker.py

Decision engine — takes scored candidate pairs and writes results to
MASTER schema (PROPERTY_MASTER, LISTING_PROPERTY_LINK, queues, logs).

Decision thresholds:
  score ≥ 0.85  → auto-confirm: link to existing PROPERTY_MASTER record
  score 0.60–0.84 → human review: insert into MATCH_CANDIDATE_QUEUE
  score < 0.60  → no match: create new PROPERTY_MASTER record (if needed)

Algorithm per unmatched listing:
  1. Pull all unmatched listing_ids from MASTER.V_UNMATCHED_LISTINGS
  2. Fetch their full records from STAGING
  3. Block via blocker.generate_candidates()
  4. Score via scorer.score_many()
  5. For score ≥ 0.85: link to highest-scoring existing master (or create new)
  6. For 0.60–0.84: queue for review
  7. For new listings with no candidate at all: create PROPERTY_MASTER record
  8. Update WAREHOUSE.FACT_LISTINGS.property_key after linking

Usage:
    # From command line:
    python -m matching.linker

    # From Dagster asset:
    from matching.linker import run_matching_pass
    stats = run_matching_pass(batch_size=500)
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Optional

import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

from scraper.utils.address_normaliser import (
    address_fingerprint,
    address_hash,
    geo_block,
    normalise_address,
)
from matching.blocker import generate_candidates, split_into_chunks
from matching.scorer import ScoreResult, score_many

logger = logging.getLogger(__name__)


# ── Thresholds ────────────────────────────────────────────────────────────────
THRESHOLD_AUTO_MATCH  = 0.85
THRESHOLD_REVIEW      = 0.60


# ── Snowflake connection ──────────────────────────────────────────────────────

def _get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.environ.get("SNOWFLAKE_ROLE", "ZIM_SCRAPER_ROLE"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        database  = "ZIM_PROPERTY_DB",
        schema    = "MASTER",
    )


# ── Data fetching ─────────────────────────────────────────────────────────────

def _fetch_unmatched_listings(conn, batch_size: int) -> list[dict]:
    """
    Pull listings that have no property_key in FACT_LISTINGS yet.
    Returns enriched records from STAGING for scoring.
    """
    sql = f"""
        SELECT
            fl.listing_id,
            cl.source,
            cl.listing_url,
            cl.property_title,
            cl.property_price_usd,
            cl.property_type,
            cl.listing_type,
            cl.suburb_clean         AS suburb,
            cl.city_clean           AS city,
            cl.address_raw,
            cl.latitude,
            cl.longitude,
            cl.number_of_bedrooms,
            cl.number_of_bathrooms,
            cl.number_of_garages,
            cl.property_size_sqm,
            cl.agent_phone,
            cl.agency_name,
            cl.scraped_at
        FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
        JOIN ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS cl
            ON cl.listing_id = fl.listing_id
        WHERE fl.property_key IS NULL
          AND cl.is_location_valid = TRUE
        ORDER BY fl.scraped_at DESC
        LIMIT {int(batch_size)}
    """
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return [{k.lower(): v for k, v in row.items()} for row in rows]


def _fetch_existing_masters(conn, city: str) -> list[dict]:
    """Pull existing PROPERTY_MASTER records for a given city."""
    sql = """
        SELECT
            pm.property_id,
            pm.canonical_address,
            pm.address_fingerprint,
            pm.address_hash,
            pm.latitude,
            pm.longitude,
            pm.suburb,
            pm.city,
            pm.property_type,
            pm.bedrooms_canonical   AS number_of_bedrooms,
            pm.current_price_usd    AS property_price_usd,
            pm.match_confidence
        FROM ZIM_PROPERTY_DB.MASTER.PROPERTY_MASTER pm
        WHERE LOWER(pm.city) = LOWER(%s)
    """
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute(sql, (city,))
    rows = cursor.fetchall()
    cursor.close()
    return [{k.lower(): v for k, v in row.items()} for row in rows]


# ── Master record creation ────────────────────────────────────────────────────

def _build_master_record(listing: dict) -> dict:
    """Derive a new PROPERTY_MASTER row from a listing dict."""
    raw_addr = listing.get("address_raw") or listing.get("property_title") or ""
    fp = address_fingerprint(raw_addr)
    ah = address_hash(raw_addr)
    lat = listing.get("latitude")
    lon = listing.get("longitude")

    return {
        "property_id":        str(uuid.uuid4()),
        "canonical_address":  raw_addr[:500],
        "address_normalised": normalise_address(raw_addr)[:500],
        "address_fingerprint": fp[:500] if fp else None,
        "address_hash":       ah,
        "suburb":             listing.get("suburb"),
        "city":               listing.get("city"),
        "latitude":           lat,
        "longitude":          lon,
        "geohash_8":          geo_block(lat, lon, precision=8),
        "property_type":      listing.get("property_type"),
        "bedrooms_canonical": listing.get("number_of_bedrooms"),
        "bathrooms_canonical": listing.get("number_of_bathrooms"),
        "garages_canonical":  listing.get("number_of_garages"),
        "size_sqm_canonical": listing.get("property_size_sqm"),
        "first_listed_date":  listing.get("scraped_at"),
        "last_listed_date":   listing.get("scraped_at"),
        "total_listings_count": 1,
        "active_source_count": 1,
        "first_price_usd":    listing.get("property_price_usd"),
        "current_price_usd":  listing.get("property_price_usd"),
        "min_price_usd":      listing.get("property_price_usd"),
        "max_price_usd":      listing.get("property_price_usd"),
        "match_confidence":   1.0,
        "match_method":       "new",
    }


# ── Snowflake writes ──────────────────────────────────────────────────────────

def _insert_master(conn, master: dict) -> None:
    sql = """
        INSERT INTO ZIM_PROPERTY_DB.MASTER.PROPERTY_MASTER (
            property_id, canonical_address, address_normalised,
            address_fingerprint, address_hash,
            suburb, city, latitude, longitude, geohash_8,
            property_type, bedrooms_canonical, bathrooms_canonical,
            garages_canonical, size_sqm_canonical,
            first_listed_date, last_listed_date, total_listings_count,
            active_source_count, first_price_usd, current_price_usd,
            min_price_usd, max_price_usd,
            match_confidence, match_method
        ) VALUES (
            %(property_id)s, %(canonical_address)s, %(address_normalised)s,
            %(address_fingerprint)s, %(address_hash)s,
            %(suburb)s, %(city)s, %(latitude)s, %(longitude)s, %(geohash_8)s,
            %(property_type)s, %(bedrooms_canonical)s, %(bathrooms_canonical)s,
            %(garages_canonical)s, %(size_sqm_canonical)s,
            %(first_listed_date)s, %(last_listed_date)s, %(total_listings_count)s,
            %(active_source_count)s, %(first_price_usd)s, %(current_price_usd)s,
            %(min_price_usd)s, %(max_price_usd)s,
            %(match_confidence)s, %(match_method)s
        )
    """
    conn.cursor().execute(sql, master)


def _insert_link(conn, listing: dict, property_id: str, result: Optional[ScoreResult], method: str) -> None:
    signals_json = json.dumps(result.signals) if result else '{}'
    score = result.score if result else 1.0
    # Use SELECT instead of VALUES so PARSE_JSON() is accepted by Snowflake
    sql = """
        INSERT INTO ZIM_PROPERTY_DB.MASTER.LISTING_PROPERTY_LINK (
            listing_id, property_id, source, listing_url,
            scraped_at, match_score, match_method, match_signals
        )
        SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
    """
    conn.cursor().execute(sql, (
        listing["listing_id"],
        property_id,
        listing.get("source"),
        listing.get("listing_url"),
        listing.get("scraped_at"),
        score,
        method,
        signals_json,
    ))


def _insert_queue(conn, result: ScoreResult, listing_a: dict, listing_b: dict) -> None:
    # Use MERGE to handle duplicates (Snowflake has no ON CONFLICT)
    sql = """
        MERGE INTO ZIM_PROPERTY_DB.MASTER.MATCH_CANDIDATE_QUEUE tgt
        USING (
            SELECT %s AS lid_a, %s AS lid_b,
                   %s AS score, %s AS addr_sim, %s AS geo_dist,
                   %s AS price_diff, %s AS bed_match, %s AS phone_match
        ) src
        ON tgt.listing_id_a = src.lid_a AND tgt.listing_id_b = src.lid_b
        WHEN NOT MATCHED THEN INSERT (
            listing_id_a, listing_id_b,
            composite_score, address_similarity, geo_distance_m,
            price_diff_pct, bedroom_match, phone_match
        ) VALUES (
            src.lid_a, src.lid_b,
            src.score, src.addr_sim, src.geo_dist,
            src.price_diff, src.bed_match, src.phone_match
        )
    """
    s = result.signals
    try:
        conn.cursor().execute(sql, (
            result.listing_id_a,
            result.listing_id_b,
            result.score,
            s.get("address_similarity"),
            s.get("geo_distance_m"),
            s.get("price_diff_pct"),
            s.get("bedroom_match"),
            s.get("phone_match"),
        ))
    except Exception:
        pass  # skip duplicate queue entries silently


def _log_decision(conn, result: ScoreResult, property_id: Optional[str], decision: str) -> None:
    sql = """
        INSERT INTO ZIM_PROPERTY_DB.MASTER.MATCH_DECISION_LOG (
            listing_id_a, listing_id_b, property_id,
            composite_score, decision, decided_by, signals_json
        )
        SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
    """
    conn.cursor().execute(sql, (
        result.listing_id_a,
        result.listing_id_b,
        property_id,
        result.score,
        decision,
        "linker.py",
        json.dumps(result.signals),
    ))


def _update_fact_property_key(conn, listing_id: str, property_id: str) -> None:
    """Back-fill property_key in FACT_LISTINGS after a link is created."""
    # Snowflake UPDATE with FROM clause (join-style) — avoids subquery LIMIT issue
    sql = """
        UPDATE ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
        SET fl.property_key = dp.property_key
        FROM ZIM_PROPERTY_DB.WAREHOUSE.DIM_PROPERTY dp
        WHERE dp.master_property_id = %s
          AND fl.listing_id = %s
          AND fl.property_key IS NULL
    """
    conn.cursor().execute(sql, (property_id, listing_id))


def _update_master_stats(conn, property_id: str) -> None:
    """Recalculate aggregate stats on PROPERTY_MASTER after a new link."""
    sql = """
        UPDATE ZIM_PROPERTY_DB.MASTER.PROPERTY_MASTER pm
        SET
            total_listings_count = stats.cnt,
            active_source_count  = stats.src_cnt,
            last_listed_date     = stats.last_date,
            current_price_usd    = stats.latest_price,
            min_price_usd        = stats.min_price,
            max_price_usd        = stats.max_price,
            updated_at           = CURRENT_TIMESTAMP()
        FROM (
            SELECT
                lpl.property_id,
                COUNT(*)                            AS cnt,
                COUNT(DISTINCT lpl.source)          AS src_cnt,
                MAX(lpl.scraped_at)::DATE           AS last_date,
                MAX_BY(fl.property_price_usd, fl.scraped_at) AS latest_price,
                MIN(fl.property_price_usd)          AS min_price,
                MAX(fl.property_price_usd)          AS max_price
            FROM ZIM_PROPERTY_DB.MASTER.LISTING_PROPERTY_LINK lpl
            JOIN ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
                ON fl.listing_id = lpl.listing_id
            WHERE lpl.property_id = %s
            GROUP BY 1
        ) stats
        WHERE pm.property_id = stats.property_id
    """
    conn.cursor().execute(sql, (property_id,))


# ── Stats container ───────────────────────────────────────────────────────────

@dataclass
class MatchingStats:
    processed:      int = 0
    new_masters:    int = 0
    auto_linked:    int = 0
    queued:         int = 0
    errors:         int = 0
    skipped:        int = 0


# ── Main pass ─────────────────────────────────────────────────────────────────

def run_matching_pass(batch_size: int = 500) -> MatchingStats:
    """
    Run one full matching pass:
      1. Fetch unmatched listings (up to batch_size)
      2. Block + score within each city group
      3. Compare against existing PROPERTY_MASTER records
      4. Write links / queue entries / new master records
      5. Back-fill FACT_LISTINGS.property_key

    Returns MatchingStats with counts per decision type.
    """
    stats = MatchingStats()
    conn = _get_connection()

    try:
        listings = _fetch_unmatched_listings(conn, batch_size)
        if not listings:
            logger.info("No unmatched listings found — nothing to do.")
            return stats

        logger.info(f"Matching pass: {len(listings)} unmatched listings")

        # Index by listing_id for O(1) lookup
        listing_index: dict[str, dict] = {r["listing_id"]: r for r in listings}

        # Pre-compute fingerprints for speed
        for rec in listings:
            raw = rec.get("address_raw") or rec.get("property_title") or ""
            rec["address_fingerprint"] = address_fingerprint(raw)
            rec["address_hash"]        = address_hash(raw)

        # Process city by city to limit block sizes
        chunks = split_into_chunks(listings)

        for chunk in chunks:
            if not chunk:
                continue

            city = (chunk[0].get("city") or "unknown").title()
            existing_masters = _fetch_existing_masters(conn, city)

            # Build a combined pool: new listings + existing master records
            # (masters get a synthetic listing_id for blocking)
            master_index: dict[str, dict] = {}
            all_records: list[dict] = list(chunk)
            for m in existing_masters:
                pseudo = {"listing_id": f"master:{m['property_id'][:8]}", **m}
                all_records.append(pseudo)
                master_index[pseudo["listing_id"]] = m

            # Block
            pairs = generate_candidates(all_records)

            # Score only pairs involving at least one new (unmatched) listing
            new_ids = {r["listing_id"] for r in chunk}
            pairs = [
                (a, b) for a, b in pairs
                if a in new_ids or b in new_ids
            ]

            scored = score_many(pairs, {r["listing_id"]: r for r in all_records})

            # Track which listings have been resolved in this pass
            resolved: dict[str, str] = {}  # listing_id → property_id

            for result in scored:
                id_a, id_b = result.listing_id_a, result.listing_id_b

                # Identify which is new and which is the candidate
                new_id     = id_a if id_a in new_ids else id_b
                cand_id    = id_b if id_a in new_ids else id_a

                if new_id in resolved:
                    continue  # already resolved in a higher-scoring pair

                new_listing = listing_index.get(new_id)
                if not new_listing:
                    continue

                try:
                    if result.score >= THRESHOLD_AUTO_MATCH:
                        # Determine the property_id to link to
                        if cand_id.startswith("master:"):
                            # Link to existing master
                            master_rec = master_index.get(cand_id)
                            prop_id = master_rec["property_id"]
                            method = "fuzzy_auto"
                        elif cand_id in resolved:
                            # Link to the same master the candidate was just linked to
                            prop_id = resolved[cand_id]
                            method = "fuzzy_auto"
                        else:
                            # Both are new listings — create one master for both
                            master_data = _build_master_record(new_listing)
                            prop_id = master_data["property_id"]
                            _insert_master(conn, master_data)
                            stats.new_masters += 1
                            method = "fuzzy_auto"
                            # Link the candidate too
                            cand_listing = listing_index.get(cand_id)
                            if cand_listing and cand_id not in resolved:
                                _insert_link(conn, cand_listing, prop_id, result, method)
                                _update_fact_property_key(conn, cand_id, prop_id)
                                resolved[cand_id] = prop_id
                                stats.auto_linked += 1

                        _insert_link(conn, new_listing, prop_id, result, method)
                        _update_fact_property_key(conn, new_id, prop_id)
                        _update_master_stats(conn, prop_id)
                        _log_decision(conn, result, prop_id, "match")
                        resolved[new_id] = prop_id
                        stats.auto_linked += 1

                    elif result.score >= THRESHOLD_REVIEW:
                        _insert_queue(conn, result, new_listing, listing_index.get(cand_id, {}))
                        _log_decision(conn, result, None, "queued_for_review")
                        stats.queued += 1

                    else:
                        _log_decision(conn, result, None, "no_match")

                except Exception as exc:
                    logger.error(f"Error processing pair ({id_a}, {id_b}): {exc}", exc_info=True)
                    stats.errors += 1

            # Create new master records for listings with no candidates at all
            for rec in chunk:
                lid = rec["listing_id"]
                if lid not in resolved:
                    try:
                        master_data = _build_master_record(rec)
                        _insert_master(conn, master_data)
                        _insert_link(conn, rec, master_data["property_id"], None, "new")
                        _update_fact_property_key(conn, lid, master_data["property_id"])
                        resolved[lid] = master_data["property_id"]
                        stats.new_masters += 1
                    except Exception as exc:
                        logger.error(f"Error creating master for {lid}: {exc}", exc_info=True)
                        stats.errors += 1

            conn.commit()
            stats.processed += len(chunk)

    finally:
        conn.close()

    logger.info(
        f"Matching pass complete — "
        f"processed={stats.processed}, new_masters={stats.new_masters}, "
        f"auto_linked={stats.auto_linked}, queued={stats.queued}, "
        f"errors={stats.errors}"
    )
    return stats


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="Run property matching/linking pass")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Number of unmatched listings to process (default: 500)")
    args = parser.parse_args()

    result_stats = run_matching_pass(batch_size=args.batch_size)
    print(f"\nDone. Stats: {result_stats}")

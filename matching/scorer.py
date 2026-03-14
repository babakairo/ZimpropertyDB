"""
matching/scorer.py

Weighted multi-signal similarity scorer.

For each candidate pair (listing_a, listing_b) produced by blocker.py,
compute a composite score in [0.0, 1.0].  The score is a weighted
average of individual signals:

  Signal                Weight   Notes
  ─────────────────── ──────── ─────────────────────────────────────
  address_similarity    0.40    Token-overlap Jaccard on fingerprints
  geo_proximity         0.25    Gaussian decay over distance (metres)
  price_similarity      0.15    Inverse relative price difference
  bedroom_match         0.10    Exact integer match
  phone_match           0.05    Normalised phone string equality
  agency_match          0.05    Agency name token overlap

Thresholds (set in linker.py):
  ≥ 0.85  → auto-confirm match
  0.60–0.84 → enqueue for human review
  < 0.60  → not a match

Usage:
    from matching.scorer import score_pair
    result = score_pair(listing_a, listing_b)
    # result.score, result.signals
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Optional

from scraper.utils.address_normaliser import (
    address_fingerprint,
    normalise_phone,
)


# ── Weights ───────────────────────────────────────────────────────────────────
_W_ADDRESS  = 0.40
_W_GEO      = 0.25
_W_PRICE    = 0.15
_W_BEDROOMS = 0.10
_W_PHONE    = 0.05
_W_AGENCY   = 0.05

assert abs((_W_ADDRESS + _W_GEO + _W_PRICE + _W_BEDROOMS + _W_PHONE + _W_AGENCY) - 1.0) < 1e-9, \
    "Weights must sum to 1.0"


# ── Result container ──────────────────────────────────────────────────────────
@dataclass
class ScoreResult:
    listing_id_a:       str
    listing_id_b:       str
    score:              float                       # composite 0.0–1.0
    signals:            dict = field(default_factory=dict)
    # Individual signal values stored in signals dict:
    #   address_similarity, geo_distance_m, geo_score, price_diff_pct,
    #   price_score, bedroom_match, phone_match, agency_score


# ── Signal: Address similarity ────────────────────────────────────────────────

def _jaccard(set_a: set, set_b: set) -> float:
    """Jaccard index: |A ∩ B| / |A ∪ B|."""
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)


def _address_similarity(a: dict, b: dict) -> float:
    """
    Token-overlap Jaccard similarity on sorted address fingerprints.
    Falls back to raw address if fingerprint not pre-computed.
    """
    fp_a = a.get("address_fingerprint") or address_fingerprint(
        a.get("address_raw") or a.get("property_title") or ""
    )
    fp_b = b.get("address_fingerprint") or address_fingerprint(
        b.get("address_raw") or b.get("property_title") or ""
    )
    if not fp_a and not fp_b:
        return 0.0
    tokens_a = set(fp_a.split()) if fp_a else set()
    tokens_b = set(fp_b.split()) if fp_b else set()
    return _jaccard(tokens_a, tokens_b)


# ── Signal: Geographic proximity ──────────────────────────────────────────────

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in metres between two lat/lon points."""
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _geo_score(a: dict, b: dict) -> tuple[float, Optional[float]]:
    """
    Gaussian proximity score.  Score = exp(-(d/sigma)²).
    sigma = 150 m → score of 0.37 at 150 m, 0.01 at 340 m, 0.0 at 500 m+.
    Returns (score, distance_m); distance_m is None if coordinates missing.
    """
    try:
        lat_a, lon_a = float(a["latitude"]),  float(a["longitude"])
        lat_b, lon_b = float(b["latitude"]),  float(b["longitude"])
    except (KeyError, TypeError, ValueError):
        return 0.5, None  # no coordinates → neutral (don't penalise)

    dist = _haversine_m(lat_a, lon_a, lat_b, lon_b)
    sigma = 150.0  # metres — tune to typical address precision in Zimbabwe
    score = math.exp(-((dist / sigma) ** 2))
    return score, dist


# ── Signal: Price similarity ──────────────────────────────────────────────────

def _price_score(a: dict, b: dict) -> tuple[float, Optional[float]]:
    """
    Price similarity.  Score = max(0, 1 - |diff| / threshold).
    threshold = 20% → score drops to 0 when prices differ by >20%.
    Returns (score, abs_diff_pct).
    """
    try:
        pa = float(a.get("property_price_usd") or a.get("property_price") or 0)
        pb = float(b.get("property_price_usd") or b.get("property_price") or 0)
    except (TypeError, ValueError):
        return 0.5, None  # missing price → neutral

    if pa <= 0 or pb <= 0:
        return 0.5, None  # at least one price missing → neutral

    diff_pct = abs(pa - pb) / max(pa, pb) * 100
    score = max(0.0, 1.0 - diff_pct / 20.0)   # 0% diff → 1.0; 20% diff → 0.0
    return score, round(diff_pct, 2)


# ── Signal: Bedroom match ─────────────────────────────────────────────────────

def _bedroom_match(a: dict, b: dict) -> bool:
    """Exact integer match on number_of_bedrooms."""
    try:
        ba = int(a.get("number_of_bedrooms") or -1)
        bb = int(b.get("number_of_bedrooms") or -1)
    except (TypeError, ValueError):
        return False
    if ba < 0 or bb < 0:
        return False  # at least one missing — not a reliable signal
    return ba == bb


# ── Signal: Phone match ───────────────────────────────────────────────────────

def _phone_match(a: dict, b: dict) -> bool:
    """Normalised +263XXXXXXXXX string equality."""
    phone_a = normalise_phone(a.get("agent_phone"))
    phone_b = normalise_phone(b.get("agent_phone"))
    if not phone_a or not phone_b:
        return False
    return phone_a == phone_b


# ── Signal: Agency name similarity ───────────────────────────────────────────

def _agency_score(a: dict, b: dict) -> float:
    """Token-overlap Jaccard on agency_name strings (lowercase)."""
    ag_a = re.sub(r"[^a-z0-9\s]", "", (a.get("agency_name") or "").lower()).split()
    ag_b = re.sub(r"[^a-z0-9\s]", "", (b.get("agency_name") or "").lower()).split()
    if not ag_a or not ag_b:
        return 0.5  # unknown agency → neutral
    return _jaccard(set(ag_a), set(ag_b))


# ── Composite score ───────────────────────────────────────────────────────────

def score_pair(
    listing_a: dict,
    listing_b: dict,
) -> ScoreResult:
    """
    Compute composite match score for a candidate pair.

    Args:
        listing_a: Listing dict with at minimum listing_id and address fields.
        listing_b: Listing dict — the candidate match.

    Returns:
        ScoreResult with composite score and breakdown per signal.
    """
    addr_sim   = _address_similarity(listing_a, listing_b)
    geo_sc, geo_dist = _geo_score(listing_a, listing_b)
    price_sc, price_diff = _price_score(listing_a, listing_b)
    bed_match  = _bedroom_match(listing_a, listing_b)
    phone_hit  = _phone_match(listing_a, listing_b)
    agency_sc  = _agency_score(listing_a, listing_b)

    composite = (
        _W_ADDRESS  * addr_sim
        + _W_GEO    * geo_sc
        + _W_PRICE  * price_sc
        + _W_BEDROOMS * (1.0 if bed_match else 0.0)
        + _W_PHONE  * (1.0 if phone_hit else 0.0)
        + _W_AGENCY * agency_sc
    )

    signals = {
        "address_similarity": round(addr_sim, 4),
        "geo_score":          round(geo_sc, 4),
        "geo_distance_m":     round(geo_dist, 1) if geo_dist is not None else None,
        "price_score":        round(price_sc, 4),
        "price_diff_pct":     price_diff,
        "bedroom_match":      bed_match,
        "phone_match":        phone_hit,
        "agency_score":       round(agency_sc, 4),
    }

    return ScoreResult(
        listing_id_a=listing_a["listing_id"],
        listing_id_b=listing_b["listing_id"],
        score=round(composite, 4),
        signals=signals,
    )


def score_many(
    pairs: list[tuple[str, str]],
    listing_index: dict[str, dict],
) -> list[ScoreResult]:
    """
    Score a list of candidate pairs.

    Args:
        pairs:          List of (id_a, id_b) tuples from blocker.
        listing_index:  Dict mapping listing_id → listing record dict.

    Returns:
        List of ScoreResult sorted by score descending.
    """
    results = []
    for id_a, id_b in pairs:
        rec_a = listing_index.get(id_a)
        rec_b = listing_index.get(id_b)
        if rec_a and rec_b:
            results.append(score_pair(rec_a, rec_b))
    return sorted(results, key=lambda r: r.score, reverse=True)

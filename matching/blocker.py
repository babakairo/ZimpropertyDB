"""
matching/blocker.py

Candidate-pair generation (blocking pass).

Blocking avoids comparing every listing against every other listing
(O(n²)) by grouping listings into overlapping "blocks" — only
listings sharing the same block key are scored against each other.

Two complementary blocking strategies:

  1. ADDRESS_HASH block
     key = address_fingerprint SHA-256[:16]
     Perfect recall for addresses that normalise identically.
     Zero recall for misspellings or different street-number formats.

  2. GEOHASH block (precision=7 → ~153m cell + 8 adjacent neighbours)
     key = geohash[:5] (2.4 km² area — broader net)
     Catches nearby listings even when address strings differ.
     Requires lat/lon; skipped when coordinates are missing.

Candidate pairs are deduplicated before being passed to scorer.py.

Usage:
    from matching.blocker import generate_candidates
    pairs = generate_candidates(listings)     # list of (id_a, id_b) tuples
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from typing import Sequence

from scraper.utils.address_normaliser import address_fingerprint, address_hash, geo_block


# ── Type alias ────────────────────────────────────────────────────────────────
# A "listing record" dict pulled from STAGING or FACT_LISTINGS.
# Required keys: listing_id, address_raw, latitude, longitude
ListingRecord = dict


# ── Geohash neighbour offsets ─────────────────────────────────────────────────
# Geohash neighbour computation for "precision=5" cells (≈2.4 km).
# We expand each cell to its 8 neighbours so near-boundary listings are caught.
_GEOHASH_NEIGHBOURS: dict[str, tuple[int, int]] = {
    "n":  (0,  1),  "ne": (1,  1),  "e":  (1,  0),
    "se": (1, -1),  "s":  (0, -1),  "sw": (-1, -1),
    "w":  (-1, 0),  "nw": (-1,  1),
}
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_BASE32_MAP = {c: i for i, c in enumerate(_BASE32)}


def _geohash_neighbours(gh: str) -> list[str]:
    """
    Return the 8 adjacent geohash cells at the same precision.
    Implemented via integer arithmetic on the base-32 encoded value.
    Returns only valid neighbours (drops edge cells that go out of range).
    """
    if not gh:
        return []
    precision = len(gh)
    # Decode to (lat_bits, lon_bits)
    bits = 0
    for ch in gh:
        bits = (bits << 5) | _BASE32_MAP.get(ch, 0)

    total_bits = precision * 5
    lat_bits_count = total_bits // 2
    lon_bits_count = total_bits - lat_bits_count

    lat_range = 1 << lat_bits_count
    lon_range = 1 << lon_bits_count

    # Split interleaved bits
    lat_val, lon_val = 0, 0
    for i in range(total_bits - 1, -1, -1):
        bit = (bits >> i) & 1
        if (total_bits - 1 - i) % 2 == 0:
            lon_val = (lon_val << 1) | bit
        else:
            lat_val = (lat_val << 1) | bit

    neighbours = []
    for (dlat, dlon) in _GEOHASH_NEIGHBOURS.values():
        nlat = lat_val + dlat
        nlon = lon_val + dlon
        if 0 <= nlat < lat_range and 0 <= nlon < lon_range:
            # Re-interleave
            nbits = 0
            for i in range(max(lat_bits_count, lon_bits_count)):
                if i < lon_bits_count:
                    nbits = (nbits << 1) | ((nlon >> (lon_bits_count - 1 - i)) & 1)
                if i < lat_bits_count:
                    nbits = (nbits << 1) | ((nlat >> (lat_bits_count - 1 - i)) & 1)
            # Encode back to base32
            chars = []
            for _ in range(precision):
                chars.append(_BASE32[nbits & 0x1F])
                nbits >>= 5
            neighbours.append("".join(reversed(chars)))

    return neighbours


# ── Block builders ────────────────────────────────────────────────────────────

def _address_blocks(listings: Sequence[ListingRecord]) -> dict[str, list[str]]:
    """Return {address_hash: [listing_id, ...]} blocks."""
    blocks: dict[str, list[str]] = defaultdict(list)
    for rec in listings:
        raw = rec.get("address_raw") or rec.get("property_title") or ""
        ah = address_hash(raw)
        if ah:
            blocks[ah].append(rec["listing_id"])
    return dict(blocks)


def _geo_blocks(
    listings: Sequence[ListingRecord],
    precision: int = 5,
) -> dict[str, list[str]]:
    """
    Return {geohash: [listing_id, ...]} blocks including adjacent cells.
    Precision=5 → ~2.4 km² per cell; expand to 9 cells per listing.
    """
    # Primary assignment: each listing → its own geohash cell
    primary: dict[str, str] = {}  # listing_id → geohash
    for rec in listings:
        lat = rec.get("latitude")
        lon = rec.get("longitude")
        gh = geo_block(lat, lon, precision=precision)
        if gh:
            primary[rec["listing_id"]] = gh

    # Expand: each listing participates in its cell + 8 neighbours
    blocks: dict[str, list[str]] = defaultdict(list)
    for lid, gh in primary.items():
        for cell in [gh] + _geohash_neighbours(gh):
            blocks[cell].append(lid)

    return {k: v for k, v in blocks.items() if len(v) > 1}


def _pairs_from_blocks(blocks: dict[str, list[str]]) -> set[tuple[str, str]]:
    """Convert block → listing lists into sorted (id_a, id_b) pair sets."""
    pairs: set[tuple[str, str]] = set()
    for members in blocks.values():
        if len(members) < 2:
            continue
        for a, b in itertools.combinations(members, 2):
            pairs.add((min(a, b), max(a, b)))
    return pairs


# ── Public API ────────────────────────────────────────────────────────────────

def generate_candidates(
    listings: Sequence[ListingRecord],
    use_geo: bool = True,
    geo_precision: int = 5,
) -> list[tuple[str, str]]:
    """
    Generate candidate pairs for scoring.

    Args:
        listings:      Sequence of listing dicts with at minimum:
                         listing_id, address_raw, latitude, longitude
        use_geo:       Also block on geohash (requires lat/lon fields).
        geo_precision: Geohash precision for geo blocking (default 5 ≈ 2.4 km).

    Returns:
        Sorted list of (listing_id_a, listing_id_b) tuples — no duplicates.
    """
    pairs: set[tuple[str, str]] = set()

    # Pass 1: address hash blocks
    addr_blocks = _address_blocks(listings)
    pairs |= _pairs_from_blocks(addr_blocks)

    # Pass 2: geohash blocks
    if use_geo:
        geo_blocks = _geo_blocks(listings, precision=geo_precision)
        pairs |= _pairs_from_blocks(geo_blocks)

    return sorted(pairs)


def split_into_chunks(
    listings: Sequence[ListingRecord],
    chunk_size: int = 10_000,
) -> list[list[ListingRecord]]:
    """
    Split a large listing batch into city-grouped chunks to keep
    blocking manageable when the full dataset is very large.
    """
    city_groups: dict[str, list[ListingRecord]] = defaultdict(list)
    for rec in listings:
        city = (rec.get("city_clean") or rec.get("city") or "unknown").lower()
        city_groups[city].append(rec)

    chunks = []
    for group in city_groups.values():
        for i in range(0, len(group), chunk_size):
            chunks.append(group[i : i + chunk_size])
    return chunks

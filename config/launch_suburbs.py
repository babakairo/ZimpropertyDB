"""
config/launch_suburbs.py
Single source of truth for which suburbs are available for purchase and at
what price tier. Edit this file to add / remove suburbs or adjust pricing.
"""
from __future__ import annotations

# ── Tier 1 — Premium suburbs ($49) ───────────────────────────────────────────
# High-density Harare residential suburbs with 15+ sale listings
TIER_1_SUBURBS: list[str] = [
    "Avondale",
    "Arlington",
    "Borrowdale",
    "Borrowdale Brooke",
    "Borrowdale West",
    "Chisipite",
    "Eastlea",
    "Glen Lorne",
    "Gletwin Park",
    "Greendale",
    "Greystone Park",
    "Helensvale",
    "Highlands",
    "Hogerty Hill",
    "Mandara",
    "Marlborough",
    "Mount Pleasant",
    "Newlands",
    "Vainona",
]

# ── Tier 2 — Standard suburbs ($35) ──────────────────────────────────────────
# Secondary Harare suburbs + regional cities with 8–14 listings
TIER_2_SUBURBS: list[str] = [
    "Avenues",
    "Brookview",
    "Bulawayo City Centre",
    "Burnside",
    "Chitungwiza",
    "Greendale North",
    "Gweru CBD",
    "Hatfield",
    "Kadoma",
    "Kwekwe",
    "Marondera",
    "Mutare CBD",
    "Pomona",
    "Quinnington",
    "Sandton Park",
    "Sunway City",
    "Tynwald",
    "Victoria Falls",
]

# ── Pricing ───────────────────────────────────────────────────────────────────
TIER_PRICES: dict[int, int] = {
    1: 49,
    2: 35,
}

# ── Report types available ────────────────────────────────────────────────────
REPORT_TYPES: list[str] = ["sale", "rent"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_suburb_tier(suburb: str) -> int:
    """Returns 1, 2, or 0 if the suburb is not available."""
    if suburb in TIER_1_SUBURBS:
        return 1
    if suburb in TIER_2_SUBURBS:
        return 2
    return 0


def get_report_price(suburb: str) -> int:
    """Returns the price in USD for a report on this suburb."""
    tier = get_suburb_tier(suburb)
    return TIER_PRICES.get(tier, 49)


def get_all_available_suburbs() -> list[dict]:
    """
    Returns all suburbs available for purchase, sorted alphabetically,
    each with their tier and price.

    Returns list of dicts: {"name": str, "tier": int, "price": int}
    """
    suburbs = []
    for s in sorted(TIER_1_SUBURBS + TIER_2_SUBURBS):
        suburbs.append({
            "name": s,
            "tier": get_suburb_tier(s),
            "price": get_report_price(s),
        })
    return suburbs

"""
scraper/utils/address_normaliser.py

Standardises raw address strings scraped from Zimbabwean property websites
so that the same physical address from different sites produces the same
normalised string — enabling cross-site deduplication in the matching engine.

Usage:
    from scraper.utils.address_normaliser import normalise_address, address_fingerprint

    # Returns cleaned, lowercase, expanded string
    normalise_address("12 Borrowdale Rd, Harare")
    → "12 borrowdale road harare"

    # Returns sorted-token fingerprint for order-independent matching
    address_fingerprint("Borrowdale Road 12, Harare")
    → "12 borrowdale harare road"

    # Returns geohash (requires lat/lon)
    geo_block("Borrowdale", -17.7504, 31.0780, precision=7)
    → "kv9tqwu"
"""
import re
import unicodedata
import hashlib
from typing import Optional


# ── Abbreviation expansions ──────────────────────────────────────────────────
# Word-boundary patterns mapped to full expansion (lowercase).
_ABBREVIATIONS: list[tuple[str, str]] = [
    # Road types
    (r"\brd\b",     "road"),
    (r"\bst\b",     "street"),
    (r"\bstr\b",    "street"),
    (r"\bave?\b",   "avenue"),
    (r"\bdr\b",     "drive"),
    (r"\bcl\b",     "close"),
    (r"\bcres\b",   "crescent"),
    (r"\bcr\b",     "crescent"),
    (r"\bct\b",     "court"),
    (r"\blane\b",   "lane"),
    (r"\bln\b",     "lane"),
    (r"\bblvd\b",   "boulevard"),
    (r"\bhwy\b",    "highway"),
    (r"\bsq\b",     "square"),
    (r"\bpl\b",     "place"),
    (r"\bpde\b",    "parade"),
    (r"\bext\b",    "extension"),
    (r"\bextn\b",   "extension"),
    # Unit prefixes
    (r"\bno\.?\s*",  ""),          # "No. 14" → "14"
    (r"\bunit\b",   ""),           # "Unit 3" → "3"
    (r"\bflat\b",   ""),           # "Flat 2" → "2" (remove for matching)
    (r"\bapt\.?\b", ""),           # "Apt 5" → "5"
    # Common directional
    (r"\bmt\.?\b",  "mount"),
    (r"\bst\.?\b",  "saint"),      # St Mary's → saint marys
]

# ── Suburb / area aliases ─────────────────────────────────────────────────────
# Key = raw fragment (lowercase), Value = canonical form (lowercase)
SUBURB_ALIASES: dict[str, str] = {
    # Harare
    "borro":            "borrowdale",
    "borrowd":          "borrowdale",
    "avond":            "avondale",
    "avondale west":    "avondale",
    "gun hill":         "gunhill",
    "glen lorne":       "glenlorne",
    "mt pleasant":      "mount pleasant",
    "msasa pk":         "msasa park",
    "msasa p":          "msasa park",
    "highlands":        "highlands",
    "hatf":             "hatfield",
    "mabelr":           "mabelreign",
    "eastl":            "eastlea",
    "belved":           "belvedere",
    "greend":           "greendale",
    "greenvale":        "greendale",
    "pomona":           "pomona",
    "chishaw":          "chisawasha",
    "tynw":             "tynwald",
    "kuwad":            "kuwadzana",
    "ma'ari":           "maari",
    "harare cbd":       "harare central",
    "cbd":              "harare central",
    # Bulawayo
    "bulawayo cbd":     "bulawayo central",
    "luveve":           "luveve",
    "nketa":            "nketa",
    "sunning":          "sunninghill",
    # Mutare
    "mutare cbd":       "mutare central",
    # Norton
    "norton":           "norton",
    "johannesburgn":    "johannesburg norton",  # common spider artefact
}

# ── Noise words to strip after expansion ─────────────────────────────────────
_NOISE_WORDS: set[str] = {
    "zimbabwe", "zim", "property", "properties", "house", "home",
    "flat", "apartment", "stand", "plot", "land", "farm",
    "for", "sale", "to", "rent", "lease", "the", "a", "an",
    "bedroom", "bedroomed", "bed", "bath", "garage",
    "furnished", "unfurnished", "brand", "new", "modern",
    "spacious", "beautiful", "prime", "luxury",
}

# ── City names for Zimbabwe ───────────────────────────────────────────────────
_ZIM_CITIES: set[str] = {
    "harare", "bulawayo", "mutare", "gweru", "kwekwe", "kadoma",
    "masvingo", "chinhoyi", "norton", "marondera", "ruwa", "chitungwiza",
    "bindura", "zvishavane", "chegutu", "victoria falls", "kariba",
    "hwange", "beitbridge", "plumtree", "lupane", "chiredzi",
}


# ── Core normalisation ────────────────────────────────────────────────────────

def normalise_address(raw: Optional[str]) -> str:
    """
    Return a cleaned, lowercase, expanded address string suitable for
    fuzzy matching.

    Steps:
      1. Unicode → ASCII (handle curly quotes, em-dashes, etc.)
      2. Lowercase
      3. Remove punctuation (keep hyphens within words)
      4. Expand abbreviations (Rd → road)
      5. Apply suburb aliases
      6. Collapse whitespace
    """
    if not raw:
        return ""

    # 1. Normalise unicode → ASCII (drop accents, curly quotes, em-dashes)
    text = unicodedata.normalize("NFKD", raw)
    text = text.encode("ascii", "ignore").decode("ascii")

    # 2. Lowercase
    text = text.lower().strip()

    # 3. Remove punctuation except hyphens-between-words
    text = re.sub(r"[^\w\s\-]", " ", text)
    # Remove standalone hyphens (not within words)
    text = re.sub(r"(?<!\w)-|-(?!\w)", " ", text)

    # 4. Expand abbreviations (word-boundary aware)
    for pattern, replacement in _ABBREVIATIONS:
        text = re.sub(pattern, replacement + " " if replacement else " ", text, flags=re.I)

    # 5. Apply suburb aliases using word-boundary regex (longest first to avoid substring clobbering)
    for alias, canonical in sorted(SUBURB_ALIASES.items(), key=lambda x: -len(x[0])):
        escaped = re.escape(alias)
        text = re.sub(r"\b" + escaped + r"\b", canonical, text, flags=re.I)

    # 6. Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text


def address_fingerprint(raw: Optional[str]) -> str:
    """
    Return a sorted-token fingerprint of the normalised address.

    Order-independent: "12 Borrowdale Road Harare" and
    "Borrowdale Road, 12, Harare" both produce the same fingerprint.
    Noise words are removed before sorting.
    """
    normalised = normalise_address(raw)
    tokens = [
        t for t in normalised.split()
        if t not in _NOISE_WORDS and len(t) > 1
    ]
    return " ".join(sorted(tokens))


def address_hash(raw: Optional[str]) -> Optional[str]:
    """
    SHA-256 hex digest of the address fingerprint.
    Used as a fast blocking key in the matching engine.
    Returns None if the address is empty.
    """
    fp = address_fingerprint(raw)
    if not fp:
        return None
    return hashlib.sha256(fp.encode()).hexdigest()[:16]


# ── Geohash blocking ──────────────────────────────────────────────────────────

def geo_block(lat: Optional[float], lon: Optional[float], precision: int = 7) -> Optional[str]:
    """
    Encode lat/lon as a geohash of given precision.

    Precision guide (approximate cell size):
      5 → 4.9km × 4.9km   (city-level blocking)
      7 → 153m × 153m     (suburb-level blocking)
      8 → 38m × 19m       (street-level blocking — best for dedup)
      9 → 4.8m × 4.8m     (building-level — use for exact matches)

    Returns None if coordinates are missing or invalid.
    """
    if lat is None or lon is None:
        return None
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None

    # Validate Zimbabwe bounding box: lat -23 to -15, lon 25 to 34
    if not (-23.5 <= lat <= -15.0 and 25.0 <= lon <= 34.0):
        return None

    return _encode_geohash(lat, lon, precision)


def _encode_geohash(lat: float, lon: float, precision: int) -> str:
    """Pure-Python geohash encoder (no external dependency required)."""
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_min, lat_max = -90.0, 90.0
    lon_min, lon_max = -180.0, 180.0
    geohash = []
    bits = [16, 8, 4, 2, 1]
    bit_idx = 0
    ch = 0
    even = True

    while len(geohash) < precision:
        if even:
            mid = (lon_min + lon_max) / 2
            if lon >= mid:
                ch |= bits[bit_idx]
                lon_min = mid
            else:
                lon_max = mid
        else:
            mid = (lat_min + lat_max) / 2
            if lat >= mid:
                ch |= bits[bit_idx]
                lat_min = mid
            else:
                lat_max = mid

        even = not even
        if bit_idx < 4:
            bit_idx += 1
        else:
            geohash.append(BASE32[ch])
            bit_idx = 0
            ch = 0

    return "".join(geohash)


# ── Phone normalisation ───────────────────────────────────────────────────────

def normalise_phone(raw: Optional[str]) -> Optional[str]:
    """
    Normalise Zimbabwe phone numbers to +263XXXXXXXXX format.

    Examples:
        "0771 234 567"  → "+263771234567"
        "+263 71 234 567" → "+263771234567"
        "263771234567"  → "+263771234567"
    """
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("263"):
        return f"+{digits}"
    if digits.startswith("0") and len(digits) >= 10:
        return f"+263{digits[1:]}"
    if len(digits) >= 9:
        return f"+263{digits}"
    return None


# ── Suburb extraction from title ─────────────────────────────────────────────

def extract_location_from_title(title: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Parse suburb and city from titles like:
      "4 Bed House for Sale in Borrowdale, Harare"
      "Stand for Sale – Ruwa, Harare"
      "Prime Flat, Avondale West, Harare"

    Returns (suburb, city) or (None, None).
    """
    if not title:
        return None, None

    # Pattern: "in Suburb, City" or "– Suburb, City"
    m = re.search(
        r"(?:in|–|-|,)\s+([A-Za-z][A-Za-z\s]{2,40}),\s*([A-Za-z][A-Za-z\s]{2,30})$",
        title, re.I
    )
    if m:
        suburb_raw = m.group(1).strip()
        city_raw   = m.group(2).strip()
        city_clean = city_raw.lower()
        city = city_raw.title() if city_clean in _ZIM_CITIES else city_raw.title()
        return suburb_raw.title(), city

    return None, None

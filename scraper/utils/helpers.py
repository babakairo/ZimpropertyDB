"""
Shared utility functions used across all spiders.
"""
import re
import hashlib
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, unquote


# ─── Price Parsing ──────────────────────────────────────────────────────────────

_PRICE_PATTERN = re.compile(r"[\d\s,\.]+")

def parse_price(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """
    Extract numeric price and currency code from a raw string.

    Examples:
        "USD 250,000"  → (250000.0, "USD")
        "ZWL 5 000 000" → (5000000.0, "ZWL")
        "$120 000"      → (120000.0, "USD")
        "POA"           → (None, None)

    Returns:
        (price_float, currency_code) or (None, None)
    """
    if not raw:
        return None, None

    raw = raw.strip().upper()

    currency = None
    if "ZIG" in raw:
        currency = "ZIG"
    elif "ZWL" in raw or "RTGS" in raw or "BOND" in raw:
        currency = "ZWL"
    elif "USD" in raw or "$" in raw:
        currency = "USD"

    # Extract one or more numeric components (supports ranges)
    number_parts = re.findall(r"\d[\d\s,\.]*", raw)
    values: list[float] = []
    for part in number_parts:
        digits = re.sub(r"[^\d]", "", part)
        if digits:
            values.append(float(digits))

    if not values:
        return None, currency

    # Range support: "120k - 150k", "US$120,000 to 150,000"
    # For ranges we take midpoint to keep one numeric price for analytics.
    is_range = bool(re.search(r"\bTO\b|\-|–|—", raw)) and len(values) >= 2
    if is_range:
        lo = min(values[0], values[1])
        hi = max(values[0], values[1])
        return round((lo + hi) / 2.0, 2), currency

    return values[0], currency


def parse_size(raw: Optional[str]) -> Optional[float]:
    """
    Parse property size to square metres.

    Examples:
        "1 500 m²"  → 1500.0
        "0.25 ha"   → 2500.0    (1 ha = 10 000 m²)
        "3 000sqft" → 278.7
    """
    if not raw:
        return None
    raw = raw.strip().lower().replace(",", "").replace(" ", "")
    num_match = re.search(r"[\d\.]+", raw)
    if not num_match:
        return None
    value = float(num_match.group())

    if "ha" in raw or "hectare" in raw:
        return round(value * 10_000, 2)
    if "sqft" in raw or "ft²" in raw or "sq ft" in raw.replace(" ", ""):
        return round(value * 0.0929, 2)
    return value  # assume m²


def parse_int(raw: Optional[str]) -> Optional[int]:
    """Extract the first integer found in a string."""
    if not raw:
        return None
    m = re.search(r"\d+", str(raw))
    return int(m.group()) if m else None


# ─── Location Helpers ─────────────────────────────────────────────────────────

# Known Zimbabwe cities for normalisation
ZIMBABWE_CITIES = {
    "harare", "bulawayo", "mutare", "gweru", "kwekwe", "kadoma",
    "masvingo", "chinhoyi", "norton", "marondera", "ruwa", "chitungwiza",
    "bindura", "zvishavane", "chegutu", "victoria falls", "kariba",
    "hwange", "beitbridge", "plumtree", "lupane", "chiredzi",
}

_UNKNOWN_LOCATION_TOKENS = {
    "unknown", "unk", "n/a", "na", "none", "null", "other", "-", "",
}

SUBURB_CITY_MAP = {
    "avondale": "Harare",
    "ballantyne park": "Harare",
    "belgravia": "Harare",
    "borrowdale": "Harare",
    "burnside": "Bulawayo",
    "greendale": "Harare",
    "hatfield": "Harare",
    "mount pleasant": "Harare",
    "mt pleasant": "Harare",
    "queens park": "Bulawayo",
    "queens park east": "Bulawayo",
    "ruwa": "Ruwa",
    "stapleford gardens": "Harare",
    "twentydales estate": "Ruwa",
}

_PROPERTY_WORDS = {
    "house", "home", "land", "stand", "plot", "farm", "flat", "apartment",
    "cluster", "townhouse", "commercial", "office", "retail", "industrial",
    "warehouse", "sale", "rent", "lease", "letting", "property", "estate",
}


def _norm_location_key(raw: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (raw or "").strip().lower()).strip()


def clean_location_value(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(raw)).strip(" ,|-\t\r\n")
    if not cleaned:
        return None
    if cleaned.lower() in _UNKNOWN_LOCATION_TOKENS:
        return None
    return cleaned


def normalise_email(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    cleaned = str(raw).replace("mailto:", "").strip().lower()
    match = re.search(r"[\w.+-]+@[\w.-]+\.\w{2,}", cleaned)
    return match.group(0) if match else None


def extract_first_email(*values: Optional[str]) -> Optional[str]:
    for value in values:
        email = normalise_email(value)
        if email:
            return email
    return None


def extract_first_phone(*values: Optional[str]) -> Optional[str]:
    for value in values:
        phone = normalise_phone(value)
        if phone:
            return phone
    return None


def clean_agent_name(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    cleaned = re.sub(r"\s+", " ", str(raw)).strip(" ,|-\t\r\n")
    cleaned = re.sub(r"^(call|contact|agent|listed by)\s*:?\s*", "", cleaned, flags=re.I)
    if not cleaned or cleaned.lower() in _UNKNOWN_LOCATION_TOKENS:
        return None
    if len(cleaned) > 80:
        cleaned = cleaned[:80].strip()
    return cleaned.title()


def extract_agent_name(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if not value:
            continue
        text = re.sub(r"<[^>]+>", " ", str(value))
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        match = re.search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b", text)
        if match:
            return clean_agent_name(match.group(1))
        cleaned = clean_agent_name(text)
        if cleaned and len(cleaned.split()) <= 5:
            return cleaned
    return None


def coalesce_agent_fields(
    agent_name: Optional[str],
    agent_phone: Optional[str],
    agent_email: Optional[str],
    agency_name: Optional[str],
    *,
    fallback_text: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    agency_name = clean_agent_name(agency_name)
    agent_name = clean_agent_name(agent_name) or extract_agent_name(fallback_text)
    agent_phone = extract_first_phone(agent_phone, fallback_text)
    agent_email = extract_first_email(agent_email, fallback_text)

    if not agent_name:
        agent_name = agency_name
    if not agency_name and agent_name:
        agency_name = agent_name

    return agent_name, agent_phone, agent_email, agency_name

def normalise_city(raw: Optional[str]) -> Optional[str]:
    """Lowercase & strip; return None if not a known Zimbabwean city."""
    raw = clean_location_value(raw)
    if not raw:
        return None
    cleaned = raw.strip().lower()
    # Exact or startswith match
    for city in ZIMBABWE_CITIES:
        if cleaned == city or cleaned.startswith(city):
            return city.title()
    return raw.strip().title()


def infer_city_from_suburb(suburb: Optional[str]) -> Optional[str]:
    key = _norm_location_key(suburb)
    if not key:
        return None
    for suburb_name, city_name in SUBURB_CITY_MAP.items():
        if key == suburb_name or key.startswith(suburb_name) or suburb_name in key:
            return city_name
    return None


def is_known_zimbabwe_city(raw: Optional[str]) -> bool:
    key = _norm_location_key(raw)
    return key in ZIMBABWE_CITIES


def extract_location_from_text(text: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_location_value(text)
    if not text:
        return None, None

    compact = re.sub(r"\s+", " ", text)

    m = re.search(
        r"(?:in|at|,|–|-)\s*([A-Za-z][A-Za-z\s]{2,40}),\s*([A-Za-z][A-Za-z\s]{2,30})(?:,\s*Zimbabwe)?$",
        compact,
        re.I,
    )
    if m:
        suburb = clean_location_value(m.group(1))
        city = normalise_city(m.group(2))
        return suburb.title() if suburb else None, city

    m = re.search(r"\b([A-Za-z][A-Za-z\s]{2,40}),\s*([A-Za-z][A-Za-z\s]{2,30}),\s*Zimbabwe\b", compact, re.I)
    if m:
        suburb = clean_location_value(m.group(1))
        city = normalise_city(m.group(2))
        return suburb.title() if suburb else None, city

    for suburb_name, city_name in sorted(SUBURB_CITY_MAP.items(), key=lambda x: -len(x[0])):
        if re.search(rf"\b{re.escape(suburb_name)}\b", compact, re.I):
            return suburb_name.title(), city_name

    m = re.match(r"^([A-Za-z][A-Za-z\s]{2,40})\s+(?:house|home|land|stand|plot|farm|flat|apartment|cluster|townhouse|commercial|office)\b", compact, re.I)
    if m:
        suburb = clean_location_value(m.group(1))
        return suburb.title() if suburb else None, infer_city_from_suburb(suburb)

    m = re.search(r"\bfor\s+(?:sale|rent|lease)[:\-\s]+([A-Za-z][A-Za-z\s]{2,40})$", compact, re.I)
    if m:
        suburb = clean_location_value(m.group(1))
        return suburb.title() if suburb else None, infer_city_from_suburb(suburb)

    return None, None


def extract_location_from_url(url: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None

    path = unquote(urlparse(url).path or "")
    segments = [re.sub(r"[-_]+", " ", s).strip() for s in path.split("/") if s.strip()]
    city = None
    suburb = None

    for idx, seg in enumerate(segments):
        seg_clean = _norm_location_key(seg)
        if seg_clean in ZIMBABWE_CITIES:
            city = normalise_city(seg_clean)
            if idx + 1 < len(segments):
                next_seg = clean_location_value(segments[idx + 1])
                if next_seg and _norm_location_key(next_seg) not in _PROPERTY_WORDS:
                    suburb = next_seg.title()
            break

    if not suburb:
        for seg in segments:
            seg_clean = _norm_location_key(seg)
            for suburb_name, city_name in sorted(SUBURB_CITY_MAP.items(), key=lambda x: -len(x[0])):
                if suburb_name == seg_clean:
                    suburb = suburb_name.title()
                    city = city or city_name
                    break
            if suburb:
                break

    return suburb, city


def enrich_location_fields(
    suburb: Optional[str],
    city: Optional[str],
    *,
    title: Optional[str] = None,
    address: Optional[str] = None,
    listing_url: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    suburb = clean_location_value(suburb)
    city = normalise_city(city)
    address = clean_location_value(address)

    if suburb and not city:
        city = infer_city_from_suburb(suburb)

    if not suburb or not city:
        for text in (address, title):
            guess_suburb, guess_city = extract_location_from_text(text)
            suburb = suburb or guess_suburb
            city = city or guess_city
            if suburb and city:
                break

    if not suburb or not city:
        url_suburb, url_city = extract_location_from_url(listing_url)
        suburb = suburb or url_suburb
        city = city or url_city

    if suburb and city and _norm_location_key(suburb) == _norm_location_key(city):
        alt_suburb, alt_city = extract_location_from_text(title)
        if not alt_suburb:
            alt_suburb, alt_city = extract_location_from_url(listing_url)
        if alt_suburb and _norm_location_key(alt_suburb) != _norm_location_key(city):
            suburb = alt_suburb
            city = alt_city or city

    if suburb:
        suburb = suburb.title()
    inferred_city = infer_city_from_suburb(suburb) if suburb else None
    if suburb and (
        not city
        or _norm_location_key(city) == _norm_location_key(suburb)
        or (inferred_city and not is_known_zimbabwe_city(city))
    ):
        city = inferred_city or city

    if not address:
        address = ", ".join([p for p in [suburb, city] if p]) or title or None

    return suburb, city, address


# ─── Listing ID ──────────────────────────────────────────────────────────────

def make_listing_id(source: str, listing_url: str) -> str:
    """Create a stable, unique ID for a listing."""
    key = f"{source}::{listing_url.split('?')[0].rstrip('/')}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# ─── Timestamps ──────────────────────────────────────────────────────────────

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── Property Type Normalisation ─────────────────────────────────────────────

_TYPE_MAP = {
    # ── Residential ────────────────────────────────────────────
    "house":             "house",
    "home":              "house",
    "residential":       "house",   # added P0 Fix 4
    "cottage":           "house",   # added P0 Fix 4
    "villa":             "house",
    "cluster":           "house",
    "townhouse complex": "townhouse",  # must come before "townhouse"
    "townhouse":         "townhouse",
    # ── Flat / apartment ───────────────────────────────────────
    "flat":              "flat",
    "apartment":         "flat",
    "studio":            "flat",
    "bedsitter":         "flat",
    "room":              "flat",    # added P0 Fix 4 (room/rooms)
    "bachelor":          "flat",    # added P0 Fix 4
    # ── Land ───────────────────────────────────────────────────
    "vacant land":       "land",    # added P0 Fix 4 (must come before "land")
    "vacant stand":      "land",    # added P0 Fix 4
    "land":              "land",
    "stand":             "land",
    "plot":              "land",
    "erf":               "land",
    # ── Farm / agricultural ────────────────────────────────────
    "farm":              "farm",
    "agricultural":      "farm",    # added P0 Fix 4
    "smallholding":      "farm",    # added P0 Fix 4
    # ── Commercial ─────────────────────────────────────────────
    "commercial":        "commercial",
    "office":            "commercial",
    "retail":            "commercial",
    "shop":              "commercial",  # covers "shops" via substring
    "warehouse":         "commercial",
    "industrial":        "commercial",
    "factory":           "commercial",  # added P0 Fix 4
    "workshop":          "commercial",  # added P0 Fix 4
    "property development": "commercial",  # added P0 Fix 4
    "hospitality":       "commercial",  # covers "hospitality bnb" etc.
    "student accommodation": "flat",    # added P0 Fix 4
    # ── NULL sentinel — unknown/invalid values ─────────────────
    # "unknown" is handled in the function body below (returns None)
}

# Values that should produce NULL rather than a raw passthrough
_TYPE_NULL_SENTINEL = {"unknown", "unk", "n/a", "na", "none", "null", "other", ""}


def normalise_property_type(raw: Optional[str]) -> Optional[str]:
    """
    Normalise a raw property type string to one of:
      house | townhouse | flat | land | farm | commercial | None

    - Input is lowercased before matching.
    - Unknown/invalid values return None (not stored as "unknown").
    - Any unmapped type that doesn't match a sentinel is returned as-is
      in lowercase so it is visible for future mapping.
    """
    if not raw:
        return None
    key = raw.strip().lower()
    if key in _TYPE_NULL_SENTINEL:
        return None
    # Longer patterns first (dict is insertion-ordered in Python 3.7+)
    for pattern, normalised in _TYPE_MAP.items():
        if pattern in key:
            return normalised
    # Passthrough for genuinely unmapped types (stored as-is, flagged by DQ score)
    return key


def normalise_listing_type(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    key = raw.strip().lower()
    if any(w in key for w in ["rent", "lease", "to let", "let"]):
        return "rent"
    if any(w in key for w in ["sale", "sell", "buy", "purchase"]):
        return "sale"
    return None

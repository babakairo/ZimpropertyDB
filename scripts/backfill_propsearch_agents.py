"""
backfill_propsearch_agents.py
─────────────────────────────
One-off backfill: fetches agent contact details from the propsearch.co.zw
API for the 1,043 existing RAW records that have null agent_name/phone/email.

Steps:
  1. Paginate through /api/properties to build {listing_id: first_userId} map
  2. Fetch unique agent profiles from /api/agents/{userId}
  3. UPDATE RAW.ZW_PROPERTY_LISTINGS with agent_name, agent_phone, agent_email
  4. Re-run 07_clean_and_segment.sql so STAGING picks up the new data

Run:
    python scripts/backfill_propsearch_agents.py
"""
import os, re, sys, json, time, hashlib, logging, urllib.request
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_agents")

BASE_URL  = "https://propsearch.co.zw"
SOURCE    = "propsearch.co.zw"
PER_PAGE  = 50
DELAY_SEC = 0.5   # polite delay between API calls

HEADERS = {
    "Accept": "application/json, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{BASE_URL}/property-for-sale",
    "User-Agent": "Mozilla/5.0 (compatible; ZimPropertyBot/1.0)",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_json(url: str) -> dict | None:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning(f"Fetch failed {url}: {e}")
        return None


def make_listing_id(listing_url: str) -> str:
    key = f"{SOURCE}::{listing_url.split('?')[0].rstrip('/')}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def extract_phone(profile: dict) -> str | None:
    phone = None
    for p in profile.get("userPhones") or []:
        raw = f"{p.get('dialingCode', '')}{p.get('phoneNumber', '')}".strip()
        if raw:
            if p.get("use", "").lower() == "business":
                return raw
            phone = phone or raw
    return phone


def extract_email(profile: dict) -> str | None:
    for e in profile.get("userEmails") or []:
        addr = (e.get("email") or "").strip().lower()
        if addr and "@" in addr:
            return addr
    return None


def extract_name(profile: dict) -> str | None:
    first = (profile.get("firstName") or "").strip()
    last  = (profile.get("lastName")  or "").strip()
    name  = f"{first} {last}".strip()
    return name or None


# ── Step 1: Paginate API to build {listing_id: user_id} map ──────────────────

def collect_listing_user_ids() -> dict[str, int]:
    """Return {listing_id: first_userId} for all propsearch listings."""
    mapping: dict[str, int] = {}
    page = 1
    while True:
        url = f"{BASE_URL}/api/properties?currentPage={page}&perPage={PER_PAGE}"
        data = fetch_json(url)
        if not data:
            break
        rows = data.get("data") or []
        total_pages = int(data.get("totalPages") or 1)
        for raw in rows:
            ref = (
                raw.get("propDeskRef")
                or raw.get("internalRef")
                or str(raw.get("listingId") or "")
            ).strip()
            if not ref:
                continue
            listing_url = f"{BASE_URL}/property/{ref}"
            lid = make_listing_id(listing_url)
            agents = raw.get("agents") or []
            if agents and isinstance(agents[0], dict):
                user_id = agents[0].get("userId")
                if user_id:
                    mapping[lid] = user_id
        log.info(f"  Page {page}/{total_pages}: collected {len(rows)} listings")
        if page >= total_pages:
            break
        page += 1
        time.sleep(DELAY_SEC)
    return mapping


# ── Step 2: Fetch unique agent profiles ──────────────────────────────────────

def fetch_agent_profiles(user_ids: set[int]) -> dict[int, dict]:
    """Return {user_id: {name, phone, email}} for each userId."""
    profiles: dict[int, dict] = {}
    for i, uid in enumerate(sorted(user_ids), 1):
        data = fetch_json(f"{BASE_URL}/api/agents/{uid}")
        if data:
            profiles[uid] = {
                "name":  extract_name(data),
                "phone": extract_phone(data),
                "email": extract_email(data),
            }
        if i % 10 == 0:
            log.info(f"  Fetched {i}/{len(user_ids)} agent profiles")
        time.sleep(DELAY_SEC)
    return profiles


# ── Step 3: Update RAW table ──────────────────────────────────────────────────

def update_raw_table(conn, listing_user_map: dict, profiles: dict) -> int:
    cur = conn.cursor()
    updated = 0
    batch = []
    for lid, uid in listing_user_map.items():
        profile = profiles.get(uid)
        if not profile:
            continue
        name  = profile.get("name")
        phone = profile.get("phone")
        email = profile.get("email")
        if not any([name, phone, email]):
            continue
        batch.append((name, phone, email, lid))

    if batch:
        cur.executemany("""
            UPDATE ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            SET agent_name  = COALESCE(%s, agent_name),
                agent_phone = COALESCE(%s, agent_phone),
                agent_email = COALESCE(%s, agent_email)
            WHERE listing_id = %s
              AND source = 'propsearch.co.zw'
        """, batch)
        conn.commit()
        updated = len(batch)

    cur.close()
    return updated


# ── Step 4: Re-run staging SQL ────────────────────────────────────────────────

def refresh_staging(conn):
    sql_path = Path(__file__).parent.parent / "snowflake" / "07_clean_and_segment.sql"
    with open(sql_path, encoding="utf-8") as f:
        sql_raw = f.read()
    sql_no_comments = re.sub(r"--[^\n]*", "", sql_raw)
    statements = [s.strip() for s in sql_no_comments.split(";") if s.strip() and len(s.strip()) > 5]
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    log.info("STAGING.CLEANED_PROPERTY_LISTINGS refreshed")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

    # Before
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN agent_name IS NULL THEN 1 ELSE 0 END) as null_agent
        FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
        WHERE source = 'propsearch.co.zw'
    """)
    r = cur.fetchone()
    log.info(f"BEFORE — propsearch RAW: {r[0]} records, {r[1]} null agent_name ({round(r[1]/r[0]*100,1)}%)")

    log.info("Step 1: Collecting listing → userId mappings from API ...")
    listing_user_map = collect_listing_user_ids()
    log.info(f"  Found {len(listing_user_map)} listings with agent userIds")

    unique_user_ids = set(listing_user_map.values())
    log.info(f"Step 2: Fetching {len(unique_user_ids)} unique agent profiles ...")
    profiles = fetch_agent_profiles(unique_user_ids)
    log.info(f"  Got profiles for {len(profiles)} agents")

    log.info("Step 3: Updating RAW table ...")
    updated = update_raw_table(conn, listing_user_map, profiles)
    log.info(f"  Updated {updated} rows in RAW")

    # After RAW update
    cur.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN agent_name IS NULL THEN 1 ELSE 0 END) as null_agent
        FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
        WHERE source = 'propsearch.co.zw'
    """)
    r = cur.fetchone()
    log.info(f"AFTER RAW update — propsearch: {r[0]} records, {r[1]} null agent_name ({round(r[1]/r[0]*100,1)}%)")

    log.info("Step 4: Refreshing STAGING ...")
    refresh_staging(conn)

    # After STAGING refresh
    cur.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN agent_name IS NULL THEN 1 ELSE 0 END) as null_agent
        FROM ZIM_PROPERTY_DB.STAGING.CLEANED_PROPERTY_LISTINGS
        WHERE source = 'propsearch.co.zw'
    """)
    r = cur.fetchone()
    log.info(f"AFTER STAGING refresh — propsearch: {r[0]} records, {r[1]} null agent_name ({round(r[1]/r[0]*100,1)}%)")

    cur.close()
    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()

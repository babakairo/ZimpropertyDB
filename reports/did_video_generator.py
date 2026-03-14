"""
reports/did_video_generator.py
Generates 7 daily presenter videos using ElevenLabs (cloned voice) + D-ID Clips API.
Each video features a professional AI presenter delivering Zimbabwe property market data.

Usage:
    python reports/did_video_generator.py            # generate today's day
    python reports/did_video_generator.py --all      # generate all 7 days
    python reports/did_video_generator.py --day thursday
"""

import os
import sys
import time
import argparse
import requests
import snowflake.connector
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

# ── Credentials ────────────────────────────────────────────────────────────────
EL_API_KEY   = os.getenv("ELEVENLABS_API_KEY")
EL_VOICE_ID  = os.getenv("ELEVENLABS_VOICE_ID")
DID_API_KEY  = os.getenv("DID_API_KEY")

# ── Output ─────────────────────────────────────────────────────────────────────
TODAY    = date.today().isoformat()
OUT_DIR  = Path(__file__).parent / "output" / "cards" / TODAY / "did"
WEEK     = date.today().strftime("%d %B %Y")

# ── D-ID Presenter — Jaimie (Black man, burgundy t-shirt, white background) ───
# Preview: https://clips-presenters.d-id.com/v2/jaimie/Isfx_UxygI/XABHPKdLtj/preview.mp4
PRESENTER_ID = "v2_public_jaimie@Isfx_UxygI"

DID_HEADERS = {
    "Authorization": f"Basic {DID_API_KEY}",
    "Content-Type":  "application/json",
}


# ── Snowflake data fetch ───────────────────────────────────────────────────────
def fetch_data() -> dict:
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(CASE WHEN LISTING_TYPE='sale' THEN 1 END),
                   COUNT(CASE WHEN LISTING_TYPE='rent' THEN 1 END),
                   COUNT(CASE WHEN PROPERTY_TYPE='land' THEN 1 END)
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE SCRAPED_AT >= DATEADD(day, -7, CURRENT_DATE)
        """)
        row = cur.fetchone()
        sales, rentals, land = (row[0] or 0), (row[1] or 0), (row[2] or 0)

        cur.execute("""
            SELECT suburb_clean, city_clean, growth_3m_pct, avg_price_current_month_usd
            FROM ZIM_PROPERTY_DB.ANALYTICS.SUBURB_PRICE_GROWTH
            WHERE growth_3m_pct IS NOT NULL AND growth_3m_pct > 0
            ORDER BY growth_3m_pct DESC LIMIT 3
        """)
        top_growth = cur.fetchall()

        cur.execute("""
            SELECT suburb_clean, city_clean, MIN(property_price_usd), AVG(property_size_sqm)
            FROM ZIM_PROPERTY_DB.ANALYTICS.LAND_LISTINGS
            WHERE property_price_usd BETWEEN 5000 AND 30000
            GROUP BY suburb_clean, city_clean
            ORDER BY MIN(property_price_usd) ASC LIMIT 3
        """)
        best_value = cur.fetchall()

        cur.execute("""
            SELECT suburb_clean, city_clean, gross_rental_yield_pct, avg_monthly_rent
            FROM ZIM_PROPERTY_DB.ANALYTICS.RENTAL_YIELD_BY_SUBURB
            WHERE gross_rental_yield_pct IS NOT NULL
            ORDER BY gross_rental_yield_pct DESC LIMIT 3
        """)
        top_yield = cur.fetchall()

        cur.execute("""
            SELECT ROUND(AVG(property_price_usd), 0)
            FROM ZIM_PROPERTY_DB.ANALYTICS.HOUSE_SALE_LISTINGS
            WHERE number_of_bedrooms = 3
              AND city_clean ILIKE '%harare%'
              AND property_price_usd > 0
        """)
        three_bed = cur.fetchone()

        cur.execute("""
            SELECT ROUND(AVG(property_price_usd), 0)
            FROM ZIM_PROPERTY_DB.ANALYTICS.HOUSE_SALE_LISTINGS
            WHERE listing_type = 'rent'
              AND city_clean ILIKE '%harare%'
              AND property_price_usd > 0
        """)
        avg_rent = cur.fetchone()

        cur.close()
        conn.close()

        return {
            "sales":        sales,
            "rentals":      rentals,
            "land":         land,
            "total":        sales + rentals + land,
            "top_growth":   top_growth,
            "best_value":   best_value,
            "top_yield":    top_yield,
            "three_bed_avg": int(three_bed[0]) if three_bed and three_bed[0] else 0,
            "avg_rent":     int(avg_rent[0])   if avg_rent  and avg_rent[0]  else 0,
        }
    except Exception as e:
        print(f"[WARN] Snowflake failed: {e} — using placeholder data")
        return {
            "sales": 2100, "rentals": 890, "land": 319, "total": 3309,
            "top_growth":  [("Pomona","Harare",18.2,95000),("Borrowdale","Harare",14.5,185000),("Ruwa","Harare",11.8,45000)],
            "best_value":  [("Spitzkop","Harare",9000,250),("Norton","Norton",10500,300),("Charlotte Brooke","Harare",14000,1000)],
            "top_yield":   [("Avenues","Harare",28.9,917),("Goodhope","Harare",8.2,850),("Avondale","Harare",7.9,750)],
            "three_bed_avg": 241742,
            "avg_rent": 850,
        }


# ── Voiceover scripts ──────────────────────────────────────────────────────────
def script_monday(d):
    g = d["top_growth"]
    lines = ", ".join(f"{r[0]} up {r[2]:.0f} percent" for r in g) if g else "data not available"
    return (
        f"Good morning. Zimbabwe Property Market Pulse for the week of {WEEK}. "
        f"We tracked {d['total']:,} listings this week — {d['sales']:,} for sale, "
        f"{d['rentals']:,} rentals, and {d['land']:,} land stands. "
        f"The fastest-growing suburbs right now are: {lines}. "
        "If you are watching from the diaspora, this is your weekly edge. "
        "Message us on WhatsApp for the full report."
    )

def script_tuesday(d):
    g = d["top_growth"]
    if not g:
        return f"Tuesday Suburb Spotlight for {WEEK}. Market data coming soon. Message us on WhatsApp."
    top = g[0]
    return (
        f"Suburb spotlight. {top[0]} in {top[1]} is the fastest-growing suburb in Zimbabwe right now. "
        f"Prices are up {top[2]:.0f} percent over the last three months, "
        f"with the average property now at {top[3]:,.0f} US dollars. "
        f"If you bought here six months ago, you are already in profit. "
        "The question is — who is buying in the next hot suburb before prices rise? "
        "Message us on WhatsApp to get the full suburb breakdown."
    )

def script_wednesday(d):
    bv = d["best_value"]
    if not bv:
        return "Wednesday Price Check. Message us on WhatsApp for current stand prices."
    lines = ". ".join(f"{r[0]}, {r[1]}, from {r[2]:,.0f} US dollars" for r in bv)
    return (
        "Wednesday price check. Here are the most affordable stands in Zimbabwe right now. "
        f"{lines}. "
        "A stand bought today in the right area can double in value within five years. "
        "You do not need one hundred thousand dollars to start investing in Zimbabwean property. "
        "Message us on WhatsApp for the full list with agent contacts."
    )

def script_thursday(d):
    g = d["top_growth"]
    if not g:
        return "Thursday Investment Signal. Message us on WhatsApp for suburb growth data."
    lines = ". ".join(f"{r[0]}, up {r[2]:.0f} percent" for r in g)
    fastest = g[0][0]
    return (
        f"Investment signal for week of {WEEK}. "
        "These Zimbabwe suburbs are showing the strongest price growth right now. "
        f"{lines}. "
        f"Early buyers in {fastest} are already sitting on paper gains. "
        "The data does not lie — prices move before most people notice. "
        "This is market data only. Always do your own due diligence. "
        "Message us on WhatsApp for the full investment report."
    )

def script_friday(d):
    three = d["three_bed_avg"]
    rent  = d["avg_rent"]
    bv    = d["best_value"]
    parts = [f"Zimbabwe property data fact for the week of {WEEK}."]
    if three:
        parts.append(f"The average three-bedroom house in Harare now costs {three:,} US dollars.")
    if rent:
        parts.append(f"Average monthly rent in Harare is {rent:,} US dollars per month.")
    if bv:
        parts.append(f"The most affordable stands start from {bv[0][2]:,} US dollars in {bv[0][0]}.")
    parts.append("Save this post. Share it with someone who is thinking of investing back home.")
    return " ".join(parts)

def script_saturday(d):
    bv = d["best_value"]
    if not bv:
        return "Diaspora Special. Message us on WhatsApp before you send money home for property."
    lines = ". ".join(f"{r[0]}: from {r[2]:,} US dollars" for r in bv)
    return (
        "This one is for the diaspora. Before you send money home for property, listen to this. "
        "Here are the most affordable verified stand prices in Zimbabwe right now. "
        f"{lines}. "
        "We have seen diaspora buyers overpay by tens of thousands of dollars simply because they had no data. "
        "Our weekly report gives you the numbers before you negotiate. "
        "Message us on WhatsApp. First report is free."
    )

def script_sunday(d):
    g  = d["top_growth"]
    ty = d["top_yield"]
    three = d["three_bed_avg"]
    parts = [f"Zimbabwe Property Weekly Wrap. Week of {WEEK}. Here is what moved this week."]
    if g:
        parts.append(f"Fastest growing suburb: {g[0][0]}, up {g[0][2]:.0f} percent.")
    if ty:
        parts.append(f"Best rental yield: {ty[0][0]} at {ty[0][2]:.0f} percent gross yield.")
    if three:
        parts.append(f"Average three-bedroom house in Harare: {three:,} US dollars.")
    parts.append(
        "That is your weekly Zimbabwe property intelligence. "
        "Follow for weekly data every Monday to Sunday. "
        "Message us on WhatsApp for the full detailed report. First report is free."
    )
    return " ".join(parts)


DAY_SCRIPTS = {
    "monday":    script_monday,
    "tuesday":   script_tuesday,
    "wednesday": script_wednesday,
    "thursday":  script_thursday,
    "friday":    script_friday,
    "saturday":  script_saturday,
    "sunday":    script_sunday,
}


# ── ElevenLabs TTS ─────────────────────────────────────────────────────────────
def generate_audio(text: str) -> bytes:
    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{EL_VOICE_ID}",
        headers={"xi-api-key": EL_API_KEY, "Content-Type": "application/json"},
        json={
            "text": text,
            "model_id": "eleven_multilingual_v2",
            "voice_settings": {"stability": 0.5, "similarity_boost": 0.80, "style": 0.2},
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.content


# ── D-ID Clips ─────────────────────────────────────────────────────────────────
def upload_audio_to_did(audio_bytes: bytes) -> str:
    r = requests.post(
        "https://api.d-id.com/audios",
        headers={"Authorization": f"Basic {DID_API_KEY}"},
        files={"audio": ("voiceover.mp3", audio_bytes, "audio/mpeg")},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["url"]


def create_clip(audio_url: str) -> str:
    r = requests.post(
        "https://api.d-id.com/clips",
        headers=DID_HEADERS,
        json={
            "presenter_id": PRESENTER_ID,
            "script": {
                "type":      "audio",
                "audio_url": audio_url,
            },
            "background": {
                "color": "#0a0e1e",   # deep navy — matches brand
            },
            "config": {
                "fluent":    True,
                "pad_audio": 0.5,
                "result_format": "mp4",
            },
        },
        timeout=30,
    )
    if not r.ok:
        print(f"  [ERROR] D-ID clip creation failed: {r.status_code} {r.text}")
        r.raise_for_status()
    return r.json()["id"]


def poll_clip(clip_id: str, max_wait: int = 300) -> str:
    for i in range(max_wait // 5):
        time.sleep(5)
        r = requests.get(
            f"https://api.d-id.com/clips/{clip_id}",
            headers={"Authorization": f"Basic {DID_API_KEY}"},
            timeout=15,
        )
        status = r.json().get("status", "")
        if i % 6 == 0:  # print every 30s
            print(f"    [{i*5}s] {status}")
        if status == "done":
            return r.json()["result_url"]
        if status == "error":
            raise RuntimeError(f"D-ID clip error: {r.json()}")
    raise TimeoutError("D-ID clip timed out after 5 minutes")


def download_video(url: str, out_path: Path):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    out_path.write_bytes(r.content)


# ── Main generation ────────────────────────────────────────────────────────────
def generate_day(day: str, data: dict):
    print(f"\n[{day.upper()}]")
    slug = f"{day}_{['market_pulse','suburb_spotlight','price_check','investment_signal','data_fact','diaspora_special','weekly_summary'][list(DAY_SCRIPTS.keys()).index(day)]}"
    out_path = OUT_DIR / f"{slug}_did.mp4"

    if out_path.exists():
        print(f"  [SKIP] Already exists: {out_path.name}")
        return

    # 1. Generate voiceover text
    text = DAY_SCRIPTS[day](data)
    print(f"  Script: {text[:80]}...")

    # 2. ElevenLabs audio
    print("  Generating audio (ElevenLabs)...")
    audio = generate_audio(text)
    print(f"  Audio: {len(audio):,} bytes")

    # 3. Upload audio to D-ID
    print("  Uploading audio to D-ID...")
    audio_url = upload_audio_to_did(audio)

    # 4. Create clip
    print("  Creating D-ID clip...")
    clip_id = create_clip(audio_url)
    print(f"  Clip ID: {clip_id} — rendering...")

    # 5. Poll
    result_url = poll_clip(clip_id)

    # 6. Download
    print(f"  Downloading video...")
    download_video(result_url, out_path)
    print(f"  [OK] Saved: {out_path.name} ({out_path.stat().st_size:,} bytes)")


def main():
    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--day", choices=list(DAY_SCRIPTS.keys()))
    grp.add_argument("--all", action="store_true")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching market data from Snowflake...")
    data = fetch_data()
    print(f"  Total listings: {data['total']:,}")

    today_name = date.today().strftime("%A").lower()

    if args.all:
        days = list(DAY_SCRIPTS.keys())
    elif args.day:
        days = [args.day]
    else:
        days = [today_name]

    print(f"\nGenerating {len(days)} D-ID video(s) -> {OUT_DIR}")
    for day in days:
        try:
            generate_day(day, data)
        except Exception as e:
            print(f"  [FAILED] {day}: {e}")

    print(f"\nDone. Videos saved to: {OUT_DIR}")


if __name__ == "__main__":
    main()

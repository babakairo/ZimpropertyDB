"""
reports/image_cards.py
Generates 7 branded social media image cards (1080x1080 PNG) from Snowflake data,
then stitches each into an MP4 video with AI voiceover (Microsoft Edge neural TTS).

Voice: en-ZA-LeahNeural  (South African English — closest to Zimbabwean English)
       Change VOICE below to use a different voice.

Output folder: reports/output/cards/YYYY-MM-DD/
  monday_market_pulse.mp4          (+ .png)
  tuesday_suburb_spotlight.mp4
  wednesday_price_check.mp4
  thursday_investment_signal.mp4
  friday_data_fact.mp4
  saturday_diaspora_special.mp4
  sunday_weekly_summary.mp4

Usage:
    python reports/image_cards.py                  # full run (PNG + MP4 + voiceover)
    python reports/image_cards.py --images-only    # PNGs only, no video
    python reports/image_cards.py --no-voice       # video without voiceover
    python reports/image_cards.py --list-voices    # show all available TTS voices
"""
import os
import sys
import asyncio
import argparse
import textwrap
from datetime import date
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

# ── TTS voice ─────────────────────────────────────────────────────────────────
# South African English neural voice — closest accent to Zimbabwe.
# Run with --list-voices to see all available options.
VOICE = "en-ZA-LeahNeural"

# ── Dimensions & brand colours ────────────────────────────────────────────────
W, H       = 1080, 1080
TW, TH     = 1080, 1920      # TikTok 9:16 dimensions
BG_DIR     = Path(__file__).parent / "assets" / "backgrounds"
C_NAVY     = (27,  58,  92)      # #1B3A5C
C_NAVY_MID = (35,  74,  115)     # slightly lighter header band
C_GOLD     = (201, 168, 76)      # #C9A84C
C_WHITE    = (255, 255, 255)
C_LIGHT    = (234, 240, 246)     # #EAF0F6
C_GREEN    = (39,  174, 96)
C_RED      = (231, 76,  60)
C_GREY     = (149, 165, 166)

WEEK       = date.today().strftime("%d %B %Y")
TODAY      = date.today().isoformat()
OUT_DIR    = Path(__file__).parent / "output" / "cards" / TODAY

FONT_DIR   = Path("C:/Windows/Fonts")

# ── Font loader ───────────────────────────────────────────────────────────────
def _font(size: int, bold=False) -> ImageFont.FreeTypeFont:
    candidates = (
        ["calibrib.ttf", "arialbd.ttf"] if bold
        else ["calibri.ttf", "arial.ttf"]
    )
    for name in candidates:
        p = FONT_DIR / name
        if p.exists():
            return ImageFont.truetype(str(p), size)
    return ImageFont.load_default()


# ── Drawing helpers ───────────────────────────────────────────────────────────
def _load_bg(w: int, h: int) -> Image.Image | None:
    """
    Load a random background image from BG_DIR, resize/crop to fill (w, h),
    apply a dark navy overlay so text stays readable.
    Returns None if no backgrounds found.
    """
    import random
    BG_DIR.mkdir(parents=True, exist_ok=True)
    images = list(BG_DIR.glob("*.jpg")) + list(BG_DIR.glob("*.png"))
    if not images:
        return None
    path = random.choice(images)
    try:
        bg = Image.open(path).convert("RGB")
        # Crop to fill target aspect ratio
        src_ratio = bg.width / bg.height
        tgt_ratio = w / h
        if src_ratio > tgt_ratio:
            new_h = bg.height
            new_w = int(bg.height * tgt_ratio)
        else:
            new_w = bg.width
            new_h = int(bg.width / tgt_ratio)
        left = (bg.width - new_w) // 2
        top  = (bg.height - new_h) // 2
        bg   = bg.crop((left, top, left + new_w, top + new_h))
        bg   = bg.resize((w, h), Image.LANCZOS)
        # Dark charcoal overlay — lets photo colours show naturally, text still readable
        overlay = Image.new("RGB", (w, h), (15, 15, 18))
        bg = Image.blend(bg, overlay, alpha=0.58)
        return bg
    except Exception:
        return None


def _new_canvas(w: int = W, h: int = H, use_bg: bool = False):
    """Create canvas — with optional photo background."""
    if use_bg:
        bg = _load_bg(w, h)
        if bg:
            img  = bg.copy()
            draw = ImageDraw.Draw(img)
            return img, draw
    img  = Image.new("RGB", (w, h), C_NAVY)
    draw = ImageDraw.Draw(img)
    return img, draw


def _gold_bar(draw, y1=0, y2=8):
    draw.rectangle([(0, y1), (W, y2)], fill=C_GOLD)


def _footer(draw):
    """Bottom brand strip."""
    draw.rectangle([(0, H - 80), (W, H)], fill=C_NAVY_MID)
    draw.rectangle([(0, H - 82), (W, H - 80)], fill=C_GOLD)
    draw.text((W // 2, H - 46),
              "Zimbabwe Property Intelligence  |  wa.me/447459920895",
              font=_font(22), fill=C_GOLD, anchor="mm")


def _header(draw, title: str, subtitle: str = ""):
    draw.rectangle([(0, 0), (W, 140)], fill=C_NAVY_MID)
    _gold_bar(draw, 0, 8)
    draw.text((54, 46), title,    font=_font(44, bold=True), fill=C_WHITE)
    if subtitle:
        draw.text((54, 104), subtitle, font=_font(24),           fill=C_GOLD)


def _wrapped_text(draw, text: str, x: int, y: int, width_chars: int,
                  font=None, fill=C_WHITE, line_spacing=10) -> int:
    """Draw wrapped text, return y after last line."""
    font = font or _font(28)
    for line in textwrap.wrap(text, width=width_chars):
        draw.text((x, y), line, font=font, fill=fill)
        bbox = font.getbbox(line)
        y += (bbox[3] - bbox[1]) + line_spacing
    return y


def _bullet_table(draw, rows: list[tuple], x: int, y_start: int,
                  col_widths=(420, 200), row_h=52,
                  label_font=None, value_font=None) -> int:
    """Two-column key/value table with alternating row fills."""
    label_font = label_font or _font(28)
    value_font = value_font or _font(28, bold=True)
    img_draw_ref = draw  # alias for clarity
    for i, (label, value) in enumerate(rows):
        ry = y_start + i * row_h
        fill = (*C_NAVY_MID, 180) if i % 2 else (*C_NAVY, 0)
        img_draw_ref.rectangle(
            [(x - 14, ry - 6), (x + col_widths[0] + col_widths[1] + 14, ry + row_h - 10)],
            fill=C_NAVY_MID if i % 2 else C_NAVY,
        )
        img_draw_ref.text((x, ry), str(label), font=label_font, fill=C_LIGHT)
        img_draw_ref.text((x + col_widths[0], ry), str(value), font=value_font, fill=C_GOLD)
    return y_start + len(rows) * row_h


def _divider(draw, y: int):
    draw.line([(54, y), (W - 54, y)], fill=C_GOLD, width=2)


# ── Card builders ─────────────────────────────────────────────────────────────

def card_monday(data: dict) -> Image.Image:
    img, draw = _new_canvas(use_bg=True)
    _header(draw, "MARKET PULSE", f"Week of {WEEK}")

    stands = data.get("top5_stands", [])
    totals = data.get("totals", (0, 0, 0))

    draw.text((54, 170), "HARARE STAND PRICES THIS WEEK",
              font=_font(26, bold=True), fill=C_GOLD)

    rows = [(r[0], f"${r[1]:,.0f}/sqm") for r in stands]
    y = _bullet_table(draw, rows, 54, 210, col_widths=(500, 200))

    _divider(draw, y + 20)

    draw.text((54, y + 40),
              f"Tracking {totals[0]:,} properties  •  {totals[1]} suburbs  •  {totals[2]} cities",
              font=_font(26), fill=C_GREY)

    if stands:
        top    = stands[0]
        fair   = top[1] * 500
        draw.text((54, y + 90),
                  f"Fair price for 500sqm in {top[0]}:  ${fair:,.0f}",
                  font=_font(30, bold=True), fill=C_WHITE)

    _divider(draw, y + 150)
    draw.text((54, y + 170), "Know the numbers before you negotiate.",
              font=_font(30, bold=True), fill=C_WHITE)
    draw.text((54, y + 220), "#ZimbabweProperty  #HarareProperty  #ZimDiaspora",
              font=_font(22), fill=C_GREY)
    _footer(draw)
    return img


def card_tuesday(data: dict) -> Image.Image:
    img, draw = _new_canvas(use_bg=True)
    stands = data.get("top5_stands", [])
    name   = stands[0][0] if stands else "—"
    ppsqm  = stands[0][1] if stands else 0

    _header(draw, f"SUBURB SPOTLIGHT", name.upper())

    rows = [
        ("Average stand price",   f"${ppsqm:,.0f}/sqm"),
        ("500sqm stand",          f"~${ppsqm * 500:,.0f}"),
        ("750sqm stand",          f"~${ppsqm * 750:,.0f}"),
        ("1,000sqm stand",        f"~${ppsqm * 1000:,.0f}"),
    ]
    y = _bullet_table(draw, rows, 54, 200, col_widths=(480, 250))

    _divider(draw, y + 30)
    y = _wrapped_text(
        draw,
        f"If you are quoted more than these figures for a stand in {name}, "
        f"ask your agent to justify the premium.",
        54, y + 60, 44, font=_font(28), fill=C_LIGHT, line_spacing=12,
    )
    draw.text((54, y + 30),
              f'DM us "{name.upper()}" for the full suburb report.',
              font=_font(28, bold=True), fill=C_GOLD)
    draw.text((54, y + 80), "#StandPrices  #ZimProperty",
              font=_font(22), fill=C_GREY)
    _footer(draw)
    return img


def card_wednesday(data: dict) -> Image.Image:
    img, draw = _new_canvas(use_bg=True)
    best  = data.get("best_value", [])
    _header(draw, "PRICE CHECK", "Overpriced or Fair?")

    draw.text((54, 170), "MOST AFFORDABLE STANDS — HARARE",
              font=_font(26, bold=True), fill=C_GOLD)

    rows = [(r[0], f"${r[1]:,.0f}/sqm") for r in best]
    y = _bullet_table(draw, rows, 54, 210)

    _divider(draw, y + 20)
    draw.text((54, y + 44), "HOW TO USE THIS:", font=_font(28, bold=True), fill=C_GOLD)
    draw.text((54, y + 90),
              "Stand size (sqm)  x  price per sqm  =  fair market value",
              font=_font(26), fill=C_WHITE)

    if best:
        bv_name  = best[0][0]
        bv_ppsqm = best[0][1]
        draw.text((54, y + 146),
                  f"Example: 500sqm in {bv_name}  =  ${bv_ppsqm * 500:,.0f}",
                  font=_font(30, bold=True), fill=C_WHITE)

    _divider(draw, y + 210)
    draw.text((54, y + 230), "If quoted more — negotiate hard or walk away.",
              font=_font(30, bold=True), fill=C_RED)
    draw.text((54, y + 280), "#KnowBeforeYouBuy  #PropertyZimbabwe  #ZimDiaspora",
              font=_font(22), fill=C_GREY)
    _footer(draw)
    return img


def card_thursday(data: dict) -> Image.Image:
    img, draw = _new_canvas(use_bg=True)
    growth = data.get("top_growth", [])
    _header(draw, "INVESTMENT SIGNAL", "Rising Suburbs — 6 Month Trend")

    draw.text((54, 170), "FASTEST GROWING SUBURBS",
              font=_font(26, bold=True), fill=C_GOLD)

    rows = [(r[0], f"+{r[1]:.1f}%") for r in growth]
    y = _bullet_table(draw, rows, 54, 210, col_widths=(500, 180))

    # Colour the pct values green
    for i, r in enumerate(growth):
        ry = 210 + i * 52
        draw.text((54 + 500, ry), f"+{r[1]:.1f}%",
                  font=_font(28, bold=True), fill=C_GREEN)

    _divider(draw, y + 20)
    _wrapped_text(
        draw,
        "Early buyers in these suburbs are already sitting on paper gains. "
        "Those who waited are paying more today.",
        54, y + 50, 44, font=_font(28), fill=C_LIGHT, line_spacing=12,
    )
    draw.text((54, y + 160),
              "Data only. Always do your own due diligence.",
              font=_font(24), fill=C_GREY)
    draw.text((54, y + 200), "#ZimPropertyInvestment  #HarareProperty  #ZimDiaspora",
              font=_font(22), fill=C_GREY)
    _footer(draw)
    return img


def card_friday(data: dict) -> Image.Image:
    img, draw = _new_canvas(use_bg=True)
    three_bed = data.get("three_bed_avg")
    avg_rent  = data.get("avg_rent")
    best      = data.get("best_value", [])
    top5      = data.get("top5_stands", [])

    _header(draw, "DATA FACTS", f"Week of {WEEK}")

    rows = [
        ("Avg 3-bed house (Harare)",    f"${three_bed:,.0f}" if three_bed else "—"),
        ("Avg monthly rent (Harare)",   f"${avg_rent:,.0f}/mo" if avg_rent else "—"),
        ("Best value stands",           f"{best[0][0]}  ${best[0][1]:,.0f}/sqm" if best else "—"),
        ("Most expensive stands",       f"{top5[0][0]}  ${top5[0][1]:,.0f}/sqm" if top5 else "—"),
    ]
    y = _bullet_table(draw, rows, 54, 200, col_widths=(460, 320))

    _divider(draw, y + 20)
    draw.text((54, y + 50),
              "Know which suburb matches your budget.",
              font=_font(30, bold=True), fill=C_WHITE)
    draw.text((54, y + 100), "Save this post — share it to protect someone.",
              font=_font(26), fill=C_LIGHT)
    draw.text((54, y + 150), "#ZimbabweProperty  #PropertyFacts  #HarareHouses",
              font=_font(22), fill=C_GREY)
    _footer(draw)
    return img


def card_saturday(data: dict) -> Image.Image:
    img, draw = _new_canvas(use_bg=True)
    top5 = data.get("top5_stands", [])
    _header(draw, "DIASPORA SPECIAL", "Buying Property from Abroad?")

    draw.text((54, 170), "THIS WEEK'S HARARE STAND BENCHMARKS",
              font=_font(26, bold=True), fill=C_GOLD)

    rows = [(r[0], f"${r[1]:,.0f}/sqm") for r in top5[:4]]
    y = _bullet_table(draw, rows, 54, 210)

    _divider(draw, y + 20)
    draw.text((54, y + 50), "Your protection formula:",
              font=_font(28, bold=True), fill=C_GOLD)
    draw.text((54, y + 100),
              "Stand size  x  price/sqm  =  fair market value",
              font=_font(28), fill=C_WHITE)
    draw.text((54, y + 150),
              "Quoted significantly more?  Walk away.",
              font=_font(28, bold=True), fill=C_RED)

    _divider(draw, y + 210)
    _wrapped_text(
        draw,
        "Diaspora buyers overpay by $10,000–$50,000 every week "
        "simply because they have no market data.",
        54, y + 240, 46, font=_font(26), fill=C_LIGHT, line_spacing=10,
    )
    draw.text((54, y + 330), "#ZimDiaspora  #BuyingFromAbroad  #PropertyZimbabwe",
              font=_font(22), fill=C_GREY)
    _footer(draw)
    return img


def card_sunday(data: dict) -> Image.Image:
    img, draw = _new_canvas(use_bg=True)
    totals    = data.get("totals", (0, 0, 0))
    best      = data.get("best_value", [])
    growth    = data.get("top_growth", [])
    top_yield = data.get("top_yield", [])
    three_bed = data.get("three_bed_avg")

    _header(draw, "WEEKLY WRAP", f"Week of {WEEK}")

    draw.text((54, 170), "THIS WEEK IN NUMBERS",
              font=_font(26, bold=True), fill=C_GOLD)

    rows = [
        ("Properties tracked",         f"{totals[0]:,}"),
        ("Suburbs covered",            f"{totals[1]}"),
        ("Cities monitored",           f"{totals[2]}"),
        ("Best value stands",          f"{best[0][0]}  ${best[0][1]:,.0f}/sqm" if best else "—"),
        ("Fastest-growing suburb",     f"{growth[0][0]}  +{growth[0][1]:.1f}%" if growth else "—"),
        ("Best rental yield suburb",   f"{top_yield[0][0]}  {top_yield[0][1]:.1f}%" if top_yield else "—"),
        ("Avg 3-bed house — Harare",   f"${three_bed:,.0f}" if three_bed else "—"),
    ]
    y = _bullet_table(draw, rows, 54, 210, col_widths=(460, 310), row_h=50)

    _divider(draw, y + 16)
    draw.text((W // 2, y + 54),
              "Get the full weekly report — first one FREE",
              font=_font(28, bold=True), fill=C_GOLD, anchor="mm")
    draw.text((W // 2, y + 104),
              "WhatsApp: wa.me/447459920895",
              font=_font(30, bold=True), fill=C_WHITE, anchor="mm")
    draw.text((54, y + 154), "#ZimPropertyMarket  #WeeklyWrap  #ZimDiaspora",
              font=_font(22), fill=C_GREY)
    _footer(draw)
    return img


# ── Listing image fetcher ──────────────────────────────────────────────────────

def fetch_listing_backgrounds(limit: int = 30):
    """
    Pull real Zimbabwe property listing image URLs from Snowflake,
    download them and save to BG_DIR for use as card backgrounds.
    Run once (or weekly) — images are cached, not re-downloaded.
    """
    import requests as req
    import json

    BG_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching listing image URLs from Snowflake...")
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT image_urls
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE image_urls IS NOT NULL
              AND ARRAY_SIZE(image_urls) > 0
            ORDER BY RANDOM()
            LIMIT {limit * 3}
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Snowflake query failed: {e}")
        return

    urls = []
    for (image_urls,) in rows:
        try:
            parsed = json.loads(image_urls) if isinstance(image_urls, str) else image_urls
            if isinstance(parsed, list) and parsed:
                urls.append(parsed[0])   # take first image per listing
        except Exception:
            continue

    print(f"  Found {len(urls)} listing image URLs — downloading up to {limit}...")
    downloaded = 0
    for i, url in enumerate(urls):
        if downloaded >= limit:
            break
        fname = BG_DIR / f"listing_{i:03d}.jpg"
        if fname.exists():
            downloaded += 1
            continue
        try:
            r = req.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            # Only save if it looks like a real image (>10KB)
            if len(r.content) > 10_000:
                fname.write_bytes(r.content)
                print(f"  Saved: {fname.name}")
                downloaded += 1
        except Exception:
            continue

    print(f"\n{downloaded} listing images saved to {BG_DIR}")


# ── Background downloader (no API key required) ────────────────────────────────

# Curated free Unsplash photo IDs — Zimbabwe / Africa / property / aerial / land
# These are permanent CDN URLs, no API key needed.
_UNSPLASH_PHOTO_IDS = [
    "1523413651479-597eb2da0ad6",  # Harare aerial skyline
    "1580674684081-7617fbf3d745",  # Africa residential suburb
    "1558618666-fcd25c85cd64",     # Green African landscape aerial
    "1617788138860-d294a6090e72",  # African city property
    "1599809275283-bb87c7d9fdc2",  # Africa land / savanna aerial
    "1560179406-1c6c60e0dc76",     # Africa house exterior
    "1512917774080-9991f1c4c750",  # Luxury property exterior
    "1448630360625-af7e83d4baf7",  # Aerial suburb residential
    "1486325212027-8081e485255e",  # City aerial property
    "1497366216548-37526070297c",  # Modern house / architecture
    "1545324418-cc1a3fa10c00",     # Land / open field aerial
    "1600596542815-ffad4c1539a9",  # House exterior / property
    "1584738766473-61c414d5a959",  # Africa town residential
    "1567168544813-cc03cbc5bf67",  # Aerial Africa landscape
    "1574362848149-11496d93a7c7",  # Green suburb aerial
]


def download_backgrounds(api_key: str = "", count: int = 15):
    """
    Download Zimbabwe/Africa property background photos.
    - No API key: uses curated Unsplash photo IDs (free, no signup)
    - With Pixabay key: also searches Pixabay for Zimbabwe-specific photos
    - With Pexels key: also searches Pexels

    Get a free Pixabay key at pixabay.com/api/docs (instant, no review)
    """
    import requests as req
    BG_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = 0

    # ── Option 1: Curated Unsplash (no key needed) ────────────────────────────
    print("Downloading curated property backgrounds from Unsplash (no key needed)...")
    for photo_id in _UNSPLASH_PHOTO_IDS[:count]:
        if downloaded >= count:
            break
        fname = BG_DIR / f"unsplash_{photo_id[:8]}.jpg"
        if fname.exists():
            print(f"  Already exists: {fname.name}")
            downloaded += 1
            continue
        url = f"https://images.unsplash.com/photo-{photo_id}?w=1920&q=80&fit=crop"
        try:
            r = req.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            fname.write_bytes(r.content)
            print(f"  Downloaded: {fname.name}")
            downloaded += 1
        except Exception as e:
            print(f"  [WARN] {photo_id}: {e}")

    # ── Option 2: Pixabay (free key from pixabay.com/api/docs) ───────────────
    if api_key and downloaded < count:
        print("\nSearching Pixabay for Zimbabwe property photos...")
        queries = [
            "Zimbabwe Harare",
            "Zimbabwe property land",
            "Africa residential suburb aerial",
        ]
        for query in queries:
            if downloaded >= count:
                break
            try:
                r = req.get(
                    "https://pixabay.com/api/",
                    params={
                        "key":          api_key,
                        "q":            query,
                        "image_type":   "photo",
                        "orientation":  "horizontal",
                        "per_page":     5,
                        "safesearch":   "true",
                    },
                    timeout=15,
                )
                r.raise_for_status()
                hits = r.json().get("hits", [])
                for hit in hits:
                    if downloaded >= count:
                        break
                    img_url = hit.get("largeImageURL") or hit.get("webformatURL")
                    fname   = BG_DIR / f"pixabay_{hit['id']}.jpg"
                    if fname.exists():
                        downloaded += 1
                        continue
                    ir = req.get(img_url, timeout=30)
                    ir.raise_for_status()
                    fname.write_bytes(ir.content)
                    print(f"  Downloaded: {fname.name}")
                    downloaded += 1
            except Exception as e:
                print(f"  [WARN] {query}: {e}")

    print(f"\n{downloaded} background images saved to {BG_DIR}")


# ── TikTok 9:16 card builders (1080 × 1920) ───────────────────────────────────

def _tk_header(draw, title: str, subtitle: str = "", h: int = TH):
    """TikTok vertical header — taller, bigger text."""
    draw.rectangle([(0, 0), (TW, 200)], fill=C_NAVY_MID)
    draw.rectangle([(0, 0), (TW, 10)], fill=C_GOLD)
    draw.text((54, 60),  title,    font=_font(54, bold=True), fill=C_WHITE)
    if subtitle:
        draw.text((54, 132), subtitle, font=_font(30), fill=C_GOLD)


def _tk_footer(draw):
    """TikTok vertical footer."""
    draw.rectangle([(0, TH - 120), (TW, TH)], fill=C_NAVY_MID)
    draw.rectangle([(0, TH - 122), (TW, TH - 120)], fill=C_GOLD)
    draw.text((TW // 2, TH - 80),
              "Zimbabwe Property Intelligence",
              font=_font(28, bold=True), fill=C_GOLD, anchor="mm")
    draw.text((TW // 2, TH - 40),
              "@izeremhepo  |  wa.me/447459920895",
              font=_font(24), fill=C_LIGHT, anchor="mm")


def _tk_stat_block(draw, label: str, value: str, y: int, accent=C_GOLD):
    """Big hero stat block for TikTok cards."""
    draw.text((TW // 2, y),      value, font=_font(96, bold=True), fill=accent,    anchor="mm")
    draw.text((TW // 2, y + 70), label, font=_font(30),            fill=C_LIGHT,   anchor="mm")


def card_monday_tiktok(data: dict) -> Image.Image:
    img, draw = _new_canvas(TW, TH, use_bg=True)
    _tk_header(draw, "MARKET PULSE", f"Week of {WEEK}")

    stands = data.get("top5_stands", [])
    totals = data.get("totals", (0, 0, 0))

    # Hero stat
    _tk_stat_block(draw, "Properties Tracked This Week", f"{totals[0]:,}", 380)

    draw.line([(54, 510), (TW - 54, 510)], fill=C_GOLD, width=2)

    # Stand prices table (larger)
    draw.text((TW // 2, 570), "HARARE STAND PRICES",
              font=_font(34, bold=True), fill=C_GOLD, anchor="mm")

    y = 630
    for i, r in enumerate(stands[:5]):
        row_fill = C_NAVY_MID if i % 2 else C_NAVY
        draw.rectangle([(54, y - 8), (TW - 54, y + 58)], fill=row_fill)
        draw.text((80,         y + 10), r[0],                  font=_font(34),            fill=C_LIGHT)
        draw.text((TW - 80,    y + 10), f"${r[1]:,.0f}/sqm",   font=_font(34, bold=True), fill=C_GOLD, anchor="ra")
        y += 68

    draw.line([(54, y + 20), (TW - 54, y + 20)], fill=C_GOLD, width=2)

    if stands:
        top  = stands[0]
        fair = top[1] * 500
        draw.text((TW // 2, y + 80),
                  f"500sqm in {top[0]} = ${fair:,.0f}",
                  font=_font(38, bold=True), fill=C_WHITE, anchor="mm")

    draw.text((TW // 2, y + 160),
              "Know the numbers before you negotiate.",
              font=_font(30), fill=C_LIGHT, anchor="mm")

    draw.text((TW // 2, TH - 200),
              "#ZimbabweProperty  #HarareProperty  #ZimDiaspora",
              font=_font(26), fill=C_GREY, anchor="mm")

    _tk_footer(draw)
    return img


def card_tuesday_tiktok(data: dict) -> Image.Image:
    img, draw = _new_canvas(TW, TH, use_bg=True)
    stands = data.get("top5_stands", [])
    name   = stands[0][0] if stands else "—"
    ppsqm  = stands[0][1] if stands else 0

    _tk_header(draw, "SUBURB SPOTLIGHT", name.upper())

    # Hero stat
    _tk_stat_block(draw, f"Avg Stand Price per sqm — {name}", f"${ppsqm:,.0f}", 380)

    draw.line([(54, 510), (TW - 54, 510)], fill=C_GOLD, width=2)

    draw.text((TW // 2, 570), "PRICE CALCULATOR",
              font=_font(34, bold=True), fill=C_GOLD, anchor="mm")

    rows = [
        ("500 sqm stand",   f"~${ppsqm * 500:,.0f}"),
        ("750 sqm stand",   f"~${ppsqm * 750:,.0f}"),
        ("1,000 sqm stand", f"~${ppsqm * 1000:,.0f}"),
    ]
    y = 630
    for i, (label, value) in enumerate(rows):
        row_fill = C_NAVY_MID if i % 2 else C_NAVY
        draw.rectangle([(54, y - 8), (TW - 54, y + 68)], fill=row_fill)
        draw.text((80,      y + 12), label, font=_font(36),            fill=C_LIGHT)
        draw.text((TW - 80, y + 12), value, font=_font(36, bold=True), fill=C_GOLD, anchor="ra")
        y += 80

    draw.line([(54, y + 20), (TW - 54, y + 20)], fill=C_GOLD, width=2)

    _wrapped_text(draw,
        f"Being quoted more than these figures in {name}? "
        f"Ask your agent to justify the premium.",
        80, y + 60, 36, font=_font(32), fill=C_LIGHT, line_spacing=14)

    draw.text((TW // 2, TH - 200),
              f'DM us "{name.upper()}" for the full suburb report',
              font=_font(30, bold=True), fill=C_GOLD, anchor="mm")

    draw.text((TW // 2, TH - 155),
              "#StandPrices  #ZimProperty  #ZimDiaspora",
              font=_font(26), fill=C_GREY, anchor="mm")

    _tk_footer(draw)
    return img


def card_wednesday_tiktok(data: dict) -> Image.Image:
    img, draw = _new_canvas(TW, TH, use_bg=True)
    best = data.get("best_value", [])
    _tk_header(draw, "PRICE CHECK", "Overpriced or Fair?")

    if best:
        _tk_stat_block(draw, f"Best Value Stands — {best[0][0]}", f"${best[0][1]:,.0f}/sqm", 380)

    draw.line([(54, 510), (TW - 54, 510)], fill=C_GOLD, width=2)

    draw.text((TW // 2, 570), "MOST AFFORDABLE STANDS",
              font=_font(34, bold=True), fill=C_GOLD, anchor="mm")

    y = 630
    for i, r in enumerate(best):
        row_fill = C_NAVY_MID if i % 2 else C_NAVY
        draw.rectangle([(54, y - 8), (TW - 54, y + 68)], fill=row_fill)
        draw.text((80,      y + 12), r[0],               font=_font(36),            fill=C_LIGHT)
        draw.text((TW - 80, y + 12), f"${r[1]:,.0f}/sqm", font=_font(36, bold=True), fill=C_GOLD, anchor="ra")
        y += 80

    draw.line([(54, y + 20), (TW - 54, y + 20)], fill=C_GOLD, width=2)

    draw.text((TW // 2, y + 70), "HOW TO CALCULATE FAIR VALUE:",
              font=_font(32, bold=True), fill=C_GOLD, anchor="mm")
    draw.text((TW // 2, y + 130),
              "Stand size (sqm)  ×  price/sqm  =  fair price",
              font=_font(30), fill=C_WHITE, anchor="mm")

    if best:
        bv = best[0]
        draw.text((TW // 2, y + 210),
                  f"E.g. 500sqm in {bv[0]} = ${bv[1] * 500:,.0f}",
                  font=_font(36, bold=True), fill=C_WHITE, anchor="mm")

    draw.text((TW // 2, TH - 200),
              "Quoted more? Negotiate hard or walk away.",
              font=_font(30, bold=True), fill=C_RED, anchor="mm")
    draw.text((TW // 2, TH - 155),
              "#KnowBeforeYouBuy  #PropertyZimbabwe",
              font=_font(26), fill=C_GREY, anchor="mm")

    _tk_footer(draw)
    return img


def card_thursday_tiktok(data: dict) -> Image.Image:
    img, draw = _new_canvas(TW, TH, use_bg=True)
    growth = data.get("top_growth", [])
    _tk_header(draw, "INVESTMENT SIGNAL", "Rising Suburbs — 6 Month Trend")

    if growth:
        _tk_stat_block(draw, f"Top Growth — {growth[0][0]}", f"+{growth[0][1]:.1f}%", 380, C_GREEN)

    draw.line([(54, 510), (TW - 54, 510)], fill=C_GOLD, width=2)

    draw.text((TW // 2, 570), "FASTEST GROWING SUBURBS",
              font=_font(34, bold=True), fill=C_GOLD, anchor="mm")

    y = 630
    for i, r in enumerate(growth):
        row_fill = C_NAVY_MID if i % 2 else C_NAVY
        draw.rectangle([(54, y - 8), (TW - 54, y + 68)], fill=row_fill)
        draw.text((80,      y + 12), r[0],           font=_font(36),            fill=C_LIGHT)
        draw.text((TW - 80, y + 12), f"+{r[1]:.1f}%", font=_font(36, bold=True), fill=C_GREEN, anchor="ra")
        y += 80

    draw.line([(54, y + 20), (TW - 54, y + 20)], fill=C_GOLD, width=2)

    _wrapped_text(draw,
        "Early buyers in these suburbs are already sitting on paper gains. "
        "Those who waited are paying more today.",
        80, y + 60, 34, font=_font(32), fill=C_LIGHT, line_spacing=14)

    draw.text((TW // 2, TH - 200),
              "Data only. Always do your own due diligence.",
              font=_font(28), fill=C_GREY, anchor="mm")
    draw.text((TW // 2, TH - 155),
              "#ZimPropertyInvestment  #HarareProperty",
              font=_font(26), fill=C_GREY, anchor="mm")

    _tk_footer(draw)
    return img


def card_friday_tiktok(data: dict) -> Image.Image:
    img, draw = _new_canvas(TW, TH, use_bg=True)
    three_bed = data.get("three_bed_avg")
    avg_rent  = data.get("avg_rent")
    best      = data.get("best_value", [])
    top5      = data.get("top5_stands", [])

    _tk_header(draw, "DATA FACTS", f"Week of {WEEK}")

    if three_bed:
        _tk_stat_block(draw, "Avg 3-Bed House Price — Harare", f"${three_bed:,.0f}", 380)

    draw.line([(54, 510), (TW - 54, 510)], fill=C_GOLD, width=2)

    rows = [
        ("Avg 3-bed house (Harare)",  f"${three_bed:,.0f}"         if three_bed else "—"),
        ("Avg monthly rent (Harare)", f"${avg_rent:,.0f}/mo"        if avg_rent  else "—"),
        ("Best value stands",         f"{best[0][0]} ${best[0][1]:,.0f}/sqm" if best  else "—"),
        ("Priciest stands",           f"{top5[0][0]} ${top5[0][1]:,.0f}/sqm" if top5  else "—"),
    ]
    y = 570
    for i, (label, value) in enumerate(rows):
        row_fill = C_NAVY_MID if i % 2 else C_NAVY
        draw.rectangle([(54, y - 8), (TW - 54, y + 68)], fill=row_fill)
        draw.text((80,      y + 12), label, font=_font(30),            fill=C_LIGHT)
        draw.text((TW - 80, y + 12), value, font=_font(30, bold=True), fill=C_GOLD, anchor="ra")
        y += 80

    draw.line([(54, y + 20), (TW - 54, y + 20)], fill=C_GOLD, width=2)

    draw.text((TW // 2, y + 80),
              "Know which suburb matches your budget.",
              font=_font(34, bold=True), fill=C_WHITE, anchor="mm")
    draw.text((TW // 2, y + 140),
              "Save this post — share it to protect someone.",
              font=_font(30), fill=C_LIGHT, anchor="mm")

    draw.text((TW // 2, TH - 155),
              "#ZimbabweProperty  #PropertyFacts  #HarareHouses",
              font=_font(26), fill=C_GREY, anchor="mm")

    _tk_footer(draw)
    return img


def card_saturday_tiktok(data: dict) -> Image.Image:
    img, draw = _new_canvas(TW, TH, use_bg=True)
    top5 = data.get("top5_stands", [])
    _tk_header(draw, "DIASPORA SPECIAL", "Buying Property from Abroad?")

    _tk_stat_block(draw, "Diaspora buyers overpay by up to", "$50,000", 380, C_RED)

    draw.line([(54, 510), (TW - 54, 510)], fill=C_GOLD, width=2)

    draw.text((TW // 2, 570), "THIS WEEK'S HARARE BENCHMARKS",
              font=_font(34, bold=True), fill=C_GOLD, anchor="mm")

    y = 630
    for i, r in enumerate(top5[:4]):
        row_fill = C_NAVY_MID if i % 2 else C_NAVY
        draw.rectangle([(54, y - 8), (TW - 54, y + 68)], fill=row_fill)
        draw.text((80,      y + 12), r[0],               font=_font(36),            fill=C_LIGHT)
        draw.text((TW - 80, y + 12), f"${r[1]:,.0f}/sqm", font=_font(36, bold=True), fill=C_GOLD, anchor="ra")
        y += 80

    draw.line([(54, y + 20), (TW - 54, y + 20)], fill=C_GOLD, width=2)

    draw.text((TW // 2, y + 70), "Your protection formula:",
              font=_font(32, bold=True), fill=C_GOLD, anchor="mm")
    draw.text((TW // 2, y + 130),
              "Size (sqm)  ×  price/sqm  =  what to pay",
              font=_font(30), fill=C_WHITE, anchor="mm")
    draw.text((TW // 2, y + 200),
              "Quoted significantly more? Walk away.",
              font=_font(34, bold=True), fill=C_RED, anchor="mm")

    draw.text((TW // 2, TH - 155),
              "#ZimDiaspora  #BuyingFromAbroad  #PropertyZimbabwe",
              font=_font(26), fill=C_GREY, anchor="mm")

    _tk_footer(draw)
    return img


def card_sunday_tiktok(data: dict) -> Image.Image:
    img, draw = _new_canvas(TW, TH, use_bg=True)
    totals    = data.get("totals", (0, 0, 0))
    best      = data.get("best_value", [])
    growth    = data.get("top_growth", [])
    top_yield = data.get("top_yield", [])
    three_bed = data.get("three_bed_avg")

    _tk_header(draw, "WEEKLY WRAP", f"Week of {WEEK}")

    _tk_stat_block(draw, "Properties Tracked This Week", f"{totals[0]:,}", 380)

    draw.line([(54, 510), (TW - 54, 510)], fill=C_GOLD, width=2)

    draw.text((TW // 2, 570), "THIS WEEK IN NUMBERS",
              font=_font(34, bold=True), fill=C_GOLD, anchor="mm")

    rows = [
        ("Suburbs covered",           str(totals[1])),
        ("Cities monitored",          str(totals[2])),
        ("Best value stands",         f"{best[0][0]}  ${best[0][1]:,.0f}/sqm" if best      else "—"),
        ("Fastest-growing suburb",    f"{growth[0][0]}  +{growth[0][1]:.1f}%" if growth    else "—"),
        ("Best rental yield",         f"{top_yield[0][0]}  {top_yield[0][1]:.1f}%"          if top_yield else "—"),
        ("Avg 3-bed — Harare",        f"${three_bed:,.0f}"                                  if three_bed else "—"),
    ]
    y = 630
    for i, (label, value) in enumerate(rows):
        row_fill = C_NAVY_MID if i % 2 else C_NAVY
        draw.rectangle([(54, y - 8), (TW - 54, y + 60)], fill=row_fill)
        draw.text((80,      y + 8), label, font=_font(28),            fill=C_LIGHT)
        draw.text((TW - 80, y + 8), value, font=_font(28, bold=True), fill=C_GOLD, anchor="ra")
        y += 68

    draw.line([(54, y + 20), (TW - 54, y + 20)], fill=C_GOLD, width=2)

    draw.text((TW // 2, y + 80),
              "Get the full weekly report — first one FREE",
              font=_font(34, bold=True), fill=C_GOLD, anchor="mm")
    draw.text((TW // 2, y + 140),
              "WhatsApp: wa.me/447459920895",
              font=_font(32, bold=True), fill=C_WHITE, anchor="mm")

    draw.text((TW // 2, TH - 155),
              "#ZimPropertyMarket  #WeeklyWrap  #ZimDiaspora",
              font=_font(26), fill=C_GREY, anchor="mm")

    _tk_footer(draw)
    return img


# ── Data fetcher ──────────────────────────────────────────────────────────────
def get_connection():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )



def load_data(cursor) -> dict:
    def q(sql):
        cursor.execute(sql)
        return cursor.fetchall()

    def q1(sql):
        cursor.execute(sql)
        r = cursor.fetchone()
        return r[0] if r else None

    def q_row(sql):
        cursor.execute(sql)
        return cursor.fetchone() or (0, 0, 0)

    return {
        "top5_stands": q("""
            SELECT suburb_clean, ROUND(AVG(price_per_sqm_usd), 0)
            FROM ANALYTICS.LAND_LISTINGS
            WHERE price_per_sqm_usd IS NOT NULL AND city_clean ILIKE '%Harare%'
            GROUP BY suburb_clean HAVING COUNT(*) >= 2
            ORDER BY 2 DESC LIMIT 5
        """),
        "best_value": q("""
            SELECT suburb_clean, ROUND(AVG(price_per_sqm_usd), 0)
            FROM ANALYTICS.LAND_LISTINGS
            WHERE price_per_sqm_usd IS NOT NULL AND city_clean ILIKE '%Harare%'
            GROUP BY suburb_clean HAVING COUNT(*) >= 2
            ORDER BY 2 ASC LIMIT 3
        """),
        "top_growth": q("""
            SELECT suburb_clean, ROUND(growth_6m_pct, 1)
            FROM ANALYTICS.SUBURB_PRICE_GROWTH
            WHERE growth_6m_pct IS NOT NULL AND listing_count_current >= 3
            ORDER BY growth_6m_pct DESC LIMIT 3
        """),
        "top_yield": q("""
            SELECT suburb_clean, ROUND(gross_rental_yield_pct, 1)
            FROM ANALYTICS.RENTAL_YIELD_BY_SUBURB
            WHERE gross_rental_yield_pct IS NOT NULL
            ORDER BY gross_rental_yield_pct DESC LIMIT 3
        """),
        "totals": q_row("""
            SELECT COUNT(*), COUNT(DISTINCT suburb_clean), COUNT(DISTINCT city_clean)
            FROM STAGING.CLEANED_PROPERTY_LISTINGS
        """),
        "three_bed_avg": q1("""
            SELECT ROUND(AVG(property_price_usd), 0)
            FROM ANALYTICS.HOUSE_SALE_LISTINGS
            WHERE number_of_bedrooms = 3 AND city_clean ILIKE '%Harare%'
              AND property_price_usd IS NOT NULL
        """),
        "avg_rent": q1("""
            SELECT ROUND(AVG(monthly_rent_usd), 0)
            FROM ANALYTICS.RENTAL_LISTINGS
            WHERE monthly_rent_usd IS NOT NULL AND city_clean ILIKE '%Harare%'
        """),
    }


# ── Voiceover scripts ─────────────────────────────────────────────────────────
def voiceover_monday(data: dict) -> str:
    stands = data.get("top5_stands", [])
    totals = data.get("totals", (0, 0, 0))
    top    = stands[0] if stands else None
    lines  = [f"{r[0]}: {r[1]:,.0f} dollars per square metre" for r in stands[:3]]
    intro  = (
        f"Zimbabwe Property Market Pulse — week of {WEEK}. "
        f"This week we tracked {totals[0]:,} properties across "
        f"{totals[1]} suburbs in {totals[2]} cities. "
    )
    body = "Harare stand prices this week: " + ". ".join(lines) + ". "
    if top:
        fair = top[1] * 500
        body += (
            f"A five-hundred square metre stand in {top[0]} "
            f"should cost approximately {fair:,.0f} US dollars at current market rates. "
        )
    cta = "Know the numbers before you negotiate. Get our weekly report at Zimbabwe Property Intel dot com."
    return intro + body + cta


def voiceover_tuesday(data: dict) -> str:
    stands = data.get("top5_stands", [])
    if not stands:
        return "Zimbabwe Property Intelligence. Suburb spotlight data will be available next week."
    name   = stands[0][0]
    ppsqm  = stands[0][1]
    return (
        f"Suburb spotlight: {name}, Harare. "
        f"The average stand price in {name} this week is {ppsqm:,.0f} US dollars per square metre. "
        f"A five-hundred square metre stand should cost around {ppsqm * 500:,.0f} dollars. "
        f"A thousand square metre stand should cost around {ppsqm * 1000:,.0f} dollars. "
        f"If you are being quoted significantly more than this, ask your agent to justify the premium. "
        f"Message us the word {name.upper()} for the full suburb breakdown."
    )


def voiceover_wednesday(data: dict) -> str:
    best = data.get("best_value", [])
    if not best:
        return "Zimbabwe Property Price Check. Message us on WhatsApp for the full report."
    lines = [f"{r[0]} at {r[1]:,.0f} dollars per square metre" for r in best]
    bv    = best[0]
    return (
        "Overpriced or fair? Here is how to tell. "
        "The most affordable stand suburbs in Harare this week are: "
        + ". ".join(lines) + ". "
        "Here is how to use this: "
        "Take the stand size in square metres, multiply by the price per square metre, "
        "and that is your fair market value. "
        f"For example, a five-hundred square metre stand in {bv[0]} "
        f"should cost around {bv[1] * 500:,.0f} US dollars. "
        "If you are being quoted more than this, negotiate hard — or walk away. "
        "Message us on WhatsApp for the full report."
    )


def voiceover_thursday(data: dict) -> str:
    growth = data.get("top_growth", [])
    if not growth:
        return "Investment Signal. Message us on WhatsApp for the full report."
    lines  = [f"{r[0]}, up {r[1]:.1f} percent" for r in growth]
    fastest = growth[0][0]
    return (
        "Investment signal: suburbs with rising property prices in Zimbabwe. "
        "Based on six-month trend data: "
        + ". ".join(lines) + ". "
        f"Early buyers in {fastest} are already sitting on paper gains. "
        "Those who waited are paying more today. "
        "This is market data only. Always do your own due diligence. "
        "Message us on WhatsApp for the full report."
    )


def voiceover_friday(data: dict) -> str:
    three_bed = data.get("three_bed_avg")
    avg_rent  = data.get("avg_rent")
    best      = data.get("best_value", [])
    top5      = data.get("top5_stands", [])
    parts     = [f"Week of {WEEK}. Zimbabwe property facts."]
    if three_bed:
        parts.append(f"Average three-bedroom house price in Harare: {three_bed:,.0f} US dollars.")
    if avg_rent:
        parts.append(f"Average monthly rent in Harare: {avg_rent:,.0f} US dollars per month.")
    if best:
        parts.append(f"Best value stands: {best[0][0]} at {best[0][1]:,.0f} dollars per square metre.")
    if top5:
        parts.append(f"Most expensive stands: {top5[0][0]} at {top5[0][1]:,.0f} dollars per square metre.")
    parts.append("Know which suburb matches your budget. Save this post and share it to protect someone.")
    return " ".join(parts)


def voiceover_saturday(data: dict) -> str:
    top5 = data.get("top5_stands", [])
    lines = [f"{r[0]}: {r[1]:,.0f} dollars per square metre" for r in top5[:3]]
    return (
        "For Zimbabweans buying property from abroad — listen to this before sending money home. "
        "Here are this week's Harare stand price benchmarks: "
        + ". ".join(lines) + ". "
        "Use this formula to protect yourself: "
        "stand size in square metres, multiplied by the price per square metre, "
        "equals what you should pay. Anything significantly above this means you are being overcharged. "
        "We have seen diaspora buyers overpay by ten thousand to fifty thousand US dollars "
        "on a single transaction — simply because they had no market data. "
        "Our weekly report costs a fraction of being overcharged once. "
        "Message us on WhatsApp for the full report."
    )


def voiceover_sunday(data: dict) -> str:
    totals    = data.get("totals", (0, 0, 0))
    best      = data.get("best_value", [])
    growth    = data.get("top_growth", [])
    top_yield = data.get("top_yield", [])
    three_bed = data.get("three_bed_avg")
    parts     = [
        f"Zimbabwe Property Weekly Wrap — week of {WEEK}. "
        f"This week we tracked {totals[0]:,} properties across "
        f"{totals[1]} suburbs in {totals[2]} cities."
    ]
    if best:
        parts.append(f"Most affordable stands: {best[0][0]} at {best[0][1]:,.0f} dollars per square metre.")
    if growth:
        parts.append(f"Fastest-growing suburb: {growth[0][0]}, up {growth[0][1]:.1f} percent over six months.")
    if top_yield:
        parts.append(f"Best rental yield: {top_yield[0][0]} at {top_yield[0][1]:.1f} percent gross yield.")
    if three_bed:
        parts.append(f"Average three-bedroom house price in Harare: {three_bed:,.0f} US dollars.")
    parts.append(
        "Get the full detailed weekly report — all suburbs, all prices, agent contacts, and price trends. "
        "First report is free. Message us on WhatsApp for the full report."
    )
    return " ".join(parts)


VOICEOVER_SCRIPTS = {
    "monday_market_pulse":        voiceover_monday,
    "tuesday_suburb_spotlight":   voiceover_tuesday,
    "wednesday_price_check":      voiceover_wednesday,
    "thursday_investment_signal": voiceover_thursday,
    "friday_data_fact":           voiceover_friday,
    "saturday_diaspora_special":  voiceover_saturday,
    "sunday_weekly_summary":      voiceover_sunday,
}


# ── TTS synthesis ─────────────────────────────────────────────────────────────
async def synthesise(text: str, mp3_path: Path, voice: str = VOICE, retries: int = 3):
    """Generate speech MP3 using Microsoft Edge neural TTS with retry on transient errors."""
    import edge_tts
    for attempt in range(1, retries + 1):
        try:
            tts = edge_tts.Communicate(text, voice, rate="+5%")
            await tts.save(str(mp3_path))
            return
        except Exception as e:
            if attempt == retries:
                raise
            wait = attempt * 2
            print(f"\n    [TTS retry {attempt}/{retries} in {wait}s: {e}]", end=" ", flush=True)
            await asyncio.sleep(wait)


async def list_voices():
    import edge_tts
    voices = await edge_tts.list_voices()
    en_voices = [v for v in voices if v["Locale"].startswith("en-")]
    for v in sorted(en_voices, key=lambda x: x["Locale"]):
        print(f"  {v['ShortName']:<35} {v['Gender']:<8} {v['Locale']}")


# ── Video builder ─────────────────────────────────────────────────────────────
def image_to_video(img: Image.Image, out_path: Path,
                   audio_path: Path = None, duration: float = 8.0):
    """
    Build MP4 from a PIL image with optional audio.
    Uses a short 4-second zoom loop repeated to match audio length — memory efficient.
    """
    import numpy as np

    try:
        from moviepy import ImageSequenceClip, AudioFileClip
    except ImportError:
        from moviepy.editor import ImageSequenceClip, AudioFileClip

    # Determine final duration from audio
    actual_duration = duration
    audio_clip      = None
    if audio_path and audio_path.exists():
        audio_clip      = AudioFileClip(str(audio_path))
        actual_duration = audio_clip.duration + 0.5

    # Use actual image dimensions (supports both 1080×1080 and 1080×1920)
    iw, ih = img.size

    # Build a short 4-second zoom cycle (96 frames @ 24fps) — then loop it
    CYCLE_SECS = 4
    n_cycle    = CYCLE_SECS * 24
    cycle_frames = []
    for i in range(n_cycle):
        t      = i / n_cycle
        scale  = 1.0 + 0.04 * t
        new_w  = int(iw * scale)
        new_h  = int(ih * scale)
        scaled = img.resize((new_w, new_h), Image.LANCZOS)
        x0     = (new_w - iw) // 2
        y0     = (new_h - ih) // 2
        cycle_frames.append(
            np.array(scaled.crop((x0, y0, x0 + iw, y0 + ih)).convert("RGB"))
        )

    # Repeat cycle enough times to cover full duration
    n_total  = int(actual_duration * 24) + 1
    repeats  = (n_total // n_cycle) + 1
    frames   = (cycle_frames * repeats)[:n_total]

    clip = ImageSequenceClip(frames, fps=24)
    if audio_clip:
        clip = clip.with_audio(audio_clip)

    clip.write_videofile(str(out_path), codec="libx264",
                         audio_codec="aac" if audio_clip else None,
                         audio=bool(audio_clip),
                         logger=None, preset="fast")
    clip.close()
    if audio_clip:
        audio_clip.close()


# ── Main ──────────────────────────────────────────────────────────────────────
CARDS = [
    ("monday_market_pulse",        card_monday),
    ("tuesday_suburb_spotlight",   card_tuesday),
    ("wednesday_price_check",      card_wednesday),
    ("thursday_investment_signal", card_thursday),
    ("friday_data_fact",           card_friday),
    ("saturday_diaspora_special",  card_saturday),
    ("sunday_weekly_summary",      card_sunday),
]

TIKTOK_CARDS = [
    ("monday_market_pulse",        card_monday_tiktok),
    ("tuesday_suburb_spotlight",   card_tuesday_tiktok),
    ("wednesday_price_check",      card_wednesday_tiktok),
    ("thursday_investment_signal", card_thursday_tiktok),
    ("friday_data_fact",           card_friday_tiktok),
    ("saturday_diaspora_special",  card_saturday_tiktok),
    ("sunday_weekly_summary",      card_sunday_tiktok),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--images-only", action="store_true",
                        help="Generate PNGs only, skip video encoding")
    parser.add_argument("--no-voice", action="store_true",
                        help="Encode video without voiceover")
    parser.add_argument("--list-voices", action="store_true",
                        help="List available English TTS voices and exit")
    parser.add_argument("--tiktok", action="store_true",
                        help="Generate 9:16 vertical TikTok cards (saves to tiktok/ subfolder)")
    parser.add_argument("--download-backgrounds", metavar="PIXABAY_API_KEY", nargs="?", const="",
                        help="Download background images (Unsplash free + optional Pixabay key)")
    parser.add_argument("--listing-backgrounds", action="store_true",
                        help="Fetch real listing photos from Snowflake as card backgrounds")
    args = parser.parse_args()

    if args.list_voices:
        print("Available English TTS voices (edge-tts):\n")
        asyncio.run(list_voices())
        return

    if args.download_backgrounds is not None:
        download_backgrounds(args.download_backgrounds or "")
        return

    if args.listing_backgrounds:
        fetch_listing_backgrounds(limit=30)
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    BG_DIR.mkdir(parents=True, exist_ok=True)

    print("Connecting to Snowflake ...")
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        data = load_data(cursor)
    finally:
        cursor.close()
        conn.close()

    print(f"Generating {len(CARDS)} cards -> {OUT_DIR}\n")
    for slug, builder in CARDS:
        img      = builder(data)
        png_path = OUT_DIR / f"{slug}.png"
        img.save(png_path)

        if args.images_only:
            print(f"  [PNG] {slug}.png")
            continue

        mp4_path = OUT_DIR / f"{slug}.mp4"
        mp3_path = OUT_DIR / f"{slug}.mp3"
        print(f"  [MP4] {slug} ...", end=" ", flush=True)

        try:
            audio_path = None
            if not args.no_voice and slug in VOICEOVER_SCRIPTS:
                script = VOICEOVER_SCRIPTS[slug](data)
                asyncio.run(synthesise(script, mp3_path))
                audio_path = mp3_path
                import time; time.sleep(1)   # brief pause between TTS requests

            image_to_video(img, mp4_path, audio_path=audio_path)

            # Clean up temp MP3
            if mp3_path.exists():
                mp3_path.unlink()

            duration_note = "(with voiceover)" if audio_path else "(silent)"
            print(f"done {duration_note}")
        except Exception as e:
            print(f"FAILED ({e}) — PNG saved instead")

    print(f"\nAll cards saved to: {OUT_DIR}")

    if args.tiktok:
        tk_dir = OUT_DIR / "tiktok"
        tk_dir.mkdir(parents=True, exist_ok=True)
        bg_count = len(list(BG_DIR.glob("*.jpg")) + list(BG_DIR.glob("*.png")))
        print(f"\nGenerating {len(TIKTOK_CARDS)} TikTok 9:16 cards -> {tk_dir}")
        if bg_count == 0:
            print("  [NOTE] No background images found — using plain navy background.")
            print("  Run with --download-backgrounds YOUR_PEXELS_KEY to add photo backgrounds.")
        else:
            print(f"  Using {bg_count} background image(s) from {BG_DIR}")
        print()
        for slug, builder in TIKTOK_CARDS:
            img      = builder(data)
            png_path = tk_dir / f"{slug}_tiktok.png"
            img.save(png_path)

            if args.images_only:
                print(f"  [PNG] {slug}_tiktok.png")
                continue

            mp4_path = tk_dir / f"{slug}_tiktok.mp4"
            mp3_path = tk_dir / f"{slug}_tiktok.mp3"
            print(f"  [MP4] {slug}_tiktok ...", end=" ", flush=True)
            try:
                audio_path = None
                if not args.no_voice and slug in VOICEOVER_SCRIPTS:
                    script = VOICEOVER_SCRIPTS[slug](data)
                    asyncio.run(synthesise(script, mp3_path))
                    audio_path = mp3_path
                    import time
                    time.sleep(1)
                image_to_video(img, mp4_path, audio_path=audio_path)
                if mp3_path.exists():
                    mp3_path.unlink()
                print(f"done {'(with voiceover)' if audio_path else '(silent)'}")
            except Exception as e:
                print(f"FAILED ({e}) — PNG saved instead")

        print(f"\nTikTok cards saved to: {tk_dir}")


if __name__ == "__main__":
    main()

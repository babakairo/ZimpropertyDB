"""
reports/video_composer.py
Composites a D-ID talking head video with animated data slideshow overlays.

Layout (1080 x 1920 — TikTok/Reels 9:16):
  ┌─────────────────────────┐
  │  BRAND HEADER           │  ~120px
  ├─────────────────────────┤
  │                         │
  │   DATA SLIDES           │  ~980px  (animated text, key stats)
  │                         │
  ├─────────────────────────┤
  │   PRESENTER (Jaimie)    │  ~820px  (D-ID video, cropped center)
  └─────────────────────────┘

Usage (standalone):
    python reports/video_composer.py \
        --video reports/output/cards/2026-03-12/did/thursday_investment_signal_did.mp4 \
        --day thursday

Integrated into did_video_generator.py automatically after clip download.
"""

import os
import sys
import argparse
import textwrap
import numpy as np
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

from PIL import Image, ImageDraw, ImageFont
from moviepy import VideoFileClip, ImageClip, CompositeVideoClip, concatenate_videoclips

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

# ── Canvas dimensions ──────────────────────────────────────────────────────────
W, H         = 1080, 1920
HEADER_H     = 120
SLIDE_H      = 980
PRESENTER_H  = H - HEADER_H - SLIDE_H   # 820

# ── Brand colours ──────────────────────────────────────────────────────────────
C_BG         = (10,  14,  30)    # deep navy
C_HEADER_BG  = (15,  20,  45)    # slightly lighter navy
C_GOLD       = (212, 175,  55)   # gold
C_WHITE      = (255, 255, 255)
C_GREY       = (160, 160, 180)
C_ACCENT     = ( 30,  80, 160)   # blue accent
C_GREEN      = ( 46, 184, 113)   # positive green
C_PRESENTER  = ( 18,  22,  42)   # presenter bg

# ── Fonts ──────────────────────────────────────────────────────────────────────
FONTS_DIR = Path("C:/Windows/Fonts")

def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    name = "ARIALNB.TTF" if bold else "ARIALN.TTF"
    try:
        return ImageFont.truetype(str(FONTS_DIR / name), size)
    except Exception:
        try:
            fb = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
            return ImageFont.truetype(str(FONTS_DIR / fb), size)
        except Exception:
            return ImageFont.load_default()

WEEK = date.today().strftime("%d %B %Y")


# ── Slide data builders ────────────────────────────────────────────────────────
def slides_thursday(data: dict) -> list[dict]:
    """Returns list of slide dicts: {title, lines, icon, color}"""
    growth = data.get("top_growth", [])
    slides = [
        {
            "title": "INVESTMENT SIGNAL",
            "subtitle": f"Week of {WEEK}",
            "lines": ["Suburbs with the fastest", "rising property prices"],
            "icon": "📈",
            "color": C_GOLD,
        }
    ]
    for i, r in enumerate(growth[:3]):
        pct = float(r[2])
        color = C_GREEN if pct > 0 else (220, 80, 80)
        slides.append({
            "title": f"#{i+1}  {r[0].upper()}",
            "subtitle": f"{r[1]}",
            "lines": [
                f"+{pct:.0f}% in 3 months" if pct > 0 else f"{pct:.0f}% in 3 months",
                f"Avg price: ${float(r[3]):,.0f}",
            ],
            "icon": "🏘️",
            "color": color,
        })
    slides.append({
        "title": "GET THE FULL REPORT",
        "subtitle": "First report is FREE",
        "lines": ["WhatsApp us now:", "wa.me/447459920895"],
        "icon": "💬",
        "color": C_GOLD,
    })
    return slides


def slides_monday(data: dict) -> list[dict]:
    g = data.get("top_growth", [])
    slides = [{
        "title": "MARKET PULSE",
        "subtitle": f"Week of {WEEK}",
        "lines": [f"{data.get('total',0):,} listings tracked",
                  f"{data.get('sales',0):,} sales  •  {data.get('rentals',0):,} rentals  •  {data.get('land',0):,} stands"],
        "icon": "🏙️", "color": C_GOLD,
    }]
    for i, r in enumerate(g[:3]):
        slides.append({
            "title": f"#{i+1}  {r[0].upper()}",
            "subtitle": r[1],
            "lines": [f"+{float(r[2]):.0f}% growth", f"Avg ${float(r[3]):,.0f}"],
            "icon": "📊", "color": C_GREEN,
        })
    slides.append({"title": "GET THE FULL REPORT", "subtitle": "First report is FREE",
                   "lines": ["WhatsApp:", "wa.me/447459920895"], "icon": "💬", "color": C_GOLD})
    return slides


def slides_tuesday(data: dict) -> list[dict]:
    g = data.get("top_growth", [])
    top = g[0] if g else ("Pomona", "Harare", 18.2, 95000)
    return [
        {"title": "SUBURB SPOTLIGHT", "subtitle": top[0], "lines": [f"Fastest growing suburb", f"in Zimbabwe this week"], "icon": "🔦", "color": C_GOLD},
        {"title": f"+{float(top[2]):.0f}%", "subtitle": "3-month price growth", "lines": [f"Average property: ${float(top[3]):,.0f}", f"City: {top[1]}"], "icon": "📈", "color": C_GREEN},
        {"title": "GET THE FULL REPORT", "subtitle": "First report is FREE", "lines": ["WhatsApp:", "wa.me/447459920895"], "icon": "💬", "color": C_GOLD},
    ]


def slides_wednesday(data: dict) -> list[dict]:
    bv = data.get("best_value", [])
    slides = [{"title": "PRICE CHECK", "subtitle": "Best value stands this week",
                "lines": ["Affordable stands under $30,000", "Verified market data"], "icon": "💰", "color": C_GOLD}]
    for i, r in enumerate(bv[:3]):
        slides.append({
            "title": f"#{i+1}  {r[0].upper()}",
            "subtitle": r[1],
            "lines": [f"From ${float(r[2]):,.0f}", f"~{int(float(r[3]) if r[3] else 0)} sqm"],
            "icon": "🏡", "color": C_GREEN,
        })
    slides.append({"title": "GET THE FULL REPORT", "subtitle": "First report is FREE",
                   "lines": ["WhatsApp:", "wa.me/447459920895"], "icon": "💬", "color": C_GOLD})
    return slides


def slides_friday(data: dict) -> list[dict]:
    three = data.get("three_bed_avg", 0)
    rent  = data.get("avg_rent", 0)
    bv    = data.get("best_value", [])
    slides = [{"title": "DATA FACT", "subtitle": f"Week of {WEEK}",
                "lines": ["Zimbabwe property by the numbers"], "icon": "📋", "color": C_GOLD}]
    if three:
        slides.append({"title": f"${three:,}", "subtitle": "Avg 3-bed house — Harare",
                       "lines": ["Current market average", "for a 3-bedroom home"], "icon": "🏠", "color": C_GREEN})
    if rent:
        slides.append({"title": f"${rent:,}/mo", "subtitle": "Avg monthly rent — Harare",
                       "lines": ["Current rental market average"], "icon": "🔑", "color": C_ACCENT})
    if bv:
        slides.append({"title": f"${float(bv[0][2]):,.0f}", "subtitle": f"Cheapest stand — {bv[0][0]}",
                       "lines": ["Most affordable entry point", "into the property market"], "icon": "📍", "color": C_GREEN})
    slides.append({"title": "GET THE FULL REPORT", "subtitle": "First report is FREE",
                   "lines": ["WhatsApp:", "wa.me/447459920895"], "icon": "💬", "color": C_GOLD})
    return slides


def slides_saturday(data: dict) -> list[dict]:
    bv = data.get("best_value", [])
    slides = [{"title": "DIASPORA SPECIAL", "subtitle": "Before you send money home",
                "lines": ["Know the real market prices", "Protect yourself from overpaying"], "icon": "✈️", "color": C_GOLD}]
    for i, r in enumerate(bv[:3]):
        slides.append({"title": r[0].upper(), "subtitle": r[1],
                       "lines": [f"Stands from ${float(r[2]):,.0f}", f"~{int(float(r[3]) if r[3] else 0)} sqm available"],
                       "icon": "🗺️", "color": C_GREEN})
    slides.append({"title": "GET THE FULL REPORT", "subtitle": "First report is FREE",
                   "lines": ["WhatsApp:", "wa.me/447459920895"], "icon": "💬", "color": C_GOLD})
    return slides


def slides_sunday(data: dict) -> list[dict]:
    g  = data.get("top_growth", [])
    ty = data.get("top_yield", [])
    three = data.get("three_bed_avg", 0)
    slides = [{"title": "WEEKLY WRAP", "subtitle": f"Week of {WEEK}",
                "lines": [f"{data.get('total',0):,} listings tracked this week"], "icon": "📰", "color": C_GOLD}]
    if g:
        slides.append({"title": f"{g[0][0].upper()}", "subtitle": "Fastest growing suburb",
                       "lines": [f"+{float(g[0][2]):.0f}% in 3 months"], "icon": "📈", "color": C_GREEN})
    if ty:
        slides.append({"title": f"{float(ty[0][2]):.0f}% YIELD", "subtitle": f"Best rental — {ty[0][0]}",
                       "lines": [f"~${float(ty[0][3]):,.0f}/month rent"], "icon": "💵", "color": C_GREEN})
    if three:
        slides.append({"title": f"${three:,}", "subtitle": "Avg 3-bed Harare",
                       "lines": ["Current sale price average"], "icon": "🏠", "color": C_ACCENT})
    slides.append({"title": "GET THE FULL REPORT", "subtitle": "First report is FREE",
                   "lines": ["WhatsApp:", "wa.me/447459920895"], "icon": "💬", "color": C_GOLD})
    return slides


SLIDE_BUILDERS = {
    "monday":    slides_monday,
    "tuesday":   slides_tuesday,
    "wednesday": slides_wednesday,
    "thursday":  slides_thursday,
    "friday":    slides_friday,
    "saturday":  slides_saturday,
    "sunday":    slides_sunday,
}


# ── Image rendering ────────────────────────────────────────────────────────────
def render_header() -> np.ndarray:
    img = Image.new("RGB", (W, HEADER_H), C_HEADER_BG)
    d   = ImageDraw.Draw(img)
    # Gold left bar
    d.rectangle([(0, 0), (8, HEADER_H)], fill=C_GOLD)
    d.text((28, 18), "ZIMBABWE PROPERTY INTELLIGENCE", font=_font(30, bold=True), fill=C_GOLD)
    d.text((28, 72), f"@ba_kairo  •  @izeremhepo  •  wa.me/447459920895", font=_font(22), fill=C_GREY)
    return np.array(img)


def render_slide(slide: dict) -> np.ndarray:
    img = Image.new("RGB", (W, SLIDE_H), C_BG)
    d   = ImageDraw.Draw(img)

    # Subtle gradient top bar in slide accent colour
    accent = slide.get("color", C_GOLD)
    d.rectangle([(0, 0), (W, 6)], fill=accent)

    cy = SLIDE_H // 2 - 80

    # Icon
    icon = slide.get("icon", "")
    if icon:
        try:
            d.text((W // 2, cy - 80), icon, font=_font(90), fill=C_WHITE, anchor="mm")
        except Exception:
            pass
        cy -= 10

    # Title
    title = slide.get("title", "")
    d.text((W // 2, cy + 20), title, font=_font(72, bold=True), fill=accent, anchor="mm")

    # Subtitle
    subtitle = slide.get("subtitle", "")
    if subtitle:
        d.text((W // 2, cy + 110), subtitle, font=_font(40), fill=C_WHITE, anchor="mm")

    # Lines
    y = cy + 190
    for line in slide.get("lines", []):
        d.text((W // 2, y), line, font=_font(36), fill=C_GREY, anchor="mm")
        y += 55

    # Bottom divider
    d.rectangle([(60, SLIDE_H - 4), (W - 60, SLIDE_H)], fill=accent)

    return np.array(img)


def render_presenter_bg() -> np.ndarray:
    img = Image.new("RGB", (W, PRESENTER_H), C_PRESENTER)
    return np.array(img)


# ── Compositing ────────────────────────────────────────────────────────────────
def compose(video_path: Path, day: str, data: dict, out_path: Path):
    print(f"  Compositing slideshow for {day}...")

    slides = SLIDE_BUILDERS[day](data)
    clip   = VideoFileClip(str(video_path))
    dur    = clip.duration

    # Time per slide
    secs_per_slide = dur / len(slides)

    # ── Header (static for full duration) ─────────────────────────────────────
    header_arr = render_header()
    header_clip = (ImageClip(header_arr)
                   .with_duration(dur)
                   .with_position((0, 0)))

    # ── Slide clips ────────────────────────────────────────────────────────────
    slide_clips = []
    for i, slide in enumerate(slides):
        arr   = render_slide(slide)
        start = i * secs_per_slide
        end   = min((i + 1) * secs_per_slide, dur)
        sc    = (ImageClip(arr)
                 .with_duration(end - start)
                 .with_start(start)
                 .with_position((0, HEADER_H)))
        slide_clips.append(sc)

    # ── Presenter video ────────────────────────────────────────────────────────
    # Crop center square from D-ID video, resize to fill presenter area
    pres_w = W
    pres_h = PRESENTER_H

    # D-ID video is 1080x1080 — crop vertically to fit presenter area aspect ratio
    src_w, src_h = clip.size   # 1080, 1080
    target_ratio = pres_w / pres_h
    crop_h = int(src_w / target_ratio)
    if crop_h > src_h:
        crop_h = src_h
    y1 = max(0, (src_h - crop_h) // 4)   # bias toward top of frame (face)
    y2 = y1 + crop_h

    def replace_bg(frame: np.ndarray) -> np.ndarray:
        """Replace green/white D-ID background with brand navy."""
        f = frame.astype(np.int16)
        # Green screen: green channel >> red and blue
        is_green = (f[:,:,1] > 100) & (f[:,:,1] - f[:,:,0] > 40) & (f[:,:,1] - f[:,:,2] > 40)
        # White background: all channels high
        is_white = (f[:,:,0] > 220) & (f[:,:,1] > 220) & (f[:,:,2] > 220)
        # Light grey background
        is_grey  = (f[:,:,0] > 200) & (np.abs(f[:,:,0].astype(int) - f[:,:,1].astype(int)) < 15) & \
                   (np.abs(f[:,:,1].astype(int) - f[:,:,2].astype(int)) < 15)
        bg_mask = is_green | is_white | is_grey
        out = frame.copy()
        out[bg_mask] = C_PRESENTER
        return out

    raw_clip = (clip
                .cropped(x1=0, y1=y1, x2=src_w, y2=y2)
                .resized((pres_w, pres_h)))

    presenter_clip = (raw_clip
                      .image_transform(replace_bg)
                      .with_position((0, HEADER_H + SLIDE_H)))

    # ── Final composite ────────────────────────────────────────────────────────
    bg = ImageClip(np.full((H, W, 3), C_BG, dtype=np.uint8)).with_duration(dur)

    final = CompositeVideoClip(
        [bg, header_clip, *slide_clips, presenter_clip],
        size=(W, H)
    )

    print(f"  Rendering final video ({dur:.0f}s)...")
    final.write_videofile(
        str(out_path),
        fps=25,
        codec="libx264",
        audio_codec="aac",
        logger=None,
    )
    clip.close()
    final.close()
    print(f"  [OK] Saved: {out_path.name} ({out_path.stat().st_size // 1024:,} KB)")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--video", required=True, help="Path to D-ID talking head video")
    parser.add_argument("--day",   required=True, choices=list(SLIDE_BUILDERS.keys()))
    args = parser.parse_args()

    # Minimal placeholder data for standalone testing
    data = {
        "sales": 2738, "rentals": 606, "land": 359, "total": 3703,
        "top_growth":  [("Avondale","Harare",135.5,365000),("Pomona","Harare",18.2,95000),("Borrowdale","Harare",14.5,185000)],
        "best_value":  [("Spitzkop","Harare",9000,250),("Norton","Norton",10500,300),("Charlotte Brooke","Harare",14000,1000)],
        "top_yield":   [("Avenues","Harare",28.9,917),("Goodhope","Harare",8.2,850),("Avondale","Harare",7.9,750)],
        "three_bed_avg": 241742, "avg_rent": 850,
    }

    video_path = Path(args.video)
    out_path   = video_path.parent / video_path.name.replace("_did.mp4", "_composed.mp4")
    compose(video_path, args.day, data, out_path)
